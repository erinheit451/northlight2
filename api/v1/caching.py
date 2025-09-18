"""
Caching Layer for Efficient Data Serving
Redis-based caching for benchmark data and analytics
"""

import sys
import json
import pickle
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone, timedelta
import asyncio

import redis.asyncio as redis
from fastapi import Depends

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.shared import get_logger

logger = get_logger("api.caching")

# Redis connection pool
_redis_pool: Optional[redis.ConnectionPool] = None
_redis_client: Optional[redis.Redis] = None


async def get_redis_client() -> redis.Redis:
    """Get Redis client with connection pooling."""
    global _redis_pool, _redis_client

    if _redis_client is None:
        try:
            _redis_pool = redis.ConnectionPool.from_url(
                settings.REDIS_URL,
                max_connections=20,
                retry_on_timeout=True
            )
            _redis_client = redis.Redis(connection_pool=_redis_pool)

            # Test connection
            await _redis_client.ping()
            logger.info("Redis connection established")

        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Caching disabled.")
            _redis_client = None

    return _redis_client


async def close_redis_connection():
    """Close Redis connection."""
    global _redis_client, _redis_pool

    if _redis_client:
        await _redis_client.close()
        _redis_client = None

    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None


class CacheManager:
    """Manages caching operations for the unified platform."""

    def __init__(self):
        self.default_ttl = settings.CACHE_TTL
        self.namespace = "unified_northlight"

    def _make_key(self, *parts: str) -> str:
        """Create namespaced cache key."""
        return f"{self.namespace}:" + ":".join(str(part) for part in parts)

    async def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache."""
        redis_client = await get_redis_client()
        if not redis_client:
            return default

        try:
            cached_value = await redis_client.get(self._make_key(key))
            if cached_value is None:
                return default

            # Try to deserialize as JSON first, then pickle
            try:
                return json.loads(cached_value)
            except json.JSONDecodeError:
                return pickle.loads(cached_value)

        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")
            return default

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache."""
        redis_client = await get_redis_client()
        if not redis_client:
            return False

        try:
            ttl = ttl or self.default_ttl

            # Try to serialize as JSON first, then pickle
            try:
                serialized_value = json.dumps(value, default=str)
            except (TypeError, ValueError):
                serialized_value = pickle.dumps(value)

            await redis_client.setex(
                self._make_key(key),
                ttl,
                serialized_value
            )
            return True

        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        redis_client = await get_redis_client()
        if not redis_client:
            return False

        try:
            deleted = await redis_client.delete(self._make_key(key))
            return deleted > 0
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern."""
        redis_client = await get_redis_client()
        if not redis_client:
            return 0

        try:
            keys = await redis_client.keys(self._make_key(pattern))
            if keys:
                deleted = await redis_client.delete(*keys)
                return deleted
            return 0
        except Exception as e:
            logger.warning(f"Cache delete pattern failed for {pattern}: {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        redis_client = await get_redis_client()
        if not redis_client:
            return False

        try:
            return await redis_client.exists(self._make_key(key)) > 0
        except Exception as e:
            logger.warning(f"Cache exists check failed for key {key}: {e}")
            return False

    async def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache."""
        redis_client = await get_redis_client()
        if not redis_client:
            return {}

        try:
            namespaced_keys = [self._make_key(key) for key in keys]
            values = await redis_client.mget(namespaced_keys)

            result = {}
            for i, key in enumerate(keys):
                if values[i] is not None:
                    try:
                        result[key] = json.loads(values[i])
                    except json.JSONDecodeError:
                        result[key] = pickle.loads(values[i])

            return result

        except Exception as e:
            logger.warning(f"Cache get_many failed: {e}")
            return {}

    async def set_many(self, items: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Set multiple values in cache."""
        redis_client = await get_redis_client()
        if not redis_client:
            return False

        try:
            ttl = ttl or self.default_ttl

            # Prepare items for mset
            cache_items = {}
            for key, value in items.items():
                try:
                    serialized_value = json.dumps(value, default=str)
                except (TypeError, ValueError):
                    serialized_value = pickle.dumps(value)

                cache_items[self._make_key(key)] = serialized_value

            # Set all items
            await redis_client.mset(cache_items)

            # Set TTL for each key
            if ttl > 0:
                tasks = [
                    redis_client.expire(key, ttl)
                    for key in cache_items.keys()
                ]
                await asyncio.gather(*tasks)

            return True

        except Exception as e:
            logger.warning(f"Cache set_many failed: {e}")
            return False


# Global cache manager instance
cache_manager = CacheManager()


# Caching decorators and utilities
def cache_key_for_benchmarks(category: str, subcategory: str) -> str:
    """Generate cache key for benchmark data."""
    return f"benchmarks:{category}:{subcategory}"


def cache_key_for_analytics(metric: str, filters: Dict[str, Any]) -> str:
    """Generate cache key for analytics data."""
    filter_hash = hash(json.dumps(filters, sort_keys=True))
    return f"analytics:{metric}:{filter_hash}"


def cache_key_for_campaigns(filters: Dict[str, Any]) -> str:
    """Generate cache key for campaign data."""
    filter_hash = hash(json.dumps(filters, sort_keys=True))
    return f"campaigns:{filter_hash}"


async def invalidate_benchmark_cache():
    """Invalidate all benchmark-related cache entries."""
    deleted_count = await cache_manager.delete_pattern("benchmarks:*")
    logger.info(f"Invalidated {deleted_count} benchmark cache entries")
    return deleted_count


async def invalidate_analytics_cache():
    """Invalidate all analytics-related cache entries."""
    deleted_count = await cache_manager.delete_pattern("analytics:*")
    logger.info(f"Invalidated {deleted_count} analytics cache entries")
    return deleted_count


async def invalidate_campaign_cache():
    """Invalidate all campaign-related cache entries."""
    deleted_count = await cache_manager.delete_pattern("campaigns:*")
    logger.info(f"Invalidated {deleted_count} campaign cache entries")
    return deleted_count


async def warm_benchmark_cache(db_session):
    """Pre-populate benchmark cache with commonly accessed data."""
    try:
        from sqlalchemy import text

        # Get all categories
        query = """
        SELECT DISTINCT category, subcategory
        FROM northlight_benchmarks.benchmark_categories
        WHERE active = true
        """

        result = await db_session.execute(text(query))
        categories = result.fetchall()

        # Warm cache for each category
        for category, subcategory in categories:
            cache_key = cache_key_for_benchmarks(category, subcategory)

            # Check if already cached
            if await cache_manager.exists(cache_key):
                continue

            # Get benchmark data (simplified query)
            data_query = """
            SELECT bd.* FROM northlight_benchmarks.benchmark_data bd
            JOIN northlight_benchmarks.benchmark_categories bc ON bd.category_id = bc.id
            WHERE bc.category = :category AND bc.subcategory = :subcategory
            ORDER BY bd.created_at DESC LIMIT 1
            """

            data_result = await db_session.execute(text(data_query), {
                "category": category,
                "subcategory": subcategory
            })

            data_row = data_result.fetchone()
            if data_row:
                # Store in cache with longer TTL for warm data
                await cache_manager.set(cache_key, dict(data_row._mapping), ttl=7200)  # 2 hours

        logger.info(f"Warmed cache for {len(categories)} benchmark categories")

    except Exception as e:
        logger.error(f"Failed to warm benchmark cache: {e}")


# FastAPI dependency for cache manager
async def get_cache_manager() -> CacheManager:
    """Get cache manager instance."""
    return cache_manager


# Cache statistics
async def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    redis_client = await get_redis_client()
    if not redis_client:
        return {"status": "unavailable", "reason": "Redis not connected"}

    try:
        info = await redis_client.info()

        # Get our namespace keys
        our_keys = await redis_client.keys(f"{cache_manager.namespace}:*")

        return {
            "status": "healthy",
            "redis_version": info.get("redis_version"),
            "used_memory": info.get("used_memory_human"),
            "connected_clients": info.get("connected_clients"),
            "total_keys": len(our_keys),
            "namespace": cache_manager.namespace,
            "default_ttl": cache_manager.default_ttl
        }

    except Exception as e:
        logger.error(f"Failed to get cache stats: {e}")
        return {"status": "error", "error": str(e)}