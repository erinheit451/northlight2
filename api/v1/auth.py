"""
Authentication and Authorization Layer
Simple but secure authentication system for the unified platform
"""

import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import settings
from core.database import get_db
from core.shared import get_logger

router = APIRouter()
logger = get_logger("api.auth")

# Security configuration
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

# JWT settings
SECRET_KEY = settings.SECRET_KEY
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Pydantic models
class LoginRequest(BaseModel):
    """Login request model."""
    username: str
    password: str


class Token(BaseModel):
    """Token response model."""
    access_token: str
    token_type: str
    expires_in: int


class TokenData(BaseModel):
    """Token data model."""
    username: Optional[str] = None
    user_id: Optional[str] = None
    permissions: Optional[list] = None


class User(BaseModel):
    """User model."""
    id: str
    username: str
    email: str
    full_name: Optional[str] = None
    is_active: bool = True
    permissions: list = []


class CreateUserRequest(BaseModel):
    """Create user request model."""
    username: str
    email: str
    password: str
    full_name: Optional[str] = None
    permissions: list = []


# Authentication functions
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password against hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash password."""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_user_by_username(db: AsyncSession, username: str) -> Optional[Dict[str, Any]]:
    """Get user from database by username."""
    try:
        # Create users table if it doesn't exist
        await _ensure_users_table_exists(db)

        query = """
        SELECT id, username, email, full_name, hashed_password, is_active, permissions
        FROM auth_users WHERE username = :username
        """

        result = await db.execute(text(query), {"username": username})
        row = result.fetchone()

        if row:
            return {
                "id": row[0],
                "username": row[1],
                "email": row[2],
                "full_name": row[3],
                "hashed_password": row[4],
                "is_active": row[5],
                "permissions": row[6] if row[6] else []
            }
        return None

    except Exception as e:
        logger.error(f"Failed to get user {username}: {e}")
        return None


async def authenticate_user(db: AsyncSession, username: str, password: str) -> Optional[User]:
    """Authenticate user with username and password."""
    user_data = await get_user_by_username(db, username)
    if not user_data:
        return None

    if not verify_password(password, user_data["hashed_password"]):
        return None

    return User(
        id=user_data["id"],
        username=user_data["username"],
        email=user_data["email"],
        full_name=user_data["full_name"],
        is_active=user_data["is_active"],
        permissions=user_data["permissions"]
    )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
) -> User:
    """Get current user from JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception

    user_data = await get_user_by_username(db, username=token_data.username)
    if user_data is None:
        raise credentials_exception

    return User(
        id=user_data["id"],
        username=user_data["username"],
        email=user_data["email"],
        full_name=user_data["full_name"],
        is_active=user_data["is_active"],
        permissions=user_data["permissions"]
    )


async def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """Get current active user."""
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def require_permission(permission: str):
    """Decorator to require specific permission."""
    def permission_checker(current_user: User = Depends(get_current_active_user)):
        if permission not in current_user.permissions and "admin" not in current_user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission}' required"
            )
        return current_user
    return permission_checker


async def _ensure_users_table_exists(db: AsyncSession):
    """Ensure the users table exists."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS auth_users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        full_name VARCHAR(100),
        hashed_password VARCHAR(255) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        permissions TEXT[] DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
    """

    await db.execute(text(create_table_query))

    # Create default admin user if no users exist
    count_query = "SELECT COUNT(*) FROM auth_users"
    result = await db.execute(text(count_query))
    user_count = result.scalar()

    if user_count == 0:
        # Create default admin user
        admin_password_hash = get_password_hash("admin123")  # Change in production!

        insert_admin_query = """
        INSERT INTO auth_users (username, email, full_name, hashed_password, permissions)
        VALUES ('admin', 'admin@unified-northlight.local', 'Administrator', :password_hash, :permissions)
        """

        await db.execute(text(insert_admin_query), {
            "password_hash": admin_password_hash,
            "permissions": ["admin", "etl_management", "benchmarking", "analytics", "reporting"]
        })

        await db.commit()
        logger.info("Created default admin user (username: admin, password: admin123)")


# API Endpoints
@router.post("/login", response_model=Token)
async def login(
    login_request: LoginRequest,
    db: AsyncSession = Depends(get_db)
):
    """Authenticate user and return access token."""
    user = await authenticate_user(db, login_request.username, login_request.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username, "user_id": user.id, "permissions": user.permissions},
        expires_delta=access_token_expires
    )

    return Token(
        access_token=access_token,
        token_type="bearer",
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


@router.get("/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """Get current user information."""
    return current_user


@router.post("/users", response_model=User)
async def create_user(
    user_request: CreateUserRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission("admin"))
):
    """Create a new user (admin only)."""
    try:
        await _ensure_users_table_exists(db)

        # Check if user already exists
        existing_user = await get_user_by_username(db, user_request.username)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )

        # Create new user
        hashed_password = get_password_hash(user_request.password)

        insert_query = """
        INSERT INTO auth_users (username, email, full_name, hashed_password, permissions)
        VALUES (:username, :email, :full_name, :hashed_password, :permissions)
        RETURNING id
        """

        result = await db.execute(text(insert_query), {
            "username": user_request.username,
            "email": user_request.email,
            "full_name": user_request.full_name,
            "hashed_password": hashed_password,
            "permissions": user_request.permissions
        })

        user_id = result.scalar()
        await db.commit()

        return User(
            id=user_id,
            username=user_request.username,
            email=user_request.email,
            full_name=user_request.full_name,
            is_active=True,
            permissions=user_request.permissions
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to create user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )


@router.get("/users")
async def list_users(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission("admin"))
):
    """List all users (admin only)."""
    try:
        query = """
        SELECT id, username, email, full_name, is_active, permissions, created_at
        FROM auth_users
        ORDER BY created_at DESC
        """

        result = await db.execute(text(query))
        rows = result.fetchall()

        users = []
        for row in rows:
            users.append({
                "id": row[0],
                "username": row[1],
                "email": row[2],
                "full_name": row[3],
                "is_active": row[4],
                "permissions": row[5] or [],
                "created_at": row[6].isoformat() if row[6] else None
            })

        return {"users": users, "count": len(users)}

    except Exception as e:
        logger.error(f"Failed to list users: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list users"
        )


@router.post("/logout")
async def logout(current_user: User = Depends(get_current_active_user)):
    """Logout user (token invalidation would be handled by client)."""
    return {"message": "Successfully logged out"}


# Permission helpers
async def require_etl_permission(current_user: User = Depends(require_permission("etl_management"))):
    """Require ETL management permission."""
    return current_user


async def require_benchmarking_permission(current_user: User = Depends(require_permission("benchmarking"))):
    """Require benchmarking permission."""
    return current_user


async def require_analytics_permission(current_user: User = Depends(require_permission("analytics"))):
    """Require analytics permission."""
    return current_user


async def require_reporting_permission(current_user: User = Depends(require_permission("reporting"))):
    """Require reporting permission."""
    return current_user