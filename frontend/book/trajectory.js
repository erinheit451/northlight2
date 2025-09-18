// frontend/book/trajectory.js
// Performance Trajectory Component for Campaign Cards

// Cache for trajectory data to avoid repeated API calls
const trajectoryCache = new Map();

/**
 * Render trajectory section for a campaign card
 * @param {string} campaignId - The campaign ID
 * @param {Element} container - DOM element to render into
 */
export async function renderTrajectorySection(campaignId, container) {
  try {
    // Check cache first
    if (trajectoryCache.has(campaignId)) {
      renderTrajectoryUI(trajectoryCache.get(campaignId), container);
      return;
    }

    // Show loading state
    container.innerHTML = '<div class="trajectory-loading">Loading trajectory...</div>';

    // Fetch trajectory data
    const response = await fetch(`/api/v1/trajectory/campaign/${campaignId}`);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const trajectoryData = await response.json();

    // Cache the data
    trajectoryCache.set(campaignId, trajectoryData);

    // Render the UI
    renderTrajectoryUI(trajectoryData, container);

  } catch (error) {
    console.warn(`Failed to load trajectory for campaign ${campaignId}:`, error);
    renderTrajectoryError(container);
  }
}

/**
 * Render the trajectory UI based on data quality
 */
function renderTrajectoryUI(data, container) {
  const { data_quality, current_cpl, goal_cpl, campaign_age_days } = data;

  switch (data_quality) {
    case 'none':
      renderNewCampaignState(data, container);
      break;
    case 'limited':
      renderLimitedDataState(data, container);
      break;
    case 'moderate':
    case 'rich':
      renderFullTrajectoryState(data, container);
      break;
    default:
      renderTrajectoryError(container);
  }
}

/**
 * Render trajectory for new campaigns with no historical data
 */
function renderNewCampaignState(data, container) {
  const { current_cpl, goal_cpl, campaign_age_days } = data;

  container.innerHTML = `
    <div class="trajectory-section new-campaign">
      <div class="trajectory-header">
        📊 New Campaign (${campaign_age_days} days)
      </div>
      <div class="trajectory-metrics">
        Current: ${formatCPL(current_cpl)} • Goal: ${formatCPL(goal_cpl)}
      </div>
      <div class="trajectory-note">Building trajectory data...</div>
    </div>
  `;
}

/**
 * Render trajectory for campaigns with limited historical data
 */
function renderLimitedDataState(data, container) {
  const { current_cpl, goal_cpl, trend, earliest_data, data_points } = data;

  const trendIcon = getTrendIcon(trend.direction);
  const trendText = trend.direction !== 'insufficient_data'
    ? `${trendIcon} ${formatTrendText(trend)}`
    : 'Building trend data';

  container.innerHTML = `
    <div class="trajectory-section limited-data">
      <div class="trajectory-header">
        📊 ${trendText}
      </div>
      <div class="trajectory-metrics">
        Current: ${formatCPL(current_cpl)} • Goal: ${formatCPL(goal_cpl)} • Since: ${formatDate(earliest_data)}
      </div>
    </div>
  `;
}

/**
 * Render full trajectory with sparkline and metrics
 */
function renderFullTrajectoryState(data, container) {
  const { current_cpl, goal_cpl, trend, sparkline, metrics } = data;

  const trendIcon = getTrendIcon(trend.direction);
  const sparklineHTML = renderSparkline(sparkline);

  container.innerHTML = `
    <div class="trajectory-section full-data">
      <div class="trajectory-header">
        📊 Trajectory: ${trendIcon} ${formatTrendText(trend)} ${sparklineHTML} Goal: ${formatCPL(goal_cpl)}
      </div>
      <div class="trajectory-metrics">
        ${formatTimeMetrics(metrics)}
      </div>
    </div>
  `;
}

/**
 * Render error state
 */
function renderTrajectoryError(container) {
  container.innerHTML = `
    <div class="trajectory-section error">
      <div class="trajectory-note">Trajectory data unavailable</div>
    </div>
  `;
}

/**
 * Get trend direction icon
 */
function getTrendIcon(direction) {
  const icons = {
    'improving': '↗️',
    'declining': '↘️',
    'stable': '➡️',
    'insufficient_data': '📊'
  };
  return icons[direction] || '📊';
}

/**
 * Format trend text with percentage and confidence
 */
function formatTrendText(trend) {
  if (trend.direction === 'insufficient_data') {
    return 'Insufficient data';
  }

  const direction = trend.direction.charAt(0).toUpperCase() + trend.direction.slice(1);
  const percentage = trend.percentage ? ` ${Math.round(trend.percentage)}%` : '';
  const period = trend.period_days ? ` (${Math.round(trend.period_days/30)}mo)` : '';

  return `${direction}${percentage}${period}`;
}

/**
 * Render sparkline as unicode blocks
 */
function renderSparkline(sparkline) {
  if (!sparkline || sparkline.length === 0) {
    return '';
  }

  // Map normalized values (0-100) to unicode blocks
  const blocks = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

  const sparklineChars = sparkline.map(value => {
    const index = Math.min(Math.floor(value / 12.5), 7); // 100/8 = 12.5
    return blocks[index];
  }).join('');

  return `<span class="sparkline">[${sparklineChars}]</span>`;
}

/**
 * Format time-based metrics
 */
function formatTimeMetrics(metrics) {
  const parts = [];

  if (metrics.last_30d_avg !== null) {
    parts.push(`Last 30d: ${formatCPL(metrics.last_30d_avg)}`);
  }

  if (metrics.last_90d_avg !== null) {
    parts.push(`Last 90d: ${formatCPL(metrics.last_90d_avg)}`);
  }

  if (metrics.ytd_avg !== null) {
    parts.push(`YTD: ${formatCPL(metrics.ytd_avg)}`);
  }

  return parts.join(' • ') || 'Calculating metrics...';
}

/**
 * Format CPL value for display
 */
function formatCPL(cpl) {
  if (cpl === null || cpl === undefined) {
    return '—';
  }

  if (cpl >= 1000) {
    return `$${Math.round(cpl/100)*100}`;
  } else if (cpl >= 100) {
    return `$${Math.round(cpl)}`;
  } else {
    return `$${Math.round(cpl * 10) / 10}`;
  }
}

/**
 * Format date for display
 */
function formatDate(dateStr) {
  if (!dateStr) return '—';

  try {
    const date = new Date(dateStr + '-01'); // Add day since we get YYYY-MM
    return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
  } catch {
    return dateStr;
  }
}

/**
 * Clear trajectory cache (useful for data refreshes)
 */
export function clearTrajectoryCache() {
  trajectoryCache.clear();
}

/**
 * Preload trajectory data for multiple campaigns
 */
export async function preloadTrajectoryData(campaignIds) {
  const promises = campaignIds.map(async (campaignId) => {
    if (!trajectoryCache.has(campaignId)) {
      try {
        const response = await fetch(`/api/v1/trajectory/campaign/${campaignId}`);
        if (response.ok) {
          const data = await response.json();
          trajectoryCache.set(campaignId, data);
        }
      } catch (error) {
        console.warn(`Failed to preload trajectory for ${campaignId}:`, error);
      }
    }
  });

  await Promise.all(promises);
}