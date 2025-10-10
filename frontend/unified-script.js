/* ===========================
   UNIFIED NORTHLIGHT PLATFORM
   Frontend Application
   =========================== */

// Configuration
const API_BASE = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
    ? 'http://localhost:4000/api/v1'
    : '/api/v1';

const STORAGE_KEY = 'unified_northlight_v1';

// Global state
let currentUser = null;
let authToken = null;
let currentTab = 'benchmarks';

// Initialize application
document.addEventListener('DOMContentLoaded', async () => {
    console.log(`Unified Northlight Platform - API: ${API_BASE}`);

    // Initialize components
    initializeNavigation();
    initializeAuth();
    await initializeBenchmarksTab();

    // Set footer year
    document.getElementById('footerYear').textContent = new Date().getFullYear();

    // Check for stored auth token
    const storedToken = localStorage.getItem('auth_token');
    if (storedToken) {
        authToken = storedToken;
        await verifyAuthToken();
    }
});

/* ===========================
   AUTHENTICATION SYSTEM
   =========================== */

function initializeAuth() {
    const loginBtn = document.getElementById('loginBtn');
    const logoutBtn = document.getElementById('logoutBtn');
    const loginModal = document.getElementById('loginModal');
    const loginForm = document.getElementById('loginForm');
    const cancelLoginBtn = document.getElementById('cancelLoginBtn');

    loginBtn?.addEventListener('click', () => {
        loginModal.style.display = 'flex';
    });

    cancelLoginBtn?.addEventListener('click', () => {
        loginModal.style.display = 'none';
    });

    loginForm?.addEventListener('submit', async (e) => {
        e.preventDefault();
        await handleLogin();
    });

    logoutBtn?.addEventListener('click', handleLogout);
}

async function handleLogin() {
    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;

    try {
        const response = await fetch(`${API_BASE}/auth/login`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, password })
        });

        if (response.ok) {
            const data = await response.json();
            authToken = data.access_token;
            localStorage.setItem('auth_token', authToken);

            await verifyAuthToken();
            document.getElementById('loginModal').style.display = 'none';
            document.getElementById('loginForm').reset();

            showNotification('Login successful!', 'success');
        } else {
            const error = await response.json();
            showNotification(error.detail || 'Login failed', 'error');
        }
    } catch (error) {
        console.error('Login error:', error);
        showNotification('Login failed - network error', 'error');
    }
}

async function verifyAuthToken() {
    if (!authToken) return;

    try {
        const response = await fetch(`${API_BASE}/auth/me`, {
            headers: {
                'Authorization': `Bearer ${authToken}`
            }
        });

        if (response.ok) {
            currentUser = await response.json();
            updateAuthUI();
        } else {
            handleLogout();
        }
    } catch (error) {
        console.error('Auth verification error:', error);
        handleLogout();
    }
}

function handleLogout() {
    authToken = null;
    currentUser = null;
    localStorage.removeItem('auth_token');
    updateAuthUI();
    showNotification('Logged out successfully', 'info');
}

function updateAuthUI() {
    const loginBtn = document.getElementById('loginBtn');
    const userInfo = document.getElementById('userInfo');
    const username = document.getElementById('username');

    if (currentUser) {
        loginBtn.style.display = 'none';
        userInfo.style.display = 'flex';
        username.textContent = currentUser.username;
    } else {
        loginBtn.style.display = 'block';
        userInfo.style.display = 'none';
    }
}

/* ===========================
   NAVIGATION SYSTEM
   =========================== */

function initializeNavigation() {
    const tabs = ['benchmarks', 'etl', 'analytics', 'reports'];

    tabs.forEach(tab => {
        const tabElement = document.getElementById(`${tab}Tab`);
        tabElement?.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab(tab);
        });
    });
}

function switchTab(tabName) {
    // Update active nav item
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
    });
    document.getElementById(`${tabName}Tab`)?.classList.add('active');

    // Show/hide content sections
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
        content.style.display = 'none';
    });

    const activeSection = document.getElementById(`${tabName}Section`);
    if (activeSection) {
        activeSection.classList.add('active');
        activeSection.style.display = 'block';
    }

    currentTab = tabName;

    // Initialize tab-specific functionality
    switch (tabName) {
        case 'etl':
            initializeETLTab();
            break;
        case 'analytics':
            initializeAnalyticsTab();
            break;
        case 'reports':
            initializeReportsTab();
            break;
    }
}

/* ===========================
   BENCHMARKS TAB (Original Northlight)
   =========================== */

async function initializeBenchmarksTab() {
    // Load existing Northlight benchmark functionality
    await loadBenchmarkMetadata();
    initializeBenchmarkForm();
}

async function loadBenchmarkMetadata() {
    try {
        const response = await fetch(`${API_BASE}/benchmarks/meta`);
        if (response.ok) {
            const data = await response.json();
            populateCategoryDropdowns(data.categories);
        }
    } catch (error) {
        console.error('Failed to load benchmark metadata:', error);
    }
}

function populateCategoryDropdowns(categories) {
    const categorySelect = document.getElementById('category');
    const subcategorySelect = document.getElementById('subcategory');

    if (!categorySelect || !subcategorySelect) return;

    // Clear existing options
    categorySelect.innerHTML = '<option value="">Select Category</option>';
    subcategorySelect.innerHTML = '<option value="">Select Subcategory</option>';

    // Group by category
    const grouped = {};
    categories.forEach(cat => {
        if (!grouped[cat.category]) {
            grouped[cat.category] = [];
        }
        grouped[cat.category].push(cat.subcategory);
    });

    // Populate category dropdown
    Object.keys(grouped).forEach(category => {
        const option = document.createElement('option');
        option.value = category;
        option.textContent = category;
        categorySelect.appendChild(option);
    });

    // Handle category selection
    categorySelect.addEventListener('change', () => {
        const selectedCategory = categorySelect.value;
        subcategorySelect.innerHTML = '<option value="">Select Subcategory</option>';

        if (selectedCategory && grouped[selectedCategory]) {
            grouped[selectedCategory].forEach(subcategory => {
                const option = document.createElement('option');
                option.value = subcategory;
                option.textContent = subcategory;
                subcategorySelect.appendChild(option);
            });
        }
    });
}

function initializeBenchmarkForm() {
    const runBtn = document.getElementById('runBtn');
    const resetBtn = document.getElementById('resetBtn');

    runBtn?.addEventListener('click', runBenchmarkAnalysis);
    resetBtn?.addEventListener('click', resetBenchmarkForm);

    // Load saved inputs
    loadSavedInputs();
}

async function runBenchmarkAnalysis() {
    const formData = {
        category: document.getElementById('category').value,
        subcategory: document.getElementById('subcategory').value,
        goal_cpl: parseFloat(document.getElementById('goal_cpl').value),
        budget: parseFloat(document.getElementById('budget').value),
        clicks: parseInt(document.getElementById('clicks').value),
        leads: parseInt(document.getElementById('leads').value)
    };

    // Validate inputs
    if (!formData.category || !formData.subcategory) {
        showNotification('Please select category and subcategory', 'error');
        return;
    }

    if (!formData.goal_cpl || !formData.budget || !formData.clicks || !formData.leads) {
        showNotification('Please fill in all numeric fields', 'error');
        return;
    }

    // Save inputs
    saveInputs(formData);

    // Show loading
    const runBtn = document.getElementById('runBtn');
    const originalText = runBtn.textContent;
    runBtn.textContent = 'Analyzing...';
    runBtn.disabled = true;

    try {
        const response = await fetch(`${API_BASE}/benchmarks/diagnose`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(authToken && { 'Authorization': `Bearer ${authToken}` })
            },
            body: JSON.stringify(formData)
        });

        if (response.ok) {
            const results = await response.json();
            // Use the proper rendering function from benchmark-rendering.js
            if (typeof renderBenchmarkResults === 'function') {
                renderBenchmarkResults(results);
            } else {
                displayBenchmarkResults(results);
            }
        } else {
            const error = await response.json();
            showNotification(error.detail || 'Analysis failed', 'error');
        }
    } catch (error) {
        console.error('Benchmark analysis error:', error);
        showNotification('Analysis failed - network error', 'error');
    } finally {
        runBtn.textContent = originalText;
        runBtn.disabled = false;
    }
}

// Benchmark rendering utilities (from original Northlight)
function fmtPct1(v) { return (v == null) ? "—" : `${(v*100).toFixed(1)}%`; }
function fmtMoney(v){ return (v == null) ? "—" : `$${Number(v).toFixed(2)}`; }

function ensureTargetRange(metricData, unit, metricName) {
    if (metricData?.target_range?.low != null && metricData?.target_range?.high != null) {
        return metricData.target_range;
    }
    const m = metricData?.median ?? metricData?.value ?? (unit === '%' ? 0.05 : (metricName === 'CPC' ? 3 : 50));
    const p25 = metricData?.p25, p75 = metricData?.p75;
    if (p25 != null && p75 != null) return { low: Math.max(0, p25), high: Math.max(p75, p25) };
    const span = Math.max(0.00001, m * 0.15);
    return { low: Math.max(0, m - span), high: m + span };
}

function deriveVerdict(value, targetRange, direction) {
    if (value == null || targetRange?.low == null || targetRange?.high == null) return 'unknown';
    const { low, high } = targetRange;
    if (value >= low && value <= high) return 'on_target';
    if (direction === 'lower-is-better') {
        return (value < low) ? 'exceeds_target' : 'outside_target';
    } else {
        return (value > high) ? 'exceeds_target' : 'outside_target';
    }
}

function roundUpToNiceCurrency(x) {
    const targets = [0.5, 1, 2, 3, 5, 7.5, 10, 12, 15, 20, 25, 30, 40, 50, 60, 75, 100, 150, 200, 250, 300, 400, 500, 750, 1000];
    const want = x * 1.15;
    for (const t of targets) if (want <= t) return t;
    return Math.ceil(want / 100) * 100;
}

function calculateBarData(userValue, targetRange, median, direction, metricUnit, metricName) {
    if (!targetRange || targetRange.low == null || targetRange.high == null) {
        targetRange = ensureTargetRange({ value: userValue, median }, metricUnit, metricName);
    }

    const baseTop = Math.max(userValue ?? 0, targetRange.high ?? 0, median ?? 0);
    let minScale = 0, maxScale;
    if (metricUnit === '%') {
        maxScale = Math.min(1.0, Math.max(0.10, baseTop * 1.4));
    } else {
        maxScale = Math.max(baseTop * 1.4, targetRange.high * 1.6, (median || 0) * 1.5);
        maxScale = roundUpToNiceCurrency(maxScale);
    }

    const totalRange = maxScale - minScale;
    if (totalRange <= 0) return null;

    let userSection = 'target-range';
    if (userValue < targetRange.low) {
        userSection = (direction === 'lower-is-better') ? 'excellent' : 'needs-improvement';
    } else if (userValue > targetRange.high) {
        userSection = (direction === 'lower-is-better') ? 'needs-improvement' : 'excellent';
    }

    const valueFormatter = metricUnit === '$' ? fmtMoney : fmtPct1;
    const clamp01 = v => Math.max(0, Math.min(1, v));
    const userPosition = clamp01((userValue - minScale) / totalRange) * 100;

    let excellentWidth = clamp01((targetRange.low - minScale) / totalRange) * 100;
    let targetWidth = clamp01((targetRange.high - targetRange.low) / totalRange) * 100;
    let improvementWidth = Math.max(0, 100 - excellentWidth - targetWidth);

    const sum = excellentWidth + targetWidth + improvementWidth;
    if (sum !== 100 && sum > 0) {
        excellentWidth = (excellentWidth / sum) * 100;
        targetWidth = (targetWidth / sum) * 100;
        improvementWidth = (improvementWidth / sum) * 100;
    }

    const sectionWidths = (direction === 'lower-is-better')
        ? { 'excellent': excellentWidth, 'target-range': targetWidth, 'needs-improvement': improvementWidth }
        : { 'needs-improvement': excellentWidth, 'target-range': targetWidth, 'excellent': improvementWidth };

    const scalePoints = [
        { value: minScale }, { value: targetRange.low },
        { value: targetRange.high }, { value: maxScale }
    ].map(p => ({
        label: valueFormatter(p.value),
        position: clamp01((p.value - minScale) / totalRange) * 100
    }));

    return { userValue, userPosition, userSection, sectionWidths, scalePoints, valueFormatter, direction, metricName };
}

function generateBar(data) {
    if (!data) return '';
    const scaleHtml = data.scalePoints.map(p => `<div class="scale-point" style="left: ${p.position}%; position:absolute; font-size:11px; transform:translateX(-50%);">${p.label}</div>`).join('');
    const sections = (data.direction === 'lower-is-better')
        ? ['excellent', 'target-range', 'needs-improvement']
        : ['needs-improvement', 'target-range', 'excellent'];

    const zoneColors = {
        'excellent': '#10b981',
        'target-range': '#3b82f6',
        'needs-improvement': '#f59e0b'
    };

    return `
        <div style="position:relative; height:30px; margin:20px 0 40px 0;">
            <div style="position:relative; height:4px; top:25px;">${scaleHtml}</div>
        </div>
        <div style="display:flex; height:40px; border-radius:8px; overflow:hidden; position:relative;">
            ${sections.map(key => `<div style="flex-basis: ${(data.sectionWidths[key] || 0)}%; background: ${zoneColors[key]}30; border-right: 1px solid #e5e7eb;"></div>`).join('')}
            <div style="position:absolute; left:${data.userPosition}%; top:0; bottom:0; width:4px; background:${zoneColors[data.userSection]}; transform:translateX(-50%); z-index:10;">
                <div style="position:absolute; top:-30px; left:50%; transform:translateX(-50%); background:${zoneColors[data.userSection]}; color:white; padding:4px 8px; border-radius:4px; white-space:nowrap; font-size:12px; font-weight:600;">
                    ${data.valueFormatter(data.userValue)}
                </div>
            </div>
        </div>
        <div style="display:flex; margin-top:8px;">
            ${sections.map(key => {
                const labelText = key.replace(/-/g, ' ');
                return `<div style="flex-basis: ${(data.sectionWidths[key] || 0)}%; text-align:center; text-transform:capitalize; font-size:11px; color:${zoneColors[key]}; font-weight:${data.userSection === key ? '700' : '500'};">${labelText}</div>`;
            }).join('')}
        </div>
    `;
}

function displayBenchmarkResults(results) {
    const resultsSection = document.getElementById('results');
    resultsSection.style.display = 'block';

    // Convert API response to format expected by rendering functions
    const benchmarks = {
        cpl: {
            value: results.derived?.cpl,
            median: results.benchmarks?.medians?.cpl
        },
        cpc: {
            value: results.derived?.cpc,
            median: results.benchmarks?.medians?.cpc
        },
        cr: {
            value: results.derived?.cr,
            median: results.benchmarks?.medians?.ctr // Using CTR as CR median approximation
        }
    };

    // Primary Status Block
    const primaryStatus = document.getElementById('primaryStatusBlock');
    const goalStatus = results.overall?.goal_status || 'unknown';
    const goalStatusText = goalStatus === 'achieved' ? '✅ Goal Achieved' :
                          goalStatus === 'behind' ? '⚠️ Behind Goal' : 'Unknown';
    const goalStatusColor = goalStatus === 'achieved' ? '#10b981' :
                           goalStatus === 'behind' ? '#f59e0b' : '#6b7280';

    primaryStatus.innerHTML = `
        <h4>Goal CPL Analysis</h4>
        <div style="padding:16px;background:${goalStatusColor}20;border-left:4px solid ${goalStatusColor};border-radius:8px;">
            <div style="font-size:18px;font-weight:600;color:${goalStatusColor};">${goalStatusText}</div>
            <div style="margin-top:8px;color:#374151;">
                Your CPL: <strong>${fmtMoney(results.derived?.cpl)}</strong> |
                Goal CPL: <strong>${fmtMoney(results.input?.goal_cpl)}</strong> |
                Market Median: <strong>${fmtMoney(results.benchmarks?.medians?.cpl)}</strong>
            </div>
        </div>
    `;

    // CPL Verdict with Bar
    const cplVerdict = document.getElementById('cpl-verdict-container');
    const cplTarget = ensureTargetRange(benchmarks.cpl, '$', 'CPL');
    const cplVerdictType = deriveVerdict(benchmarks.cpl.value, cplTarget, 'lower-is-better');
    const cplBarData = calculateBarData(benchmarks.cpl.value, cplTarget, benchmarks.cpl.median, 'lower-is-better', '$', 'CPL');
    const cplVerdictLabels = { outside_target: "Outside Target", on_target: "On Target", exceeds_target: "Excellent", unknown: "Benchmark" };
    const cplVerdictIcons = { outside_target: "⚠️", on_target: "🎯", exceeds_target: "🎉", unknown: "📊" };

    cplVerdict.innerHTML = `
        <div style="font-size:14px; font-weight:600; color:#6b7280; margin-bottom:12px;">
            ${cplVerdictIcons[cplVerdictType] || "📊"} ${cplVerdictLabels[cplVerdictType] || "Benchmark"}
        </div>
        <div style="font-size:20px; font-weight:700; margin-bottom:8px;">Your CPL is ${fmtMoney(benchmarks.cpl.value)}</div>
        <div style="font-size:13px; color:#6b7280; margin-bottom:16px;">
            Target range: ${fmtMoney(cplTarget.low)}–${fmtMoney(cplTarget.high)}
        </div>
        ${generateBar(cplBarData)}
    `;

    // CPC Verdict with Bar
    const cpcVerdict = document.getElementById('cpc-verdict-container');
    const cpcTarget = ensureTargetRange(benchmarks.cpc, '$', 'CPC');
    const cpcVerdictType = deriveVerdict(benchmarks.cpc.value, cpcTarget, 'lower-is-better');
    const cpcBarData = calculateBarData(benchmarks.cpc.value, cpcTarget, benchmarks.cpc.median, 'lower-is-better', '$', 'CPC');
    const cpcVerdictLabels = { outside_target: "Outside Target", on_target: "On Target", exceeds_target: "Excellent", unknown: "Benchmark" };
    const cpcVerdictIcons = { outside_target: "⚠️", on_target: "🎯", exceeds_target: "🎉", unknown: "📊" };

    cpcVerdict.innerHTML = `
        <div style="font-size:14px; font-weight:600; color:#6b7280; margin-bottom:12px;">
            ${cpcVerdictIcons[cpcVerdictType] || "📊"} ${cpcVerdictLabels[cpcVerdictType] || "Benchmark"}
        </div>
        <div style="font-size:20px; font-weight:700; margin-bottom:8px;">Your CPC is ${fmtMoney(benchmarks.cpc.value)}</div>
        <div style="font-size:13px; color:#6b7280; margin-bottom:16px;">
            Target range: ${fmtMoney(cpcTarget.low)}–${fmtMoney(cpcTarget.high)}
        </div>
        ${generateBar(cpcBarData)}
    `;

    // CR Verdict with Bar
    const crVerdict = document.getElementById('cr-verdict-container');
    const crTarget = ensureTargetRange(benchmarks.cr, '%', 'CR');
    const crVerdictType = deriveVerdict(benchmarks.cr.value, crTarget, 'higher-is-better');
    const crBarData = calculateBarData(benchmarks.cr.value, crTarget, benchmarks.cr.median, 'higher-is-better', '%', 'CR');
    const crVerdictLabels = { outside_target: "Outside Target", on_target: "On Target", exceeds_target: "Excellent", unknown: "Benchmark" };
    const crVerdictIcons = { outside_target: "⚠️", on_target: "🎯", exceeds_target: "🎉", unknown: "📊" };

    crVerdict.innerHTML = `
        <div style="font-size:14px; font-weight:600; color:#6b7280; margin-bottom:12px;">
            ${crVerdictIcons[crVerdictType] || "📊"} ${crVerdictLabels[crVerdictType] || "Benchmark"}
        </div>
        <div style="font-size:20px; font-weight:700; margin-bottom:8px;">Your CR is ${fmtPct1(benchmarks.cr.value)}</div>
        <div style="font-size:13px; color:#6b7280; margin-bottom:16px;">
            Target range: ${fmtPct1(crTarget.low)}–${fmtPct1(crTarget.high)}
        </div>
        ${generateBar(crBarData)}
    `;

    // Diagnosis Card
    const diagnosisCard = document.getElementById('diagnosisCard');
    const primaryAction = results.diagnosis?.primary || 'optimize';
    const actionEmoji = primaryAction === 'scale' ? '📈' : primaryAction === 'optimize' ? '🔧' : '📊';
    const actionText = primaryAction === 'scale' ? 'This campaign is performing well and ready to scale' :
                      primaryAction === 'optimize' ? 'This campaign needs optimization to improve performance' :
                      'Continue monitoring this campaign';

    diagnosisCard.innerHTML = `
        <h4>${actionEmoji} Primary Recommendation: ${primaryAction.toUpperCase()}</h4>
        <p style="margin:12px 0;color:#374151;">${actionText}</p>
        <div style="background:#f3f4f6;padding:16px;border-radius:8px;margin-top:16px;">
            <h5 style="margin:0 0 12px 0;">Campaign Summary</h5>
            <div style="display:grid;gap:8px;">
                <div><strong>Category:</strong> ${results.input?.category} → ${results.input?.subcategory}</div>
                <div><strong>Budget:</strong> ${fmtMoney(results.input?.budget)}</div>
                <div><strong>Total Clicks:</strong> ${results.input?.clicks}</div>
                <div><strong>Total Leads:</strong> ${results.input?.leads}</div>
                <div><strong>Goal CPL:</strong> ${fmtMoney(results.input?.goal_cpl)}</div>
                <div><strong>Actual CPL:</strong> ${fmtMoney(results.derived?.cpl)}</div>
                <div><strong>Market Median CPL:</strong> ${fmtMoney(results.benchmarks?.medians?.cpl)}</div>
            </div>
        </div>
    `;

    // Enable copy button
    const copyBtn = document.getElementById('copySummaryBtn');
    if (copyBtn) {
        copyBtn.disabled = false;
        copyBtn.onclick = () => {
            const summary = `Campaign Analysis - ${results.input?.category}/${results.input?.subcategory}\n` +
                          `Goal Status: ${goalStatusText}\n` +
                          `Your CPL: ${fmtMoney(results.derived?.cpl)} (Goal: ${fmtMoney(results.input?.goal_cpl)})\n` +
                          `Market Median: ${fmtMoney(results.benchmarks?.medians?.cpl)}\n` +
                          `Recommendation: ${primaryAction.toUpperCase()}\n` +
                          `Budget: ${fmtMoney(results.input?.budget)} | Clicks: ${results.input?.clicks} | Leads: ${results.input?.leads}`;
            navigator.clipboard.writeText(summary);
            showNotification('Summary copied to clipboard!', 'success');
        };
    }

    // Scroll to results
    resultsSection.scrollIntoView({ behavior: 'smooth' });
}

function resetBenchmarkForm() {
    document.getElementById('category').value = '';
    document.getElementById('subcategory').value = '';
    document.getElementById('goal_cpl').value = '';
    document.getElementById('budget').value = '';
    document.getElementById('clicks').value = '';
    document.getElementById('leads').value = '';

    document.getElementById('results').style.display = 'none';
    localStorage.removeItem(STORAGE_KEY);
}

function saveInputs(data) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
}

function loadSavedInputs() {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
        try {
            const data = JSON.parse(saved);
            Object.keys(data).forEach(key => {
                const element = document.getElementById(key);
                if (element && data[key]) {
                    element.value = data[key];
                }
            });
        } catch (error) {
            console.error('Failed to load saved inputs:', error);
        }
    }
}

/* ===========================
   ETL MANAGEMENT TAB
   =========================== */

async function initializeETLTab() {
    if (!authToken) {
        showETLAuthRequired();
        return;
    }

    await loadETLStatus();
    initializeETLControls();
}

function showETLAuthRequired() {
    const pipelineStatus = document.getElementById('pipelineStatus');
    pipelineStatus.innerHTML = '<p>Please log in to access ETL management features.</p>';
}

async function loadETLStatus() {
    try {
        // Load pipeline status
        const statusResponse = await fetch(`${API_BASE}/etl/pipeline/status`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (statusResponse.ok) {
            const status = await statusResponse.json();
            displayPipelineStatus(status);
        }

        // Load jobs list
        const jobsResponse = await fetch(`${API_BASE}/etl/jobs`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (jobsResponse.ok) {
            const jobs = await jobsResponse.json();
            displayJobsList(jobs);
        }

        // Load health metrics
        const healthResponse = await fetch(`${API_BASE}/etl/health`, {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (healthResponse.ok) {
            const health = await healthResponse.json();
            displayHealthMetrics(health);
        }

    } catch (error) {
        console.error('Failed to load ETL status:', error);
        showNotification('Failed to load ETL status', 'error');
    }
}

function displayPipelineStatus(status) {
    const pipelineStatus = document.getElementById('pipelineStatus');
    pipelineStatus.innerHTML = `
        <div class="status-item ${status.overall_status || 'unknown'}">
            <div class="status-indicator ${status.overall_status || 'unknown'}"></div>
            <strong>Overall Status:</strong> ${status.overall_status || 'Unknown'}
        </div>
        <div class="status-item">
            <strong>Last Run:</strong> ${status.last_run || 'Never'}
        </div>
        <div class="status-item">
            <strong>Next Scheduled:</strong> ${status.next_scheduled || 'Not scheduled'}
        </div>
    `;
}

function displayJobsList(jobs) {
    const jobsList = document.getElementById('jobsList');
    if (!jobs.jobs || jobs.jobs.length === 0) {
        jobsList.innerHTML = '<p>No jobs found.</p>';
        return;
    }

    jobsList.innerHTML = jobs.jobs.map(job => `
        <div class="job-item ${job.status || 'unknown'}">
            <div>
                <strong>${job.job_id}</strong>
                <div class="small">${job.description || 'No description'}</div>
            </div>
            <div>
                <span class="pill ${job.status || 'unknown'}">${job.status || 'Unknown'}</span>
            </div>
        </div>
    `).join('');
}

function displayHealthMetrics(health) {
    const healthMetrics = document.getElementById('healthMetrics');
    healthMetrics.innerHTML = `
        <div class="health-metric">
            <div class="health-metric-value">${health.active_jobs || 0}</div>
            <div class="health-metric-label">Active Jobs</div>
        </div>
        <div class="health-metric">
            <div class="health-metric-value">${health.completed_today || 0}</div>
            <div class="health-metric-label">Completed Today</div>
        </div>
        <div class="health-metric">
            <div class="health-metric-value">${health.failed_today || 0}</div>
            <div class="health-metric-label">Failed Today</div>
        </div>
        <div class="health-metric">
            <div class="health-metric-value">${health.avg_runtime || 'N/A'}</div>
            <div class="health-metric-label">Avg Runtime</div>
        </div>
    `;
}

function initializeETLControls() {
    const refreshBtn = document.getElementById('refreshJobsBtn');
    const runPipelineBtn = document.getElementById('runFullPipelineBtn');

    refreshBtn?.addEventListener('click', loadETLStatus);
    runPipelineBtn?.addEventListener('click', runFullPipeline);
}

async function runFullPipeline() {
    if (!authToken) {
        showNotification('Please log in to run pipeline', 'error');
        return;
    }

    const runBtn = document.getElementById('runFullPipelineBtn');
    const originalText = runBtn.textContent;
    runBtn.textContent = 'Starting...';
    runBtn.disabled = true;

    try {
        const response = await fetch(`${API_BASE}/etl/pipeline/run`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (response.ok) {
            showNotification('Pipeline started successfully', 'success');
            setTimeout(loadETLStatus, 2000);
        } else {
            const error = await response.json();
            showNotification(error.detail || 'Failed to start pipeline', 'error');
        }
    } catch (error) {
        console.error('Pipeline start error:', error);
        showNotification('Failed to start pipeline', 'error');
    } finally {
        runBtn.textContent = originalText;
        runBtn.disabled = false;
    }
}

/* ===========================
   ANALYTICS TAB
   =========================== */

async function initializeAnalyticsTab() {
    await loadAnalyticsData();
}

async function loadAnalyticsData() {
    try {
        // Load campaign metrics
        const campaignResponse = await fetch(`${API_BASE}/analytics/campaigns/summary`, {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {}
        });

        if (campaignResponse.ok) {
            const campaign = await campaignResponse.json();
            displayCampaignMetrics(campaign);
        }

        // Load partner metrics
        const partnerResponse = await fetch(`${API_BASE}/analytics/partners/pipeline`, {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {}
        });

        if (partnerResponse.ok) {
            const partners = await partnerResponse.json();
            displayPartnerMetrics(partners);
        }

        // Load executive metrics
        const execResponse = await fetch(`${API_BASE}/analytics/executive/dashboard`, {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {}
        });

        if (execResponse.ok) {
            const exec = await execResponse.json();
            displayExecutiveMetrics(exec);
        }

    } catch (error) {
        console.error('Failed to load analytics:', error);
        showNotification('Failed to load analytics data', 'error');
    }
}

function displayCampaignMetrics(data) {
    const container = document.getElementById('campaignMetrics');
    container.innerHTML = `
        <div class="metric-item">
            <span class="metric-label">Total Campaigns</span>
            <span class="metric-value">${data.total_campaigns || 0}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">Active Campaigns</span>
            <span class="metric-value">${data.active_campaigns || 0}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">Average CPL</span>
            <span class="metric-value">${formatCurrency(data.avg_cpl) || 'N/A'}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">Total Spend</span>
            <span class="metric-value">${formatCurrency(data.total_spend) || 'N/A'}</span>
        </div>
    `;
}

function displayPartnerMetrics(data) {
    const container = document.getElementById('partnerMetrics');
    container.innerHTML = `
        <div class="metric-item">
            <span class="metric-label">Active Partners</span>
            <span class="metric-value">${data.active_partners || 0}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">Pipeline Value</span>
            <span class="metric-value">${formatCurrency(data.pipeline_value) || 'N/A'}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">Conversion Rate</span>
            <span class="metric-value">${formatPercent(data.conversion_rate) || 'N/A'}</span>
        </div>
    `;
}

function displayExecutiveMetrics(data) {
    const container = document.getElementById('executiveMetrics');
    container.innerHTML = `
        <div class="metric-item">
            <span class="metric-label">Monthly Revenue</span>
            <span class="metric-value">${formatCurrency(data.monthly_revenue) || 'N/A'}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">YTD Growth</span>
            <span class="metric-value">${formatPercent(data.ytd_growth) || 'N/A'}</span>
        </div>
        <div class="metric-item">
            <span class="metric-label">Customer Acquisition Cost</span>
            <span class="metric-value">${formatCurrency(data.cac) || 'N/A'}</span>
        </div>
    `;
}

/* ===========================
   REPORTS TAB
   =========================== */

async function initializeReportsTab() {
    await loadReportTemplates();
    initializeReportControls();
}

async function loadReportTemplates() {
    try {
        const response = await fetch(`${API_BASE}/reports/templates`, {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {}
        });

        if (response.ok) {
            const data = await response.json();
            displayReportTemplates(data.templates || []);
        }
    } catch (error) {
        console.error('Failed to load report templates:', error);
    }
}

function displayReportTemplates(templates) {
    const container = document.getElementById('reportTemplates');
    if (templates.length === 0) {
        container.innerHTML = '<p>No report templates available.</p>';
        return;
    }

    container.innerHTML = templates.map(template => `
        <div class="report-template" onclick="generateReport('${template.id}')">
            <h4>${template.name}</h4>
            <p class="small">${template.description}</p>
        </div>
    `).join('');
}

function initializeReportControls() {
    document.getElementById('dataQualityReportBtn')?.addEventListener('click', () => generateStandardReport('data-quality'));
    document.getElementById('campaignReportBtn')?.addEventListener('click', () => generateStandardReport('campaign-performance'));
    document.getElementById('partnerReportBtn')?.addEventListener('click', () => generateStandardReport('partner-pipeline'));
}

async function generateStandardReport(reportType) {
    const resultContainer = document.getElementById('reportResults');
    resultContainer.innerHTML = '<div class="loading"></div> Generating report...';

    try {
        const response = await fetch(`${API_BASE}/reports/${reportType}`, {
            headers: authToken ? { 'Authorization': `Bearer ${authToken}` } : {}
        });

        if (response.ok) {
            const report = await response.json();
            displayReportResults(report);
        } else {
            resultContainer.innerHTML = 'Failed to generate report.';
        }
    } catch (error) {
        console.error('Report generation error:', error);
        resultContainer.innerHTML = 'Report generation failed.';
    }
}

function displayReportResults(report) {
    const container = document.getElementById('reportResults');
    container.innerHTML = `
        <h4>${report.title || 'Report'}</h4>
        <div class="small">Generated: ${new Date().toLocaleString()}</div>
        <pre>${JSON.stringify(report.data || report, null, 2)}</pre>
    `;
}

/* ===========================
   UTILITY FUNCTIONS
   =========================== */

function formatCurrency(value) {
    if (value == null || isNaN(value)) return null;
    return `$${Number(value).toFixed(2)}`;
}

function formatPercent(value) {
    if (value == null || isNaN(value)) return null;
    return `${(Number(value) * 100).toFixed(1)}%`;
}

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 12px 16px;
        background: ${type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6'};
        color: white;
        border-radius: 8px;
        z-index: 10000;
        animation: slideIn 0.3s ease;
    `;
    notification.textContent = message;

    document.body.appendChild(notification);

    // Remove after 3 seconds
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

// Add slide-in animation
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
`;
document.head.appendChild(style);