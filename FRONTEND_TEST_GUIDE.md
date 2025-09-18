# Frontend Integration Test Guide

This guide provides manual testing steps to verify the unified frontend integration.

## Prerequisites

1. Start the unified platform:
   ```bash
   cd unified-northlight
   python main.py
   ```

2. Open your browser and navigate to: `http://localhost:8001/dashboard`

## Test Checklist

### ✅ Basic Page Load
- [ ] Page loads without errors
- [ ] Title shows "Unified Northlight Platform"
- [ ] Header shows "UNIFIED NORTHLIGHT v1.0"
- [ ] Tagline shows "Integrated data pipeline and benchmarking platform"
- [ ] Navigation tabs are visible: Benchmarks, ETL Management, Analytics, Reports
- [ ] Login button is visible in top right

### ✅ Navigation System
- [ ] Click "ETL Management" tab - content switches
- [ ] Click "Analytics" tab - content switches
- [ ] Click "Reports" tab - content switches
- [ ] Click "Benchmarks" tab - returns to benchmarks
- [ ] Active tab is highlighted in blue
- [ ] Only one tab content is visible at a time

### ✅ Benchmarks Tab (Original Northlight Functionality)
- [ ] Category dropdown is present
- [ ] Subcategory dropdown is present
- [ ] Input fields for Goal CPL, Budget, Clicks, Leads
- [ ] "Run Benchmark" and "Reset Inputs" buttons
- [ ] Reset button clears all form fields
- [ ] Form validation shows error for incomplete data

### ✅ Authentication System
- [ ] Click "Login" button - modal appears
- [ ] Modal has username and password fields
- [ ] "Cancel" button closes modal
- [ ] Try login with credentials: username=`admin`, password=`admin123`
- [ ] Successful login shows username in header and "Logout" button
- [ ] "Logout" button logs out and returns to login state

### ✅ ETL Management Tab
- [ ] Without login: Shows "Please log in to access ETL management features"
- [ ] After login: Shows Pipeline Status, Job Management, System Health sections
- [ ] "Refresh" and "Run Full Pipeline" buttons are present
- [ ] Status indicators use colored left borders

### ✅ Analytics Tab
- [ ] Shows three main cards: Campaign Performance, Partner Pipeline, Executive Dashboard
- [ ] Cards display metric labels and values
- [ ] Loads without login (public analytics)
- [ ] Shows "N/A" for missing data gracefully

### ✅ Reports Tab
- [ ] Shows "Available Reports" and "Generate Reports" sections
- [ ] Three report buttons: Data Quality, Campaign Performance, Partner Pipeline
- [ ] Click report button generates report in results area
- [ ] Report results show in monospace font

### ✅ API Integration
Test API calls (open browser developer tools):

#### Benchmarks API
- [ ] Category/subcategory data loads from `/api/v1/benchmarks/meta`
- [ ] Form submission calls `/api/v1/benchmarks/diagnose`

#### Authentication API
- [ ] Login calls `/api/v1/auth/login`
- [ ] After login, user info from `/api/v1/auth/me`

#### ETL API (requires login)
- [ ] Pipeline status from `/api/v1/etl/pipeline/status`
- [ ] Jobs list from `/api/v1/etl/jobs`
- [ ] Health metrics from `/api/v1/etl/health`

#### Analytics API
- [ ] Campaign data from `/api/v1/analytics/campaigns/summary`
- [ ] Partner data from `/api/v1/analytics/partners/pipeline`
- [ ] Executive data from `/api/v1/analytics/executive/dashboard`

#### Reports API
- [ ] Templates from `/api/v1/reports/templates`
- [ ] Report generation from `/api/v1/reports/{report-type}`

### ✅ Responsive Design
- [ ] Resize browser window to mobile size
- [ ] Navigation adapts to smaller screen
- [ ] Form grids stack vertically on mobile
- [ ] Cards remain readable on small screens

### ✅ Error Handling
- [ ] Network errors show notification messages
- [ ] Invalid login shows error notification
- [ ] Form validation prevents submission with missing data
- [ ] API errors are handled gracefully

### ✅ User Experience
- [ ] Transitions between tabs are smooth
- [ ] Loading states show for async operations
- [ ] Success/error notifications appear and disappear
- [ ] Form inputs remember values until reset
- [ ] Professional styling matches original Northlight design

## Expected Results

### API Endpoints Status
- ✅ `/health` - Should return healthy status
- ✅ `/api/v1/benchmarks/meta` - Should return benchmark categories
- ✅ `/api/v1/auth/login` - Should accept admin/admin123
- ✅ `/api/v1/etl/pipeline/status` - Should return pipeline status (with auth)
- ✅ `/api/v1/analytics/campaigns/summary` - Should return campaign metrics
- ✅ `/api/v1/reports/templates` - Should return available report templates

### Functional Integration
- **Benchmarks**: Original Northlight functionality preserved
- **ETL Management**: Real-time pipeline monitoring and control
- **Analytics**: Cross-platform metrics and KPIs
- **Reports**: Automated report generation
- **Authentication**: Secure access to protected features

## Troubleshooting

### Server Not Starting
```bash
# Check if port 8001 is in use
netstat -an | findstr 8001

# Try different port in core/config.py
API_PORT = 8002
```

### API Errors
- Check server logs for detailed error messages
- Verify database connection in `/health` endpoint
- Ensure all dependencies are installed: `pip install -r requirements.txt`

### Frontend Issues
- Open browser developer tools (F12)
- Check Console tab for JavaScript errors
- Check Network tab for failed API calls
- Verify static files are served correctly

### Authentication Issues
- Verify admin user exists (auto-created on first run)
- Check token in localStorage after login
- Verify JWT token in Authorization headers

## Success Criteria

The frontend integration is successful when:

1. ✅ All navigation tabs work correctly
2. ✅ Original benchmarking functionality is preserved
3. ✅ Authentication system works end-to-end
4. ✅ ETL management shows real pipeline data
5. ✅ Analytics displays unified platform metrics
6. ✅ Report generation works for all report types
7. ✅ API calls use correct unified endpoints
8. ✅ Error handling provides good user experience
9. ✅ Responsive design works on different screen sizes
10. ✅ Professional styling maintains brand consistency

## Phase 4 Completion

Phase 4: Frontend Unification is complete when all test items pass and the unified interface successfully combines:

- **Northlight's benchmarking interface** - Preserved with updated API endpoints
- **ETL management dashboard** - New interface for pipeline monitoring
- **Analytics visualization** - Unified metrics across both systems
- **Report generation interface** - Automated reporting capabilities
- **Authentication integration** - Secure access control throughout
- **Unified navigation** - Seamless switching between all features