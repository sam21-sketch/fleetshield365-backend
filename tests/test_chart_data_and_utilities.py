"""
Test suite for FleetGuard Priority Features:
1. Dashboard Chart Data API endpoint
2. Image Compression utility file exports verification
"""
import pytest
import requests
import os
from datetime import datetime

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://fleet-build-lab.preview.emergentagent.com')

class TestAuth:
    """Authentication tests for API access"""
    
    def test_login_success(self, api_client):
        """Test successful login with admin credentials"""
        response = api_client.post(f"{BASE_URL}/api/auth/login", json={
            "email": "admin@test.com",
            "password": "test123"
        })
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        assert data["user"]["email"] == "admin@test.com"
        assert data["user"]["role"] in ["super_admin", "admin"]
        print(f"✓ Login successful for {data['user']['email']}, role: {data['user']['role']}")


class TestDashboardChartData:
    """Tests for GET /api/dashboard/chart-data endpoint"""
    
    def test_chart_data_endpoint_exists(self, authenticated_client):
        """Test that the chart-data endpoint exists and returns 200"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        print("✓ Chart data endpoint exists and returns 200")
    
    def test_chart_data_returns_array(self, authenticated_client):
        """Test that chart-data returns an array"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Chart data returns array with {len(data)} items")
    
    def test_chart_data_has_required_fields(self, authenticated_client):
        """Test that each chart data point has required fields"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        data = response.json()
        
        assert len(data) > 0, "Chart data should not be empty"
        
        for point in data:
            assert "day" in point, "Missing 'day' field"
            assert "date" in point, "Missing 'date' field"
            assert "inspections" in point, "Missing 'inspections' field"
            assert "issues" in point, "Missing 'issues' field"
            assert "fuel" in point, "Missing 'fuel' field"
        
        print(f"✓ All {len(data)} data points have required fields: day, date, inspections, issues, fuel")
    
    def test_chart_data_day_names(self, authenticated_client):
        """Test that day names are valid weekday abbreviations"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        data = response.json()
        
        valid_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        for point in data:
            assert point["day"] in valid_days, f"Invalid day name: {point['day']}"
        
        print("✓ All day names are valid weekday abbreviations")
    
    def test_chart_data_date_format(self, authenticated_client):
        """Test that dates are in YYYY-MM-DD format"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        data = response.json()
        
        for point in data:
            try:
                datetime.strptime(point["date"], "%Y-%m-%d")
            except ValueError:
                pytest.fail(f"Invalid date format: {point['date']}")
        
        print("✓ All dates are in valid YYYY-MM-DD format")
    
    def test_chart_data_numeric_values(self, authenticated_client):
        """Test that numeric fields are non-negative numbers"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        data = response.json()
        
        for point in data:
            assert isinstance(point["inspections"], (int, float)), "inspections should be numeric"
            assert isinstance(point["issues"], (int, float)), "issues should be numeric"
            assert isinstance(point["fuel"], (int, float)), "fuel should be numeric"
            assert point["inspections"] >= 0, "inspections should be non-negative"
            assert point["issues"] >= 0, "issues should be non-negative"
            assert point["fuel"] >= 0, "fuel should be non-negative"
        
        print("✓ All numeric values are valid non-negative numbers")
    
    def test_chart_data_days_parameter(self, authenticated_client):
        """Test that days parameter controls data range"""
        # Test default (7 days)
        response7 = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response7.status_code == 200
        data7 = response7.json()
        assert len(data7) == 7, f"Expected 7 days of data, got {len(data7)}"
        
        # Test explicit days=7
        response_explicit = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data?days=7")
        assert response_explicit.status_code == 200
        data_explicit = response_explicit.json()
        assert len(data_explicit) == 7
        
        print("✓ Days parameter correctly controls data range")
    
    def test_chart_data_has_real_data(self, authenticated_client):
        """Test that chart data contains real inspection data (not all zeros)"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code == 200
        data = response.json()
        
        total_inspections = sum(point["inspections"] for point in data)
        total_issues = sum(point["issues"] for point in data)
        total_fuel = sum(point["fuel"] for point in data)
        
        print(f"  Total inspections: {total_inspections}")
        print(f"  Total issues: {total_issues}")
        print(f"  Total fuel: {total_fuel}")
        
        # At least one metric should have some data (from previous testing)
        has_data = total_inspections > 0 or total_issues > 0 or total_fuel > 0
        assert has_data, "Chart data appears to be all zeros - expected real data from database"
        
        print("✓ Chart data contains real data from database (not mock)")
    
    def test_chart_data_requires_auth(self):
        """Test that chart-data endpoint requires authentication"""
        # Use a fresh session without auth header
        fresh_session = requests.Session()
        response = fresh_session.get(f"{BASE_URL}/api/dashboard/chart-data")
        assert response.status_code in [401, 403]
        print("✓ Chart data endpoint requires authentication")


class TestDashboardStats:
    """Test dashboard stats endpoint (related to charts)"""
    
    def test_dashboard_stats_endpoint(self, authenticated_client):
        """Test GET /api/dashboard/stats returns data"""
        response = authenticated_client.get(f"{BASE_URL}/api/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        
        # Should have key stats
        expected_fields = ["total_vehicles", "total_drivers", "inspections_today"]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"
        
        print(f"✓ Dashboard stats returned: vehicles={data.get('total_vehicles')}, drivers={data.get('total_drivers')}")


class TestIncidentsNavigation:
    """Test Incidents page navigation from sidebar"""
    
    def test_incidents_endpoint_exists(self, authenticated_client):
        """Test that incidents endpoint is accessible"""
        response = authenticated_client.get(f"{BASE_URL}/api/incidents")
        assert response.status_code == 200
        print("✓ Incidents endpoint exists and returns 200")


class TestImageCompressionUtilities:
    """Tests to verify image compression utility files exist with correct exports"""
    
    def test_website_compression_file_exists(self):
        """Test that website image compression utility file exists"""
        path = "/app/website/src/utils/imageCompression.ts"
        assert os.path.exists(path), f"File not found: {path}"
        print(f"✓ Website compression utility exists: {path}")
    
    def test_website_compression_exports(self):
        """Test that website compression utility has expected exports"""
        path = "/app/website/src/utils/imageCompression.ts"
        with open(path, 'r') as f:
            content = f.read()
        
        expected_exports = [
            "CompressionOptions",
            "CompressionPresets",
            "compressImage",
            "compressBase64",
            "getBase64Size",
            "formatFileSize",
            "compressMultipleImages"
        ]
        
        for export in expected_exports:
            assert export in content, f"Missing export: {export}"
        
        print(f"✓ Website compression utility has all {len(expected_exports)} expected exports")
    
    def test_mobile_compression_file_exists(self):
        """Test that mobile image compression utility file exists"""
        path = "/app/frontend/src/utils/imageCompression.ts"
        assert os.path.exists(path), f"File not found: {path}"
        print(f"✓ Mobile compression utility exists: {path}")
    
    def test_mobile_compression_exports(self):
        """Test that mobile compression utility has expected exports"""
        path = "/app/frontend/src/utils/imageCompression.ts"
        with open(path, 'r') as f:
            content = f.read()
        
        expected_exports = [
            "CompressionOptions",
            "CompressionPresets",
            "compressImageUri",
            "compressImageToBase64",
            "compressMultipleImages",
            "compressMultipleImagesToBase64",
            "getBase64Size",
            "formatFileSize"
        ]
        
        for export in expected_exports:
            assert export in content, f"Missing export: {export}"
        
        print(f"✓ Mobile compression utility has all {len(expected_exports)} expected exports")
    
    def test_compression_presets(self):
        """Test that compression presets include expected use cases"""
        # Test website presets
        website_path = "/app/website/src/utils/imageCompression.ts"
        with open(website_path, 'r') as f:
            website_content = f.read()
        
        expected_presets = ["inspection", "receipt", "incident", "logo", "thumbnail"]
        for preset in expected_presets:
            assert preset in website_content, f"Website missing preset: {preset}"
        
        # Test mobile presets
        mobile_path = "/app/frontend/src/utils/imageCompression.ts"
        with open(mobile_path, 'r') as f:
            mobile_content = f.read()
        
        mobile_presets = ["inspection", "receipt", "incident", "thumbnail"]
        for preset in mobile_presets:
            assert preset in mobile_content, f"Mobile missing preset: {preset}"
        
        print("✓ Both compression utilities have expected presets (inspection, receipt, incident)")


class TestMobileIncidentForm:
    """Tests to verify mobile incident report form exists"""
    
    def test_incident_form_file_exists(self):
        """Test that mobile incident form file exists"""
        path = "/app/frontend/app/inspection/incident.tsx"
        assert os.path.exists(path), f"File not found: {path}"
        print(f"✓ Mobile incident form exists: {path}")
    
    def test_incident_form_has_emergency_call(self):
        """Test that incident form has emergency 000 call feature"""
        path = "/app/frontend/app/inspection/incident.tsx"
        with open(path, 'r') as f:
            content = f.read()
        
        # Check for emergency call feature
        assert "000" in content, "Missing emergency 000 number"
        assert "call000" in content or "Call 000" in content.lower() or "call emergency" in content.lower(), "Missing call 000 function"
        assert "Is someone injured" in content or "someone hurt" in content.lower(), "Missing injury warning message"
        
        print("✓ Mobile incident form has emergency 000 call feature")
    
    def test_incident_form_has_multi_step(self):
        """Test that incident form has multi-step flow"""
        path = "/app/frontend/app/inspection/incident.tsx"
        with open(path, 'r') as f:
            content = f.read()
        
        # Check for step navigation
        assert "step" in content, "Missing step state"
        assert "Step 1" in content or "step === 1" in content or "step: 1" in content, "Missing step indicators"
        
        print("✓ Mobile incident form has multi-step flow")
    
    def test_incident_form_has_other_party(self):
        """Test that incident form has other party details (mandatory)"""
        path = "/app/frontend/app/inspection/incident.tsx"
        with open(path, 'r') as f:
            content = f.read()
        
        # Check for other party fields
        assert "other_party" in content.lower() or "otherParty" in content, "Missing other party section"
        assert "vehicle_rego" in content or "Vehicle Registration" in content, "Missing other party vehicle rego"
        
        print("✓ Mobile incident form has other party details section")
    
    def test_incident_form_has_photos(self):
        """Test that incident form has photo capture (mandatory)"""
        path = "/app/frontend/app/inspection/incident.tsx"
        with open(path, 'r') as f:
            content = f.read()
        
        # Check for photo capture
        assert "photo" in content.lower(), "Missing photo references"
        assert "takePhoto" in content or "pickPhoto" in content, "Missing photo capture functions"
        
        print("✓ Mobile incident form has photo capture feature")
    
    def test_incident_form_uses_compression(self):
        """Test that incident form uses image compression"""
        path = "/app/frontend/app/inspection/incident.tsx"
        with open(path, 'r') as f:
            content = f.read()
        
        # Check for compression usage
        assert "imageCompression" in content or "compressImage" in content, "Not using image compression"
        assert "CompressionPresets" in content, "Not using compression presets"
        
        print("✓ Mobile incident form uses image compression utility")


# Fixtures
@pytest.fixture(scope="module")
def api_client():
    """Create a requests session for API calls"""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


@pytest.fixture(scope="module")
def auth_token(api_client):
    """Get authentication token"""
    response = api_client.post(f"{BASE_URL}/api/auth/login", json={
        "email": "admin@test.com",
        "password": "test123"
    })
    if response.status_code == 200:
        return response.json().get("access_token")
    pytest.skip("Authentication failed - skipping authenticated tests")


@pytest.fixture(scope="module")
def authenticated_client(api_client, auth_token):
    """Create session with auth header"""
    api_client.headers.update({"Authorization": f"Bearer {auth_token}"})
    return api_client


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
