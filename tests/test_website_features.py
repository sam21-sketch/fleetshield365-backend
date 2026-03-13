"""
FleetGuard Website Backend API Tests
Testing: Login, Dashboard KPIs, Alerts, Notification Preferences
"""
import pytest
import requests
import os

# Get the backend URL from environment
BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://fleet-ops-test-2.preview.emergentagent.com')

# Test credentials
ADMIN_EMAIL = "admin@test.com"
ADMIN_PASSWORD = "test123"
DRIVER_EMAIL = "driver@test.com"
DRIVER_PASSWORD = "test123"

# Global storage for test data
TEST_DATA = {}


class TestAuthentication:
    """Authentication tests - Login flow"""
    
    def test_admin_login_success(self):
        """Test admin can login with valid credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        data = response.json()
        
        # Verify response structure
        assert "access_token" in data, "Missing access_token in response"
        assert "user" in data, "Missing user in response"
        assert data["user"]["email"] == ADMIN_EMAIL
        assert data["user"]["role"] in ["admin", "super_admin"]
        
        TEST_DATA["admin_token"] = data["access_token"]
        TEST_DATA["admin_user"] = data["user"]
        print(f"Admin logged in: {data['user']['email']}, role: {data['user']['role']}")
    
    def test_admin_login_invalid_credentials(self):
        """Test login fails with invalid credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "invalid@test.com",
            "password": "wrongpassword"
        })
        assert response.status_code == 401, "Should return 401 for invalid credentials"
    
    def test_auth_me_endpoint(self):
        """Test /auth/me returns user and company data"""
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            TEST_DATA["admin_token"] = response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
        assert response.status_code == 200, f"Auth/me failed: {response.text}"
        
        data = response.json()
        # Verify user and company structure (as fixed by main agent)
        assert "user" in data, "Missing user in /auth/me response"
        assert "company" in data, "Missing company in /auth/me response"
        assert data["user"]["email"] == ADMIN_EMAIL
        print(f"Auth/me returned user: {data['user']['name']}, company: {data.get('company', {}).get('name', 'N/A')}")


class TestDashboardStats:
    """Dashboard KPI endpoint tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure admin token"""
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            assert response.status_code == 200
            TEST_DATA["admin_token"] = response.json()["access_token"]
    
    def test_dashboard_stats_returns_all_kpis(self):
        """Test /dashboard/stats returns all required KPI fields"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=headers)
        assert response.status_code == 200, f"Dashboard stats failed: {response.text}"
        
        stats = response.json()
        
        # Verify all required KPI fields exist
        required_fields = [
            "total_vehicles",
            "total_drivers",
            "inspections_today",
            "inspections_missed",
            "issues_today",
            "fuel_this_month",
            "expiring_soon"
        ]
        
        for field in required_fields:
            assert field in stats, f"Missing required field: {field}"
            assert isinstance(stats[field], (int, float)), f"Field {field} should be numeric"
        
        print(f"Dashboard KPIs:")
        print(f"  - Total Vehicles: {stats['total_vehicles']}")
        print(f"  - Total Drivers: {stats['total_drivers']}")
        print(f"  - Inspections Today: {stats['inspections_today']}")
        print(f"  - Inspections Missed: {stats['inspections_missed']}")
        print(f"  - Issues Today: {stats['issues_today']}")
        print(f"  - Fuel This Month: ${stats['fuel_this_month']}")
        print(f"  - Expiring Soon: {stats['expiring_soon']}")
    
    def test_dashboard_stats_data_types(self):
        """Test dashboard stats have correct data types"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=headers)
        stats = response.json()
        
        # All counts should be non-negative integers
        assert stats["total_vehicles"] >= 0
        assert stats["total_drivers"] >= 0
        assert stats["inspections_today"] >= 0
        assert stats["issues_today"] >= 0
        
        # Fuel can be 0 or positive
        assert stats["fuel_this_month"] >= 0


class TestAlerts:
    """Alerts endpoint tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure admin token"""
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            assert response.status_code == 200
            TEST_DATA["admin_token"] = response.json()["access_token"]
    
    def test_get_alerts_success(self):
        """Test /alerts returns alert list"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/alerts", headers=headers)
        assert response.status_code == 200, f"Get alerts failed: {response.text}"
        
        alerts = response.json()
        assert isinstance(alerts, list), "Alerts should be a list"
        print(f"Found {len(alerts)} total alerts")
        
        # Verify alert structure if alerts exist
        if alerts:
            alert = alerts[0]
            assert "id" in alert, "Alert missing id"
            assert "type" in alert, "Alert missing type"
            assert "message" in alert, "Alert missing message"
            print(f"Sample alert: {alert['type']} - {alert['message'][:50]}...")
    
    def test_get_alerts_structure(self):
        """Test alerts have correct structure for frontend display"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/alerts", headers=headers)
        alerts = response.json()
        
        if alerts:
            for alert in alerts[:5]:  # Check first 5 alerts
                # Required fields for dashboard display
                assert "id" in alert
                assert "type" in alert
                assert "message" in alert
                assert "created_at" in alert
                # Verify severity field exists (for color coding)
                if "severity" in alert:
                    assert alert["severity"] in ["critical", "warning", "info"]


class TestNotificationPreferences:
    """Notification preferences endpoint tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure admin token"""
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            assert response.status_code == 200
            TEST_DATA["admin_token"] = response.json()["access_token"]
    
    def test_get_notification_preferences(self):
        """Test GET /notification-preferences returns all settings"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/notification-preferences", headers=headers)
        assert response.status_code == 200, f"Get notification prefs failed: {response.text}"
        
        prefs = response.json()
        
        # Verify all expected preference fields
        expected_fields = [
            "expiry_alerts",
            "issue_alerts",
            "missed_inspection_alerts",
            "daily_summary",
            "push_enabled",
            "email_enabled"
        ]
        
        for field in expected_fields:
            assert field in prefs, f"Missing notification preference: {field}"
            assert isinstance(prefs[field], bool), f"Preference {field} should be boolean"
        
        print(f"Notification Preferences:")
        for field in expected_fields:
            print(f"  - {field}: {prefs[field]}")
    
    def test_update_notification_preferences(self):
        """Test PUT /notification-preferences updates settings"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # First get current state
        response = requests.get(f"{BASE_URL}/api/notification-preferences", headers=headers)
        original_prefs = response.json()
        original_daily_summary = original_prefs.get("daily_summary", True)
        
        # Toggle daily_summary
        new_value = not original_daily_summary
        response = requests.put(f"{BASE_URL}/api/notification-preferences", 
                               json={"daily_summary": new_value}, 
                               headers=headers)
        assert response.status_code == 200, f"Update notification prefs failed: {response.text}"
        
        # Verify the change
        response = requests.get(f"{BASE_URL}/api/notification-preferences", headers=headers)
        updated_prefs = response.json()
        assert updated_prefs["daily_summary"] == new_value, "Preference was not updated"
        print(f"Successfully toggled daily_summary from {original_daily_summary} to {new_value}")
        
        # Restore original value
        requests.put(f"{BASE_URL}/api/notification-preferences", 
                    json={"daily_summary": original_daily_summary}, 
                    headers=headers)
    
    def test_update_single_preference(self):
        """Test that updating one preference doesn't affect others"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # Get current state
        response = requests.get(f"{BASE_URL}/api/notification-preferences", headers=headers)
        original_prefs = response.json()
        
        # Update only push_enabled
        original_push = original_prefs.get("push_enabled", True)
        response = requests.put(f"{BASE_URL}/api/notification-preferences", 
                               json={"push_enabled": not original_push}, 
                               headers=headers)
        assert response.status_code == 200
        
        # Verify other preferences unchanged
        response = requests.get(f"{BASE_URL}/api/notification-preferences", headers=headers)
        new_prefs = response.json()
        
        # Other fields should remain the same
        assert new_prefs["expiry_alerts"] == original_prefs["expiry_alerts"]
        assert new_prefs["issue_alerts"] == original_prefs["issue_alerts"]
        assert new_prefs["email_enabled"] == original_prefs["email_enabled"]
        
        # Restore original value
        requests.put(f"{BASE_URL}/api/notification-preferences", 
                    json={"push_enabled": original_push}, 
                    headers=headers)
        print("Single preference update test passed - other fields unchanged")


class TestNavigation:
    """Test navigation-related endpoints work"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure admin token"""
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            assert response.status_code == 200
            TEST_DATA["admin_token"] = response.json()["access_token"]
    
    def test_vehicles_endpoint(self):
        """Test /vehicles endpoint returns data"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=headers)
        assert response.status_code == 200, f"Get vehicles failed: {response.text}"
        
        vehicles = response.json()
        assert isinstance(vehicles, list)
        print(f"Vehicles page API: {len(vehicles)} vehicles")
    
    def test_drivers_endpoint(self):
        """Test /drivers endpoint returns data"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/drivers", headers=headers)
        assert response.status_code == 200, f"Get drivers failed: {response.text}"
        
        drivers = response.json()
        assert isinstance(drivers, list)
        print(f"Drivers page API: {len(drivers)} drivers")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
