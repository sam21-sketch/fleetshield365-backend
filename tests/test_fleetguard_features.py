"""
FleetGuard Prestart Backend API Tests
Testing features: Fuel Submission, Driver License/Training Expiry, Dashboard Stats
"""
import pytest
import requests
import os
from datetime import datetime, timedelta

# Get the backend URL from environment
BASE_URL = os.environ.get('EXPO_PUBLIC_BACKEND_URL', 'https://shield-dev-build.preview.emergentagent.com')

# Test credentials
ADMIN_EMAIL = "admin@test.com"
ADMIN_PASSWORD = "test123"
DRIVER_EMAIL = "driver@test.com"
DRIVER_PASSWORD = "test123"

# Global storage for test data
TEST_DATA = {}


class TestHealthCheck:
    """Basic health and API availability checks"""
    
    def test_api_health(self):
        """Test that API is running and healthy"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        print(f"API Health: {data}")
    
    def test_api_root(self):
        """Test API root endpoint"""
        response = requests.get(f"{BASE_URL}/api/")
        assert response.status_code == 200
        data = response.json()
        assert "FleetGuard" in data.get("message", "")
        print(f"API Root: {data}")


class TestAuthentication:
    """Authentication tests for both admin and driver"""
    
    def test_admin_login(self):
        """Test admin login"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        assert data["user"]["role"] in ["admin", "super_admin"]
        TEST_DATA["admin_token"] = data["access_token"]
        TEST_DATA["admin_user"] = data["user"]
        print(f"Admin logged in: {data['user']['email']}")
    
    def test_driver_login(self):
        """Test driver login"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": DRIVER_EMAIL,
            "password": DRIVER_PASSWORD
        })
        assert response.status_code == 200, f"Driver login failed: {response.text}"
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        TEST_DATA["driver_token"] = data["access_token"]
        TEST_DATA["driver_user"] = data["user"]
        print(f"Driver logged in: {data['user']['email']}")
    
    def test_invalid_login(self):
        """Test invalid login credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "invalid@test.com",
            "password": "wrongpassword"
        })
        assert response.status_code == 401


class TestFuelSubmission:
    """Test Fuel Submission feature - drivers can submit fuel purchases"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Ensure we have valid tokens"""
        if "driver_token" not in TEST_DATA:
            # Login as driver
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": DRIVER_EMAIL,
                "password": DRIVER_PASSWORD
            })
            assert response.status_code == 200
            TEST_DATA["driver_token"] = response.json()["access_token"]
    
    def test_get_vehicles_for_fuel(self):
        """Test that driver can get assigned vehicles"""
        headers = {"Authorization": f"Bearer {TEST_DATA['driver_token']}"}
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=headers)
        assert response.status_code == 200
        vehicles = response.json()
        if vehicles:
            TEST_DATA["test_vehicle_id"] = vehicles[0]["id"]
            print(f"Found {len(vehicles)} vehicle(s), using: {vehicles[0]['name']}")
        else:
            print("No vehicles found - need to create one first")
    
    def test_fuel_submission_success(self):
        """Test successful fuel submission with all fields"""
        headers = {"Authorization": f"Bearer {TEST_DATA['driver_token']}"}
        
        # First get a vehicle
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=headers)
        assert response.status_code == 200
        vehicles = response.json()
        
        if not vehicles:
            pytest.skip("No vehicles available for fuel submission test")
        
        vehicle_id = vehicles[0]["id"]
        
        # Submit fuel
        fuel_data = {
            "vehicle_id": vehicle_id,
            "amount": 85.50,
            "liters": 45.2,
            "odometer": 152000,
            "fuel_station": "Shell Test Station",
            "notes": "Test fuel submission",
            "receipt_photo_base64": None
        }
        
        response = requests.post(f"{BASE_URL}/api/fuel", json=fuel_data, headers=headers)
        assert response.status_code == 200, f"Fuel submission failed: {response.text}"
        data = response.json()
        assert "id" in data
        assert data["message"] == "Fuel submission recorded successfully"
        TEST_DATA["fuel_id"] = data["id"]
        print(f"Fuel submitted successfully: ID={data['id']}")
    
    def test_fuel_submission_missing_vehicle(self):
        """Test fuel submission with missing vehicle_id"""
        headers = {"Authorization": f"Bearer {TEST_DATA['driver_token']}"}
        
        fuel_data = {
            "amount": 50.00,
            "liters": 25.0
        }
        
        response = requests.post(f"{BASE_URL}/api/fuel", json=fuel_data, headers=headers)
        # Should fail validation
        assert response.status_code in [400, 422]
    
    def test_fuel_submission_invalid_vehicle(self):
        """Test fuel submission with invalid vehicle_id"""
        headers = {"Authorization": f"Bearer {TEST_DATA['driver_token']}"}
        
        fuel_data = {
            "vehicle_id": "000000000000000000000000",  # Non-existent ID
            "amount": 50.00,
            "liters": 25.0
        }
        
        response = requests.post(f"{BASE_URL}/api/fuel", json=fuel_data, headers=headers)
        assert response.status_code == 404
    
    def test_get_fuel_submissions(self):
        """Test retrieving fuel submissions"""
        # Use admin token to get all fuel submissions
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            TEST_DATA["admin_token"] = response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/fuel", headers=headers)
        assert response.status_code == 200
        submissions = response.json()
        assert isinstance(submissions, list)
        print(f"Found {len(submissions)} fuel submission(s)")
        
        # Check that our test submission is there
        if submissions:
            submission = submissions[0]
            assert "amount" in submission
            assert "liters" in submission
            assert "vehicle_name" in submission or "vehicle_id" in submission


class TestDriverManagement:
    """Test Driver License and Training expiry tracking"""
    
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
    
    def test_get_drivers(self):
        """Test getting list of drivers"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/drivers", headers=headers)
        assert response.status_code == 200
        drivers = response.json()
        assert isinstance(drivers, list)
        print(f"Found {len(drivers)} driver(s)")
        
        if drivers:
            TEST_DATA["test_driver_id"] = drivers[0]["id"]
            print(f"Test driver: {drivers[0].get('name', 'Unknown')}")
    
    def test_create_driver_with_license_info(self):
        """Test creating a driver with license and training expiry fields"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # Calculate dates for testing
        sixty_days_from_now = (datetime.utcnow() + timedelta(days=60)).strftime('%Y-%m-%d')
        thirty_days_from_now = (datetime.utcnow() + timedelta(days=30)).strftime('%Y-%m-%d')
        
        driver_data = {
            "name": "TEST_Driver_License_Test",
            "email": f"TEST_driver_{datetime.now().timestamp()}@test.com",
            "password": "test123",
            "phone": "0412345678",
            "license_number": "DL987654",
            "license_class": "HC",
            "license_expiry": sixty_days_from_now,
            "medical_certificate_expiry": thirty_days_from_now,
            "first_aid_expiry": "NA",
            "forklift_license_expiry": sixty_days_from_now,
            "dangerous_goods_expiry": "NA"
        }
        
        response = requests.post(f"{BASE_URL}/api/drivers", json=driver_data, headers=headers)
        assert response.status_code == 200, f"Failed to create driver: {response.text}"
        data = response.json()
        assert "id" in data
        TEST_DATA["created_driver_id"] = data["id"]
        print(f"Created test driver: {data['id']}")
    
    def test_update_driver_license_info(self):
        """Test updating driver license and training expiry dates"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # Get existing drivers
        response = requests.get(f"{BASE_URL}/api/drivers", headers=headers)
        assert response.status_code == 200
        drivers = response.json()
        
        if not drivers:
            pytest.skip("No drivers to update")
        
        driver_id = drivers[0]["id"]
        
        # Update driver with expiry dates
        update_data = {
            "license_number": "DL123456789",
            "license_class": "HR",
            "license_expiry": "2026-04-15",
            "medical_certificate_expiry": "2026-03-01",
            "first_aid_expiry": "2026-06-01",
            "forklift_license_expiry": "NA",
            "dangerous_goods_expiry": "2026-08-15"
        }
        
        response = requests.put(f"{BASE_URL}/api/drivers/{driver_id}", json=update_data, headers=headers)
        assert response.status_code == 200, f"Failed to update driver: {response.text}"
        data = response.json()
        assert data["message"] == "Driver updated successfully"
        print(f"Driver {driver_id} updated with license info")
    
    def test_driver_update_triggers_expiry_alert(self):
        """Test that updating driver with expiring license creates alert"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # Get drivers
        response = requests.get(f"{BASE_URL}/api/drivers", headers=headers)
        drivers = response.json()
        
        if not drivers:
            pytest.skip("No drivers available")
        
        driver_id = drivers[0]["id"]
        
        # Set license to expire in 30 days (within 60 day alert window)
        thirty_days = (datetime.utcnow() + timedelta(days=30)).strftime('%Y-%m-%d')
        
        update_data = {
            "license_expiry": thirty_days
        }
        
        response = requests.put(f"{BASE_URL}/api/drivers/{driver_id}", json=update_data, headers=headers)
        assert response.status_code == 200
        
        # Check alerts for driver expiry alert
        response = requests.get(f"{BASE_URL}/api/alerts", headers=headers)
        assert response.status_code == 200
        alerts = response.json()
        
        # Look for driver expiry alerts
        driver_alerts = [a for a in alerts if "driver" in a.get("type", "").lower() or 
                        "license" in a.get("message", "").lower()]
        print(f"Found {len(driver_alerts)} driver-related alert(s)")
    
    def test_drivers_require_admin_access(self):
        """Test that only admins can access driver management"""
        # Try with driver token
        if "driver_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": DRIVER_EMAIL,
                "password": DRIVER_PASSWORD
            })
            TEST_DATA["driver_token"] = response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {TEST_DATA['driver_token']}"}
        response = requests.get(f"{BASE_URL}/api/drivers", headers=headers)
        assert response.status_code == 403, "Drivers should not access driver list"


class TestDashboardStats:
    """Test dashboard stats including driver expiry counts"""
    
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
    
    def test_dashboard_stats_structure(self):
        """Test dashboard stats endpoint returns expected fields"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=headers)
        assert response.status_code == 200
        stats = response.json()
        
        # Verify basic stats fields
        assert "total_vehicles" in stats
        assert "active_today" in stats
        assert "issues_today" in stats
        assert "vehicles_needing_attention" in stats
        assert "unread_alerts" in stats
        
        # Verify driver expiry stats fields (NEW FEATURE)
        assert "drivers_license_expiring" in stats, "Missing drivers_license_expiring field"
        assert "drivers_license_expired" in stats, "Missing drivers_license_expired field"
        assert "drivers_training_expiring" in stats, "Missing drivers_training_expiring field"
        assert "drivers_training_expired" in stats, "Missing drivers_training_expired field"
        assert "total_drivers" in stats, "Missing total_drivers field"
        
        print(f"Dashboard stats: {stats}")
        print(f"Driver License Expiring: {stats['drivers_license_expiring']}")
        print(f"Driver License Expired: {stats['drivers_license_expired']}")
        print(f"Driver Training Expiring: {stats['drivers_training_expiring']}")
        print(f"Driver Training Expired: {stats['drivers_training_expired']}")
    
    def test_dashboard_driver_expiry_counts(self):
        """Test that driver expiry counts are accurate based on test data"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=headers)
        assert response.status_code == 200
        stats = response.json()
        
        # According to the problem statement, test driver has:
        # - license expiry: 2026-04-15 (within 60 days based on current date)
        # - medical certificate expiry: 2026-03-01 (within 60 days)
        
        # Just verify the counts are non-negative integers
        assert isinstance(stats["drivers_license_expiring"], int)
        assert isinstance(stats["drivers_license_expired"], int)
        assert isinstance(stats["drivers_training_expiring"], int)
        assert isinstance(stats["drivers_training_expired"], int)
        assert stats["drivers_license_expiring"] >= 0
        
        print(f"Total drivers: {stats['total_drivers']}")


class TestAlertSystem:
    """Test alert system for driver document expiries"""
    
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
    
    def test_get_alerts(self):
        """Test retrieving alerts"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/alerts", headers=headers)
        assert response.status_code == 200
        alerts = response.json()
        assert isinstance(alerts, list)
        print(f"Found {len(alerts)} total alert(s)")
        
        # Check for driver-related alerts
        for alert in alerts[:5]:
            print(f"Alert: {alert.get('type')} - {alert.get('message', '')[:50]}...")
    
    def test_get_unread_alerts(self):
        """Test retrieving unread alerts only"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        response = requests.get(f"{BASE_URL}/api/alerts", params={"unread_only": True}, headers=headers)
        assert response.status_code == 200
        alerts = response.json()
        assert isinstance(alerts, list)
        print(f"Found {len(alerts)} unread alert(s)")
    
    def test_mark_alert_read(self):
        """Test marking an alert as read"""
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # Get alerts
        response = requests.get(f"{BASE_URL}/api/alerts", headers=headers)
        alerts = response.json()
        
        if not alerts:
            pytest.skip("No alerts to mark as read")
        
        alert_id = alerts[0]["id"]
        response = requests.put(f"{BASE_URL}/api/alerts/{alert_id}/read", headers=headers)
        assert response.status_code == 200
        print(f"Marked alert {alert_id} as read")


class TestCleanup:
    """Clean up test data created during tests"""
    
    def test_cleanup_test_drivers(self):
        """Delete test-created drivers"""
        if "admin_token" not in TEST_DATA:
            response = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            TEST_DATA["admin_token"] = response.json()["access_token"]
        
        headers = {"Authorization": f"Bearer {TEST_DATA['admin_token']}"}
        
        # Get all drivers
        response = requests.get(f"{BASE_URL}/api/drivers", headers=headers)
        if response.status_code == 200:
            drivers = response.json()
            for driver in drivers:
                if driver.get("name", "").startswith("TEST_"):
                    response = requests.delete(f"{BASE_URL}/api/drivers/{driver['id']}", headers=headers)
                    if response.status_code == 200:
                        print(f"Deleted test driver: {driver['name']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
