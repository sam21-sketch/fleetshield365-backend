"""
Comprehensive Backend API Tests for FleetGuard Prestart
Testing all endpoints: Auth, Dashboard, Vehicles, Drivers, Reports, Fuel, Incidents, Settings
"""
import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_API_URL', 'https://fleet-ops-test-2.preview.emergentagent.com/api')

# Test credentials
ADMIN_EMAIL = "admin@test.com"
ADMIN_PASSWORD = "test123"
DRIVER_EMAIL = "driver@test.com"
DRIVER_PASSWORD = "test123"


class TestAuthEndpoints:
    """Authentication flow tests"""
    
    def test_login_admin_success(self):
        """Test admin login returns token"""
        response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        assert data["user"]["email"] == ADMIN_EMAIL
        print(f"✓ Admin login successful")

    def test_login_driver_success(self):
        """Test driver login returns token"""
        response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": DRIVER_EMAIL,
            "password": DRIVER_PASSWORD
        })
        assert response.status_code == 200, f"Driver login failed: {response.text}"
        data = response.json()
        assert "access_token" in data
        print(f"✓ Driver login successful")

    def test_login_invalid_credentials(self):
        """Test invalid credentials return 401"""
        response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": "invalid@test.com",
            "password": "wrongpassword"
        })
        assert response.status_code == 401
        print(f"✓ Invalid credentials returns 401")

    def test_auth_me_endpoint(self):
        """Test /auth/me returns user info"""
        # Login first
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        token = login_response.json()["access_token"]
        
        # Get user info
        response = requests.get(f"{BASE_URL}/auth/me", headers={
            "Authorization": f"Bearer {token}"
        })
        assert response.status_code == 200
        data = response.json()
        assert "user" in data
        assert data["user"]["email"] == ADMIN_EMAIL
        print(f"✓ Auth/me endpoint works")


class TestDashboardEndpoints:
    """Dashboard stats and charts tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_dashboard_stats(self):
        """Test dashboard stats endpoint"""
        response = requests.get(f"{BASE_URL}/dashboard/stats", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        # Verify required fields
        assert "total_vehicles" in data
        assert "total_drivers" in data
        assert "inspections_today" in data
        print(f"✓ Dashboard stats: {data['total_vehicles']} vehicles, {data['total_drivers']} drivers")

    def test_dashboard_chart_data(self):
        """Test chart data endpoint returns weekly data"""
        response = requests.get(f"{BASE_URL}/dashboard/chart-data?days=7", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 7  # 7 days of data
        # Verify data structure
        for day_data in data:
            assert "day" in day_data
            assert "inspections" in day_data
            assert "issues" in day_data
        print(f"✓ Chart data: {len(data)} days of data")

    def test_alerts_endpoint(self):
        """Test alerts endpoint"""
        response = requests.get(f"{BASE_URL}/alerts", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Alerts endpoint: {len(data)} alerts")


class TestVehiclesEndpoints:
    """Vehicle CRUD tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_list_vehicles(self):
        """Test listing all vehicles"""
        response = requests.get(f"{BASE_URL}/vehicles", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Listed {len(data)} vehicles")

    def test_create_vehicle(self):
        """Test creating a new vehicle"""
        vehicle_data = {
            "name": "TEST_Vehicle_API",
            "registration_number": "TEST-001",
            "type": "truck",
            "status": "active",
            "rego_expiry": "2027-12-31"
        }
        response = requests.post(f"{BASE_URL}/vehicles", json=vehicle_data, headers=self.headers)
        assert response.status_code == 200, f"Create vehicle failed: {response.text}"
        data = response.json()
        assert data["name"] == vehicle_data["name"]
        assert "id" in data
        self.created_vehicle_id = data["id"]
        print(f"✓ Created vehicle: {data['name']}")
        
        # Cleanup - delete test vehicle
        requests.delete(f"{BASE_URL}/vehicles/{self.created_vehicle_id}", headers=self.headers)

    def test_update_vehicle(self):
        """Test updating a vehicle"""
        # First create a vehicle
        vehicle_data = {
            "name": "TEST_Update_Vehicle",
            "registration_number": "TEST-002"
        }
        create_response = requests.post(f"{BASE_URL}/vehicles", json=vehicle_data, headers=self.headers)
        vehicle_id = create_response.json()["id"]
        
        # Update it
        update_data = {"name": "TEST_Updated_Vehicle"}
        response = requests.put(f"{BASE_URL}/vehicles/{vehicle_id}", json=update_data, headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "TEST_Updated_Vehicle"
        print(f"✓ Updated vehicle name")
        
        # Cleanup
        requests.delete(f"{BASE_URL}/vehicles/{vehicle_id}", headers=self.headers)

    def test_delete_vehicle(self):
        """Test deleting a vehicle"""
        # First create a vehicle
        vehicle_data = {
            "name": "TEST_Delete_Vehicle",
            "registration_number": "TEST-003"
        }
        create_response = requests.post(f"{BASE_URL}/vehicles", json=vehicle_data, headers=self.headers)
        vehicle_id = create_response.json()["id"]
        
        # Delete it
        response = requests.delete(f"{BASE_URL}/vehicles/{vehicle_id}", headers=self.headers)
        assert response.status_code == 200
        print(f"✓ Deleted vehicle")


class TestDriversEndpoints:
    """Driver CRUD tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_list_drivers(self):
        """Test listing all drivers"""
        response = requests.get(f"{BASE_URL}/drivers", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Listed {len(data)} drivers")

    def test_create_driver(self):
        """Test creating a new driver"""
        driver_data = {
            "name": "TEST_Driver_API",
            "email": "test_driver_api@test.com",
            "password": "test123456",
            "phone": "0400123456"
        }
        response = requests.post(f"{BASE_URL}/drivers", json=driver_data, headers=self.headers)
        assert response.status_code == 200, f"Create driver failed: {response.text}"
        data = response.json()
        assert data["name"] == driver_data["name"]
        assert "id" in data
        driver_id = data["id"]
        print(f"✓ Created driver: {data['name']}")
        
        # Cleanup - delete test driver
        requests.delete(f"{BASE_URL}/drivers/{driver_id}", headers=self.headers)

    def test_delete_driver(self):
        """Test deleting a driver"""
        # First create a driver
        driver_data = {
            "name": "TEST_Delete_Driver",
            "email": "test_delete_driver@test.com",
            "password": "test123456"
        }
        create_response = requests.post(f"{BASE_URL}/drivers", json=driver_data, headers=self.headers)
        driver_id = create_response.json()["id"]
        
        # Delete it
        response = requests.delete(f"{BASE_URL}/drivers/{driver_id}", headers=self.headers)
        assert response.status_code == 200
        print(f"✓ Deleted driver")


class TestReportsEndpoints:
    """Inspection reports tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_list_inspections(self):
        """Test listing all inspections"""
        response = requests.get(f"{BASE_URL}/inspections?limit=50", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Listed {len(data)} inspections")

    def test_inspection_has_required_fields(self):
        """Test inspections have required fields"""
        response = requests.get(f"{BASE_URL}/inspections?limit=1", headers=self.headers)
        data = response.json()
        if len(data) > 0:
            inspection = data[0]
            assert "id" in inspection
            assert "vehicle_id" in inspection or "vehicle_name" in inspection
            print(f"✓ Inspection has required fields")
        else:
            print("⚠ No inspections to verify fields")


class TestFuelLogsEndpoints:
    """Fuel logs tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_list_fuel_logs(self):
        """Test listing fuel logs"""
        response = requests.get(f"{BASE_URL}/fuel?limit=50", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Listed {len(data)} fuel logs")


class TestIncidentsEndpoints:
    """Incident reports tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_list_incidents(self):
        """Test listing incidents"""
        response = requests.get(f"{BASE_URL}/incidents?limit=50", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Listed {len(data)} incidents")

    def test_incidents_stats(self):
        """Test incident stats summary"""
        response = requests.get(f"{BASE_URL}/incidents/stats/summary", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "by_severity" in data
        assert "by_status" in data
        print(f"✓ Incident stats: {data['total']} total incidents")


class TestSettingsEndpoints:
    """Settings and company endpoints tests"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_get_company(self):
        """Test getting company info"""
        response = requests.get(f"{BASE_URL}/company", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        assert "name" in data or "id" in data
        print(f"✓ Company info retrieved")

    def test_notification_preferences(self):
        """Test notification preferences endpoint"""
        response = requests.get(f"{BASE_URL}/notification-preferences", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        print(f"✓ Notification preferences retrieved")

    def test_subscription(self):
        """Test subscription endpoint"""
        response = requests.get(f"{BASE_URL}/subscription", headers=self.headers)
        assert response.status_code == 200
        data = response.json()
        print(f"✓ Subscription info retrieved")


class TestAPIResponseTimes:
    """API response time tests - all should be < 500ms"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Get auth token for tests"""
        login_response = requests.post(f"{BASE_URL}/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        self.token = login_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {self.token}"}
    
    def test_dashboard_stats_response_time(self):
        """Dashboard stats should respond within 500ms"""
        start = time.time()
        response = requests.get(f"{BASE_URL}/dashboard/stats", headers=self.headers)
        elapsed = (time.time() - start) * 1000
        assert response.status_code == 200
        assert elapsed < 500, f"Dashboard stats took {elapsed:.0f}ms (should be <500ms)"
        print(f"✓ Dashboard stats: {elapsed:.0f}ms")

    def test_vehicles_response_time(self):
        """Vehicles list should respond within 500ms"""
        start = time.time()
        response = requests.get(f"{BASE_URL}/vehicles", headers=self.headers)
        elapsed = (time.time() - start) * 1000
        assert response.status_code == 200
        assert elapsed < 500, f"Vehicles took {elapsed:.0f}ms (should be <500ms)"
        print(f"✓ Vehicles list: {elapsed:.0f}ms")

    def test_drivers_response_time(self):
        """Drivers list should respond within 500ms"""
        start = time.time()
        response = requests.get(f"{BASE_URL}/drivers", headers=self.headers)
        elapsed = (time.time() - start) * 1000
        assert response.status_code == 200
        assert elapsed < 500, f"Drivers took {elapsed:.0f}ms (should be <500ms)"
        print(f"✓ Drivers list: {elapsed:.0f}ms")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
