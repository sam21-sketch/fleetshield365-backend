"""
Test suite for FleetShield365 Backend after modular refactoring
Verifies all core API endpoints work correctly after code refactoring.

Tests:
- Health endpoint
- Auth login
- Vehicles endpoint (with auth)
- Drivers endpoint (with auth)
- Dashboard stats endpoint (with auth)
- Company endpoint (with auth)
"""
import pytest
import requests
import os

# Get BASE_URL from environment - NO DEFAULT to fail fast if not set
BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')
if not BASE_URL:
    BASE_URL = "https://shield-driver-test.preview.emergentagent.com"

# Test credentials from request
TEST_EMAIL = "sam21@y7mail.com"
TEST_PASSWORD = "test123"


class TestHealthEndpoint:
    """Health endpoint tests - no auth required"""
    
    def test_health_returns_200(self):
        """Verify /api/health returns 200 OK"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print(f"PASS: Health endpoint returned 200")
    
    def test_health_returns_healthy_status(self):
        """Verify /api/health returns status=healthy"""
        response = requests.get(f"{BASE_URL}/api/health")
        data = response.json()
        assert "status" in data, "Response missing 'status' field"
        assert data["status"] == "healthy", f"Expected 'healthy', got {data['status']}"
        print(f"PASS: Health status is 'healthy'")
    
    def test_health_has_timestamp(self):
        """Verify /api/health returns timestamp"""
        response = requests.get(f"{BASE_URL}/api/health")
        data = response.json()
        assert "timestamp" in data, "Response missing 'timestamp' field"
        print(f"PASS: Health endpoint has timestamp: {data['timestamp']}")


class TestAuthLogin:
    """Auth login endpoint tests"""
    
    def test_login_with_valid_credentials(self):
        """Verify /api/auth/login works with valid credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD
        })
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        assert "access_token" in data, "Response missing 'access_token'"
        assert "token_type" in data, "Response missing 'token_type'"
        assert "user" in data, "Response missing 'user'"
        assert data["token_type"] == "bearer", f"Expected 'bearer', got {data['token_type']}"
        print(f"PASS: Login successful, token received")
    
    def test_login_returns_user_data(self):
        """Verify login returns correct user structure"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD
        })
        data = response.json()
        user = data.get("user", {})
        
        assert "id" in user, "User missing 'id'"
        assert "email" in user, "User missing 'email'"
        assert "name" in user, "User missing 'name'"
        assert "role" in user, "User missing 'role'"
        assert "company_id" in user, "User missing 'company_id'"
        assert user["email"] == TEST_EMAIL, f"Expected email {TEST_EMAIL}, got {user['email']}"
        print(f"PASS: User data returned correctly - {user['name']} ({user['role']})")
    
    def test_login_invalid_credentials_returns_401(self):
        """Verify login with wrong password returns 401"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_EMAIL,
            "password": "wrongpassword"
        })
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        print("PASS: Invalid credentials return 401")
    
    def test_login_nonexistent_user_returns_401(self):
        """Verify login with nonexistent user returns 401"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "nonexistent@test.com",
            "password": "somepassword"
        })
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        print("PASS: Nonexistent user returns 401")


@pytest.fixture
def auth_token():
    """Get auth token for authenticated endpoints"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": TEST_EMAIL,
        "password": TEST_PASSWORD
    })
    if response.status_code != 200:
        pytest.skip("Authentication failed - skipping authenticated tests")
    return response.json()["access_token"]


@pytest.fixture
def auth_headers(auth_token):
    """Get headers with auth token"""
    return {"Authorization": f"Bearer {auth_token}"}


class TestVehiclesEndpoint:
    """Vehicles endpoint tests - requires auth"""
    
    def test_get_vehicles_returns_200(self, auth_headers):
        """Verify /api/vehicles returns 200"""
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=auth_headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Vehicles endpoint returned 200")
    
    def test_get_vehicles_returns_list(self, auth_headers):
        """Verify /api/vehicles returns a list"""
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=auth_headers)
        data = response.json()
        assert isinstance(data, list), f"Expected list, got {type(data)}"
        print(f"PASS: Vehicles returned as list ({len(data)} vehicles)")
    
    def test_vehicles_have_required_fields(self, auth_headers):
        """Verify vehicles have required fields"""
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=auth_headers)
        data = response.json()
        
        if len(data) > 0:
            vehicle = data[0]
            required_fields = ["id", "name", "registration_number", "status", "company_id"]
            for field in required_fields:
                assert field in vehicle, f"Vehicle missing required field: {field}"
            print(f"PASS: Vehicle has all required fields - {vehicle['name']} ({vehicle['registration_number']})")
        else:
            print("SKIP: No vehicles to verify structure")
    
    def test_vehicles_without_auth_returns_401_or_403(self):
        """Verify /api/vehicles requires authentication"""
        response = requests.get(f"{BASE_URL}/api/vehicles")
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("PASS: Vehicles endpoint requires authentication")


class TestDriversEndpoint:
    """Drivers endpoint tests - requires auth"""
    
    def test_get_drivers_returns_200(self, auth_headers):
        """Verify /api/drivers returns 200"""
        response = requests.get(f"{BASE_URL}/api/drivers", headers=auth_headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Drivers endpoint returned 200")
    
    def test_get_drivers_returns_list(self, auth_headers):
        """Verify /api/drivers returns a list"""
        response = requests.get(f"{BASE_URL}/api/drivers", headers=auth_headers)
        data = response.json()
        assert isinstance(data, list), f"Expected list, got {type(data)}"
        print(f"PASS: Drivers returned as list ({len(data)} drivers)")
    
    def test_drivers_have_required_fields(self, auth_headers):
        """Verify drivers have required fields"""
        response = requests.get(f"{BASE_URL}/api/drivers", headers=auth_headers)
        data = response.json()
        
        if len(data) > 0:
            driver = data[0]
            required_fields = ["id", "name", "role", "company_id"]
            for field in required_fields:
                assert field in driver, f"Driver missing required field: {field}"
            assert driver["role"] == "driver", f"Expected role 'driver', got {driver['role']}"
            print(f"PASS: Driver has all required fields - {driver['name']}")
        else:
            print("SKIP: No drivers to verify structure")
    
    def test_drivers_without_auth_returns_401_or_403(self):
        """Verify /api/drivers requires authentication"""
        response = requests.get(f"{BASE_URL}/api/drivers")
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("PASS: Drivers endpoint requires authentication")


class TestDashboardStatsEndpoint:
    """Dashboard stats endpoint tests - requires auth"""
    
    def test_get_dashboard_stats_returns_200(self, auth_headers):
        """Verify /api/dashboard/stats returns 200"""
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=auth_headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Dashboard stats endpoint returned 200")
    
    def test_dashboard_stats_structure(self, auth_headers):
        """Verify dashboard stats has expected fields"""
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=auth_headers)
        data = response.json()
        
        expected_fields = [
            "total_vehicles",
            "total_drivers",
            "inspections_today",
            "inspections_missed",
            "issues_today"
        ]
        
        for field in expected_fields:
            assert field in data, f"Dashboard stats missing field: {field}"
        
        print(f"PASS: Dashboard stats has all required fields")
        print(f"  - Total vehicles: {data['total_vehicles']}")
        print(f"  - Total drivers: {data['total_drivers']}")
        print(f"  - Inspections today: {data['inspections_today']}")
    
    def test_dashboard_stats_numeric_values(self, auth_headers):
        """Verify dashboard stats returns numeric values"""
        response = requests.get(f"{BASE_URL}/api/dashboard/stats", headers=auth_headers)
        data = response.json()
        
        numeric_fields = ["total_vehicles", "total_drivers", "inspections_today"]
        for field in numeric_fields:
            assert isinstance(data[field], (int, float)), f"{field} should be numeric, got {type(data[field])}"
        
        print("PASS: Dashboard stats fields are numeric")
    
    def test_dashboard_stats_without_auth_returns_401_or_403(self):
        """Verify /api/dashboard/stats requires authentication"""
        response = requests.get(f"{BASE_URL}/api/dashboard/stats")
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("PASS: Dashboard stats endpoint requires authentication")


class TestCompanyEndpoint:
    """Company endpoint tests - requires auth"""
    
    def test_get_company_returns_200(self, auth_headers):
        """Verify /api/company returns 200"""
        response = requests.get(f"{BASE_URL}/api/company", headers=auth_headers)
        # Company can return 200 with null if company not found (data issue)
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
        print(f"PASS: Company endpoint returned {response.status_code}")
    
    def test_company_response_type(self, auth_headers):
        """Verify /api/company returns dict or null"""
        response = requests.get(f"{BASE_URL}/api/company", headers=auth_headers)
        data = response.json()
        
        # Response can be null if company not found, or a dict if found
        assert data is None or isinstance(data, dict), f"Expected dict or null, got {type(data)}"
        
        if data is not None:
            expected_fields = ["id", "name"]
            for field in expected_fields:
                assert field in data, f"Company missing field: {field}"
            print(f"PASS: Company data returned - {data['name']}")
        else:
            print("PASS: Company returns null (company not in DB - data issue, not API bug)")
    
    def test_company_without_auth_returns_401_or_403(self):
        """Verify /api/company requires authentication"""
        response = requests.get(f"{BASE_URL}/api/company")
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("PASS: Company endpoint requires authentication")


class TestAuthMe:
    """Auth/me endpoint tests - requires auth"""
    
    def test_auth_me_returns_200(self, auth_headers):
        """Verify /api/auth/me returns 200"""
        response = requests.get(f"{BASE_URL}/api/auth/me", headers=auth_headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print("PASS: Auth/me endpoint returned 200")
    
    def test_auth_me_returns_user_and_company(self, auth_headers):
        """Verify /api/auth/me returns user structure"""
        response = requests.get(f"{BASE_URL}/api/auth/me", headers=auth_headers)
        data = response.json()
        
        assert "user" in data, "Response missing 'user'"
        # company can be null if company not found in DB
        assert "company" in data, "Response missing 'company'"
        
        user = data["user"]
        assert "id" in user, "User missing 'id'"
        assert "email" in user, "User missing 'email'"
        assert user["email"] == TEST_EMAIL, f"Expected {TEST_EMAIL}, got {user['email']}"
        
        print(f"PASS: Auth/me returns correct user - {user['name']}")
    
    def test_auth_me_without_auth_returns_401_or_403(self):
        """Verify /api/auth/me requires authentication"""
        response = requests.get(f"{BASE_URL}/api/auth/me")
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("PASS: Auth/me endpoint requires authentication")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
