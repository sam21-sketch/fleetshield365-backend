"""
Test suite for iOS crash fix verification
Tests that all 4 form endpoints work correctly after removing hybridSubmit/offlineSubmit
"""
import pytest
import requests
import os

BASE_URL = "https://fleetshield365-backend-production.up.railway.app"

class TestIOSCrashFix:
    """Tests for iOS crash fix - verifying API endpoints work after removing offline queue"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test with authentication"""
        login_response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": "samneel27@gmail.com", "password": "test123"}
        )
        assert login_response.status_code == 200, f"Login failed: {login_response.text}"
        self.token = login_response.json()["access_token"]
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        # Get a vehicle ID for testing
        vehicles_response = requests.get(f"{BASE_URL}/api/vehicles", headers=self.headers)
        if vehicles_response.status_code == 200 and vehicles_response.json():
            self.vehicle_id = vehicles_response.json()[0]["id"]
        else:
            self.vehicle_id = None
    
    # Health check
    def test_health_endpoint(self):
        """Test health endpoint is accessible"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        print("✓ Health endpoint working")
    
    # Test /api/inspections/prestart endpoint
    def test_prestart_endpoint_exists(self):
        """Test POST /api/inspections/prestart endpoint exists and validates input"""
        response = requests.post(
            f"{BASE_URL}/api/inspections/prestart",
            headers=self.headers,
            json={}
        )
        # Should return 422 for missing required fields (not 404)
        assert response.status_code == 422, f"Expected 422, got {response.status_code}"
        data = response.json()
        assert "detail" in data
        # Verify it's asking for required fields
        required_fields = ["vehicle_id", "odometer", "checklist_items", "photos"]
        missing_fields = [d["loc"][-1] for d in data["detail"] if d["type"] == "missing"]
        for field in required_fields:
            assert field in missing_fields, f"Expected {field} to be required"
        print("✓ POST /api/inspections/prestart endpoint exists and validates correctly")
    
    def test_prestart_requires_auth(self):
        """Test prestart endpoint requires authentication"""
        response = requests.post(
            f"{BASE_URL}/api/inspections/prestart",
            json={}
        )
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        print("✓ POST /api/inspections/prestart requires authentication")
    
    # Test /api/inspections/end-shift endpoint
    def test_endshift_endpoint_exists(self):
        """Test POST /api/inspections/end-shift endpoint exists and validates input"""
        response = requests.post(
            f"{BASE_URL}/api/inspections/end-shift",
            headers=self.headers,
            json={}
        )
        # Should return 422 for missing required fields (not 404)
        assert response.status_code == 422, f"Expected 422, got {response.status_code}"
        data = response.json()
        assert "detail" in data
        # Verify it's asking for required fields
        required_fields = ["vehicle_id", "odometer", "fuel_level", "cleanliness"]
        missing_fields = [d["loc"][-1] for d in data["detail"] if d["type"] == "missing"]
        for field in required_fields:
            assert field in missing_fields, f"Expected {field} to be required"
        print("✓ POST /api/inspections/end-shift endpoint exists and validates correctly")
    
    def test_endshift_requires_auth(self):
        """Test end-shift endpoint requires authentication"""
        response = requests.post(
            f"{BASE_URL}/api/inspections/end-shift",
            json={}
        )
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        print("✓ POST /api/inspections/end-shift requires authentication")
    
    # Test /api/fuel endpoint
    def test_fuel_endpoint_exists(self):
        """Test POST /api/fuel endpoint exists and validates input"""
        response = requests.post(
            f"{BASE_URL}/api/fuel",
            headers=self.headers,
            json={}
        )
        # Should return 422 for missing required fields (not 404)
        assert response.status_code == 422, f"Expected 422, got {response.status_code}"
        data = response.json()
        assert "detail" in data
        # Verify it's asking for required fields
        required_fields = ["vehicle_id", "amount", "liters"]
        missing_fields = [d["loc"][-1] for d in data["detail"] if d["type"] == "missing"]
        for field in required_fields:
            assert field in missing_fields, f"Expected {field} to be required"
        print("✓ POST /api/fuel endpoint exists and validates correctly")
    
    def test_fuel_requires_auth(self):
        """Test fuel endpoint requires authentication"""
        response = requests.post(
            f"{BASE_URL}/api/fuel",
            json={}
        )
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        print("✓ POST /api/fuel requires authentication")
    
    # Test /api/incidents endpoint
    def test_incidents_endpoint_exists(self):
        """Test POST /api/incidents endpoint exists and validates input"""
        response = requests.post(
            f"{BASE_URL}/api/incidents",
            headers=self.headers,
            json={}
        )
        # Should return 422 for missing required fields (not 404)
        assert response.status_code == 422, f"Expected 422, got {response.status_code}"
        data = response.json()
        assert "detail" in data
        # Verify it's asking for required fields
        required_fields = ["vehicle_id", "description", "other_party"]
        missing_fields = [d["loc"][-1] for d in data["detail"] if d["type"] == "missing"]
        for field in required_fields:
            assert field in missing_fields, f"Expected {field} to be required"
        print("✓ POST /api/incidents endpoint exists and validates correctly")
    
    def test_incidents_requires_auth(self):
        """Test incidents endpoint requires authentication"""
        response = requests.post(
            f"{BASE_URL}/api/incidents",
            json={}
        )
        assert response.status_code == 401, f"Expected 401, got {response.status_code}"
        print("✓ POST /api/incidents requires authentication")
    
    # Test GET endpoints also work
    def test_get_inspections(self):
        """Test GET /api/inspections works"""
        response = requests.get(f"{BASE_URL}/api/inspections", headers=self.headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert isinstance(response.json(), list)
        print("✓ GET /api/inspections works")
    
    def test_get_fuel(self):
        """Test GET /api/fuel works"""
        response = requests.get(f"{BASE_URL}/api/fuel", headers=self.headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert isinstance(response.json(), list)
        print("✓ GET /api/fuel works")
    
    def test_get_incidents(self):
        """Test GET /api/incidents works"""
        response = requests.get(f"{BASE_URL}/api/incidents", headers=self.headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert isinstance(response.json(), list)
        print("✓ GET /api/incidents works")
    
    def test_get_vehicles(self):
        """Test GET /api/vehicles works (needed for form submissions)"""
        response = requests.get(f"{BASE_URL}/api/vehicles", headers=self.headers)
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert isinstance(response.json(), list)
        print("✓ GET /api/vehicles works")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
