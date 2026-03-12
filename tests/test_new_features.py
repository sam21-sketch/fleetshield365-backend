"""
Test file for FleetGuard new features:
1. Logo upload API endpoint (/api/company/logo)
2. Driver search in Assign modal (verifying drivers API works)
3. Reports page (verifying inspections API works)
"""
import pytest
import requests
import os
import base64

BASE_URL = os.environ.get('EXPO_PUBLIC_BACKEND_URL', 'https://system-monitor-33.preview.emergentagent.com')

# Test credentials
ADMIN_EMAIL = "admin@test.com"
ADMIN_PASSWORD = "test123"


@pytest.fixture(scope="module")
def auth_token():
    """Get authentication token for admin user"""
    response = requests.post(
        f"{BASE_URL}/api/auth/login",
        json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    if response.status_code == 200:
        return response.json().get("access_token")
    pytest.fail(f"Authentication failed: {response.status_code} - {response.text}")


@pytest.fixture(scope="module")
def auth_headers(auth_token):
    """Create auth headers for authenticated requests"""
    return {"Authorization": f"Bearer {auth_token}"}


class TestLogoUploadAPI:
    """Test /api/company/logo endpoint"""

    def test_logo_upload_without_auth_returns_error(self):
        """Logo upload should require authentication"""
        response = requests.post(f"{BASE_URL}/api/company/logo")
        assert response.status_code in [401, 403, 422], f"Expected 401/403/422, got {response.status_code}"
        print(f"✓ Logo upload without auth returns {response.status_code}")

    def test_logo_upload_with_valid_image(self, auth_headers):
        """Logo upload with valid image should return success"""
        # Create a small 1x1 pixel PNG image
        png_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        image_bytes = base64.b64decode(png_base64)
        
        files = {
            'logo': ('test_logo.png', image_bytes, 'image/png')
        }
        
        response = requests.post(
            f"{BASE_URL}/api/company/logo",
            files=files,
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "message" in data, "Response should contain 'message'"
        assert "logo_url" in data, "Response should contain 'logo_url'"
        assert data["message"] == "Logo uploaded successfully", f"Unexpected message: {data['message']}"
        print(f"✓ Logo upload successful: {data['message']}")

    def test_logo_upload_with_invalid_file_type(self, auth_headers):
        """Logo upload with non-image file should return error"""
        files = {
            'logo': ('test.txt', b'This is not an image', 'text/plain')
        }
        
        response = requests.post(
            f"{BASE_URL}/api/company/logo",
            files=files,
            headers=auth_headers
        )
        
        assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
        print(f"✓ Logo upload with invalid file type returns 400")

    def test_company_api_returns_logo(self, auth_headers):
        """Verify company API returns the logo after upload"""
        response = requests.get(
            f"{BASE_URL}/api/company",
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        # Check if logo_base64 or logo_url exists after upload
        has_logo = data.get("logo_base64") or data.get("logo_url")
        assert has_logo, f"Company should have logo after upload. Data: {data}"
        print(f"✓ Company API returns logo data")


class TestDriversAPI:
    """Test /api/drivers endpoint for driver search feature"""

    def test_get_all_drivers(self, auth_headers):
        """GET /api/drivers should return list of drivers"""
        response = requests.get(
            f"{BASE_URL}/api/drivers",
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert isinstance(data, list), "Response should be a list"
        print(f"✓ GET /api/drivers returned {len(data)} drivers")
        
        # Verify driver structure for search functionality
        if len(data) > 0:
            driver = data[0]
            assert "id" in driver, "Driver should have 'id'"
            assert "name" in driver, "Driver should have 'name'"
            assert "email" in driver, "Driver should have 'email'"
            print(f"✓ Driver has required fields: id, name, email")


class TestVehiclesAPI:
    """Test /api/vehicles endpoint for Assign modal"""

    def test_get_all_vehicles(self, auth_headers):
        """GET /api/vehicles should return list of vehicles"""
        response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert isinstance(data, list), "Response should be a list"
        print(f"✓ GET /api/vehicles returned {len(data)} vehicles")
        
        # Get first vehicle for testing assign
        if len(data) > 0:
            return data[0]
        return None

    def test_vehicle_assign_endpoint_exists(self, auth_headers):
        """Test that vehicle assign endpoint works"""
        # First get vehicles
        vehicles_response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers=auth_headers
        )
        vehicles = vehicles_response.json()
        
        if len(vehicles) == 0:
            pytest.skip("No vehicles available to test assign")
        
        vehicle = vehicles[0]
        vehicle_id = vehicle["id"]
        
        # Get current assigned drivers
        current_drivers = vehicle.get("assigned_driver_ids", [])
        
        # Test assign endpoint with empty list
        response = requests.post(
            f"{BASE_URL}/api/vehicles/{vehicle_id}/assign",
            json={"driver_ids": current_drivers},  # Keep same drivers
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print(f"✓ Vehicle assign endpoint works for vehicle {vehicle_id}")


class TestInspectionsAPI:
    """Test /api/inspections endpoint for Reports page"""

    def test_get_all_inspections(self, auth_headers):
        """GET /api/inspections should return list of inspections"""
        response = requests.get(
            f"{BASE_URL}/api/inspections",
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert isinstance(data, list), "Response should be a list"
        print(f"✓ GET /api/inspections returned {len(data)} inspections")
        
        # Verify inspection structure for Reports page
        if len(data) > 0:
            inspection = data[0]
            required_fields = ["id", "vehicle_id", "driver_id", "type", "timestamp", "odometer"]
            for field in required_fields:
                assert field in inspection, f"Inspection should have '{field}'"
            print(f"✓ Inspection has all required fields for Reports page")

    def test_get_inspections_with_type_filter(self, auth_headers):
        """GET /api/inspections with type filter should work"""
        response = requests.get(
            f"{BASE_URL}/api/inspections",
            params={"inspection_type": "prestart"},
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        # All returned inspections should be prestart type
        for inspection in data:
            assert inspection["type"] == "prestart", f"Expected prestart, got {inspection['type']}"
        print(f"✓ Inspections filter by type works ({len(data)} prestart inspections)")

    def test_get_inspections_with_issues_filter(self, auth_headers):
        """GET /api/inspections with has_issues filter should work"""
        response = requests.get(
            f"{BASE_URL}/api/inspections",
            params={"has_issues": True},
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print(f"✓ Inspections filter by has_issues works")


class TestReportPDFAPI:
    """Test /api/inspections/{id}/pdf endpoint"""

    def test_get_inspection_pdf(self, auth_headers):
        """GET /api/inspections/{id}/pdf should return PDF data"""
        # First get an inspection
        inspections_response = requests.get(
            f"{BASE_URL}/api/inspections",
            headers=auth_headers
        )
        
        if inspections_response.status_code != 200:
            pytest.skip("Could not fetch inspections")
        
        inspections = inspections_response.json()
        if len(inspections) == 0:
            pytest.skip("No inspections available to test PDF")
        
        inspection_id = inspections[0]["id"]
        
        # Get PDF
        response = requests.get(
            f"{BASE_URL}/api/inspections/{inspection_id}/pdf",
            headers=auth_headers
        )
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "pdf_base64" in data, "Response should contain 'pdf_base64'"
        # PDF base64 should be a valid string
        assert isinstance(data["pdf_base64"], str), "pdf_base64 should be a string"
        assert len(data["pdf_base64"]) > 100, "pdf_base64 should have content"
        print(f"✓ PDF generation works for inspection {inspection_id}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
