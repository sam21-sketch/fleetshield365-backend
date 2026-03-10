"""
Test suite for FleetShield365 License Photo Feature and Registration Role Selector
- Tests registration with Company Owner (super_admin) and Admin roles
- Tests license photo upload, view with password, and delete (Owner only)
- Tests access restrictions for Admin users
"""

import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://maintenance-hub-285.preview.emergentagent.com')
if BASE_URL.endswith('/'):
    BASE_URL = BASE_URL.rstrip('/')

TEST_PASSWORD = "test123"

# Sample base64 image (1x1 red pixel PNG)
SAMPLE_IMAGE_BASE64 = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg=="


def register_user(role=None):
    """Helper to register a new user and get token + user info"""
    email = f"TEST_{role or 'default'}_{int(time.time() * 1000)}@test.com"
    payload = {
        "company_name": f"Test Company {role}",
        "name": f"Test User {role}",
        "email": email,
        "password": TEST_PASSWORD,
        "vehicle_count": 5
    }
    if role:
        payload["role"] = role
    
    response = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload)
    assert response.status_code == 200, f"Registration failed: {response.text}"
    data = response.json()
    
    # Get user details via /auth/me
    headers = {"Authorization": f"Bearer {data['access_token']}"}
    me_response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
    assert me_response.status_code == 200, f"Get user failed: {me_response.text}"
    user_data = me_response.json()
    
    return {
        "token": data["access_token"],
        "user": user_data["user"],
        "company": user_data["company"],
        "email": email,
        "headers": headers
    }


class TestRegistrationRoleSelector:
    """Test registration with Company Owner and Admin role selection"""
    
    def test_register_as_company_owner(self):
        """Register as Company Owner should create user with super_admin role"""
        result = register_user(role="super_admin")
        
        # Verify user role is super_admin
        assert result["user"]["role"] == "super_admin", f"Expected super_admin role, got {result['user']['role']}"
        
        # Verify company was created
        assert result["company"] is not None, "No company created"
    
    def test_register_as_admin(self):
        """Register as Admin should create user with admin role"""
        result = register_user(role="admin")
        
        # Verify user role is admin
        assert result["user"]["role"] == "admin", f"Expected admin role, got {result['user']['role']}"
        
        # Verify company was created
        assert result["company"] is not None, "No company created"
    
    def test_default_role_is_super_admin(self):
        """If no role provided, should default to super_admin"""
        result = register_user(role=None)
        
        # Should default to super_admin
        assert result["user"]["role"] == "super_admin", f"Default role should be super_admin, got {result['user']['role']}"
    
    def test_duplicate_email_rejected(self):
        """Registering with duplicate email should fail"""
        email = f"TEST_dup_{int(time.time() * 1000)}@test.com"
        
        # First registration
        response1 = requests.post(f"{BASE_URL}/api/auth/register-company", json={
            "company_name": "First Company",
            "name": "First User",
            "email": email,
            "password": TEST_PASSWORD,
            "vehicle_count": 5
        })
        assert response1.status_code == 200
        
        # Second registration with same email
        response2 = requests.post(f"{BASE_URL}/api/auth/register-company", json={
            "company_name": "Second Company",
            "name": "Second User",
            "email": email,
            "password": TEST_PASSWORD,
            "vehicle_count": 5
        })
        assert response2.status_code == 400, "Should reject duplicate email"
        assert "already registered" in response2.json()["detail"].lower()


class TestLicensePhotoFeature:
    """Test license photo upload, view, and delete functionality"""
    
    @pytest.fixture
    def owner_session(self):
        """Create a Company Owner session"""
        return register_user(role="super_admin")
    
    @pytest.fixture
    def admin_session(self):
        """Create an Admin session"""
        return register_user(role="admin")
    
    @pytest.fixture
    def owner_with_operator(self, owner_session):
        """Create Owner session with an operator (driver)"""
        # Create an operator
        driver_email = f"TEST_operator_{int(time.time() * 1000)}@test.com"
        response = requests.post(
            f"{BASE_URL}/api/drivers",
            json={
                "name": "Test Operator",
                "email": driver_email,
                "password": TEST_PASSWORD,
                "phone": "0400123456",
                "license_number": "ABC123",
                "license_class": "C"
            },
            headers=owner_session["headers"]
        )
        assert response.status_code == 200, f"Driver creation failed: {response.text}"
        driver = response.json()
        return {
            **owner_session,
            "driver_id": driver["id"],
            "driver": driver
        }
    
    def test_owner_can_upload_license_photos(self, owner_with_operator):
        """Company Owner can upload license photos for operators"""
        driver_id = owner_with_operator["driver_id"]
        headers = owner_with_operator["headers"]
        
        response = requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            json={
                "front_photo_base64": SAMPLE_IMAGE_BASE64,
                "back_photo_base64": SAMPLE_IMAGE_BASE64
            },
            headers=headers
        )
        
        assert response.status_code == 200, f"Upload failed: {response.text}"
        data = response.json()
        assert "message" in data
        assert "license_photo_front" in data.get("updated_fields", [])
        assert "license_photo_back" in data.get("updated_fields", [])
    
    def test_owner_can_check_has_photos(self, owner_with_operator):
        """Company Owner can check if photos exist"""
        driver_id = owner_with_operator["driver_id"]
        headers = owner_with_operator["headers"]
        
        # First upload photos
        requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            json={"front_photo_base64": SAMPLE_IMAGE_BASE64},
            headers=headers
        )
        
        # Check has photos
        response = requests.get(
            f"{BASE_URL}/api/drivers/{driver_id}/has-license-photos",
            headers=headers
        )
        
        assert response.status_code == 200, f"Check failed: {response.text}"
        data = response.json()
        assert data["has_front_photo"] == True
    
    def test_owner_can_view_photos_with_correct_password(self, owner_with_operator):
        """Company Owner can view photos after password verification"""
        driver_id = owner_with_operator["driver_id"]
        headers = owner_with_operator["headers"]
        
        # First upload photos
        requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            json={"front_photo_base64": SAMPLE_IMAGE_BASE64},
            headers=headers
        )
        
        # View with correct password
        response = requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos/view",
            json={"password": TEST_PASSWORD},
            headers=headers
        )
        
        assert response.status_code == 200, f"View failed: {response.text}"
        data = response.json()
        assert "front_photo" in data
        assert data["front_photo"] is not None
    
    def test_view_photos_wrong_password_rejected(self, owner_with_operator):
        """Viewing photos with wrong password should fail"""
        driver_id = owner_with_operator["driver_id"]
        headers = owner_with_operator["headers"]
        
        # Upload photos first
        requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            json={"front_photo_base64": SAMPLE_IMAGE_BASE64},
            headers=headers
        )
        
        # Try to view with wrong password
        response = requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos/view",
            json={"password": "wrongpassword"},
            headers=headers
        )
        
        assert response.status_code == 401, f"Should reject wrong password, got {response.status_code}"
        assert "invalid password" in response.json()["detail"].lower()
    
    def test_owner_can_delete_photos(self, owner_with_operator):
        """Company Owner can delete license photos"""
        driver_id = owner_with_operator["driver_id"]
        headers = owner_with_operator["headers"]
        
        # First upload photos
        requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            json={"front_photo_base64": SAMPLE_IMAGE_BASE64},
            headers=headers
        )
        
        # Delete photos
        response = requests.delete(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            headers=headers
        )
        
        assert response.status_code == 200, f"Delete failed: {response.text}"
        
        # Verify photos are gone
        check_response = requests.get(
            f"{BASE_URL}/api/drivers/{driver_id}/has-license-photos",
            headers=headers
        )
        assert check_response.status_code == 200
        assert check_response.json()["has_front_photo"] == False
    
    def test_admin_cannot_upload_photos(self, admin_session):
        """Admin users cannot upload license photos"""
        headers = admin_session["headers"]
        
        # First create a driver as admin
        driver_email = f"TEST_admin_driver_{int(time.time() * 1000)}@test.com"
        driver_resp = requests.post(
            f"{BASE_URL}/api/drivers",
            json={
                "name": "Admin Created Driver",
                "email": driver_email,
                "password": TEST_PASSWORD,
            },
            headers=headers
        )
        assert driver_resp.status_code == 200
        driver_id = driver_resp.json()["id"]
        
        # Try to upload photos as admin
        response = requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            json={"front_photo_base64": SAMPLE_IMAGE_BASE64},
            headers=headers
        )
        
        assert response.status_code == 403, f"Admin should not be able to upload, got {response.status_code}"
        assert "only company owners" in response.json()["detail"].lower()
    
    def test_admin_cannot_view_photos(self, admin_session):
        """Admin users cannot view license photos endpoint"""
        headers = admin_session["headers"]
        
        # Create a driver as admin
        driver_email = f"TEST_admin_view_{int(time.time() * 1000)}@test.com"
        driver_resp = requests.post(
            f"{BASE_URL}/api/drivers",
            json={
                "name": "Test Driver",
                "email": driver_email,
                "password": TEST_PASSWORD,
            },
            headers=headers
        )
        assert driver_resp.status_code == 200
        driver_id = driver_resp.json()["id"]
        
        # Try to view photos as admin
        response = requests.post(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos/view",
            json={"password": TEST_PASSWORD},
            headers=headers
        )
        
        assert response.status_code == 403, f"Admin should not be able to view, got {response.status_code}"
    
    def test_admin_cannot_check_has_photos(self, admin_session):
        """Admin users cannot check if photos exist"""
        headers = admin_session["headers"]
        
        # Create a driver as admin
        driver_email = f"TEST_admin_check_{int(time.time() * 1000)}@test.com"
        driver_resp = requests.post(
            f"{BASE_URL}/api/drivers",
            json={
                "name": "Test Driver",
                "email": driver_email,
                "password": TEST_PASSWORD,
            },
            headers=headers
        )
        assert driver_resp.status_code == 200
        driver_id = driver_resp.json()["id"]
        
        # Try to check photos as admin
        response = requests.get(
            f"{BASE_URL}/api/drivers/{driver_id}/has-license-photos",
            headers=headers
        )
        
        assert response.status_code == 403, f"Admin should not be able to check photos, got {response.status_code}"
    
    def test_admin_cannot_delete_photos(self, admin_session):
        """Admin users cannot delete license photos"""
        headers = admin_session["headers"]
        
        # Create a driver as admin
        driver_email = f"TEST_admin_delete_{int(time.time() * 1000)}@test.com"
        driver_resp = requests.post(
            f"{BASE_URL}/api/drivers",
            json={
                "name": "Test Driver",
                "email": driver_email,
                "password": TEST_PASSWORD,
            },
            headers=headers
        )
        assert driver_resp.status_code == 200
        driver_id = driver_resp.json()["id"]
        
        # Try to delete photos as admin
        response = requests.delete(
            f"{BASE_URL}/api/drivers/{driver_id}/license-photos",
            headers=headers
        )
        
        assert response.status_code == 403, f"Admin should not be able to delete, got {response.status_code}"


class TestHealthCheck:
    """Basic health check tests"""
    
    def test_api_is_accessible(self):
        """API root endpoint should be accessible"""
        response = requests.get(f"{BASE_URL}/")
        assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
