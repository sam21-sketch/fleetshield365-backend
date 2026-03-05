"""
Test Registration with Role Selector Feature
Tests the new registration flow where users can choose between 'Company Owner' (super_admin) and 'Admin' (admin) roles
"""
import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://fleet-shield-2.preview.emergentagent.com')
if BASE_URL.endswith('/api'):
    BASE_URL = BASE_URL.rstrip('/api')

class TestRegistrationWithRole:
    """Test registration endpoint with role selection"""
    
    def test_register_as_company_owner(self):
        """Test registration with 'super_admin' role (Company Owner)"""
        timestamp = int(time.time())
        payload = {
            "company_name": f"TEST_OwnerCompany_{timestamp}",
            "name": f"Owner User {timestamp}",
            "email": f"roletest_owner_{timestamp}@test.com",
            "password": "test123",
            "vehicle_count": 5,
            "role": "super_admin",
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload)
        print(f"Register as Owner Response: {response.status_code}")
        print(f"Response body: {response.json()}")
        
        # Status code assertion
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        # Data assertions
        data = response.json()
        assert "access_token" in data, "Access token should be in response"
        assert isinstance(data["access_token"], str), "Access token should be a string"
        assert len(data["access_token"]) > 0, "Access token should not be empty"
        
        # Verify user was created with correct role by calling /auth/me
        headers = {"Authorization": f"Bearer {data['access_token']}"}
        me_response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
        assert me_response.status_code == 200, f"Failed to get user info: {me_response.text}"
        
        me_data = me_response.json()
        print(f"User data: {me_data}")
        assert me_data["user"]["role"] == "super_admin", f"Expected role 'super_admin', got '{me_data['user']['role']}'"
        assert me_data["company"] is not None, "Company should be created"
        assert me_data["company"]["name"] == payload["company_name"], "Company name should match"
        
    def test_register_as_admin(self):
        """Test registration with 'admin' role"""
        timestamp = int(time.time())
        payload = {
            "company_name": f"TEST_AdminCompany_{timestamp}",
            "name": f"Admin User {timestamp}",
            "email": f"roletest_admin_{timestamp}@test.com",
            "password": "test123",
            "vehicle_count": 3,
            "role": "admin",
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload)
        print(f"Register as Admin Response: {response.status_code}")
        print(f"Response body: {response.json()}")
        
        # Status code assertion
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        # Data assertions
        data = response.json()
        assert "access_token" in data, "Access token should be in response"
        
        # Verify user was created with correct role
        headers = {"Authorization": f"Bearer {data['access_token']}"}
        me_response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
        assert me_response.status_code == 200, f"Failed to get user info: {me_response.text}"
        
        me_data = me_response.json()
        print(f"User data: {me_data}")
        assert me_data["user"]["role"] == "admin", f"Expected role 'admin', got '{me_data['user']['role']}'"
        assert me_data["company"] is not None, "Company should be created"
        assert me_data["company"]["name"] == payload["company_name"], "Company name should match"
    
    def test_register_default_role(self):
        """Test registration without specifying role (should default to super_admin)"""
        timestamp = int(time.time())
        payload = {
            "company_name": f"TEST_DefaultCompany_{timestamp}",
            "name": f"Default User {timestamp}",
            "email": f"roletest_default_{timestamp}@test.com",
            "password": "test123",
            "vehicle_count": 2,
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload)
        print(f"Register without role Response: {response.status_code}")
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        
        # Verify default role is super_admin
        headers = {"Authorization": f"Bearer {data['access_token']}"}
        me_response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
        me_data = me_response.json()
        
        assert me_data["user"]["role"] == "super_admin", f"Default role should be 'super_admin', got '{me_data['user']['role']}'"
    
    def test_register_duplicate_email(self):
        """Test registration with duplicate email returns error"""
        timestamp = int(time.time())
        email = f"roletest_dup_{timestamp}@test.com"
        
        # First registration
        payload1 = {
            "company_name": f"TEST_DupCompany1_{timestamp}",
            "name": "First User",
            "email": email,
            "password": "test123",
            "vehicle_count": 5,
            "role": "super_admin",
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response1 = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload1)
        assert response1.status_code == 200, f"First registration should succeed: {response1.text}"
        
        # Second registration with same email
        payload2 = {
            "company_name": f"TEST_DupCompany2_{timestamp}",
            "name": "Second User",
            "email": email,
            "password": "test123",
            "vehicle_count": 3,
            "role": "admin",
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response2 = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload2)
        print(f"Duplicate email Response: {response2.status_code}")
        print(f"Response body: {response2.text}")
        
        # Should fail with 400
        assert response2.status_code == 400, f"Expected 400 for duplicate email, got {response2.status_code}"
        assert "already registered" in response2.text.lower() or "email" in response2.text.lower()
    
    def test_register_invalid_role(self):
        """Test registration with invalid role defaults to super_admin"""
        timestamp = int(time.time())
        payload = {
            "company_name": f"TEST_InvalidRoleCompany_{timestamp}",
            "name": f"Invalid Role User {timestamp}",
            "email": f"roletest_invalid_{timestamp}@test.com",
            "password": "test123",
            "vehicle_count": 5,
            "role": "invalid_role",  # Invalid role
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload)
        print(f"Register with invalid role Response: {response.status_code}")
        
        # Should still succeed and default to super_admin
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        
        # Verify it defaulted to super_admin
        headers = {"Authorization": f"Bearer {data['access_token']}"}
        me_response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
        me_data = me_response.json()
        
        assert me_data["user"]["role"] == "super_admin", f"Invalid role should default to 'super_admin', got '{me_data['user']['role']}'"


class TestAuthMe:
    """Test /auth/me endpoint returns correct user and company data"""
    
    def test_auth_me_returns_role(self):
        """Test that /auth/me returns the user's role"""
        timestamp = int(time.time())
        
        # Register a new user
        payload = {
            "company_name": f"TEST_MeCompany_{timestamp}",
            "name": f"Me Test User {timestamp}",
            "email": f"roletest_me_{timestamp}@test.com",
            "password": "test123",
            "vehicle_count": 5,
            "role": "admin",
            "origin_url": "https://fleet-shield-2.preview.emergentagent.com"
        }
        
        response = requests.post(f"{BASE_URL}/api/auth/register-company", json=payload)
        assert response.status_code == 200
        
        data = response.json()
        headers = {"Authorization": f"Bearer {data['access_token']}"}
        
        # Call /auth/me
        me_response = requests.get(f"{BASE_URL}/api/auth/me", headers=headers)
        assert me_response.status_code == 200
        
        me_data = me_response.json()
        
        # Verify response structure
        assert "user" in me_data, "Response should contain 'user'"
        assert "company" in me_data, "Response should contain 'company'"
        assert "role" in me_data["user"], "User should have 'role' field"
        assert "email" in me_data["user"], "User should have 'email' field"
        assert "name" in me_data["user"], "User should have 'name' field"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
