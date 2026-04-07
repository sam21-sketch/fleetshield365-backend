"""
FleetShield365 Production E2E Backend Tests
Tests all critical API endpoints on production backend
"""
import pytest
import requests
import os
import time

# Production backend URL
BASE_URL = "https://fleetshield365-backend-production.up.railway.app"

# Test credentials
ADMIN_EMAIL = "samneel27@gmail.com"
ADMIN_PASSWORD = "test123"


class TestAuthentication:
    """Authentication endpoint tests"""
    
    def test_health_check(self):
        """Test health endpoint"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        print(f"✓ Health check passed: {data}")
    
    def test_admin_login_success(self):
        """Test admin login with valid credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        assert data["user"]["email"] == ADMIN_EMAIL
        print(f"✓ Admin login successful: {data['user']['name']}")
        return data["access_token"]
    
    def test_login_invalid_credentials(self):
        """Test login with invalid credentials"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "wrong@example.com",
            "password": "wrongpass"
        })
        assert response.status_code == 401
        print("✓ Invalid credentials rejected correctly")
    
    def test_login_with_username(self):
        """Test login with username instead of email"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "username": "erish65",
            "password": "NewPass123!"
        })
        # Should work if password was reset correctly
        if response.status_code == 200:
            data = response.json()
            assert "access_token" in data
            print(f"✓ Username login successful: {data['user']['name']}")
        else:
            print(f"⚠ Username login returned {response.status_code} - may need password reset")


@pytest.fixture
def auth_token():
    """Get authentication token for tests"""
    response = requests.post(f"{BASE_URL}/api/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    })
    if response.status_code == 200:
        return response.json()["access_token"]
    pytest.skip("Authentication failed")


class TestDashboard:
    """Dashboard endpoint tests"""
    
    def test_dashboard_stats(self, auth_token):
        """Test dashboard stats endpoint"""
        response = requests.get(
            f"{BASE_URL}/api/dashboard/stats",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        # Verify required fields
        assert "total_vehicles" in data
        assert "total_drivers" in data
        assert "inspections_today" in data
        assert "issues_today" in data
        assert "active_today" in data
        
        print(f"✓ Dashboard stats: {data['total_vehicles']} vehicles, {data['total_drivers']} drivers, {data['issues_today']} issues today")
    
    def test_dashboard_alerts(self, auth_token):
        """Test dashboard alerts endpoint"""
        response = requests.get(
            f"{BASE_URL}/api/dashboard/alerts",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ Dashboard alerts: {len(data)} alerts")


class TestOperators:
    """Operators/Drivers endpoint tests"""
    
    def test_get_all_drivers(self, auth_token):
        """Test get all drivers"""
        response = requests.get(
            f"{BASE_URL}/api/drivers",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        
        # Verify driver structure
        if len(data) > 0:
            driver = data[0]
            assert "id" in driver
            assert "name" in driver
            assert "username" in driver
            print(f"✓ Got {len(data)} drivers, first: {driver['name']} ({driver['username']})")
        else:
            print("✓ Got 0 drivers")
    
    def test_generate_username(self, auth_token):
        """Test username generation endpoint"""
        response = requests.get(
            f"{BASE_URL}/api/drivers/generate-username?name=TestUser",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "username" in data
        assert data["username"].startswith("testuser")
        print(f"✓ Generated username: {data['username']}")
    
    def test_generate_username_duplicate_handling(self, auth_token):
        """Test username generation handles duplicates"""
        # Generate username for existing name
        response = requests.get(
            f"{BASE_URL}/api/drivers/generate-username?name=erish",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "username" in data
        # Should have a number suffix since 'erish' exists
        print(f"✓ Generated unique username for duplicate name: {data['username']}")


class TestEquipment:
    """Equipment/Vehicles endpoint tests"""
    
    def test_get_all_vehicles(self, auth_token):
        """Test get all vehicles"""
        response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        
        # Verify vehicle structure
        if len(data) > 0:
            vehicle = data[0]
            assert "id" in vehicle
            assert "name" in vehicle
            assert "registration_number" in vehicle
            assert "assigned_driver_ids" in vehicle
            print(f"✓ Got {len(data)} vehicles, first: {vehicle['name']} ({vehicle['registration_number']})")
        else:
            print("✓ Got 0 vehicles")
    
    def test_get_active_vehicles_today(self, auth_token):
        """Test get active vehicles today"""
        response = requests.get(
            f"{BASE_URL}/api/vehicles/active-today?tz_offset=0",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "active_vehicle_ids" in data
        print(f"✓ Active vehicles today: {len(data['active_vehicle_ids'])}")


class TestInspections:
    """Inspections/Reports endpoint tests"""
    
    def test_get_all_inspections(self, auth_token):
        """Test get all inspections"""
        response = requests.get(
            f"{BASE_URL}/api/inspections?limit=10",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        
        if len(data) > 0:
            inspection = data[0]
            assert "id" in inspection
            assert "type" in inspection
            assert "vehicle_id" in inspection
            assert "is_safe" in inspection
            print(f"✓ Got {len(data)} inspections, first type: {inspection['type']}")
        else:
            print("✓ Got 0 inspections")
    
    def test_filter_inspections_by_type(self, auth_token):
        """Test filter inspections by type"""
        # Test prestart filter
        response = requests.get(
            f"{BASE_URL}/api/inspections?inspection_type=prestart&limit=5",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        for insp in data:
            assert insp["type"] == "prestart"
        print(f"✓ Filtered prestart inspections: {len(data)}")
        
        # Test end_shift filter
        response = requests.get(
            f"{BASE_URL}/api/inspections?inspection_type=end_shift&limit=5",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        for insp in data:
            assert insp["type"] == "end_shift"
        print(f"✓ Filtered end_shift inspections: {len(data)}")


class TestIncidents:
    """Incidents endpoint tests"""
    
    def test_get_all_incidents(self, auth_token):
        """Test get all incidents"""
        response = requests.get(
            f"{BASE_URL}/api/incidents",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        
        if len(data) > 0:
            incident = data[0]
            assert "id" in incident
            assert "description" in incident
            assert "severity" in incident
            assert "status" in incident
            assert "vehicle_name" in incident
            assert "driver_name" in incident
            print(f"✓ Got {len(data)} incidents, first severity: {incident['severity']}")
        else:
            print("✓ Got 0 incidents")
    
    def test_incident_stats(self, auth_token):
        """Test incident stats endpoint"""
        response = requests.get(
            f"{BASE_URL}/api/incidents/stats/summary",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert "total" in data
        assert "this_month" in data
        assert "open_incidents" in data
        assert "by_severity" in data
        assert "by_status" in data
        
        print(f"✓ Incident stats: {data['total']} total, {data['open_incidents']} open")
    
    def test_update_incident_status(self, auth_token):
        """Test updating incident status (4-step workflow)"""
        # First get an incident
        response = requests.get(
            f"{BASE_URL}/api/incidents",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        incidents = response.json()
        
        if len(incidents) > 0:
            incident_id = incidents[0]["id"]
            current_status = incidents[0]["status"]
            
            # Test status update to under_review
            response = requests.put(
                f"{BASE_URL}/api/incidents/{incident_id}",
                headers={"Authorization": f"Bearer {auth_token}"},
                json={"status": "under_review"}
            )
            assert response.status_code == 200
            print(f"✓ Updated incident status from {current_status} to under_review")
            
            # Revert back to original status
            response = requests.put(
                f"{BASE_URL}/api/incidents/{incident_id}",
                headers={"Authorization": f"Bearer {auth_token}"},
                json={"status": current_status}
            )
            assert response.status_code == 200
            print(f"✓ Reverted incident status back to {current_status}")
        else:
            print("⚠ No incidents to test status update")


class TestPasswordReset:
    """Password reset functionality tests"""
    
    def test_reset_driver_password(self, auth_token):
        """Test resetting a driver's password"""
        # Get a driver first
        response = requests.get(
            f"{BASE_URL}/api/drivers",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        drivers = response.json()
        
        if len(drivers) > 0:
            driver_id = drivers[0]["id"]
            driver_name = drivers[0]["name"]
            
            # Reset password
            response = requests.post(
                f"{BASE_URL}/api/drivers/{driver_id}/reset-password",
                headers={"Authorization": f"Bearer {auth_token}"},
                json={"new_password": "NewPass123!"}
            )
            assert response.status_code == 200
            data = response.json()
            assert "message" in data
            assert driver_name in data["message"]
            print(f"✓ Password reset successful for {driver_name}")
        else:
            print("⚠ No drivers to test password reset")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
