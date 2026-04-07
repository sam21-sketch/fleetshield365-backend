"""
FleetShield365 Pre-Launch E2E Backend Tests
Tests all critical CRUD operations, auth flows, file exports (PDF/CSV), and offline-related endpoints.
"""
import pytest
import requests
import os
import json
from datetime import datetime

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://shield-dev-build.preview.emergentagent.com').rstrip('/')

# Test credentials from test_credentials.md
ADMIN_EMAIL = "admin@test.com"
ADMIN_PASSWORD = "test123"
DRIVER_USERNAME = "erish"
DRIVER_PASSWORD = "test123"


class TestAuthFlows:
    """Authentication endpoint tests - Login as driver, login as admin"""
    
    def test_health_check(self):
        """Verify API is healthy"""
        response = requests.get(f"{BASE_URL}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        print(f"✓ Health check passed: {data}")
    
    def test_admin_login(self):
        """Test admin login with email"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert response.status_code == 200, f"Admin login failed: {response.text}"
        data = response.json()
        assert "access_token" in data
        assert "user" in data
        assert data["user"]["role"] in ["admin", "super_admin"]
        print(f"✓ Admin login successful: {data['user']['email']} (role: {data['user']['role']})")
        return data["access_token"]
    
    def test_driver_login_with_username(self):
        """Test driver login with username"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "username": DRIVER_USERNAME,
            "password": DRIVER_PASSWORD
        })
        # Driver may not exist in preview env, so we check for valid response
        if response.status_code == 200:
            data = response.json()
            assert "access_token" in data
            assert "user" in data
            print(f"✓ Driver login successful: {data['user'].get('username', data['user'].get('email'))}")
            return data["access_token"]
        elif response.status_code == 401:
            print(f"⚠ Driver '{DRIVER_USERNAME}' not found in preview env - expected for fresh database")
            pytest.skip("Driver not found in preview environment")
        else:
            pytest.fail(f"Unexpected response: {response.status_code} - {response.text}")
    
    def test_auth_me_endpoint(self):
        """Test /auth/me endpoint returns user info"""
        # First login
        login_response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]
        
        # Then get user info
        response = requests.get(
            f"{BASE_URL}/api/auth/me",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "user" in data
        print(f"✓ Auth/me endpoint works: {data['user']['email']}")


class TestVehicleAPI:
    """Vehicle API tests - GET all vehicles, vehicle assignment"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    def test_get_all_vehicles(self, auth_token):
        """Test GET /api/vehicles returns list of vehicles"""
        response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/vehicles returned {len(data)} vehicles")
        return data
    
    def test_create_vehicle(self, auth_token):
        """Test POST /api/vehicles creates a new vehicle"""
        vehicle_data = {
            "name": f"TEST_Vehicle_{datetime.now().strftime('%H%M%S')}",
            "registration_number": f"TEST{datetime.now().strftime('%H%M%S')}",
            "type": "truck",
            "status": "active"
        }
        response = requests.post(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=vehicle_data
        )
        assert response.status_code in [200, 201], f"Create vehicle failed: {response.text}"
        data = response.json()
        assert "id" in data
        print(f"✓ Created vehicle: {data['name']} (ID: {data['id']})")
        return data
    
    def test_get_vehicle_by_id(self, auth_token):
        """Test GET /api/vehicles/{id} returns specific vehicle"""
        # First create a vehicle
        vehicle_data = {
            "name": f"TEST_GetById_{datetime.now().strftime('%H%M%S')}",
            "registration_number": f"GETID{datetime.now().strftime('%H%M%S')}",
            "type": "truck"
        }
        create_response = requests.post(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=vehicle_data
        )
        assert create_response.status_code in [200, 201]
        vehicle_id = create_response.json()["id"]
        
        # Then get by ID
        response = requests.get(
            f"{BASE_URL}/api/vehicles/{vehicle_id}",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == vehicle_id
        print(f"✓ GET /api/vehicles/{vehicle_id} returned correct vehicle")
    
    def test_vehicle_assignment(self, auth_token):
        """Test POST /api/vehicles/{id}/assign for driver assignment"""
        # First get or create a vehicle
        vehicles_response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        vehicles = vehicles_response.json()
        
        if not vehicles:
            # Create a vehicle first
            create_response = requests.post(
                f"{BASE_URL}/api/vehicles",
                headers={"Authorization": f"Bearer {auth_token}"},
                json={
                    "name": "TEST_Assignment_Vehicle",
                    "registration_number": "ASSIGN123",
                    "type": "truck"
                }
            )
            vehicle_id = create_response.json()["id"]
        else:
            vehicle_id = vehicles[0]["id"]
        
        # Test assignment endpoint (even with empty driver list)
        response = requests.post(
            f"{BASE_URL}/api/vehicles/{vehicle_id}/assign",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={"driver_ids": []}
        )
        assert response.status_code == 200
        print(f"✓ Vehicle assignment endpoint works for vehicle {vehicle_id}")


class TestInspectionAPI:
    """Inspection API tests - Create prestart, end-shift, get inspections list"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    @pytest.fixture
    def test_vehicle_id(self, auth_token):
        """Get or create a test vehicle"""
        vehicles_response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        vehicles = vehicles_response.json()
        
        if vehicles:
            return vehicles[0]["id"]
        
        # Create a vehicle
        create_response = requests.post(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "name": "TEST_Inspection_Vehicle",
                "registration_number": "INSP123",
                "type": "truck"
            }
        )
        return create_response.json()["id"]
    
    def test_create_prestart_inspection(self, auth_token, test_vehicle_id):
        """Test POST /api/inspections/prestart creates prestart inspection"""
        # Prestart requires 6 mandatory photos: front, rear, left, right, cabin, odometer
        # Using a minimal valid base64 image (1x1 transparent PNG)
        dummy_photo_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        
        required_photos = [
            {"photo_type": "front", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "rear", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "left", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "right", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "cabin", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "odometer", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
        ]
        
        inspection_data = {
            "vehicle_id": test_vehicle_id,
            "odometer": 50000,
            "checklist_items": [
                {"name": "Brakes", "section": "Safety", "status": "ok"},
                {"name": "Lights", "section": "Safety", "status": "ok"},
                {"name": "Tires", "section": "Safety", "status": "ok"},
                {"name": "Oil Level", "section": "Engine", "status": "ok"}
            ],
            "photos": required_photos,
            "declaration_confirmed": True,
            "gps_latitude": -33.8688,
            "gps_longitude": 151.2093,
            "location_address": "Sydney, NSW"
        }
        response = requests.post(
            f"{BASE_URL}/api/inspections/prestart",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=inspection_data
        )
        assert response.status_code in [200, 201], f"Create prestart failed: {response.text}"
        data = response.json()
        assert "id" in data
        assert data["type"] == "prestart"
        print(f"✓ Created prestart inspection: {data['id']}")
        return data
    
    def test_create_end_shift_inspection(self, auth_token, test_vehicle_id):
        """Test POST /api/inspections/end-shift creates end-shift inspection"""
        inspection_data = {
            "vehicle_id": test_vehicle_id,
            "odometer": 50100,
            "fuel_level": "3/4",
            "new_damage": False,
            "incident_today": False,
            "cleanliness": "clean",
            "photos": [],
            "declaration_confirmed": True,
            "gps_latitude": -33.8688,
            "gps_longitude": 151.2093,
            "location_address": "Sydney, NSW"
        }
        response = requests.post(
            f"{BASE_URL}/api/inspections/end-shift",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=inspection_data
        )
        assert response.status_code in [200, 201], f"Create end-shift failed: {response.text}"
        data = response.json()
        assert "id" in data
        assert data["type"] == "end_shift"
        print(f"✓ Created end-shift inspection: {data['id']}")
        return data
    
    def test_get_inspections_list(self, auth_token):
        """Test GET /api/inspections returns list of inspections"""
        response = requests.get(
            f"{BASE_URL}/api/inspections",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/inspections returned {len(data)} inspections")
        return data
    
    def test_get_inspection_pdf(self, auth_token, test_vehicle_id):
        """Test GET /api/inspections/{id}/pdf generates PDF"""
        # First create an inspection with required photos
        dummy_photo_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        required_photos = [
            {"photo_type": "front", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "rear", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "left", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "right", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "cabin", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
            {"photo_type": "odometer", "base64_data": dummy_photo_base64, "timestamp": datetime.now().isoformat()},
        ]
        
        inspection_data = {
            "vehicle_id": test_vehicle_id,
            "odometer": 50200,
            "checklist_items": [
                {"name": "Brakes", "section": "Safety", "status": "ok"}
            ],
            "photos": required_photos,
            "declaration_confirmed": True
        }
        create_response = requests.post(
            f"{BASE_URL}/api/inspections/prestart",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=inspection_data
        )
        assert create_response.status_code in [200, 201], f"Create inspection for PDF test failed: {create_response.text}"
        inspection_id = create_response.json()["id"]
        
        # Get PDF
        response = requests.get(
            f"{BASE_URL}/api/inspections/{inspection_id}/pdf",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"PDF generation failed: {response.text}"
        data = response.json()
        assert "pdf_base64" in data
        assert len(data["pdf_base64"]) > 100  # Should have substantial content
        print(f"✓ Inspection PDF generated successfully (size: {len(data['pdf_base64'])} chars)")


class TestFuelAPI:
    """Fuel API tests - Create fuel log, get fuel logs, export CSV"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    @pytest.fixture
    def test_vehicle_id(self, auth_token):
        """Get or create a test vehicle"""
        vehicles_response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        vehicles = vehicles_response.json()
        
        if vehicles:
            return vehicles[0]["id"]
        
        create_response = requests.post(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "name": "TEST_Fuel_Vehicle",
                "registration_number": "FUEL123",
                "type": "truck"
            }
        )
        return create_response.json()["id"]
    
    def test_create_fuel_log(self, auth_token, test_vehicle_id):
        """Test POST /api/fuel creates fuel log"""
        fuel_data = {
            "vehicle_id": test_vehicle_id,
            "amount": 150.50,
            "liters": 75.25,
            "odometer": 51000,
            "fuel_station": "Shell Test Station",
            "notes": "Test fuel submission"
        }
        response = requests.post(
            f"{BASE_URL}/api/fuel",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=fuel_data
        )
        assert response.status_code in [200, 201], f"Create fuel log failed: {response.text}"
        data = response.json()
        assert "id" in data
        print(f"✓ Created fuel log: {data['id']} (${fuel_data['amount']}, {fuel_data['liters']}L)")
        return data
    
    def test_get_fuel_logs(self, auth_token):
        """Test GET /api/fuel returns list of fuel logs"""
        response = requests.get(
            f"{BASE_URL}/api/fuel",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Get fuel logs failed: {response.text}"
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/fuel returned {len(data)} fuel logs")
        return data
    
    def test_fuel_export_csv(self, auth_token):
        """Test GET /api/fuel/export/csv exports fuel data as CSV"""
        response = requests.get(
            f"{BASE_URL}/api/fuel/export/csv",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Fuel CSV export failed: {response.text}"
        # Check content type is CSV
        content_type = response.headers.get("content-type", "")
        assert "text/csv" in content_type or "application/octet-stream" in content_type or response.text.startswith("Date")
        print(f"✓ Fuel CSV export successful (size: {len(response.text)} chars)")


class TestIncidentAPI:
    """Incident API tests - Create incident, get incidents, export CSV, generate PDF"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    @pytest.fixture
    def test_vehicle_id(self, auth_token):
        """Get or create a test vehicle"""
        vehicles_response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        vehicles = vehicles_response.json()
        
        if vehicles:
            return vehicles[0]["id"]
        
        create_response = requests.post(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "name": "TEST_Incident_Vehicle",
                "registration_number": "INC123",
                "type": "truck"
            }
        )
        return create_response.json()["id"]
    
    def test_create_incident(self, auth_token, test_vehicle_id):
        """Test POST /api/incidents creates incident report"""
        incident_data = {
            "vehicle_id": test_vehicle_id,
            "description": "Test incident - minor fender bender in parking lot",
            "severity": "minor",
            "location_address": "123 Test Street, Sydney NSW",
            "gps_latitude": -33.8688,
            "gps_longitude": 151.2093,
            "other_party": {
                "name": "John Test",
                "phone": "0400000000",
                "vehicle_rego": "ABC123"
            },
            "witnesses": [],
            "injuries_occurred": False,
            "damage_photos": [],
            "other_vehicle_photos": [],
            "scene_photos": []
        }
        response = requests.post(
            f"{BASE_URL}/api/incidents",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=incident_data
        )
        assert response.status_code in [200, 201], f"Create incident failed: {response.text}"
        data = response.json()
        assert "id" in data
        print(f"✓ Created incident: {data['id']} (severity: {incident_data['severity']})")
        return data
    
    def test_get_incidents_list(self, auth_token):
        """Test GET /api/incidents returns list of incidents"""
        response = requests.get(
            f"{BASE_URL}/api/incidents",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Get incidents failed: {response.text}"
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/incidents returned {len(data)} incidents")
        return data
    
    def test_incidents_export_csv(self, auth_token):
        """Test GET /api/incidents/export/csv exports incidents as CSV"""
        response = requests.get(
            f"{BASE_URL}/api/incidents/export/csv",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Incidents CSV export failed: {response.text}"
        print(f"✓ Incidents CSV export successful (size: {len(response.text)} chars)")
    
    def test_incident_pdf(self, auth_token, test_vehicle_id):
        """Test GET /api/incidents/{id}/pdf generates PDF"""
        # First create an incident
        incident_data = {
            "vehicle_id": test_vehicle_id,
            "description": "Test incident for PDF generation",
            "severity": "moderate",
            "other_party": {"name": "PDF Test Party"},
            "injuries_occurred": False,
            "damage_photos": [],
            "other_vehicle_photos": [],
            "scene_photos": []
        }
        create_response = requests.post(
            f"{BASE_URL}/api/incidents",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=incident_data
        )
        incident_id = create_response.json()["id"]
        
        # Get PDF
        response = requests.get(
            f"{BASE_URL}/api/incidents/{incident_id}/pdf",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Incident PDF generation failed: {response.text}"
        data = response.json()
        assert "pdf_base64" in data
        assert len(data["pdf_base64"]) > 100
        print(f"✓ Incident PDF generated successfully (size: {len(data['pdf_base64'])} chars)")


class TestDashboardAPI:
    """Dashboard API tests - Get stats, get alerts"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    def test_get_dashboard_stats(self, auth_token):
        """Test GET /api/dashboard/stats returns dashboard statistics"""
        response = requests.get(
            f"{BASE_URL}/api/dashboard/stats",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Dashboard stats failed: {response.text}"
        data = response.json()
        # Check expected fields
        expected_fields = ["total_vehicles", "total_drivers", "inspections_today", "issues_today"]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"
        print(f"✓ Dashboard stats: {data['total_vehicles']} vehicles, {data['total_drivers']} drivers, {data['inspections_today']} inspections today")
        return data
    
    def test_get_alerts(self, auth_token):
        """Test GET /api/alerts returns alerts list"""
        response = requests.get(
            f"{BASE_URL}/api/alerts",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Get alerts failed: {response.text}"
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/alerts returned {len(data)} alerts")
        return data
    
    def test_get_chart_data(self, auth_token):
        """Test GET /api/dashboard/chart-data returns chart data"""
        response = requests.get(
            f"{BASE_URL}/api/dashboard/chart-data",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Chart data failed: {response.text}"
        data = response.json()
        print(f"✓ Dashboard chart data retrieved successfully")
        return data


class TestDriverAPI:
    """Driver API tests - Get drivers list, create driver"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    def test_get_drivers_list(self, auth_token):
        """Test GET /api/drivers returns list of drivers"""
        response = requests.get(
            f"{BASE_URL}/api/drivers",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Get drivers failed: {response.text}"
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/drivers returned {len(data)} drivers")
        return data
    
    def test_create_driver(self, auth_token):
        """Test POST /api/drivers creates a new driver"""
        driver_data = {
            "name": f"TEST_Driver_{datetime.now().strftime('%H%M%S')}",
            "email": f"test_driver_{datetime.now().strftime('%H%M%S')}@test.com",
            "password": "test123",
            "phone": "0400000001",
            "license_number": "DL123456",
            "license_class": "HR"
        }
        response = requests.post(
            f"{BASE_URL}/api/drivers",
            headers={"Authorization": f"Bearer {auth_token}"},
            json=driver_data
        )
        assert response.status_code in [200, 201], f"Create driver failed: {response.text}"
        data = response.json()
        assert "id" in data
        print(f"✓ Created driver: {data.get('name', data.get('username'))} (ID: {data['id']})")
        return data
    
    def test_generate_username(self, auth_token):
        """Test GET /api/drivers/generate-username generates unique username"""
        response = requests.get(
            f"{BASE_URL}/api/drivers/generate-username",
            headers={"Authorization": f"Bearer {auth_token}"},
            params={"name": "John Smith"}
        )
        assert response.status_code == 200, f"Generate username failed: {response.text}"
        data = response.json()
        assert "username" in data
        print(f"✓ Generated username: {data['username']}")


class TestOfflineSyncEndpoints:
    """Offline sync related endpoint tests"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    def test_vehicles_endpoint_for_offline_cache(self, auth_token):
        """Test vehicles endpoint returns data suitable for offline caching"""
        response = requests.get(
            f"{BASE_URL}/api/vehicles",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        # Verify structure is suitable for offline storage
        if data:
            vehicle = data[0]
            assert "id" in vehicle
            assert "name" in vehicle
            assert "registration_number" in vehicle
        print(f"✓ Vehicles endpoint returns offline-cacheable data ({len(data)} vehicles)")
    
    def test_inspections_endpoint_for_offline_sync(self, auth_token):
        """Test inspections endpoint supports offline sync patterns"""
        response = requests.get(
            f"{BASE_URL}/api/inspections",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        # Verify structure supports offline sync
        if data:
            inspection = data[0]
            assert "id" in inspection
            assert "timestamp" in inspection or "created_at" in inspection
        print(f"✓ Inspections endpoint returns sync-compatible data ({len(data)} inspections)")
    
    def test_fuel_endpoint_for_offline_sync(self, auth_token):
        """Test fuel endpoint supports offline sync patterns"""
        response = requests.get(
            f"{BASE_URL}/api/fuel",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200
        data = response.json()
        print(f"✓ Fuel endpoint returns sync-compatible data ({len(data)} fuel logs)")


class TestReportsExport:
    """Reports and export functionality tests"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    def test_service_records_csv_export(self, auth_token):
        """Test GET /api/service-records/export/csv"""
        response = requests.get(
            f"{BASE_URL}/api/service-records/export/csv",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Service records CSV export failed: {response.text}"
        print(f"✓ Service records CSV export successful")
    
    def test_incidents_stats_summary(self, auth_token):
        """Test GET /api/incidents/stats/summary"""
        response = requests.get(
            f"{BASE_URL}/api/incidents/stats/summary",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Incidents stats failed: {response.text}"
        data = response.json()
        print(f"✓ Incidents stats summary: {data}")


class TestSubscriptionAndSupport:
    """Subscription and support endpoint tests"""
    
    @pytest.fixture
    def auth_token(self):
        """Get admin auth token"""
        response = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if response.status_code != 200:
            pytest.skip("Admin login failed")
        return response.json()["access_token"]
    
    def test_get_subscription_status(self, auth_token):
        """Test GET /api/subscription returns subscription info"""
        response = requests.get(
            f"{BASE_URL}/api/subscription",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"Subscription status failed: {response.text}"
        data = response.json()
        print(f"✓ Subscription status: {data.get('status', 'unknown')}")
    
    def test_get_faq(self, auth_token):
        """Test GET /api/faq returns FAQ list"""
        response = requests.get(
            f"{BASE_URL}/api/faq",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        assert response.status_code == 200, f"FAQ endpoint failed: {response.text}"
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ FAQ endpoint returned {len(data)} items")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
