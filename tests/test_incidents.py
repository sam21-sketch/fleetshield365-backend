"""
Test suite for FleetGuard Incidents Feature
Tests: GET /api/incidents, GET /api/incidents/stats/summary, PUT /api/incidents/{id}
"""
import pytest
import requests
import os
from datetime import datetime

# Use production URL from environment
BASE_URL = "https://shield-dev-build.preview.emergentagent.com"

# Test credentials
TEST_EMAIL = "admin@test.com"
TEST_PASSWORD = "test123"

class TestIncidentsFeature:
    """Test suite for Incidents API endpoints"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - authenticate and get token"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        # Login to get auth token
        login_response = self.session.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": TEST_EMAIL, "password": TEST_PASSWORD}
        )
        
        if login_response.status_code == 200:
            data = login_response.json()
            self.token = data.get("access_token")
            self.user = data.get("user")
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            print(f"Authenticated as: {self.user.get('email')} (role: {self.user.get('role')})")
        else:
            pytest.skip(f"Authentication failed: {login_response.status_code}")
    
    def test_auth_login_success(self):
        """Test authentication is working"""
        response = self.session.get(f"{BASE_URL}/api/auth/me")
        assert response.status_code == 200, f"Auth me failed: {response.text}"
        data = response.json()
        assert "user" in data
        assert data["user"]["email"] == TEST_EMAIL
        print(f"Auth verified - User: {data['user']['email']}")
    
    # ============ GET /api/incidents Tests ============
    
    def test_get_incidents_list_no_filters(self):
        """Test GET /api/incidents - list all incidents without filters"""
        response = self.session.get(f"{BASE_URL}/api/incidents")
        assert response.status_code == 200, f"Get incidents failed: {response.text}"
        
        data = response.json()
        assert isinstance(data, list), "Response should be a list"
        print(f"Incidents returned: {len(data)}")
        
        # Verify incident structure if any incidents exist
        if len(data) > 0:
            incident = data[0]
            # Check required fields
            assert "id" in incident, "Incident should have 'id'"
            assert "vehicle_id" in incident, "Incident should have 'vehicle_id'"
            assert "driver_id" in incident, "Incident should have 'driver_id'"
            assert "description" in incident, "Incident should have 'description'"
            assert "severity" in incident, "Incident should have 'severity'"
            assert "status" in incident, "Incident should have 'status'"
            assert "vehicle_name" in incident, "Incident should have enriched 'vehicle_name'"
            assert "driver_name" in incident, "Incident should have enriched 'driver_name'"
            print(f"First incident: {incident['id']}, severity: {incident['severity']}, status: {incident['status']}")
        return data
    
    def test_get_incidents_filter_by_severity(self):
        """Test GET /api/incidents with severity filter"""
        for severity in ["minor", "moderate", "severe"]:
            response = self.session.get(
                f"{BASE_URL}/api/incidents",
                params={"severity": severity}
            )
            assert response.status_code == 200, f"Filter by severity={severity} failed: {response.text}"
            
            data = response.json()
            # Verify all returned incidents have correct severity
            for incident in data:
                assert incident["severity"] == severity, f"Incident severity mismatch: expected {severity}, got {incident['severity']}"
            print(f"Filter severity={severity}: {len(data)} incidents")
    
    def test_get_incidents_filter_by_status(self):
        """Test GET /api/incidents with status filter"""
        for status in ["reported", "under_review", "resolved", "closed"]:
            response = self.session.get(
                f"{BASE_URL}/api/incidents",
                params={"status": status}
            )
            assert response.status_code == 200, f"Filter by status={status} failed: {response.text}"
            
            data = response.json()
            # Verify all returned incidents have correct status
            for incident in data:
                assert incident["status"] == status, f"Incident status mismatch: expected {status}, got {incident['status']}"
            print(f"Filter status={status}: {len(data)} incidents")
    
    def test_get_incidents_with_limit(self):
        """Test GET /api/incidents with limit parameter"""
        response = self.session.get(
            f"{BASE_URL}/api/incidents",
            params={"limit": 5}
        )
        assert response.status_code == 200, f"Limit test failed: {response.text}"
        
        data = response.json()
        assert len(data) <= 5, f"Expected max 5 incidents, got {len(data)}"
        print(f"Limit=5 test: {len(data)} incidents returned")
    
    # ============ GET /api/incidents/stats/summary Tests ============
    
    def test_get_incidents_stats_summary(self):
        """Test GET /api/incidents/stats/summary"""
        response = self.session.get(f"{BASE_URL}/api/incidents/stats/summary")
        assert response.status_code == 200, f"Stats summary failed: {response.text}"
        
        data = response.json()
        # Verify required fields
        assert "total" in data, "Stats should have 'total'"
        assert "this_month" in data, "Stats should have 'this_month'"
        assert "open_incidents" in data, "Stats should have 'open_incidents'"
        assert "by_severity" in data, "Stats should have 'by_severity'"
        assert "by_status" in data, "Stats should have 'by_status'"
        
        # Verify by_severity structure
        by_severity = data["by_severity"]
        assert "minor" in by_severity, "by_severity should have 'minor'"
        assert "moderate" in by_severity, "by_severity should have 'moderate'"
        assert "severe" in by_severity, "by_severity should have 'severe'"
        
        # Verify by_status structure
        by_status = data["by_status"]
        assert "reported" in by_status, "by_status should have 'reported'"
        assert "under_review" in by_status, "by_status should have 'under_review'"
        assert "resolved" in by_status, "by_status should have 'resolved'"
        assert "closed" in by_status, "by_status should have 'closed'"
        
        # Verify values are integers
        assert isinstance(data["total"], int), "total should be int"
        assert isinstance(data["this_month"], int), "this_month should be int"
        assert isinstance(data["open_incidents"], int), "open_incidents should be int"
        
        print(f"Stats: Total={data['total']}, This Month={data['this_month']}, Open={data['open_incidents']}")
        print(f"By Severity: {by_severity}")
        print(f"By Status: {by_status}")
        return data
    
    # ============ PUT /api/incidents/{id} Tests ============
    
    def test_update_incident_status(self):
        """Test PUT /api/incidents/{id} - update status"""
        # First, get an incident to update
        list_response = self.session.get(f"{BASE_URL}/api/incidents")
        assert list_response.status_code == 200, f"Get incidents failed: {list_response.text}"
        
        incidents = list_response.json()
        if len(incidents) == 0:
            pytest.skip("No incidents available to test update")
        
        # Get the first incident
        incident = incidents[0]
        incident_id = incident["id"]
        original_status = incident["status"]
        
        # Determine new status (cycle through statuses)
        status_cycle = ["reported", "under_review", "resolved", "closed"]
        current_index = status_cycle.index(original_status) if original_status in status_cycle else 0
        new_status = status_cycle[(current_index + 1) % len(status_cycle)]
        
        print(f"Updating incident {incident_id}: {original_status} -> {new_status}")
        
        # Update the incident
        update_response = self.session.put(
            f"{BASE_URL}/api/incidents/{incident_id}",
            json={"status": new_status}
        )
        assert update_response.status_code == 200, f"Update failed: {update_response.text}"
        
        # Verify update via GET
        verify_response = self.session.get(f"{BASE_URL}/api/incidents/{incident_id}")
        assert verify_response.status_code == 200, f"Get incident failed: {verify_response.text}"
        
        updated_incident = verify_response.json()
        assert updated_incident["status"] == new_status, f"Status not updated: expected {new_status}, got {updated_incident['status']}"
        print(f"Status updated successfully: {new_status}")
        
        # Restore original status
        restore_response = self.session.put(
            f"{BASE_URL}/api/incidents/{incident_id}",
            json={"status": original_status}
        )
        assert restore_response.status_code == 200, f"Restore failed: {restore_response.text}"
        print(f"Restored original status: {original_status}")
    
    def test_update_incident_admin_notes(self):
        """Test PUT /api/incidents/{id} - update admin notes"""
        # Get an incident
        list_response = self.session.get(f"{BASE_URL}/api/incidents", params={"limit": 1})
        incidents = list_response.json()
        
        if len(incidents) == 0:
            pytest.skip("No incidents available to test update")
        
        incident_id = incidents[0]["id"]
        test_notes = f"Test admin note - {datetime.now().isoformat()}"
        
        # Update admin notes
        update_response = self.session.put(
            f"{BASE_URL}/api/incidents/{incident_id}",
            json={"admin_notes": test_notes}
        )
        assert update_response.status_code == 200, f"Update admin_notes failed: {update_response.text}"
        
        # Verify
        verify_response = self.session.get(f"{BASE_URL}/api/incidents/{incident_id}")
        updated = verify_response.json()
        assert updated.get("admin_notes") == test_notes, f"Admin notes not updated"
        print(f"Admin notes updated successfully: {test_notes[:50]}...")
    
    def test_update_nonexistent_incident(self):
        """Test PUT /api/incidents/{id} - update non-existent incident"""
        fake_id = "000000000000000000000000"  # Valid ObjectId format but doesn't exist
        
        response = self.session.put(
            f"{BASE_URL}/api/incidents/{fake_id}",
            json={"status": "resolved"}
        )
        # Should return 404
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print(f"Correctly returned 404 for non-existent incident")
    
    # ============ GET /api/incidents/{id} Tests ============
    
    def test_get_incident_by_id(self):
        """Test GET /api/incidents/{id} - get specific incident"""
        # Get list first
        list_response = self.session.get(f"{BASE_URL}/api/incidents", params={"limit": 1})
        incidents = list_response.json()
        
        if len(incidents) == 0:
            pytest.skip("No incidents available to test")
        
        incident_id = incidents[0]["id"]
        
        # Get by ID
        response = self.session.get(f"{BASE_URL}/api/incidents/{incident_id}")
        assert response.status_code == 200, f"Get incident by ID failed: {response.text}"
        
        incident = response.json()
        assert incident["id"] == incident_id, "ID mismatch"
        
        # Verify enriched data
        assert "vehicle_name" in incident, "Should have vehicle_name"
        assert "vehicle_rego" in incident, "Should have vehicle_rego"
        assert "driver_name" in incident, "Should have driver_name"
        
        print(f"Got incident: {incident_id}")
        print(f"  Vehicle: {incident['vehicle_name']} ({incident['vehicle_rego']})")
        print(f"  Driver: {incident['driver_name']}")
        print(f"  Severity: {incident['severity']}, Status: {incident['status']}")


class TestIncidentsDataIntegrity:
    """Test data integrity and consistency"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup - authenticate"""
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        login_response = self.session.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": TEST_EMAIL, "password": TEST_PASSWORD}
        )
        
        if login_response.status_code == 200:
            data = login_response.json()
            self.token = data.get("access_token")
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        else:
            pytest.skip("Authentication failed")
    
    def test_stats_match_list_counts(self):
        """Verify stats summary matches actual list counts"""
        # Get stats
        stats_response = self.session.get(f"{BASE_URL}/api/incidents/stats/summary")
        stats = stats_response.json()
        
        # Get all incidents
        list_response = self.session.get(f"{BASE_URL}/api/incidents", params={"limit": 1000})
        incidents = list_response.json()
        
        # Verify total
        assert stats["total"] == len(incidents), f"Total mismatch: stats={stats['total']}, list={len(incidents)}"
        
        # Verify by_severity counts
        for severity in ["minor", "moderate", "severe"]:
            filtered = self.session.get(f"{BASE_URL}/api/incidents", params={"severity": severity, "limit": 1000}).json()
            assert stats["by_severity"][severity] == len(filtered), f"Severity {severity} mismatch"
        
        # Verify by_status counts
        for status in ["reported", "under_review", "resolved", "closed"]:
            filtered = self.session.get(f"{BASE_URL}/api/incidents", params={"status": status, "limit": 1000}).json()
            assert stats["by_status"][status] == len(filtered), f"Status {status} mismatch"
        
        print("Stats and list counts are consistent!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
