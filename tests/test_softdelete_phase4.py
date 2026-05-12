"""Unit tests for Phase 4 soft-delete helpers.

Pure helpers — no Mongo required. Covers:
  * _soft_delete_filter      excludes by default, includes on opt-in
  * _soft_delete_update      sets deleted_at + deleted_by
  * _restore_update          unsets both
  * SOFT_DELETE_COLLECTIONS  every entity that participates
  * SOFT_DELETE_GRACE_DAYS   default = 30
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

os.environ.setdefault("JWT_SECRET", "test-secret-not-real")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017/test")
os.environ.setdefault("OBJECT_STORE_ACCESS_KEY", "test-access")
os.environ.setdefault("OBJECT_STORE_SECRET_KEY", "test-secret")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import server  # noqa: E402


# ---------------------------------------------------------------------------
# _soft_delete_filter
# ---------------------------------------------------------------------------


def test_soft_delete_filter_default_excludes_deleted():
    f = server._soft_delete_filter()
    assert f == {"deleted_at": None}


def test_soft_delete_filter_include_deleted_returns_empty():
    f = server._soft_delete_filter(include_deleted=True)
    assert f == {}


# ---------------------------------------------------------------------------
# _soft_delete_update / _restore_update
# ---------------------------------------------------------------------------


def test_soft_delete_update_stamps_user_and_timestamp():
    update = server._soft_delete_update("507f1f77bcf86cd799439011")
    assert "$set" in update
    assert isinstance(update["$set"]["deleted_at"], datetime)
    assert update["$set"]["deleted_by"] == "507f1f77bcf86cd799439011"


def test_soft_delete_update_handles_missing_user():
    update = server._soft_delete_update(None)
    assert update["$set"]["deleted_by"] is None
    assert isinstance(update["$set"]["deleted_at"], datetime)


def test_restore_update_unsets_both_fields():
    restore = server._restore_update()
    assert restore == {"$unset": {"deleted_at": "", "deleted_by": ""}}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_soft_delete_collections_covers_expected_entities():
    expected = {"vehicles", "users", "companies", "service_records",
                "maintenance_logs", "incidents"}
    assert set(server.SOFT_DELETE_COLLECTIONS) == expected


def test_inspections_and_fuel_NOT_in_soft_delete_set():
    # NHVR compliance: inspections + fuel must be immutable, never
    # quietly hidden by a soft-delete filter. Audit_trail likewise.
    forbidden = {"inspections", "fuel_submissions", "audit_trail"}
    assert forbidden.isdisjoint(server.SOFT_DELETE_COLLECTIONS)


def test_grace_period_defaults_to_30_days():
    # Set in TODO.md as the soft-delete retention window. The env var
    # SOFT_DELETE_GRACE_DAYS can override at runtime.
    assert server.SOFT_DELETE_GRACE_DAYS == 30
