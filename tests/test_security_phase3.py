"""Unit tests for Phase 3 security helpers.

Pure-Python tests — no Mongo / MinIO / SMTP required. Covers:

* sanitize_user_doc           strips every secret field
* validate_password_policy    enforces min length + complexity
* validate_redirect_url       allows in-zone URLs, rejects everything else
* _safe_html                  HTML-escapes user-supplied strings
* _account_locked_until       returns the right datetime / None
* compute jti on token mint   tokens always carry a jti claim
"""
from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("JWT_SECRET", "test-secret-not-real")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017/test")
os.environ.setdefault("OBJECT_STORE_ACCESS_KEY", "test-access")
os.environ.setdefault("OBJECT_STORE_SECRET_KEY", "test-secret")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import HTTPException  # noqa: E402

import server  # noqa: E402


# ---------------------------------------------------------------------------
# sanitize_user_doc
# ---------------------------------------------------------------------------


def test_sanitize_strips_password_hash():
    doc = {
        "email": "x@y.com",
        "password_hash": "$2b$bcrypt...",
        "hashed_password": "legacy-shape",
        "offline_cred_hash": "blob",
        "failed_login_attempts": [1, 2, 3],
        "locked_until": "whatever",
        "id": "1",
    }
    out = server.sanitize_user_doc(doc)
    assert "password_hash" not in out
    assert "hashed_password" not in out
    assert "offline_cred_hash" not in out
    assert "failed_login_attempts" not in out
    assert "locked_until" not in out
    assert out["email"] == "x@y.com"
    assert out["id"] == "1"


def test_sanitize_handles_list_and_none():
    assert server.sanitize_user_doc(None) is None
    assert server.sanitize_user_doc([]) == []
    out = server.sanitize_user_doc([
        {"email": "a", "password_hash": "x"},
        {"email": "b", "hashed_password": "y"},
    ])
    assert out == [{"email": "a"}, {"email": "b"}]


def test_sanitize_does_not_mutate_input():
    doc = {"email": "x@y.com", "password_hash": "secret"}
    server.sanitize_user_doc(doc)
    # Original dict still has the field — sanitizer returns a copy.
    assert "password_hash" in doc


# ---------------------------------------------------------------------------
# validate_password_policy
# ---------------------------------------------------------------------------


def test_password_policy_accepts_strong():
    server.validate_password_policy("Strong1Password")  # no raise


def test_password_policy_min_length():
    with pytest.raises(HTTPException) as exc:
        server.validate_password_policy("Abc1")
    assert exc.value.status_code == 400


def test_password_policy_requires_upper_lower_digit():
    for bad in ["alllowercase1", "ALLUPPERCASE1", "NoDigitsHere", "12345678"]:
        with pytest.raises(HTTPException):
            server.validate_password_policy(bad)


def test_password_policy_rejects_non_string():
    for bad in [None, 12345678, b"Bytes123"]:
        with pytest.raises(HTTPException):
            server.validate_password_policy(bad)


# ---------------------------------------------------------------------------
# validate_redirect_url
# ---------------------------------------------------------------------------


def test_redirect_url_accepts_apex_and_subdomain():
    assert server.validate_redirect_url("https://fleetshield365.com/dashboard")
    assert server.validate_redirect_url("https://lalitco.fleetshield365.com/dashboard")
    assert server.validate_redirect_url("https://api.fleetshield365.com/health")


def test_redirect_url_rejects_off_domain():
    for bad in [
        "https://evil.com/",
        "https://fleetshield365.com.evil.com/",
        "http://fleetshield365.com.attacker.example/",
        "//evil.com/",
        "javascript:alert(1)",
        "ftp://fleetshield365.com/",
    ]:
        assert server.validate_redirect_url(bad) is None, bad


def test_redirect_url_rejects_none_and_garbage():
    assert server.validate_redirect_url(None) is None
    assert server.validate_redirect_url("") is None
    assert server.validate_redirect_url("not a url") is None


# ---------------------------------------------------------------------------
# _safe_html
# ---------------------------------------------------------------------------


def test_safe_html_escapes_xss_payload():
    out = server._safe_html("<script>alert(1)</script>")
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


def test_safe_html_handles_quotes_and_none():
    assert server._safe_html(None) == ""
    out = server._safe_html('"><img src=x onerror=alert(1)>')
    assert "<" not in out
    assert "&quot;" in out


# ---------------------------------------------------------------------------
# _account_locked_until
# ---------------------------------------------------------------------------


def test_account_lock_in_future_returns_datetime():
    from datetime import datetime, timedelta
    locked_until = datetime.utcnow() + timedelta(minutes=10)
    assert server._account_locked_until({"locked_until": locked_until}) == locked_until


def test_account_lock_in_past_returns_none():
    from datetime import datetime, timedelta
    locked_until = datetime.utcnow() - timedelta(minutes=10)
    assert server._account_locked_until({"locked_until": locked_until}) is None


def test_account_lock_absent_returns_none():
    assert server._account_locked_until({}) is None
    assert server._account_locked_until({"locked_until": None}) is None


# ---------------------------------------------------------------------------
# Token mint carries jti
# ---------------------------------------------------------------------------


def test_create_access_token_includes_jti():
    import jwt as pyjwt
    token = server.create_access_token({"sub": "user_id_123"})
    decoded = pyjwt.decode(token, server.SECRET_KEY, algorithms=[server.ALGORITHM])
    assert "jti" in decoded
    assert len(decoded["jti"]) >= 16


def test_create_access_token_unique_jti_per_call():
    import jwt as pyjwt
    t1 = server.create_access_token({"sub": "u1"})
    t2 = server.create_access_token({"sub": "u1"})
    d1 = pyjwt.decode(t1, server.SECRET_KEY, algorithms=[server.ALGORITHM])
    d2 = pyjwt.decode(t2, server.SECRET_KEY, algorithms=[server.ALGORITHM])
    assert d1["jti"] != d2["jti"]


# ---------------------------------------------------------------------------
# CORS tightened (no wildcard methods/headers)
# ---------------------------------------------------------------------------


def test_cors_middleware_uses_explicit_method_allowlist():
    # Inspect the CORS middleware in the app's middleware stack.
    cors_middleware = None
    for mw in server.app.user_middleware:
        if getattr(mw, "cls", None).__name__ == "CORSMiddleware":
            cors_middleware = mw
            break
    assert cors_middleware is not None
    options = cors_middleware.kwargs
    assert options["allow_methods"] != ["*"], "CORS allow_methods must not be wildcard"
    assert "GET" in options["allow_methods"]
    assert options["allow_headers"] != ["*"], "CORS allow_headers must not be wildcard"
    assert "Authorization" in options["allow_headers"]
