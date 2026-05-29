from fastapi import FastAPI, APIRouter, HTTPException, Depends, status, Request, UploadFile, File, BackgroundTasks, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import List, Optional, Dict, Any
from enum import Enum
import uuid
from datetime import datetime, timedelta, timezone
import bcrypt
import jwt
from bson import ObjectId
import base64
from io import BytesIO
import zipfile
import object_store
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
from reportlab.lib.units import inch, cm
import json
import re
import stripe
import httpx
import asyncio
import aiosmtplib
from email.message import EmailMessage
import subprocess
import shutil
import tempfile
from PIL import Image as PILImage


class MissingRequiredEnvVarError(RuntimeError):
    """Raised at module load when a required environment variable is missing or empty."""
    pass


class SubdomainValidationError(ValueError):
    """Base error for subdomain validation + uniqueness failures.

    Subclasses carry a ``code`` attribute (``"reserved"``, ``"malformed"``,
    ``"taken"``) which the register endpoints map to HTTP status codes per
    Requirements 9.9 (reserved → 400), 9.10 (malformed → 400), and 9.11
    (already used → 409). ``subdomain`` is the (already-normalized when
    possible) offending value, so callers can echo it back in error bodies
    without re-parsing the request.
    """

    # Overridden by each concrete subclass below.
    code: str = "invalid"

    def __init__(self, subdomain: str, message: str | None = None) -> None:
        self.subdomain = subdomain
        super().__init__(message or f"Invalid subdomain: {subdomain!r}")


class ReservedSubdomainError(SubdomainValidationError):
    """Raised when a submitted subdomain is in Reserved_Subdomain_List (Req 9.9, 10.1)."""

    code = "reserved"


class MalformedSubdomainError(SubdomainValidationError):
    """Raised when a submitted subdomain fails SUBDOMAIN_REGEX (Req 9.3, 9.4, 9.10)."""

    code = "malformed"


class SubdomainTakenError(SubdomainValidationError):
    """Raised when a submitted subdomain is already stored on another company (Req 9.2, 9.11)."""

    code = "taken"


# Stripe Configuration
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY', '')

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')


def _require_env(name: str) -> str:
    """Read a required environment variable, failing fast if missing or empty.

    Raises MissingRequiredEnvVarError when the value is None or empty after
    whitespace stripping, so the error surface is clear at startup.
    """
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise MissingRequiredEnvVarError(
            f"Required env var {name!r} is missing or empty"
        )
    return value


# Fail-fast validation of required environment variables at module load.
# Must run before FastAPI app initialization and Mongo client construction.
JWT_SECRET = _require_env('JWT_SECRET')
MONGO_URL = _require_env('MONGO_URL')
OBJECT_STORE_ACCESS_KEY = _require_env('OBJECT_STORE_ACCESS_KEY')
OBJECT_STORE_SECRET_KEY = _require_env('OBJECT_STORE_SECRET_KEY')

# Origin / CORS configuration loaded from environment.
# DEFAULT_ORIGIN_URL is used as the fallback origin for password-reset links
# and any other flow that needs to construct an absolute URL to the website.
# Falls back to the marketing apex when the env var is unset or blank.
DEFAULT_ORIGIN_URL: str = (
    os.environ.get('DEFAULT_ORIGIN_URL', '').strip()
    or 'https://www.fleetshield365.com'
)

# CORS_ALLOWED_ORIGINS is parsed as a comma-separated list of absolute origins.
# Entries are stripped; empty entries are dropped. When the env var is unset or
# parses to an empty list, we fall back to the default production whitelist
# required by Requirement 8.3 (apex, www, app, owner).
_DEFAULT_CORS_ALLOWED_ORIGINS: list[str] = [
    'https://fleetshield365.com',
    'https://www.fleetshield365.com',
    'https://app.fleetshield365.com',
    'https://owner.fleetshield365.com',
]
CORS_ALLOWED_ORIGINS: list[str] = [
    o.strip()
    for o in os.environ.get('CORS_ALLOWED_ORIGINS', '').split(',')
    if o.strip()
] or list(_DEFAULT_CORS_ALLOWED_ORIGINS)

# Tenant subdomain regex used to decide whether an origin like
# https://<slug>.fleetshield365.com is a legitimate tenant host even when it
# is not explicitly listed in CORS_ALLOWED_ORIGINS. Mirrors the CORS
# middleware's allow_origin_regex (Requirement 8.4) and is reused by
# _is_allowed_origin for password-reset URL validation (Requirement 6.3).
_TENANT_ORIGIN_REGEX: re.Pattern[str] = re.compile(
    r'^https://[a-z0-9-]+\.fleetshield365\.com$'
)


def _is_allowed_origin(origin: str | None) -> bool:
    """Return True if ``origin`` is a trusted FleetShield365_Web host.

    An origin is trusted when, after stripping surrounding whitespace and a
    single trailing slash, it either:

    * exactly matches an entry in ``CORS_ALLOWED_ORIGINS``, or
    * matches the tenant subdomain regex ``^https://[a-z0-9-]+\\.fleetshield365\\.com$``.

    Empty, ``None``, or otherwise non-matching values return False. Used to
    prevent open-redirect via untrusted ``origin_url`` values on flows like
    password reset (Requirement 6.3).
    """
    if not origin:
        return False
    normalized = origin.strip().rstrip('/')
    if not normalized:
        return False
    if normalized in CORS_ALLOWED_ORIGINS:
        return True
    if _TENANT_ORIGIN_REGEX.fullmatch(normalized) is not None:
        return True
    return False

# PLATFORM_OWNER_USER_IDS is parsed as a comma-separated list of MongoDB
# ObjectId string representations (24-char hex). Entries are stripped; empty
# entries are dropped. The login handler consults this frozenset to decide
# whether to mint a JWT with role="platform_owner" for a given user
# (Requirement 15.6). We deliberately do NOT validate that each entry is a
# syntactically valid ObjectId at load time — invalid or stale values simply
# never match a real user _id, which fails closed. frozenset is used so the
# set is immutable at runtime and cheap to probe on every login.
PLATFORM_OWNER_USER_IDS: frozenset[str] = frozenset(
    entry.strip()
    for entry in os.environ.get('PLATFORM_OWNER_USER_IDS', '').split(',')
    if entry.strip()
)

# Reserved subdomains (Requirement 10.1). Values are the canonical lowercase
# form; Requirement 10.2 requires case-insensitive comparison, so callers
# must normalize to lowercase before checking membership. This frozenset
# contains exactly the 23 values enumerated in Requirement 10.1 and is the
# sole source of truth for reserved-name collision checks across the
# Slug_Generator (Req 9.8), validate_subdomain (Req 9.9), and
# POST /api/tenant/resolve (Req 11.5).
RESERVED_SUBDOMAINS: frozenset[str] = frozenset({
    'www', 'api', 'admin', 'owner', 'mail', 'app',
    'autodiscover', 'autoconfig', 'zmail', '_domainkey', '_dmarc',
    'em', 's1', 's2', 'cdn', 'static', 'assets',
    'help', 'docs', 'blog', 'status', 'support', 'security',
})

# Subdomain format regex (Requirements 9.3, 9.4): 3-30 characters,
# lowercase alphanumeric plus hyphen, no leading/trailing hyphen. This is
# the canonical "Reserved Slug Regex" from the spec glossary; validators
# lowercase input first (Req 9.3) before matching, so the character class
# intentionally excludes uppercase letters.
SUBDOMAIN_REGEX: re.Pattern[str] = re.compile(
    r'^[a-z0-9](?:[a-z0-9-]{1,28}[a-z0-9])?$'
)

# ObjectStore (MinIO) connection + presigning configuration.
# Loaded from env with safe defaults so local dev works out of the box and
# the production EC2_Instance single-box layout (MinIO on 127.0.0.1:9000
# behind Nginx_Proxy at /files/) is reflected by the defaults.
# The access/secret key pair is already fail-fast loaded above via
# OBJECT_STORE_ACCESS_KEY / OBJECT_STORE_SECRET_KEY; this block only covers
# the non-secret, defaultable connection + presigning settings.
# (Requirements 3.1, 21.12, 21.13)

# OBJECT_STORE_ENDPOINT is the internal endpoint the FleetShield365_API uses
# to reach MinIO for uploads and to compute presigned URLs. On the EC2 box
# MinIO binds to 127.0.0.1:9000 so the default matches that layout and is
# also valid for local dev against a default MinIO install.
OBJECT_STORE_ENDPOINT: str = (
    os.environ.get('OBJECT_STORE_ENDPOINT', '').strip()
    or 'http://127.0.0.1:9000'
)

# OBJECT_STORE_PUBLIC_ENDPOINT is the public-facing base URL presigned GET
# URLs must be rewritten to, so external clients fetch objects through
# Nginx_Proxy at https://api.fleetshield365.com/files/... rather than
# hitting the MinIO origin directly (Requirement 21.13).
OBJECT_STORE_PUBLIC_ENDPOINT: str = (
    os.environ.get('OBJECT_STORE_PUBLIC_ENDPOINT', '').strip()
    or 'https://api.fleetshield365.com/files'
)

# OBJECT_STORE_REGION is the S3-compatible region string passed to the MinIO
# client. MinIO accepts any value; us-east-1 is used by convention to match
# common S3 SDK defaults.
OBJECT_STORE_REGION: str = (
    os.environ.get('OBJECT_STORE_REGION', '').strip()
    or 'us-east-1'
)

# OBJECT_STORE_PRESIGN_TTL_SECONDS caps the lifetime of every presigned GET
# URL issued by the API (Requirements 21.12, 21.15). Parsed as a positive
# integer; when the env var is missing, empty, non-integer, or non-positive,
# we fall back to the documented default of 3600 seconds rather than failing
# startup, since this is a tunable rather than a secret.
_DEFAULT_PRESIGN_TTL_SECONDS: int = 3600
_raw_presign_ttl: str = os.environ.get(
    'OBJECT_STORE_PRESIGN_TTL_SECONDS', ''
).strip()
try:
    _parsed_presign_ttl: int = (
        int(_raw_presign_ttl) if _raw_presign_ttl else _DEFAULT_PRESIGN_TTL_SECONDS
    )
except ValueError:
    _parsed_presign_ttl = _DEFAULT_PRESIGN_TTL_SECONDS
OBJECT_STORE_PRESIGN_TTL_SECONDS: int = (
    _parsed_presign_ttl
    if _parsed_presign_ttl > 0
    else _DEFAULT_PRESIGN_TTL_SECONDS
)

client = AsyncIOMotorClient(MONGO_URL)
db = client[os.environ.get('DB_NAME', 'fleetguard_db')]

# Verify the replica set config is visible to the driver (Req 4.4, 4.6,
# 12.4). The production ``MONGO_URL`` includes ``?replicaSet=rs0`` so
# change streams and multi-document transactions work on the single-node
# MongoDB_Instance. This is a defensive log — we do not crash here
# because local dev environments often run a standalone mongod without a
# replica set, and the application still functions in that mode. The
# module-level ``logger`` is not yet defined at this point in module
# load order, so emit via the ``logging`` root logger instead.
if "replicaSet=" not in MONGO_URL:
    logging.warning(
        "MONGO_URL is missing ?replicaSet=<name>. Change streams and "
        "transactions require the single-node replica set 'rs0' on the "
        "EC2 MongoDB_Instance — production deployments MUST include "
        "replicaSet=rs0 in the connection string (Req 4.4)."
    )

# JWT Configuration
SECRET_KEY = JWT_SECRET
ALGORITHM = "HS256"
# Default token TTL — 30 days. Long-lived sessions keep drivers
# from being bounced to the login screen mid-shift. The Phase 3
# revocation list (/auth/logout + revoked_tokens) is what
# actually ends sessions early when needed; expiry is the safety
# net.
ACCESS_TOKEN_EXPIRE_HOURS = 24 * 30

# Create the main app
app = FastAPI(title="FleetShield365 API")
api_router = APIRouter(prefix="/api")
security = HTTPBearer()


# ---------------------------------------------------------------------------
# Phase 9 — observability + transport security middleware.
# ---------------------------------------------------------------------------
import contextvars  # noqa: E402

_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


# Email regex used by the PII-redact log filter. Tight enough to
# avoid mangling code-like strings; greedy enough to catch real
# emails inside log messages.
_PII_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)
# Australian mobile pattern + generic 10+ digit runs. We're conservative
# here — anything matching is masked, false positives are acceptable
# (better to mask a vehicle rego that looks like a phone than to leak
# a real number).
_PII_PHONE_RE = re.compile(r"(?<!\d)(?:\+?61\s?)?0?\d{9,10}(?!\d)")


class _PIIRedactingFilter(logging.Filter):
    """Mask emails + phone-like digit runs from log records.

    Applied to the root handler so every log line — including those
    written by third-party libraries — passes through. Field-level
    redaction in handlers (record.email, record.phone) is opt-in via
    the LogRecord ``extra`` kwarg.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            redacted = _PII_EMAIL_RE.sub("<email>", msg)
            redacted = _PII_PHONE_RE.sub("<phone>", redacted)
            if redacted != msg:
                # Rewriting record.msg + clearing args so getMessage()
                # returns the redacted form. Some handlers re-call
                # getMessage(), so we make it stable.
                record.msg = redacted
                record.args = None
        except Exception:
            pass
        return True


class _RequestIdFormatter(logging.Formatter):
    """JSON-shaped log lines including the per-request UUID.

    Phase 9 — gives logs a structure that CloudWatch / Loki / any
    log-shipper can index without regex. The request_id pulls from
    a contextvar so every log line written during a request carries
    the same id, even from deep async tasks.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": _request_id_ctx.get(),
        }
        if record.exc_info:
            import traceback
            payload["exception"] = "".join(traceback.format_exception(*record.exc_info))
        return json.dumps(payload, default=str)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Assign a UUID to every request + echo back as ``X-Request-ID``.

    Phase 9. The id propagates through the contextvar so every log
    line written during request handling carries it. Clients that
    pass their own X-Request-ID header have it honoured (trusts the
    client's correlation id for distributed tracing); otherwise a
    fresh UUID is generated.
    """
    incoming = request.headers.get("X-Request-ID")
    rid = incoming if (incoming and len(incoming) <= 128) else uuid.uuid4().hex
    token = _request_id_ctx.set(rid)
    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response
    finally:
        _request_id_ctx.reset(token)


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """Add transport-security headers to every response.

    Phase 9. CSP intentionally omitted — needs a per-page audit to
    avoid breaking inline scripts and the presigned MinIO image URLs.
    The headers below are universally safe: nosniff prevents MIME
    confusion attacks, DENY blocks click-jacking, strict-origin
    keeps the Referer header from leaking tenant subdomains to
    third-party sites, and HSTS pins HTTPS for a year.
    """
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

# Phase 3 — slowapi rate limiter (per-IP). Defaults are conservative
# (no global rate); endpoints opt in via @limiter.limit decorators.
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Phase 3 — generic unhandled-exception handler. HTTPException keeps its
# intentional message; anything else gets a flat "Internal server error"
# response with the full traceback logged server-side so production
# never leaks a stack trace through the JSON detail field.
from fastapi import Request as _FastAPIRequest
from fastapi.responses import JSONResponse as _JSONResponse


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: _FastAPIRequest, exc: Exception):
    # HTTPException flows through FastAPI's own handler — we only catch
    # the "didn't expect this" case.
    if isinstance(exc, HTTPException):
        raise exc
    logger.exception(
        "Unhandled exception on %s %s",
        request.method, request.url.path,
    )
    return _JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# Log Pydantic validation errors with the offending fields so 422s are
# debuggable in production without having to wire up a request body
# logger. FastAPI's default handler returns the detail list to the
# client but doesn't log it server-side — which means the first
# investigation step (journalctl) yielded only "422 Unprocessable" and
# we had to guess.
from fastapi.exceptions import RequestValidationError as _RequestValidationError


@app.exception_handler(_RequestValidationError)
async def _validation_error_handler(request: _FastAPIRequest, exc: _RequestValidationError):
    try:
        compact = [
            {
                "loc": ".".join(str(p) for p in e.get("loc", []) if p != "body"),
                "msg": e.get("msg"),
                "type": e.get("type"),
            }
            for e in exc.errors()
        ]
    except Exception:
        compact = []
    logger.warning(
        "422 validation on %s %s — %s",
        request.method, request.url.path, compact,
    )
    return _JSONResponse(status_code=422, content={"detail": exc.errors()})

# Timezone helpers for consistent date/time handling
from zoneinfo import ZoneInfo
SYDNEY_TZ = ZoneInfo('Australia/Sydney')
UTC_TZ = ZoneInfo('UTC')
DEFAULT_TIMEZONE = 'Australia/Sydney'

# List of supported timezones
SUPPORTED_TIMEZONES = [
    # Australia
    "Australia/Sydney",
    "Australia/Brisbane", 
    "Australia/Melbourne",
    "Australia/Perth",
    "Australia/Adelaide",
    "Australia/Darwin",
    "Australia/Hobart",
    # New Zealand
    "Pacific/Auckland",
    "Pacific/Fiji",
    # Asia - South
    "Asia/Kolkata",       # India (IST)
    "Asia/Karachi",       # Pakistan (PKT)
    "Asia/Dhaka",         # Bangladesh (BST)
    "Asia/Colombo",       # Sri Lanka
    "Asia/Kathmandu",     # Nepal
    # Asia - Southeast
    "Asia/Singapore",
    "Asia/Bangkok",       # Thailand
    "Asia/Jakarta",       # Indonesia
    "Asia/Manila",        # Philippines
    "Asia/Kuala_Lumpur",  # Malaysia
    "Asia/Ho_Chi_Minh",   # Vietnam
    # Asia - East
    "Asia/Hong_Kong",
    "Asia/Shanghai",      # China
    "Asia/Tokyo",         # Japan
    "Asia/Seoul",         # South Korea
    "Asia/Taipei",        # Taiwan
    # Asia - Middle East
    "Asia/Dubai",         # UAE
    "Asia/Riyadh",        # Saudi Arabia
    "Asia/Qatar",         # Qatar
    "Asia/Kuwait",        # Kuwait
    "Asia/Jerusalem",     # Israel
    "Asia/Tehran",        # Iran
    # Europe
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Europe/Amsterdam",
    "Europe/Rome",
    "Europe/Madrid",
    "Europe/Dublin",
    "Europe/Brussels",
    "Europe/Vienna",
    "Europe/Stockholm",
    "Europe/Warsaw",
    "Europe/Moscow",
    "Europe/Istanbul",
    # Americas - North
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Toronto",
    "America/Vancouver",
    "America/Phoenix",
    # Americas - Central/South
    "America/Mexico_City",
    "America/Sao_Paulo",
    "America/Buenos_Aires",
    "America/Lima",
    "America/Bogota",
    "America/Santiago",
    # Africa
    "Africa/Johannesburg",
    "Africa/Cairo",
    "Africa/Lagos",
    "Africa/Nairobi",
    "Africa/Casablanca",
    # UTC
    "UTC",
]

def get_timezone(tz_name: str) -> ZoneInfo:
    """Get ZoneInfo for a timezone name, with fallback to Sydney"""
    try:
        return ZoneInfo(tz_name) if tz_name else SYDNEY_TZ
    except Exception:
        return SYDNEY_TZ

def format_timestamp(timestamp_str: str, timezone: str = DEFAULT_TIMEZONE) -> str:
    """Convert ISO timestamp to specified timezone formatted string (DD/MM/YYYY HH:MM)"""
    try:
        if not timestamp_str:
            return 'N/A'
        # Parse the timestamp
        if isinstance(timestamp_str, datetime):
            dt = timestamp_str
        else:
            # Handle various ISO formats
            timestamp_str = str(timestamp_str).replace('Z', '+00:00')
            if '+' not in timestamp_str and 'T' in timestamp_str:
                timestamp_str += '+00:00'
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        
        # If naive datetime, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC_TZ)
        
        # Convert to specified timezone
        tz = get_timezone(timezone)
        local_dt = dt.astimezone(tz)
        return local_dt.strftime('%d/%m/%Y %H:%M')
    except Exception as e:
        return str(timestamp_str)[:16] if timestamp_str else 'N/A'

# Keep old function name for backward compatibility
def format_timestamp_sydney(timestamp_str: str) -> str:
    """Legacy function - use format_timestamp with timezone parameter instead"""
    return format_timestamp(timestamp_str, DEFAULT_TIMEZONE)

async def get_company_timezone(db, company_id: str) -> str:
    """Get the timezone setting for a company"""
    try:
        if not company_id:
            return DEFAULT_TIMEZONE
        company = await db.companies.find_one({"_id": ObjectId(company_id)}, {"timezone": 1})
        return company.get("timezone", DEFAULT_TIMEZONE) if company else DEFAULT_TIMEZONE
    except Exception:
        return DEFAULT_TIMEZONE

def get_today_range_for_timezone(timezone: str = DEFAULT_TIMEZONE):
    """Get start and end of 'today' in specified timezone, returned as UTC datetimes."""
    tz = get_timezone(timezone)
    now_local = datetime.now(tz)
    today_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end_local = today_start_local + timedelta(days=1)
    
    # Convert to UTC (naive datetime for MongoDB)
    today_start_utc = today_start_local.astimezone(UTC_TZ).replace(tzinfo=None)
    today_end_utc = today_end_local.astimezone(UTC_TZ).replace(tzinfo=None)
    
    return today_start_utc, today_end_utc

def get_sydney_today_range():
    """Get start and end of 'today' in Sydney timezone, returned as UTC datetimes.
    Use this for ALL 'today' queries to ensure dashboard and detail views match."""
    now_sydney = datetime.now(SYDNEY_TZ)
    today_start_sydney = now_sydney.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end_sydney = today_start_sydney + timedelta(days=1)
    
    # Convert to UTC (naive datetime for MongoDB)
    today_start_utc = today_start_sydney.astimezone(UTC_TZ).replace(tzinfo=None)
    today_end_utc = today_end_sydney.astimezone(UTC_TZ).replace(tzinfo=None)
    
    return today_start_utc, today_end_utc

def parse_date_flexible(date_str: str) -> datetime:
    """Parse date string in various formats: DD/MM/YYYY, YYYY-MM-DD, or ISO format.
    Returns datetime object or None if parsing fails."""
    if not date_str or date_str.upper() == "NA":
        return None
    
    date_str = date_str.strip()
    
    # Try DD/MM/YYYY format first
    if '/' in date_str:
        try:
            parts = date_str.split('/')
            if len(parts) == 3:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                return datetime(year, month, day)
        except (ValueError, IndexError):
            pass
    
    # Try YYYY-MM-DD format
    if '-' in date_str and len(date_str) >= 10:
        try:
            return datetime.strptime(date_str[:10], '%Y-%m-%d')
        except ValueError:
            pass
    
    # Try ISO format
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        pass
    
    return None

def format_date_display(date_str: str) -> str:
    """Convert any date format to DD/MM/YYYY for display."""
    parsed = parse_date_flexible(date_str)
    if parsed:
        return parsed.strftime('%d/%m/%Y')
    return date_str or 'N/A'

def get_sydney_date_as_utc(date_str: str, is_end_of_day: bool = False):
    """Convert a date string (YYYY-MM-DD) to UTC datetime, treating it as Sydney timezone.
    Use this when clients pass date filters to ensure consistent interpretation."""
    try:
        # Parse the date
        date_parts = date_str.split('-')
        year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
        
        # Create datetime in Sydney timezone
        if is_end_of_day:
            sydney_dt = datetime(year, month, day, 23, 59, 59, tzinfo=SYDNEY_TZ)
        else:
            sydney_dt = datetime(year, month, day, 0, 0, 0, tzinfo=SYDNEY_TZ)
        
        # Convert to UTC (naive for MongoDB)
        return sydney_dt.astimezone(UTC_TZ).replace(tzinfo=None)
    except Exception:
        # Fallback to direct parse if format is different
        return datetime.fromisoformat(date_str)

# Universal in-memory cache for API responses
api_cache: Dict[str, Any] = {}
CACHE_TTL = {
    "dashboard": 30,    # Dashboard stats: 30 seconds
    "vehicles": 30,     # Vehicles list: 30 seconds
    "drivers": 30,      # Drivers list: 30 seconds
    "inspections": 15,  # Inspections: 15 seconds (more dynamic)
}

def get_cached(cache_type: str, company_id: str) -> Optional[Any]:
    """Get cached data if still valid"""
    cache_key = f"{cache_type}_{company_id}"
    if cache_key in api_cache:
        cached = api_cache[cache_key]
        ttl = CACHE_TTL.get(cache_type, 30)
        if utcnow().timestamp() - cached["timestamp"] < ttl:
            return cached["data"]
    return None

def set_cached(cache_type: str, company_id: str, data: Any):
    """Cache API response data"""
    cache_key = f"{cache_type}_{company_id}"
    api_cache[cache_key] = {
        "timestamp": utcnow().timestamp(),
        "data": data
    }

def invalidate_cache(cache_type: str, company_id: str):
    """Invalidate cache when data changes"""
    cache_key = f"{cache_type}_{company_id}"
    if cache_key in api_cache:
        del api_cache[cache_key]

# Legacy functions for backwards compatibility
dashboard_cache = api_cache
CACHE_TTL_SECONDS = 30

def get_cached_stats(company_id: str) -> Optional[dict]:
    return get_cached("dashboard", company_id)

def set_cached_stats(company_id: str, data: dict):
    set_cached("dashboard", company_id, data)

# Configure logging
# Phase 9 — JSON log formatter + PII redaction filter applied at the
# root handler so every log line written by every module passes
# through. Falls back to the original human-readable format when
# LOG_JSON env var is "false" — useful in local dev.
_LOG_JSON_ENABLED = os.environ.get("LOG_JSON", "true").strip().lower() not in ("false", "0", "no")
_log_root = logging.getLogger()
for _h in list(_log_root.handlers):
    _log_root.removeHandler(_h)
_stream_handler = logging.StreamHandler()
if _LOG_JSON_ENABLED:
    _stream_handler.setFormatter(_RequestIdFormatter())
else:
    _stream_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
_stream_handler.addFilter(_PIIRedactingFilter())
_log_root.addHandler(_stream_handler)
_log_root.setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# SMTP Configuration (Namecheap PrivateEmail) — two mailboxes:
#   alerts@   — operational alerts (inspections, incidents, expiries, summaries, contact)
#   noreply@  — system emails (verification, invites, password reset)
SMTP_HOST             = os.environ.get('SMTP_HOST', 'mail.privateemail.com')
SMTP_PORT             = int(os.environ.get('SMTP_PORT', '465') or 465)
SMTP_USER             = os.environ.get('SMTP_USER', '')
SMTP_PASSWORD         = os.environ.get('SMTP_PASSWORD', '')
SMTP_NOREPLY_USER     = os.environ.get('SMTP_NOREPLY_USER', '')
SMTP_NOREPLY_PASSWORD = os.environ.get('SMTP_NOREPLY_PASSWORD', '')
SENDER_EMAIL          = os.environ.get('SENDER_EMAIL', 'alerts@fleetshield365.com')
NOREPLY_EMAIL         = os.environ.get('NOREPLY_EMAIL', 'noreply@fleetshield365.com')

# Brand colour used in email templates.
_BRAND_PRIMARY = "#0d9488"
_BRAND_DARK    = "#0f172a"


def _email_template_branded(heading: str, body_html: str, button_label: str = None, button_url: str = None) -> str:
    """Render a branded FleetShield365 HTML email.

    `body_html` is the main content block (one or more <p> tags). When
    `button_label` and `button_url` are both supplied, a primary CTA button is
    inserted between body and footer.
    """
    button_html = ""
    if button_label and button_url:
        button_html = (
            f'<div style="text-align:center; margin:32px 0;">'
            f'  <a href="{button_url}" '
            f'     style="background-color:{_BRAND_PRIMARY}; color:#ffffff; padding:14px 32px; '
            f'            text-decoration:none; border-radius:8px; font-weight:600; display:inline-block;">'
            f'{button_label}</a>'
            f'</div>'
        )
    return f"""
    <html>
    <body style="font-family:Arial,Helvetica,sans-serif; padding:20px; background-color:#f8fafc; margin:0;">
      <div style="max-width:560px; margin:0 auto; background:#ffffff; padding:36px 32px; border-radius:14px; box-shadow:0 2px 8px rgba(15,23,42,0.06);">
        <div style="text-align:center; margin-bottom:24px;">
          <div style="display:inline-block; padding:10px 18px; background-color:{_BRAND_DARK}; border-radius:10px;">
            <span style="color:{_BRAND_PRIMARY}; font-size:20px; font-weight:700; letter-spacing:0.5px;">FleetShield365</span>
          </div>
        </div>
        <h2 style="color:{_BRAND_DARK}; margin:0 0 16px 0; font-size:22px;">{heading}</h2>
        <div style="color:#475569; font-size:15px; line-height:1.6;">
          {body_html}
        </div>
        {button_html}
        <hr style="border:none; border-top:1px solid #e2e8f0; margin:28px 0 16px 0;">
        <p style="color:#94a3b8; font-size:12px; margin:0; text-align:center;">
          FleetShield365 — Equipment Inspection &amp; Fleet Management<br>
          This is an automated message. Please do not reply directly.
        </p>
      </div>
    </body>
    </html>
    """


async def _send_via_smtp(to_email: str, subject: str, html_content: str, *, sender: str = "alerts") -> bool:
    """Internal helper. Routes to one of two mailboxes:
       sender="alerts"  → alerts@   (operational alerts; uses SMTP_USER/SMTP_PASSWORD)
       sender="noreply" → noreply@  (system emails; uses SMTP_NOREPLY_USER/SMTP_NOREPLY_PASSWORD)
    """
    if sender == "noreply":
        from_addr = SMTP_NOREPLY_USER or NOREPLY_EMAIL
        password  = SMTP_NOREPLY_PASSWORD
        from_name = "FleetShield365"
    else:
        from_addr = SMTP_USER or SENDER_EMAIL
        password  = SMTP_PASSWORD
        from_name = "FleetShield365 Alerts"

    if not password:
        logger.warning(f"[SMTP:{sender}] mailbox password not configured, skipping email to {to_email}")
        return False
    try:
        msg = EmailMessage()
        msg["From"]    = f"{from_name} <{from_addr}>"
        msg["To"]      = to_email
        msg["Subject"] = subject
        msg.set_content("This email requires an HTML-capable mail client.")
        msg.add_alternative(html_content, subtype="html")
        await aiosmtplib.send(
            msg,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=from_addr,
            password=password,
            use_tls=(SMTP_PORT == 465),
            start_tls=(SMTP_PORT == 587),
            timeout=30,
        )
        logger.info(f"[SMTP:{sender}] Email sent to {to_email}: {subject}")
        return True
    except Exception as e:
        logger.error(f"[SMTP:{sender}] Error sending email to {to_email}: {e}")
        return False


async def send_email_notification(to_email: str, subject: str, html_content: str):
    """Send an operational/alert email via the alerts@ mailbox."""
    return await _send_via_smtp(to_email, subject, html_content, sender="alerts")


async def send_system_email(to_email: str, subject: str, html_content: str):
    """Send a system email (verification, invite, password reset) via the noreply@ mailbox."""
    return await _send_via_smtp(to_email, subject, html_content, sender="noreply")

async def send_expiry_alert_email(admin_email: str, company_name: str, alerts: List[dict]):
    """Send expiry alert email to admin"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Expiry Alerts</h2>
        <p>Hi {company_name} Admin,</p>
        <p>The following items require your attention:</p>
        <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
            <tr style="background-color: #1E293B; color: white;">
                <th style="padding: 12px; text-align: left;">Item</th>
                <th style="padding: 12px; text-align: left;">Type</th>
                <th style="padding: 12px; text-align: left;">Expiry Date</th>
                <th style="padding: 12px; text-align: left;">Status</th>
            </tr>
            {''.join([f'''
            <tr style="border-bottom: 1px solid #E2E8F0;">
                <td style="padding: 12px;">{alert.get('item_name', 'N/A')}</td>
                <td style="padding: 12px;">{alert.get('alert_type', 'N/A')}</td>
                <td style="padding: 12px;">{alert.get('expiry_date', 'N/A')}</td>
                <td style="padding: 12px; color: {'#DC2626' if alert.get('is_expired') else '#F97316'};">
                    {'EXPIRED' if alert.get('is_expired') else 'Expiring Soon'}
                </td>
            </tr>
            ''' for alert in alerts])}
        </table>
        <p>Please log in to FleetShield365 to take action.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] {len(alerts)} Expiry Alert(s) Require Attention", html_content)

async def send_issue_alert_email(admin_email: str, company_name: str, vehicle_name: str, driver_name: str, issue_summary: str, inspection_type: str, photos: List[dict] = None, inspection_id: str = None, extra_details: dict = None):
    """Send issue alert email when an inspection has issues - WITH PHOTOS"""
    
    # Build photo HTML if photos provided
    photo_html = ""
    if photos and len(photos) > 0:
        photo_html = """
        <div style="margin: 20px 0;">
            <h3 style="color: #374151;">Inspection Photos:</h3>
            <div style="display: flex; flex-wrap: wrap; gap: 10px;">
        """
        for photo in photos[:8]:  # Limit to 8 photos
            photo_type = photo.get('photo_type', 'Photo').replace('_', ' ').title()
            base64_data = photo.get('base64_data', '')
            if base64_data:
                # Ensure proper data URL format
                if not base64_data.startswith('data:'):
                    base64_data = f"data:image/jpeg;base64,{base64_data}"
                photo_html += f"""
                <div style="text-align: center;">
                    <img src="{base64_data}" style="width: 150px; height: 120px; object-fit: cover; border-radius: 8px; border: 2px solid {'#DC2626' if 'damage' in photo_type.lower() else '#E5E7EB'};" />
                    <p style="font-size: 11px; color: #6B7280; margin: 4px 0;">{photo_type}</p>
                </div>
                """
        photo_html += "</div></div>"
    
    # Build extra details rows (fuel, odometer, cleanliness, checklist etc)
    extra_rows = ""
    if extra_details:
        if extra_details.get("odometer"):
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Odometer:</td><td style="padding: 8px 0;">{extra_details["odometer"]} km</td></tr>'
        if extra_details.get("fuel_level"):
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Fuel Level:</td><td style="padding: 8px 0;">{extra_details["fuel_level"]}</td></tr>'
        if extra_details.get("cleanliness"):
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Cleanliness:</td><td style="padding: 8px 0;">{extra_details["cleanliness"]}</td></tr>'
        if extra_details.get("incident_today"):
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Incident Today:</td><td style="padding: 8px 0; color: #DC2626; font-weight: bold;">Yes</td></tr>'
        if extra_details.get("incident_comment"):
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Incident Details:</td><td style="padding: 8px 0;">{extra_details["incident_comment"]}</td></tr>'
        if extra_details.get("total_items") and extra_details.get("failed_items"):
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Checklist:</td><td style="padding: 8px 0; color: #DC2626; font-weight: bold;">{extra_details["failed_items"]} of {extra_details["total_items"]} items failed</td></tr>'
        if extra_details.get("checklist_issues"):
            issues_list = ', '.join(extra_details["checklist_issues"])
            extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">Failed Items:</td><td style="padding: 8px 0;">{issues_list}</td></tr>'
        if extra_details.get("checklist_comments"):
            for item_name, comment in extra_details["checklist_comments"].items():
                extra_rows += f'<tr><td style="padding: 8px 0; color: #6B7280;">{item_name} Note:</td><td style="padding: 8px 0; font-style: italic;">{comment}</td></tr>'

    # Dashboard link
    dashboard_link = f"https://www.fleetshield365.com/dashboard"
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #DC2626; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">DEFECT ALERT — Immediate Attention Required</h2>
        </div>
        
        <div style="border: 1px solid #E5E7EB; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
            <p>Hi {company_name} Admin,</p>
            <p><strong>A defect has been reported and requires your immediate attention:</strong></p>
            
            <div style="background-color: #FEF2F2; border-left: 4px solid #DC2626; padding: 16px; margin: 20px 0;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 8px 0; color: #6B7280;">Vehicle:</td><td style="padding: 8px 0; font-weight: bold;">{vehicle_name}</td></tr>
                    <tr><td style="padding: 8px 0; color: #6B7280;">Driver:</td><td style="padding: 8px 0;">{driver_name}</td></tr>
                    <tr><td style="padding: 8px 0; color: #6B7280;">Inspection Type:</td><td style="padding: 8px 0;">{inspection_type}</td></tr>
                    <tr><td style="padding: 8px 0; color: #6B7280;">Time:</td><td style="padding: 8px 0;">{datetime.now(SYDNEY_TZ).strftime('%I:%M %p, %B %d, %Y')} (Sydney)</td></tr>
                    {extra_rows}
                </table>
                <hr style="border: none; border-top: 1px solid #FECACA; margin: 15px 0;" />
                <p style="color: #DC2626; font-weight: bold; margin: 0;">Issue Reported:</p>
                <p style="color: #991B1B; margin: 8px 0 0 0;">{issue_summary}</p>
            </div>
            
            {photo_html}
            
            <div style="margin-top: 25px; text-align: center;">
                <a href="{dashboard_link}" style="background-color: #0891B2; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">View Full Inspection Report</a>
            </div>
            
            <p style="color: #9CA3AF; font-size: 12px; margin-top: 30px; text-align: center;">
                This is an automated alert from FleetShield365.<br/>
                Vehicle may need to be taken off road pending inspection.
            </p>
        </div>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[DEFECT ALERT] {vehicle_name} — {issue_summary[:50]}", html_content)

async def send_missed_inspection_email(admin_email: str, company_name: str, vehicles: List[dict]):
    """Send missed inspection alert email"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Missed Inspection Alert</h2>
        <p>Hi {company_name} Admin,</p>
        <p>The following vehicles did not complete their prestart inspection today:</p>
        <ul style="margin: 20px 0;">
            {''.join([f'<li style="padding: 8px 0;">{v.get("name", "Unknown")} ({v.get("registration_number", "N/A")})</li>' for v in vehicles])}
        </ul>
        <p>Please follow up with the assigned drivers.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] {len(vehicles)} Vehicle(s) Missed Inspection Today", html_content)

async def send_repeated_issues_email(company_id: str, vehicle_name: str, recent_inspections: list):
    """Send detailed repeated issues email showing pattern of failures"""
    # Get driver names for each inspection
    driver_ids = list(set(i.get("driver_id") for i in recent_inspections if i.get("driver_id")))
    drivers = {}
    for did in driver_ids:
        driver = await db.users.find_one({"_id": ObjectId(did)})
        if driver:
            drivers[did] = driver.get("name", driver.get("username", "Unknown"))
    
    # Get company info
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "Your Company") if company else "Your Company"
    
    # Build issue history rows
    issue_rows = ""
    for insp in recent_inspections:
        driver_name = drivers.get(insp.get("driver_id", ""), "Unknown")
        insp_type = insp.get("type", "prestart").replace("_", " ").title()
        timestamp = insp.get("timestamp")
        date_str = format_timestamp_sydney(timestamp) if timestamp else "N/A"
        
        # Get issue description
        if insp.get("type") == "end_shift":
            issue = insp.get("damage_comment", "Damage reported")
            if insp.get("incident_today"):
                issue += f" | Incident: {insp.get('incident_comment', 'reported')}"
        else:
            checklist = insp.get("checklist_items", [])
            failed = [item.get("name", "") for item in checklist if item.get("status") == "issue"]
            issue = ", ".join(failed) if failed else "Issues reported"
        
        issue_rows += f"""
        <tr>
            <td style="padding: 10px; border-bottom: 1px solid #E5E7EB; font-size: 13px;">{date_str}</td>
            <td style="padding: 10px; border-bottom: 1px solid #E5E7EB; font-size: 13px;">{insp_type}</td>
            <td style="padding: 10px; border-bottom: 1px solid #E5E7EB; font-size: 13px;">{driver_name}</td>
            <td style="padding: 10px; border-bottom: 1px solid #E5E7EB; font-size: 13px;">{issue[:60]}</td>
        </tr>
        """
    
    count = len(recent_inspections)
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; max-width: 650px; margin: 0 auto;">
        <div style="background-color: #F97316; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">REPEATED ISSUES — {vehicle_name}</h2>
        </div>
        
        <div style="border: 1px solid #E5E7EB; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
            <p>Hi {company_name} Admin,</p>
            <p><strong>{vehicle_name}</strong> has had <strong style="color: #DC2626;">{count} issues in the last 7 days</strong>. This pattern suggests the vehicle may need a full inspection or should be taken offline.</p>
            
            <h3 style="color: #374151; margin-top: 20px;">Issue History:</h3>
            <table style="width: 100%; border-collapse: collapse; margin: 15px 0;">
                <thead>
                    <tr style="background-color: #F8FAFC;">
                        <th style="padding: 10px; text-align: left; border-bottom: 2px solid #E5E7EB; font-size: 12px; color: #6B7280;">Date/Time</th>
                        <th style="padding: 10px; text-align: left; border-bottom: 2px solid #E5E7EB; font-size: 12px; color: #6B7280;">Type</th>
                        <th style="padding: 10px; text-align: left; border-bottom: 2px solid #E5E7EB; font-size: 12px; color: #6B7280;">Driver</th>
                        <th style="padding: 10px; text-align: left; border-bottom: 2px solid #E5E7EB; font-size: 12px; color: #6B7280;">Issue</th>
                    </tr>
                </thead>
                <tbody>
                    {issue_rows}
                </tbody>
            </table>
            
            <div style="background-color: #FFF7ED; border-left: 4px solid #F97316; padding: 16px; margin: 20px 0;">
                <p style="color: #9A3412; font-weight: bold; margin: 0;">Recommendation:</p>
                <p style="color: #9A3412; margin: 8px 0 0 0;">Consider taking {vehicle_name} offline for a full mechanical inspection before further use.</p>
            </div>
            
            <div style="margin-top: 25px; text-align: center;">
                <a href="https://www.fleetshield365.com/dashboard" style="background-color: #F97316; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">View Vehicle History</a>
            </div>
            
            <p style="color: #9CA3AF; font-size: 12px; margin-top: 30px; text-align: center;">
                This is an automated pattern detection alert from FleetShield365.
            </p>
        </div>
    </body>
    </html>
    """
    
    # Send to all admins
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None
    }).to_list(100)
    
    for admin in admins:
        if admin.get("email"):
            await send_email_notification(
                admin["email"],
                f"[PATTERN ALERT] {vehicle_name} — {count} issues in 7 days",
                html_content
            )

async def send_missed_inspection_email(admin_email: str, company_name: str, vehicles: List[dict]):
    """Send missed inspection alert email"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Missed Inspection Alert</h2>
        <p>Hi {company_name} Admin,</p>
        <p>The following vehicles did not complete their prestart inspection today:</p>
        <ul style="margin: 20px 0;">
            {''.join([f'<li style="padding: 8px 0;">{v.get("name", "Unknown")} ({v.get("registration_number", "N/A")})</li>' for v in vehicles])}
        </ul>
        <p>Please follow up with the assigned drivers.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] {len(vehicles)} Vehicle(s) Missed Inspection Today", html_content)

async def send_daily_summary_email(admin_email: str, company_name: str, summary: dict):
    """Send daily summary email"""
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #F97316;">FleetShield365 Daily Summary</h2>
        <p>Hi {company_name} Admin,</p>
        <p>Here's your fleet summary for today:</p>
        <div style="background-color: #F8FAFC; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <p><strong>Inspections Completed:</strong> {summary.get('completed', 0)}</p>
            <p><strong>Inspections Missed:</strong> {summary.get('missed', 0)}</p>
            <p><strong>Issues Reported:</strong> {summary.get('issues', 0)}</p>
            <p><strong>Fuel Submissions:</strong> {summary.get('fuel_submissions', 0)}</p>
            <p><strong>Total Fuel (L):</strong> {summary.get('total_fuel', 0):.1f}</p>
        </div>
        <p>Log in to FleetShield365 for detailed reports.</p>
        <p style="color: #64748B; font-size: 12px;">This is an automated message from FleetShield365.</p>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[FleetShield365] Daily Summary - {datetime.now(SYDNEY_TZ).strftime('%B %d, %Y')}", html_content)

async def generate_weekly_summary():
    """Generate and send weekly summary email to all company admins"""
    try:
        now = datetime.now(SYDNEY_TZ)
        week_ago = now - timedelta(days=7)
        week_ago_utc = week_ago.astimezone(timezone.utc).replace(tzinfo=None)
        
        companies = await db.companies.find().to_list(1000)
        
        for company in companies:
            company_id = str(company["_id"])
            company_name = company.get("name", "Your Company")
            
            # Gather weekly stats
            total_inspections = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": week_ago_utc}
            })
            
            passed_inspections = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": week_ago_utc},
                "is_safe": True
            })
            
            failed_inspections = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": week_ago_utc},
                "is_safe": False
            })
            
            prestart_count = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": week_ago_utc},
                "type": "prestart"
            })
            
            endshift_count = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": week_ago_utc},
                "type": "end_shift"
            })
            
            incidents = await db.incidents.count_documents({
                "company_id": company_id,
                "created_at": {"$gte": week_ago_utc}
            })
            
            fuel_pipeline = [
                {"$match": {"company_id": company_id, "timestamp": {"$gte": week_ago_utc}}},
                {"$group": {"_id": None, "total_litres": {"$sum": "$litres"}, "total_cost": {"$sum": "$total_cost"}, "count": {"$sum": 1}}}
            ]
            fuel_result = await db.fuel_submissions.aggregate(fuel_pipeline).to_list(1)
            fuel_data = fuel_result[0] if fuel_result else {"total_litres": 0, "total_cost": 0, "count": 0}
            
            total_vehicles = await db.vehicles.count_documents({"company_id": company_id})
            total_drivers = await db.users.count_documents({"company_id": company_id, "role": "driver"})
            
            pass_rate = round((passed_inspections / total_inspections * 100), 1) if total_inspections > 0 else 0
            
            # Determine pass rate color
            if pass_rate >= 90:
                rate_color = "#16A34A"
                rate_label = "Excellent"
            elif pass_rate >= 70:
                rate_color = "#EAB308"
                rate_label = "Needs Attention"
            else:
                rate_color = "#DC2626"
                rate_label = "Critical"
            
            week_start = week_ago.strftime('%d %b')
            week_end = now.strftime('%d %b %Y')
            
            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; padding: 20px; max-width: 600px; margin: 0 auto;">
                <div style="background-color: #0891B2; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0;">
                    <h2 style="margin: 0;">FleetShield365 Weekly Summary</h2>
                    <p style="margin: 5px 0 0 0; opacity: 0.9;">{week_start} - {week_end}</p>
                </div>
                
                <div style="border: 1px solid #E5E7EB; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
                    <p>Hi {company_name} Admin,</p>
                    <p>Here's your weekly fleet overview:</p>
                    
                    <!-- Pass Rate Banner -->
                    <div style="background-color: #F8FAFC; border-left: 4px solid {rate_color}; padding: 16px; margin: 20px 0; text-align: center;">
                        <p style="font-size: 36px; font-weight: bold; color: {rate_color}; margin: 0;">{pass_rate}%</p>
                        <p style="color: #6B7280; margin: 5px 0 0 0;">Inspection Pass Rate — {rate_label}</p>
                    </div>
                    
                    <!-- Stats Grid -->
                    <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                        <tr>
                            <td style="padding: 12px; background-color: #F0FDFA; border-radius: 8px; text-align: center; width: 33%;">
                                <p style="font-size: 24px; font-weight: bold; color: #0891B2; margin: 0;">{total_inspections}</p>
                                <p style="color: #6B7280; font-size: 12px; margin: 4px 0 0 0;">Total Inspections</p>
                            </td>
                            <td style="width: 4%;"></td>
                            <td style="padding: 12px; background-color: #F0FDF4; border-radius: 8px; text-align: center; width: 33%;">
                                <p style="font-size: 24px; font-weight: bold; color: #16A34A; margin: 0;">{passed_inspections}</p>
                                <p style="color: #6B7280; font-size: 12px; margin: 4px 0 0 0;">Passed</p>
                            </td>
                            <td style="width: 4%;"></td>
                            <td style="padding: 12px; background-color: #FEF2F2; border-radius: 8px; text-align: center; width: 33%;">
                                <p style="font-size: 24px; font-weight: bold; color: #DC2626; margin: 0;">{failed_inspections}</p>
                                <p style="color: #6B7280; font-size: 12px; margin: 4px 0 0 0;">Failed</p>
                            </td>
                        </tr>
                    </table>
                    
                    <!-- Breakdown -->
                    <h3 style="color: #374151; border-bottom: 1px solid #E5E7EB; padding-bottom: 8px;">Breakdown</h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr><td style="padding: 8px 0; color: #6B7280;">Pre-start Inspections:</td><td style="padding: 8px 0; font-weight: bold;">{prestart_count}</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">End-of-Shift Inspections:</td><td style="padding: 8px 0; font-weight: bold;">{endshift_count}</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">Incidents Reported:</td><td style="padding: 8px 0; font-weight: bold; color: {'#DC2626' if incidents > 0 else '#16A34A'};">{incidents}</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">Fuel Submissions:</td><td style="padding: 8px 0; font-weight: bold;">{fuel_data.get('count', 0)}</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">Total Fuel:</td><td style="padding: 8px 0; font-weight: bold;">{fuel_data.get('total_litres', 0):.1f} L</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">Fuel Spend:</td><td style="padding: 8px 0; font-weight: bold;">${fuel_data.get('total_cost', 0):,.2f}</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">Active Vehicles:</td><td style="padding: 8px 0; font-weight: bold;">{total_vehicles}</td></tr>
                        <tr><td style="padding: 8px 0; color: #6B7280;">Active Drivers:</td><td style="padding: 8px 0; font-weight: bold;">{total_drivers}</td></tr>
                    </table>
                    
                    <div style="margin-top: 25px; text-align: center;">
                        <a href="https://www.fleetshield365.com/dashboard" style="background-color: #0891B2; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">View Full Dashboard</a>
                    </div>
                    
                    <p style="color: #9CA3AF; font-size: 12px; margin-top: 30px; text-align: center;">
                        This weekly summary is sent every Monday at 7:00 AM (Sydney time).<br/>
                        FleetShield365 — A product of Prime Mover Rentals Pty Ltd.
                    </p>
                </div>
            </body>
            </html>
            """
            
            # Send to admins who haven't opted out. Phase 6 (2026-05-18):
            # the weekly summary is now per-user opt-in. ``weekly_summary``
            # is the new flag (default True so users who never visited the
            # toggle still receive the digest); ``email_enabled`` is the
            # master kill-switch.
            admins = await db.users.find({
                "company_id": company_id,
                "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None
            }).to_list(100)

            admin_ids = [str(a["_id"]) for a in admins]
            all_prefs = await db.notification_preferences.find(
                {"user_id": {"$in": admin_ids}}
            ).to_list(100)
            prefs_map = {p["user_id"]: p for p in all_prefs}

            for admin in admins:
                if not admin.get("email"):
                    continue
                prefs = prefs_map.get(str(admin["_id"]), {})
                if not prefs.get("email_enabled", True):
                    continue
                if not prefs.get("weekly_summary", True):
                    continue
                await send_email_notification(
                    admin["email"],
                    f"[FleetShield365] Weekly Summary — {week_start} to {week_end}",
                    html_content
                )
        
        logger.info("Weekly summary emails sent to all companies")
    except Exception as e:
        logger.error(f"Failed to generate weekly summary: {e}")

async def weekly_summary_scheduler():
    """Background task that sends weekly summary every Monday at 7 AM Sydney time"""
    while True:
        try:
            now = datetime.now(SYDNEY_TZ)
            # Calculate next Monday 7 AM
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0 and now.hour >= 7:
                days_until_monday = 7
            next_monday = now.replace(hour=7, minute=0, second=0, microsecond=0) + timedelta(days=days_until_monday)
            wait_seconds = (next_monday - now).total_seconds()
            logger.info(f"Weekly summary scheduled for {next_monday.strftime('%Y-%m-%d %H:%M %Z')} (in {wait_seconds/3600:.1f} hours)")
            await asyncio.sleep(wait_seconds)
            await generate_weekly_summary()
        except Exception as e:
            logger.error(f"Weekly summary scheduler error: {e}")
            await asyncio.sleep(3600)  # Retry in 1 hour on error


async def _next_run_at(hour: int, minute: int = 0) -> float:
    """Seconds until the next Sydney-local hour:minute. If we're past today's
    slot, schedule for tomorrow."""
    now = datetime.now(SYDNEY_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


async def generate_daily_summary():
    """Per-company daily summary email at 8 PM Sydney time.

    Sends only to admins whose notification_preferences.daily_summary is True
    (default OFF — opt-in). Gathers today's prestart/end-shift counts,
    issues raised, fuel logs."""
    try:
        now = datetime.now(SYDNEY_TZ)
        day_start_local = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc = day_start_local.astimezone(timezone.utc).replace(tzinfo=None)
        day_end_utc = now.astimezone(timezone.utc).replace(tzinfo=None)

        companies = await db.companies.find({"deleted_at": None}).to_list(1000)

        for company in companies:
            company_id = str(company["_id"])
            company_name = company.get("name", "Your Company")

            completed = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": day_start_utc, "$lte": day_end_utc},
            })
            issues = await db.inspections.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": day_start_utc, "$lte": day_end_utc},
                "is_safe": False,
            })
            fuel_subs = await db.fuel_submissions.count_documents({
                "company_id": company_id,
                "timestamp": {"$gte": day_start_utc, "$lte": day_end_utc},
            })
            fuel_cursor = db.fuel_submissions.find({
                "company_id": company_id,
                "timestamp": {"$gte": day_start_utc, "$lte": day_end_utc},
            }, {"litres": 1, "_id": 0})
            total_fuel = 0.0
            async for doc in fuel_cursor:
                try:
                    total_fuel += float(doc.get("litres") or 0)
                except (TypeError, ValueError):
                    pass

            active_vehicles = await db.vehicles.count_documents({
                "company_id": company_id,
                "deleted_at": None,
                "is_active": True,
            })
            missed = max(0, active_vehicles - completed)

            summary = {
                "completed": completed,
                "missed": missed,
                "issues": issues,
                "fuel_submissions": fuel_subs,
                "total_fuel": total_fuel,
            }

            admins = await db.users.find({
                "company_id": company_id,
                "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None,
            }).to_list(100)
            for admin in admins:
                if not admin.get("email"):
                    continue
                prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])}) or {}
                if not prefs.get("email_enabled", True):
                    continue
                if not prefs.get("daily_summary", False):
                    continue
                await send_daily_summary_email(admin["email"], company_name, summary)

        logger.info("Daily summary run complete")
    except Exception as e:
        logger.error(f"Daily summary generation failed: {e}")


async def daily_summary_scheduler():
    """Runs generate_daily_summary every day at 8 PM Sydney time."""
    while True:
        try:
            wait_seconds = await _next_run_at(hour=20, minute=0)
            logger.info(f"Daily summary scheduled in {wait_seconds/3600:.1f} hours")
            await asyncio.sleep(wait_seconds)
            await generate_daily_summary()
        except Exception as e:
            logger.error(f"Daily summary scheduler error: {e}")
            await asyncio.sleep(3600)


async def generate_missed_inspection_check():
    """Per-company nightly check at 11:30 PM Sydney — emails admins whose
    notification_preferences.missed_inspection_alerts is True about any
    active vehicle that didn't get a prestart today."""
    try:
        now = datetime.now(SYDNEY_TZ)
        day_start_local = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc = day_start_local.astimezone(timezone.utc).replace(tzinfo=None)
        day_end_utc = now.astimezone(timezone.utc).replace(tzinfo=None)

        companies = await db.companies.find({"deleted_at": None}).to_list(1000)

        for company in companies:
            company_id = str(company["_id"])
            company_name = company.get("name", "Your Company")

            active_vehicles = await db.vehicles.find({
                "company_id": company_id,
                "deleted_at": None,
                "is_active": True,
            }).to_list(2000)
            if not active_vehicles:
                continue

            # Vehicle IDs that DID get a prestart inspection today
            inspected_ids = await db.inspections.distinct("vehicle_id", {
                "company_id": company_id,
                "type": "prestart",
                "timestamp": {"$gte": day_start_utc, "$lte": day_end_utc},
            })
            inspected_set = {str(vid) for vid in inspected_ids}

            missed = [v for v in active_vehicles if str(v.get("_id")) not in inspected_set]
            if not missed:
                continue

            admins = await db.users.find({
                "company_id": company_id,
                "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None,
            }).to_list(100)
            for admin in admins:
                if not admin.get("email"):
                    continue
                prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])}) or {}
                if not prefs.get("email_enabled", True):
                    continue
                if not prefs.get("missed_inspection_alerts", True):
                    continue
                await send_missed_inspection_email(admin["email"], company_name, missed)

        logger.info("Missed inspection check complete")
    except Exception as e:
        logger.error(f"Missed inspection check failed: {e}")


async def missed_inspection_scheduler():
    """Runs generate_missed_inspection_check every day at 11:30 PM Sydney."""
    while True:
        try:
            wait_seconds = await _next_run_at(hour=23, minute=30)
            logger.info(f"Missed inspection check scheduled in {wait_seconds/3600:.1f} hours")
            await asyncio.sleep(wait_seconds)
            await generate_missed_inspection_check()
        except Exception as e:
            logger.error(f"Missed inspection scheduler error: {e}")
            await asyncio.sleep(3600)

# ============== Push Notification Service ==============

async def send_push_notification(push_tokens: List[str], title: str, body: str, data: dict = None):
    """Send push notification via Expo Push Notification service"""
    if not push_tokens:
        return
    
    messages = []
    for token in push_tokens:
        if token and token.startswith('ExponentPushToken'):
            messages.append({
                "to": token,
                "sound": "default",
                "title": title,
                "body": body,
                "data": data or {}
            })
    
    if not messages:
        return
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://exp.host/--/api/v2/push/send",
                json=messages,
                headers={"Content-Type": "application/json"}
            )
            logger.info(f"Push notifications sent: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send push notification: {e}")

async def notify_admins(company_id: str, notification_type: str, title: str, body: str, data: dict = None, email_func=None, email_args: tuple = None):
    """Send notifications to all admins of a company based on their preferences"""
    # Get all admins for this company
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None
    }).to_list(100)
    
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "Your Company") if company else "Your Company"
    
    for admin in admins:
        # Get notification preferences
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True, "expiry_alerts": True, "issue_alerts": True, "missed_inspection_alerts": True, "daily_summary": True}
        
        # Check if this notification type is enabled
        type_enabled = prefs.get(f"{notification_type}_alerts", True) if notification_type != "daily_summary" else prefs.get("daily_summary", False)
        
        if not type_enabled:
            continue
        
        # Send push notification
        if prefs.get("push_enabled", True):
            tokens = await db.push_tokens.find({"user_id": str(admin["_id"])}).to_list(10)
            push_tokens = [t["token"] for t in tokens if t.get("token")]
            await send_push_notification(push_tokens, title, body, data)
        
        # Send email notification
        if prefs.get("email_enabled", True) and email_func and email_args:
            await email_func(admin.get("email"), company_name, *email_args)

async def send_activity_email(
    company_id: str,
    activity_pref_key: str,
    subject: str,
    html_body: str,
):
    """Per-activity admin notification (prestart / end-shift / fuel).

    Only sends to admins where both ``email_enabled`` AND the matching
    activity flag (e.g. ``prestart_email``) are True. Default for these
    activity flags is OFF for prestart / end-shift / fuel — so a
    company that hasn't opted in will not get one email per inspection
    submission. Incident emails default ON and are wired separately.
    """
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None,
    }).to_list(100)
    for admin in admins:
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])}) or {}
        if not prefs.get("email_enabled", True):
            continue
        # default OFF for prestart/endshift/fuel — admins must opt in
        if not prefs.get(activity_pref_key, False):
            continue
        if admin.get("email"):
            await send_email_notification(admin["email"], subject, html_body)


async def notify_admins_with_photos(company_id: str, vehicle_name: str, driver_name: str, issue_summary: str, inspection_type: str, photos: List[dict], inspection_id: str, extra_details: dict = None):
    """Send issue alert notifications to admins with photos included"""
    # Get all admins for this company
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None
    }).to_list(100)
    
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "Your Company") if company else "Your Company"
    
    for admin in admins:
        # Get notification preferences
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True, "issue_alerts": True}
        
        # Check if issue alerts are enabled
        if not prefs.get("issue_alerts", True):
            continue
        
        # Send push notification
        if prefs.get("push_enabled", True):
            tokens = await db.push_tokens.find({"user_id": str(admin["_id"])}).to_list(10)
            push_tokens = [t["token"] for t in tokens if t.get("token")]
            await send_push_notification(
                push_tokens, 
                f"DEFECT: {vehicle_name}",
                f"Driver reported: {issue_summary}",
                {"inspection_id": inspection_id, "type": "defect_alert"}
            )
        
        # Send email notification WITH PHOTOS
        if prefs.get("email_enabled", True) and admin.get("email"):
            await send_issue_alert_email(
                admin.get("email"),
                company_name,
                vehicle_name,
                driver_name,
                issue_summary,
                inspection_type,
                photos,
                inspection_id,
                extra_details
            )

# ============== Helper Functions ==============

def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


def utcnow() -> datetime:
    """Naive UTC datetime — Python 3.14-safe replacement for utcnow().

    Phase 11 of TODO.md. ``utcnow()`` is deprecated and scheduled
    for removal in Python 3.14. Every previous call site has been
    migrated to this helper, which preserves the naive-UTC shape Mongo
    docs and downstream comparisons rely on. New code should call
    ``utcnow()`` instead of ``utcnow()``.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Phase 3 — security helpers (STORAGE-PLAN.txt / TODO.md Phase 3).
# ---------------------------------------------------------------------------

# Field names that must never reach the wire. Used by sanitize_user_doc()
# and by the JSON response audit in test_security_phase3.py.
_USER_SECRET_FIELDS: frozenset = frozenset({
    "password_hash",
    "hashed_password",
    "offline_cred_hash",
    "failed_login_attempts",
    "locked_until",
})


def sanitize_user_doc(doc):
    """Return a shallow copy of ``doc`` with every secret field stripped.

    Accepts a dict, a list of dicts, or None. Mutates nothing.
    Centralizes the "never leak the bcrypt hash" rule so callers don't
    each have to remember which fields to pop.
    """
    if doc is None:
        return None
    if isinstance(doc, list):
        return [sanitize_user_doc(d) for d in doc]
    if not isinstance(doc, dict):
        return doc
    return {k: v for k, v in doc.items() if k not in _USER_SECRET_FIELDS}


# Password policy: min 8 chars, at least one upper + lower + digit.
# Applied uniformly at every site that sets a password (register,
# reset, accept-invite, admin reset).
_PASSWORD_POLICY_MIN_LEN = 6


def validate_password_policy(password: str) -> None:
    """Raise HTTPException 400 if the password doesn't meet platform policy.

    Single source of truth for admin / super_admin / company-owner
    passwords (drivers use a 4-digit PIN via validate_driver_pin).

    Owner request 2026-05-29 — simplified to a plain 6-character
    minimum; the upper/lower/digit complexity requirement was dropped.
    This is a SET-TIME check only — login never re-validates, so
    existing users keep their current passwords and continue to
    authenticate unchanged.
    """
    if not isinstance(password, str):
        raise HTTPException(status_code=400, detail="Password is required")
    if len(password) < _PASSWORD_POLICY_MIN_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Password must be at least {_PASSWORD_POLICY_MIN_LEN} characters",
        )


def validate_driver_pin(pin: str) -> None:
    """Validate a 4-digit driver sign-in PIN.

    Drivers sign in on the mobile app with a username + 4-digit PIN
    instead of a password. The PIN is stored bcrypt-hashed in the same
    `password_hash` field — login flow doesn't change. We just skip the
    8-char-with-complexity policy for drivers.
    """
    if not isinstance(pin, str):
        raise HTTPException(status_code=400, detail="PIN is required")
    pin = pin.strip()
    if len(pin) != 4 or not pin.isdigit():
        raise HTTPException(
            status_code=400,
            detail="PIN must be exactly 4 digits",
        )


def _persist_custom_documents(
    custom_documents: Optional[List[Any]],
    company_id: str,
    driver_id: str,
) -> List[dict]:
    """Persist each custom document's front/back to MinIO and return the
    storable shape (label + number + issue + expiry + object keys).

    Accepts either a list of CustomDocumentInput pydantic instances or
    plain dicts — the create + update endpoints differ in which they
    receive. Skips entries with an empty label. The label is the only
    required field.
    """
    if not custom_documents:
        return []
    stored: List[dict] = []
    for idx, raw in enumerate(custom_documents):
        if hasattr(raw, "model_dump"):
            doc = raw.model_dump()
        elif hasattr(raw, "dict"):
            doc = raw.dict()
        elif isinstance(raw, dict):
            doc = dict(raw)
        else:
            continue
        label = (doc.get("label") or "").strip()
        if not label:
            continue
        # Stable per-document ID — we reuse it on update so the object
        # keys stay constant. Generate one if the client didn't send it.
        doc_id = (doc.get("id") or "").strip() or str(uuid.uuid4())
        record: dict = {
            "id": doc_id,
            "label": label[:80],
            "number": (doc.get("number") or "").strip()[:80] or None,
            "issue": (doc.get("issue") or "").strip() or None,
            "expiry": (doc.get("expiry") or "").strip() or None,
        }
        # Preserve any previously-uploaded keys when the client sends
        # the full doc back during edit (front_base64 will be absent).
        if doc.get("front_object_key"):
            record["front_object_key"] = doc["front_object_key"]
        if doc.get("back_object_key"):
            record["back_object_key"] = doc["back_object_key"]
        # Upload new front bytes when present.
        front_b64 = doc.get("front_base64")
        if front_b64:
            key = f"driver-docs/{company_id}/{driver_id}/custom/{doc_id}-front.bin"
            _upload_base64_or_400(
                "compliance", key, front_b64, "bin",
                f"custom_documents[{idx}].front_base64",
                expected_company_id=company_id,
                type_key="driver_doc",
            )
            record["front_object_key"] = key
        back_b64 = doc.get("back_base64")
        if back_b64:
            key = f"driver-docs/{company_id}/{driver_id}/custom/{doc_id}-back.bin"
            _upload_base64_or_400(
                "compliance", key, back_b64, "bin",
                f"custom_documents[{idx}].back_base64",
                expected_company_id=company_id,
                type_key="driver_doc",
            )
            record["back_object_key"] = key
        stored.append(record)
    return stored


def _attach_custom_document_urls(user_doc: Optional[dict]) -> Optional[dict]:
    """Attach presigned `*_url` siblings to each custom_documents entry.

    Mutates and returns the user doc. No-op when the user has no custom
    docs. Safe to call on any user shape.
    """
    if not user_doc or not isinstance(user_doc.get("custom_documents"), list):
        return user_doc
    for entry in user_doc["custom_documents"]:
        if not isinstance(entry, dict):
            continue
        front_key = entry.get("front_object_key")
        back_key = entry.get("back_object_key")
        if front_key:
            try:
                entry["front_url"] = object_store.presign_get(
                    "compliance", front_key,
                )
            except Exception:
                entry["front_url"] = None
        if back_key:
            try:
                entry["back_url"] = object_store.presign_get(
                    "compliance", back_key,
                )
            except Exception:
                entry["back_url"] = None
    return user_doc


def reject_same_password(new_password: str, current_hash: Optional[str]) -> None:
    """Reject when the proposed password matches the user's current one.

    Used by every reset/change endpoint so a user can't "reset" to
    the same password they had before. Silently no-op if the user has
    no current hash (e.g. invite-only account never set one).
    """
    if not current_hash:
        return
    try:
        if verify_password(new_password, current_hash):
            raise HTTPException(
                status_code=400,
                detail="New password must be different from your current password",
            )
    except HTTPException:
        raise
    except Exception:
        # If hash verification fails for some other reason (corrupt
        # hash, library mismatch), don't block the reset — let the
        # caller proceed and rehash with the new password.
        return


# Account lockout: 5 failed login attempts inside the window → 30-min lock.
# Local int parser — _env_int is defined later in the file (next to the
# upload validation helpers), so we inline a tolerant parse here to
# avoid forward-reference ordering issues.
def _phase3_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


ACCOUNT_LOCKOUT_MAX_FAILURES = _phase3_env_int("ACCOUNT_LOCKOUT_MAX_FAILURES", 5)
ACCOUNT_LOCKOUT_WINDOW_SECONDS = _phase3_env_int("ACCOUNT_LOCKOUT_WINDOW_SECONDS", 15 * 60)
ACCOUNT_LOCKOUT_DURATION_SECONDS = _phase3_env_int("ACCOUNT_LOCKOUT_DURATION_SECONDS", 30 * 60)


async def _record_failed_login(user_doc: dict) -> Optional[int]:
    """Bump the failed-login counter and lock the account on threshold.

    Counter resets if the prior failures fall outside the window. When
    threshold is hit, ``locked_until`` is set and we return the number
    of seconds until the lock lifts so the caller can put it in a 423
    response body.
    """
    now = utcnow()
    failures = user_doc.get("failed_login_attempts") or []
    if isinstance(failures, int):
        # Legacy schema migration tolerance.
        failures = []
    cutoff = now - timedelta(seconds=ACCOUNT_LOCKOUT_WINDOW_SECONDS)
    fresh = [
        f for f in failures
        if isinstance(f, datetime) and f > cutoff
    ]
    fresh.append(now)
    update: dict = {"failed_login_attempts": fresh}
    locked_seconds: Optional[int] = None
    if len(fresh) >= ACCOUNT_LOCKOUT_MAX_FAILURES:
        locked_until = now + timedelta(seconds=ACCOUNT_LOCKOUT_DURATION_SECONDS)
        update["locked_until"] = locked_until
        locked_seconds = ACCOUNT_LOCKOUT_DURATION_SECONDS
    await db.users.update_one(
        {"_id": user_doc["_id"]},
        {"$set": update},
    )
    return locked_seconds


async def _clear_failed_logins(user_id) -> None:
    """Drop the failure counter + lockout on a successful authentication."""
    await db.users.update_one(
        {"_id": user_id},
        {"$unset": {"failed_login_attempts": "", "locked_until": ""}},
    )


def _account_locked_until(user_doc: dict) -> Optional[datetime]:
    """Return the locked-until timestamp (UTC) if the account is still locked, else None."""
    locked_until = user_doc.get("locked_until")
    if not isinstance(locked_until, datetime):
        return None
    if locked_until > utcnow():
        return locked_until
    return None


# Redirect-to allowlist — prevents open-redirect by checking the resolved
# host is on the FleetShield365 zone before echoing it back to the client.
_REDIRECT_ALLOWED_HOST_RE = re.compile(
    r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.fleetshield365\.com$",
    re.IGNORECASE,
)


def validate_redirect_url(url: Optional[str]) -> Optional[str]:
    """Echo a redirect URL only when it points inside the FleetShield365 zone.

    Returns the URL on success, None when the URL is missing, malformed,
    or off-domain. Used by the login response so an attacker cannot
    seed a tenant_subdomain that causes a redirect to evil.com.
    """
    if not url:
        return None
    try:
        from urllib.parse import urlsplit
        parts = urlsplit(url)
    except Exception:
        return None
    if parts.scheme not in ("http", "https"):
        return None
    host = (parts.netloc or "").split(":")[0]
    if host == "fleetshield365.com":
        return url
    if _REDIRECT_ALLOWED_HOST_RE.match(host):
        return url
    return None


# HTML escape helper for user-supplied strings interpolated into emails.
# We import the stdlib `html` module on demand so the email helpers can
# call ``_safe_html(value)`` for every user-controlled value.
import html as _html_lib  # noqa: E402


def _safe_html(value) -> str:
    """Escape user-controlled text for HTML email bodies (XSS-via-email defence)."""
    if value is None:
        return ""
    return _html_lib.escape(str(value), quote=True)


async def _check_idempotency(collection, company_id: str, idempotency_key: Optional[str]) -> Optional[dict]:
    """Return an existing doc when (company_id, idempotency_key) matches.

    Used by inspections / fuel / incidents POST handlers so the mobile
    upload queue can safely retry a request after a network blip
    without creating a duplicate record. Mobile sends its
    ``uploadQueue.dedupHash`` as the ``idempotency_key`` — the value
    is stable across retries of the same submission but distinct
    across genuinely-new submissions.

    Returns None when no key is provided (legacy clients) or no
    match is found.
    """
    if not idempotency_key:
        return None
    key = str(idempotency_key).strip()
    if not key:
        return None
    try:
        return await collection.find_one({
            "company_id": company_id,
            "idempotency_key": key,
        })
    except Exception as exc:
        logger.warning("idempotency lookup failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Phase 4 — soft-delete + audit trail helpers (TODO.md Phase 4).
#
# Soft-delete pattern: instead of removing a document, mark it with
# ``deleted_at`` (UTC datetime) + ``deleted_by`` (user_id). All reads
# default to filtering them out. A "Trash" view can opt-in to see them
# (and a Restore action unsets the fields). Permanent removal is only
# triggered via the manual /api/admin/purge-old-deleted button, per
# user requirement — no automated nightly purge.
# ---------------------------------------------------------------------------

# Collections that participate in soft-delete. inspections + fuel +
# audit_trail are explicitly excluded — they're immutable compliance
# records (NHVR) and must never be silently hidden from queries.
SOFT_DELETE_COLLECTIONS: tuple = (
    "vehicles",
    "users",
    "companies",
    "service_records",
    "maintenance_logs",
    "incidents",
)

# How far back a soft-deleted row is considered "recoverable" before
# the manual purge button can drop it. 30 days matches the platform
# retention policy in TODO.md Phase 4.
SOFT_DELETE_GRACE_DAYS = _phase3_env_int("SOFT_DELETE_GRACE_DAYS", 30)


def _soft_delete_filter(include_deleted: bool = False) -> dict:
    """Return a Mongo filter clause that excludes soft-deleted rows by default.

    Use as a base filter for every read query against soft-delete
    collections::

        query = {**_soft_delete_filter(), "company_id": cid, ...}

    Passing ``include_deleted=True`` (used by the Trash view) lifts
    the filter so tombstoned rows surface.

    Implementation note: we look for ``deleted_at`` being missing OR
    null. Mongo treats ``{deleted_at: null}`` as matching docs whose
    field is either absent or explicitly null, so a single comparison
    covers both. Documents written before Phase 4 ship have no
    ``deleted_at`` field — they match.
    """
    if include_deleted:
        return {}
    return {"deleted_at": None}


def _soft_delete_update(user_id: Optional[str]) -> dict:
    """Mongo $set clause that marks a row as soft-deleted."""
    return {
        "$set": {
            "deleted_at": utcnow(),
            "deleted_by": str(user_id) if user_id else None,
        }
    }


def _restore_update() -> dict:
    """Mongo $unset clause that restores a soft-deleted row."""
    return {"$unset": {"deleted_at": "", "deleted_by": ""}}


# ---------------------------------------------------------------------------
# Phase 8 — tenant suspension dependency. The actual ``require_active_tenant``
# function is defined further down, after ``get_current_user`` so the
# Depends() forward-reference resolves. Look for "Phase 8 dependency"
# below.
# ---------------------------------------------------------------------------

import re
async def generate_unique_username(name: str, company_id: str) -> str:
    """Generate a GLOBALLY unique username from the person's name with random numbers"""
    import random
    
    # Clean the name: lowercase, remove special chars, keep only first name
    clean_name = re.sub(r'[^a-z0-9]', '', name.lower().strip().split()[0] if name.strip() else 'user')
    
    if not clean_name:
        clean_name = "user"
    
    # Try base username first, then add random numbers
    username = clean_name
    attempts = 0
    max_attempts = 50
    
    # Check GLOBALLY (all companies) to avoid login confusion
    while await db.users.find_one({"username": username}):
        # Generate random 1-2 digit number (1-99)
        random_num = random.randint(1, 99)
        username = f"{clean_name}{random_num}"
        attempts += 1
        if attempts >= max_attempts:
            # Fallback to 3 digit random if too many collisions
            username = f"{clean_name}{random.randint(100, 999)}"
            break
    
    return username

def _resolve_platform_owner(user_id: str, user_doc: Optional[dict]) -> bool:
    """Return True if the given user is a platform owner (Req 15.6, 12.2).

    A user is treated as platform_owner when either:

    * their MongoDB ``_id`` stringifies to a value listed in the
      ``PLATFORM_OWNER_USER_IDS`` env-driven frozenset, or
    * their user document has ``is_platform_owner`` set to True.

    Both gates are defence-in-depth: the env var lets the operator
    bootstrap ownership without a DB round-trip, the DB flag lets
    day-to-day promotions happen without a redeploy. Either one being
    true is sufficient.
    """

    if user_id in PLATFORM_OWNER_USER_IDS:
        return True
    if user_doc and user_doc.get("is_platform_owner") is True:
        return True
    return False


async def _mint_access_token(
    user_id: str,
    *,
    user_doc: Optional[dict] = None,
    company_id: Optional[str] = None,
    subdomain: Optional[str] = None,
    role: Optional[str] = None,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Mint a JWT carrying the full tenant + role claim set (Req 12.1-12.3).

    The resulting token payload contains ``sub``, ``company_id``,
    ``subdomain``, ``role``, ``iat`` and ``exp``. ``role`` is one of
    ``super_admin`` / ``admin`` / ``driver`` / ``platform_owner`` — a
    user listed in ``PLATFORM_OWNER_USER_IDS`` or carrying
    ``users.is_platform_owner == true`` is minted a token with
    ``role="platform_owner"`` regardless of their ``users.role`` field
    (Req 15.6).

    When ``company_id`` / ``subdomain`` / ``role`` are not supplied the
    helper fetches the user + company documents to fill them in so the
    minted token always reflects the current state of the DB at mint
    time (Req 12.3).
    """

    # Fetch user if the caller didn't already have it.
    if user_doc is None:
        try:
            user_doc = await db.users.find_one({"_id": ObjectId(user_id)})
        except Exception:
            user_doc = None

    resolved_company_id = company_id or (
        user_doc.get("company_id") if user_doc else None
    )

    resolved_role: str
    if _resolve_platform_owner(user_id, user_doc):
        resolved_role = UserRole.PLATFORM_OWNER
    elif role and role in ALLOWED_JWT_ROLES:
        resolved_role = role
    else:
        user_role = (user_doc or {}).get("role") if user_doc else None
        resolved_role = (
            user_role
            if user_role in ALLOWED_JWT_ROLES
            else UserRole.DRIVER
        )

    resolved_subdomain = subdomain
    if resolved_subdomain is None and resolved_company_id:
        try:
            company_doc = await db.companies.find_one(
                {"_id": ObjectId(resolved_company_id)},
                {"subdomain": 1},
            )
            if company_doc:
                resolved_subdomain = company_doc.get("subdomain")
        except Exception:
            resolved_subdomain = None

    now = utcnow()
    payload = {
        "sub": str(user_id),
        "company_id": str(resolved_company_id) if resolved_company_id else None,
        "subdomain": resolved_subdomain,
        "role": resolved_role,
        "iat": int(now.timestamp()),
        "exp": now + (
            expires_delta
            or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
        ),
        # Phase 3 — jti (JWT ID) lets the server revoke individual tokens
        # via the revoked_tokens collection. Without jti, logout would
        # be cosmetic (token kept working until natural expiry).
        "jti": uuid.uuid4().hex,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def create_access_token(data: dict, expires_delta: timedelta = None):
    """Synchronous thin wrapper around ``jwt.encode`` for legacy call sites.

    Prefer ``_mint_access_token`` (async, fetches company + role) for new
    code paths. This shim keeps a handful of older callers working with a
    ``{"sub": user_id}`` payload; it does NOT populate ``company_id`` /
    ``subdomain`` / ``role`` claims, so ``get_current_user`` is tolerant
    of their absence (Req 12.4 only enforces the stale-subdomain check
    when the ``subdomain`` claim is present).
    """
    to_encode = data.copy()
    now = utcnow()
    expire = now + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode.setdefault("iat", int(now.timestamp()))
    to_encode["exp"] = expire
    # Phase 3 — make every token revocable.
    to_encode.setdefault("jti", uuid.uuid4().hex)
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# In-process throttle for the last_active_at writeback. We don't want
# a logged-in driver doing 20 requests in a minute to trigger 20 Mongo
# writes for the same field. The map is bounded and ephemeral per
# worker; missing entries just mean a fresh write happens (idempotent).
_LAST_ACTIVE_CACHE: dict = {}
_LAST_ACTIVE_THROTTLE_S = 60


def _touch_last_active(user_id: str) -> None:
    import time as _time
    now_ts = _time.time()
    last = _LAST_ACTIVE_CACHE.get(user_id)
    if last is not None and (now_ts - last) < _LAST_ACTIVE_THROTTLE_S:
        return
    _LAST_ACTIVE_CACHE[user_id] = now_ts
    # Bound the cache so a long-running worker doesn't grow unbounded.
    if len(_LAST_ACTIVE_CACHE) > 5000:
        # Drop the oldest half — cheap O(n) pass, runs at most once per
        # 5000 distinct user IDs which is well past the realistic ceiling.
        cutoff = sorted(_LAST_ACTIVE_CACHE.values())[len(_LAST_ACTIVE_CACHE) // 2]
        for k in list(_LAST_ACTIVE_CACHE):
            if _LAST_ACTIVE_CACHE[k] <= cutoff:
                _LAST_ACTIVE_CACHE.pop(k, None)
    # Fire and forget — we don't want to add per-request latency for
    # a non-critical telemetry write.
    try:
        asyncio.create_task(
            db.users.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {"last_active_at": utcnow()}},
            )
        )
    except Exception:
        pass


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        # Phase 3 — token revocation list. /auth/logout adds the jti
        # here with a TTL = original token exp, so the collection
        # self-cleans. Tokens minted before jti existed (legacy) have
        # ``jti is None`` — they bypass this check; the stale-subdomain
        # check (below) still applies. New tokens always carry jti.
        jti = payload.get("jti")
        if jti:
            revoked = await db.revoked_tokens.find_one({"_id": jti})
            if revoked:
                raise HTTPException(
                    status_code=401,
                    detail="Token has been revoked. Please log in again.",
                )

        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")

        # Phase 8 of TODO.md — flag the user's tenant suspension status
        # on the returned dict. The require_active_tenant dependency
        # (below) uses it to block writes; reads continue to work so
        # the admin can review their data and pay/upgrade. We do the
        # lookup here once per request rather than per-endpoint.
        co_id = user.get("company_id")
        if co_id:
            try:
                co_doc = await db.companies.find_one(
                    {"_id": ObjectId(co_id)},
                    {"suspended": 1, "suspended_at": 1, "suspended_reason": 1},
                )
                user["_company_suspended"] = bool(co_doc and co_doc.get("suspended"))
                user["_company_suspended_reason"] = (co_doc or {}).get("suspended_reason")
            except Exception:
                user["_company_suspended"] = False

        # Stale-subdomain rejection (Req 12.4, 12.5). When the JWT was
        # minted with a ``subdomain`` claim we look up the current
        # ``companies.subdomain`` and 401 on mismatch so a token survives
        # only as long as the tenant slug on the token does. Tokens minted
        # before the subdomain claim existed (legacy path) carry no
        # ``subdomain`` and are accepted unchanged — the guarantee only
        # applies to new-style tokens.
        token_subdomain = payload.get("subdomain")
        token_company_id = payload.get("company_id")
        if token_subdomain is not None:
            compare_company_id = token_company_id or user.get("company_id")
            if compare_company_id:
                try:
                    company_doc = await db.companies.find_one(
                        {"_id": ObjectId(compare_company_id)},
                        {"subdomain": 1},
                    )
                except Exception:
                    company_doc = None
                current_subdomain = (
                    company_doc.get("subdomain") if company_doc else None
                )
                if current_subdomain != token_subdomain:
                    raise HTTPException(
                        status_code=401,
                        detail="Stale token: tenant subdomain has changed; please log in again",
                    )

        user['id'] = str(user['_id'])
        # Expose the JWT role on the returned user object so downstream
        # dependencies like ``require_platform_owner`` can gate on the
        # minted role (which is NOT necessarily equal to ``users.role``
        # — ``platform_owner`` is mint-time-only, not persisted there).
        user['jwt_role'] = payload.get('role')
        user['jwt_company_id'] = payload.get('company_id')
        user['jwt_subdomain'] = payload.get('subdomain')

        # Touch users.last_active_at — throttled to once per 60 s per
        # user via an in-process LRU so a logged-in driver doing rapid
        # requests doesn't generate one Mongo write per request. The
        # field powers the owner-panel "inactive organizations" view.
        try:
            _touch_last_active(str(user["_id"]))
        except Exception:
            pass

        return user
    except HTTPException:
        raise
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        # PyJWT raises ``InvalidTokenError`` (and subclasses) for every
        # malformed-token failure mode. Older ``python-jose`` style
        # ``jwt.JWTError`` does not exist in PyJWT, so using that here
        # bubbles an ``AttributeError`` up and turns a 401 into a 500.
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception:
        # Defensive catch so a malformed bearer never crashes to 500.
        raise HTTPException(status_code=401, detail="Invalid token")


async def require_active_tenant(
    current_user: dict = Depends(get_current_user),
) -> dict:
    """Phase 8 dependency — block writes from suspended tenants (HTTP 423).

    Platform_owner tokens are unaffected (they need to un-suspend
    even when a tenant is in this state). Reads remain available so
    admins can still see their data while sorting out payment.
    """
    if current_user.get("jwt_role") == UserRole.PLATFORM_OWNER:
        return current_user
    if current_user.get("_company_suspended"):
        reason = current_user.get("_company_suspended_reason") or "Account suspended"
        raise HTTPException(
            status_code=423,
            detail=(
                f"{reason}. Read access works; writes are blocked until "
                f"the suspension is lifted. Contact support."
            ),
        )
    return current_user


async def require_active_subscription(
    current_user: dict = Depends(require_active_tenant),
) -> dict:
    """Phase 12 dependency — block driver write actions on expired plans.

    Returns HTTP 402 (Payment Required) when:
      * The trial has ended AND there is no active paid subscription, OR
      * The subscription is explicitly in ``status="canceled"`` or
        ``status="past_due"`` past its grace.

    Admins + owners are NOT blocked — they need access to renew/pay.
    Platform_owner tokens are likewise unaffected.

    The mobile / web client should catch 402 and render an
    "Account expired — please ask your owner" screen for drivers, or
    "Your plan ended — renew to continue" for admins.
    """
    role = current_user.get("jwt_role") or current_user.get("role")
    if role == UserRole.PLATFORM_OWNER:
        return current_user
    # Owners + admins can still write — they need to manage billing.
    if role in (UserRole.SUPER_ADMIN, UserRole.ADMIN):
        return current_user

    co_id = current_user.get("company_id")
    if not co_id:
        return current_user

    company = await db.companies.find_one(
        {"_id": ObjectId(co_id)},
        {
            "subscription_status": 1,
            "trial_ends_at": 1,
            "subscription_ends_at": 1,
        },
    )
    if not company:
        return current_user

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    sub_status = (company.get("subscription_status") or "").lower()
    trial_ends = company.get("trial_ends_at")
    sub_ends = company.get("subscription_ends_at")

    in_trial = isinstance(trial_ends, datetime) and trial_ends > now
    in_paid = sub_status == "active" and (
        not isinstance(sub_ends, datetime) or sub_ends > now
    )

    if not (in_trial or in_paid):
        raise HTTPException(
            status_code=402,
            detail=(
                "Your company's FleetShield365 plan has ended. "
                "Ask your company owner to renew."
            ),
        )
    return current_user


async def require_platform_owner(
    current_user: dict = Depends(get_current_user),
) -> dict:
    """FastAPI dependency that enforces the ``platform_owner`` role.

    Returns the current user when the request bearer token carries
    ``role == "platform_owner"``. Raises HTTP 403 on any other role so the
    owner dashboard at ``owner.fleetshield365.com`` is the only UI whose
    users can exercise the ``/api/developer/*`` surface (Req 15.3, 15.4,
    15.5). HTTP 401 is handled upstream by ``get_current_user`` when the
    bearer is missing or invalid.
    """

    role = current_user.get("jwt_role") or current_user.get("role")
    if role != UserRole.PLATFORM_OWNER:
        raise HTTPException(
            status_code=403,
            detail="Platform owner role required",
        )
    return current_user

def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable dict"""
    if doc is None:
        return None
    if isinstance(doc, list):
        return [serialize_doc(d) for d in doc]
    if isinstance(doc, dict):
        result = {}
        for key, value in doc.items():
            if key == '_id':
                result['id'] = str(value)
            elif isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, list):
                result[key] = serialize_doc(value)
            elif isinstance(value, dict):
                result[key] = serialize_doc(value)
            else:
                result[key] = value
        return result
    return doc


# ---------------------------------------------------------------------------
# Upload validation (Phase 1 of STORAGE-PLAN.txt)
#
# Every upload — whether base64-in-JSON or multipart — passes through
# ``_validate_upload_or_400`` which enforces both a per-type size cap and
# a magic-byte allowlist. The client-supplied ``content_type`` header is
# never trusted: only the first bytes of the decoded body are.
#
# Sizes default to STORAGE-PLAN values when env vars are absent. Magic-
# byte detection covers JPEG, PNG, WebP, and PDF — the only formats the
# platform stores.
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    """Read an int env var with a fallback on any parse error."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


UPLOAD_MAX_BYTES: Dict[str, int] = {
    "logo": _env_int("UPLOAD_MAX_BYTES_LOGO", 2 * 1024 * 1024),
    "inspection": _env_int("UPLOAD_MAX_BYTES_INSPECTION", 3 * 1024 * 1024),
    "fuel": _env_int("UPLOAD_MAX_BYTES_FUEL", 2 * 1024 * 1024),
    "incident_photo": _env_int("UPLOAD_MAX_BYTES_INCIDENT_PHOTO", 3 * 1024 * 1024),
    "incident_pdf": _env_int("UPLOAD_MAX_BYTES_INCIDENT_PDF", 5 * 1024 * 1024),
    "license": _env_int("UPLOAD_MAX_BYTES_LICENSE", 2 * 1024 * 1024),
    "driver_doc": _env_int("UPLOAD_MAX_BYTES_DRIVER_DOC", 5 * 1024 * 1024),
    "service": _env_int("UPLOAD_MAX_BYTES_SERVICE", 5 * 1024 * 1024),
    "maintenance": _env_int("UPLOAD_MAX_BYTES_MAINTENANCE", 5 * 1024 * 1024),
    "signature": _env_int("UPLOAD_MAX_BYTES_SIGNATURE", 512 * 1024),
    "profile": _env_int("UPLOAD_MAX_BYTES_PROFILE", 1024 * 1024),
    "vehicle_doc": _env_int("UPLOAD_MAX_BYTES_VEHICLE_DOC", 5 * 1024 * 1024),
    "default": _env_int("UPLOAD_MAX_BYTES_DEFAULT", 5 * 1024 * 1024),
}

# Per-type allowlists. "image" means JPEG/PNG/WebP. "pdf" means PDF only.
# "image_or_pdf" allows both (used by training certs / service / maintenance).
_FORMAT_GROUPS: Dict[str, frozenset] = {
    "image": frozenset({"jpeg", "png", "webp"}),
    "pdf": frozenset({"pdf"}),
    "image_or_pdf": frozenset({"jpeg", "png", "webp", "pdf"}),
    "png": frozenset({"png"}),  # signatures only
}

UPLOAD_FORMAT_GROUP: Dict[str, str] = {
    "logo": "image",
    "inspection": "image",
    "fuel": "image",
    "incident_photo": "image",
    "incident_pdf": "pdf",
    "license": "image_or_pdf",
    "driver_doc": "image_or_pdf",
    "service": "image_or_pdf",
    "maintenance": "image_or_pdf",
    "signature": "png",
    "profile": "image",
    # 2026-05-20 — rego / insurance / safety-cert / COI supporting docs.
    "vehicle_doc": "image_or_pdf",
}

# Count caps
MAX_PHOTOS_PER_INSPECTION = _env_int("MAX_PHOTOS_PER_INSPECTION", 20)
MAX_INCIDENT_PHOTOS_PER_CATEGORY = _env_int("MAX_INCIDENT_PHOTOS_PER_CATEGORY", 8)
MAX_SERVICE_ATTACHMENTS = _env_int("MAX_SERVICE_ATTACHMENTS", 5)


def _enforce_count_or_413(items, cap: int, field_name: str) -> None:
    """Raise HTTP 413 when a list of uploads exceeds the per-collection cap.

    Designed to be cheap: just a length check + tight error message that
    names the field and the cap so the client UI can render a friendly
    explanation without parsing the body. Treats None / empty as a no-op.
    """
    if not items:
        return
    if len(items) > cap:
        raise HTTPException(
            status_code=413,
            detail=(
                f"{field_name} contains {len(items)} items; max allowed is "
                f"{cap}. Please remove some items and resubmit."
            ),
        )


def _detect_format(data: bytes) -> Optional[str]:
    """Return canonical format name based on magic bytes, or None if unknown.

    Covers the four formats the platform stores. Bytes after position 12 are
    ignored — these prefixes are enough to disambiguate every supported
    type. Client-supplied content_type is irrelevant because we trust only
    the raw byte stream after base64 decode.
    """
    if len(data) < 8:
        return None
    if data[:3] == b"\xFF\xD8\xFF":
        return "jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:5] == b"%PDF-":
        return "pdf"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    return None


def _validate_upload_or_400(
    data: bytes,
    type_key: str,
    field_name: str,
) -> str:
    """Validate decoded bytes against per-type size + magic-byte allowlist.

    Returns the detected format string (``jpeg`` / ``png`` / ``webp`` /
    ``pdf``) so the caller can stamp the right Content-Type on the MinIO
    object. Raises:

    * HTTP 413 — payload exceeds ``UPLOAD_MAX_BYTES[type_key]``
    * HTTP 415 — magic-byte format not in the allowlist for ``type_key``
    """
    max_bytes = UPLOAD_MAX_BYTES.get(type_key, UPLOAD_MAX_BYTES["default"])
    if len(data) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=(
                f"{field_name} is {len(data)} bytes; max allowed is "
                f"{max_bytes} bytes for type {type_key!r}. Please compress "
                f"the file before uploading."
            ),
        )

    detected = _detect_format(data)
    group_name = UPLOAD_FORMAT_GROUP.get(type_key, "image")
    allowed = _FORMAT_GROUPS[group_name]

    if detected is None or detected not in allowed:
        raise HTTPException(
            status_code=415,
            detail=(
                f"{field_name} has unsupported format "
                f"{detected or 'unknown'!r}; allowed: {sorted(allowed)}"
            ),
        )

    return detected


_FORMAT_TO_CONTENT_TYPE: Dict[str, str] = {
    "jpeg": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp",
    "pdf": "application/pdf",
}


_FORMAT_TO_EXT: Dict[str, str] = {
    "jpeg": "jpg",
    "png": "png",
    "webp": "webp",
    "pdf": "pdf",
}


def _generate_thumbnail(image_bytes: bytes, max_side: int = 300) -> Optional[bytes]:
    """Return a JPEG thumbnail (max_side x max_side, q80) or None on failure.

    Best-effort. Pillow may not support an exotic image variant; if
    generation fails we log and skip rather than failing the parent
    upload — thumbnails are a UX optimisation, not a correctness
    requirement.
    """
    try:
        with PILImage.open(BytesIO(image_bytes)) as img:
            # Convert to RGB for JPEG (drops alpha channel from PNGs/WebPs).
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            img.thumbnail((max_side, max_side), PILImage.LANCZOS)
            buf = BytesIO()
            img.save(buf, "JPEG", quality=80, optimize=True)
            return buf.getvalue()
    except Exception as exc:
        logger.warning("Thumbnail generation failed: %s", exc)
        return None


def _thumbnail_key_for(key: str) -> str:
    """Derive the thumbnail object key from the original key.

    Example: ``"<company>/<inspection>/<uuid>.jpg"`` →
    ``"<company>/<inspection>/<uuid>_thumb.jpg"``. Same prefix, ``_thumb``
    suffix before the extension. Always JPEG regardless of source format.
    """
    if "." in key:
        stem, _, _ = key.rpartition(".")
        return f"{stem}_thumb.jpg"
    return f"{key}_thumb.jpg"


def _upload_with_thumbnail(
    bucket: str,
    key: str,
    data: bytes,
    content_type: str,
    expected_company_id: Optional[str],
) -> Optional[str]:
    """Upload original + generate-and-upload thumbnail. Returns thumb key or None.

    Used by every image upload path. Thumbnail upload runs after the
    original so a thumbnail failure cannot block the primary write. The
    thumbnail is always JPEG into the same bucket with the ``_thumb``
    suffix — keeps presign permissions identical and avoids cross-bucket
    list mistakes.
    """
    object_store.upload_bytes(
        bucket, key, data, content_type,
        expected_company_id=expected_company_id,
    )

    if content_type == "application/pdf":
        return None  # no thumbnail for PDFs

    thumb_bytes = _generate_thumbnail(data)
    if not thumb_bytes:
        return None

    thumb_key = _thumbnail_key_for(key)
    try:
        object_store.upload_bytes(
            bucket, thumb_key, thumb_bytes, "image/jpeg",
            expected_company_id=expected_company_id,
        )
        return thumb_key
    except Exception as exc:
        logger.warning("Thumbnail upload failed for %s/%s: %s", bucket, key, exc)
        return None


# ---------------------------------------------------------------------------
# Async PDF compression via Ghostscript (Phase 2)
# ---------------------------------------------------------------------------

PDF_COMPRESS_ENABLED: bool = (
    os.environ.get("PDF_COMPRESS_ENABLED", "true").strip().lower()
    in ("true", "1", "yes", "on")
)
PDF_COMPRESS_MIN_BYTES: int = _env_int("PDF_COMPRESS_MIN_BYTES", 1024 * 1024)
PDF_COMPRESS_GS_BINARY: str = (
    os.environ.get("PDF_COMPRESS_GS_BINARY", "").strip() or "gs"
)


async def compress_pdf_async(
    bucket: str,
    key: str,
    original_size: int,
    expected_company_id: Optional[str] = None,
) -> None:
    """Re-encode the PDF at ``<bucket>/<key>`` through Ghostscript /ebook.

    Designed to run as a FastAPI BackgroundTask after the upload response
    has been returned. Safe to call when:

    * ``PDF_COMPRESS_ENABLED`` is false  → skipped (no-op)
    * ``original_size`` < ``PDF_COMPRESS_MIN_BYTES``  → skipped (not worth it)
    * Ghostscript binary not on PATH  → skipped (logged warning)
    * Compressed output >= original  → skipped (kept original)

    All failure modes leave the original object intact. The MinIO write
    is atomic: we re-upload the smaller bytes under the same key, so a
    crash mid-process leaves either the original or the smaller version
    — never a truncated file.
    """
    if not PDF_COMPRESS_ENABLED:
        return
    if original_size < PDF_COMPRESS_MIN_BYTES:
        return
    if shutil.which(PDF_COMPRESS_GS_BINARY) is None:
        logger.info(
            "PDF compression skipped: ghostscript binary %r not on PATH",
            PDF_COMPRESS_GS_BINARY,
        )
        return

    try:
        original_bytes = object_store.get_bytes(bucket, key)
    except Exception as exc:
        logger.warning("PDF compression skipped: cannot fetch %s/%s: %s", bucket, key, exc)
        return

    with tempfile.TemporaryDirectory(prefix="fs365-pdf-") as tmpdir:
        in_path = os.path.join(tmpdir, "in.pdf")
        out_path = os.path.join(tmpdir, "out.pdf")
        with open(in_path, "wb") as fh:
            fh.write(original_bytes)

        proc = await asyncio.create_subprocess_exec(
            PDF_COMPRESS_GS_BINARY,
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            "-dPDFSETTINGS=/ebook",
            "-dNOPAUSE",
            "-dQUIET",
            "-dBATCH",
            f"-sOutputFile={out_path}",
            in_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=120)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            logger.warning("PDF compression timed out for %s/%s", bucket, key)
            return

        if proc.returncode != 0:
            logger.warning(
                "Ghostscript exited %s for %s/%s",
                proc.returncode, bucket, key,
            )
            return

        try:
            new_size = os.path.getsize(out_path)
        except OSError:
            return

        # Skip if compression did not help (or made it bigger). Keep
        # original under the same key.
        if new_size >= original_size:
            logger.info(
                "PDF compression no-op for %s/%s (%d -> %d bytes)",
                bucket, key, original_size, new_size,
            )
            return

        with open(out_path, "rb") as fh:
            compressed_bytes = fh.read()

    try:
        object_store.upload_bytes(
            bucket, key, compressed_bytes, "application/pdf",
            expected_company_id=expected_company_id,
        )
        logger.info(
            "PDF compressed for %s/%s: %d -> %d bytes (%.1f%%)",
            bucket, key, original_size, len(compressed_bytes),
            100.0 * (1 - len(compressed_bytes) / original_size),
        )
    except Exception as exc:
        logger.warning(
            "PDF compression upload failed for %s/%s: %s",
            bucket, key, exc,
        )


def _upload_base64_or_400(
    bucket: str,
    key: str,
    b64_string: str,
    default_ext: str,
    field_name: str,
    expected_company_id: Optional[str] = None,
    type_key: Optional[str] = None,
    background_tasks: Optional[BackgroundTasks] = None,
) -> Optional[str]:
    """Decode base64 → validate → upload to MinIO. Returns thumb key or None.

    The behaviour matches the prior contract for callers that pass only
    the original args (bucket / key / b64 / ext / field name /
    company_id). When ``type_key`` is provided, the decoded bytes are
    additionally validated against the per-type size cap + magic-byte
    allowlist and a thumbnail is generated for image types. PDF uploads
    where ``background_tasks`` is non-None are scheduled for async
    Ghostscript compression after the response is returned.

    Errors:

    * 400 — invalid base64 payload
    * 403 — tenant prefix mismatch on the object key
    * 413 — payload exceeds ``UPLOAD_MAX_BYTES[type_key]``
    * 415 — magic-byte format not in the allowlist for ``type_key``
    """
    # Decode once here so we own the bytes and can validate + thumbnail
    # without going through ``object_store.upload_base64`` (which would
    # re-decode internally).
    try:
        payload = object_store._DATA_URL_PREFIX_RE.sub("", b64_string).strip() \
            if isinstance(b64_string, str) else ""
        if not isinstance(b64_string, str) or not payload:
            raise ValueError("empty or non-string payload")
        data = base64.b64decode(payload, validate=True)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid base64 payload in {field_name}: {exc}",
        )

    # When the caller has classified the upload, enforce size + magic.
    detected_format: Optional[str] = None
    if type_key:
        detected_format = _validate_upload_or_400(data, type_key, field_name)
        content_type = _FORMAT_TO_CONTENT_TYPE[detected_format]
    else:
        # Legacy fallback: trust default_ext only.
        ext = (default_ext or "").lower().lstrip(".")
        content_type = (
            "image/jpeg" if ext in ("jpg", "jpeg")
            else "image/png" if ext == "png"
            else "image/webp" if ext == "webp"
            else "application/pdf" if ext == "pdf"
            else "application/octet-stream"
        )

    try:
        if type_key and detected_format and detected_format != "pdf":
            thumb_key = _upload_with_thumbnail(
                bucket, key, data, content_type, expected_company_id,
            )
        else:
            object_store.upload_bytes(
                bucket, key, data, content_type,
                expected_company_id=expected_company_id,
            )
            thumb_key = None
    except object_store.TenantPrefixViolation as exc:
        raise HTTPException(
            status_code=403,
            detail=f"Forbidden Object_Key for {field_name}: {exc}",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid base64 payload in {field_name}: {exc}",
        )

    # Schedule async PDF compression for large PDFs.
    if (
        detected_format == "pdf"
        and background_tasks is not None
        and len(data) >= PDF_COMPRESS_MIN_BYTES
    ):
        background_tasks.add_task(
            compress_pdf_async,
            bucket,
            key,
            len(data),
            expected_company_id,
        )

    return thumb_key


def _presign_if_key(bucket: str, key: Optional[str]) -> Optional[str]:
    """Return a presigned GET URL for ``<bucket>/<key>`` or ``None``.

    Read-path helper used by every handler that returns a document carrying
    an ``<name>_object_key`` field. Emits a sibling ``<name>_url`` so the
    frontend can render the asset directly from
    ``https://api.fleetshield365.com/files/<bucket>/<key>?X-Amz-...`` via
    Nginx_Proxy (Requirements 21.12, 21.13).

    Returns ``None`` when ``key`` is falsy (missing, empty string, ``None``)
    so callers can unconditionally emit ``<name>_url: _presign_if_key(...)``
    for both populated and empty key fields.

    Any exception from ``object_store.presign_get`` (MinIO outage, transient
    network error, misconfigured client) is logged and swallowed by
    returning ``None``. Presigning is a read-side convenience; a single
    MinIO hiccup must not 500 the entire read path and block the rest of
    the response body.
    """
    if not key:
        return None
    try:
        return object_store.presign_get(bucket, key)
    except Exception as exc:
        logger.warning(
            "Failed to presign %s/%s: %s", bucket, key, exc
        )
        return None


def _presign_keys(bucket: str, keys: Optional[List[str]]) -> List[Optional[str]]:
    """Return a parallel list of presigned URLs for a list of object keys.

    Used for fields like ``incidents.damage_photos`` that are stored as a
    flat ``List[str]`` of object keys. The returned list preserves
    positional alignment so a frontend can pair ``damage_photos[i]`` with
    ``damage_photo_urls[i]``. Falsy entries (``None``, empty string) map to
    ``None`` in the output.
    """
    return [_presign_if_key(bucket, k) for k in (keys or [])]


def _presign_photos(
    photos: List[dict],
    bucket: str,
    *,
    url_field: str = "object_url",
    key_field: str = "object_key",
) -> List[dict]:
    """Return a shallow copy of ``photos`` with a presigned URL added.

    Each input dict gets ``<url_field>`` populated from
    ``object_store.presign_get(bucket, entry[<key_field>])`` when
    ``entry[<key_field>]`` is set; otherwise ``<url_field>`` is ``None``.
    The original dicts are not mutated; a new list of new dicts is
    returned so callers can pass the result straight into ``serialize_doc``
    without affecting their source data.
    """
    enriched: List[dict] = []
    for entry in photos or []:
        if not isinstance(entry, dict):
            enriched.append(entry)
            continue
        copy = dict(entry)
        copy[url_field] = _presign_if_key(bucket, copy.get(key_field))
        enriched.append(copy)
    return enriched

# ============== Subdomain helpers (Task 6) ==============
#
# These helpers implement the tenant subdomain validation and slug
# generation described in Requirements 9 and 10:
#
# * validate_subdomain — normalize + format-check + reserved-name check
#   (Req 9.3, 9.4, 9.5, 9.9, 9.10)
# * ensure_subdomain_unique — DB uniqueness check (Req 9.2, 9.11)
# * slug_generator — derive a candidate slug from a company name and
#   resolve collisions against reserved list + existing subdomains
#   (Req 9.7, 9.8)
#
# The register endpoints wire these together (Req 9.1, 9.3, 9.7) and
# translate the raised ``SubdomainValidationError`` subclasses into HTTP
# status codes (Req 9.9, 9.10, 9.11).


def validate_subdomain(value: str) -> str:
    """Normalize and validate a user-submitted subdomain slug.

    Returns the lowercased, stripped canonical form on success. Raises
    ``MalformedSubdomainError`` when the value fails ``SUBDOMAIN_REGEX``
    (Requirements 9.3, 9.4, 9.10) and ``ReservedSubdomainError`` when the
    normalized value is in ``RESERVED_SUBDOMAINS`` (Requirements 9.5,
    9.9, 10.1, 10.2). Uniqueness against the ``companies`` collection is
    a separate concern and lives in ``ensure_subdomain_unique`` so callers
    that only need format validation can skip the DB round-trip.

    ``value`` being ``None`` or a non-string is treated as malformed so
    this function is safe to call on raw request bodies.
    """
    if not isinstance(value, str):
        raise MalformedSubdomainError(
            subdomain=str(value) if value is not None else "",
            message="subdomain must be a string",
        )
    normalized = value.strip().lower()
    if SUBDOMAIN_REGEX.fullmatch(normalized) is None:
        raise MalformedSubdomainError(
            subdomain=normalized,
            message=(
                "subdomain must be 3-30 characters of lowercase letters, "
                "digits, or hyphens, with no leading or trailing hyphen"
            ),
        )
    if normalized in RESERVED_SUBDOMAINS:
        raise ReservedSubdomainError(
            subdomain=normalized,
            message=f"subdomain {normalized!r} is reserved",
        )
    return normalized


async def ensure_subdomain_unique(
    subdomain: str,
    db_,
    *,
    exclude_company_id: Optional[str] = None,
) -> None:
    """Raise ``SubdomainTakenError`` if another company already holds the slug.

    ``subdomain`` must already be normalized (lowercase, trimmed) —
    callers should run ``validate_subdomain`` first. ``exclude_company_id``
    lets the rename flow exempt the current tenant from the collision
    check (Requirement 17.1 — a no-op rename must not 409 itself).

    The query uses a simple equality filter against ``companies.subdomain``;
    Requirement 9.6 pairs this with a unique sparse case-insensitive index
    that the ``ensure_indexes()`` bootstrap creates (Task 12.3), giving
    the DB the authoritative uniqueness guarantee under concurrent writes.
    This application-level check runs first so we can return a clean 409
    rather than surfacing a raw ``DuplicateKeyError``.
    """
    existing = await db_.companies.find_one(
        {"subdomain": subdomain},
        {"_id": 1},
    )
    if existing is None:
        return
    if exclude_company_id and str(existing["_id"]) == exclude_company_id:
        return
    raise SubdomainTakenError(
        subdomain=subdomain,
        message=f"subdomain {subdomain!r} is already in use",
    )


async def slug_generator(name: str, db_) -> str:
    """Derive a unique, non-reserved tenant slug from a company ``name``.

    Implements Requirements 9.7 and 9.8:

    1. Lowercase ``name``.
    2. Replace runs of non-alphanumeric characters with a single hyphen.
    3. Strip leading and trailing hyphens.
    4. Truncate to 30 characters.
    5. If the candidate is empty, fails ``SUBDOMAIN_REGEX``, collides with
       ``RESERVED_SUBDOMAINS``, or already exists in
       ``companies.subdomain``, append ``-N`` starting at N=2 and
       incrementing until a valid, unreserved, unique slug is found.

    When the normalized base is shorter than the 3-character minimum
    (e.g., a pathological name like ``"!"`` that strips to empty) we
    fall back to a random ``tenant-<hex>`` base so the generator always
    terminates with a regex-valid slug rather than raising.

    The caller (register endpoints) treats the returned value as
    already-validated; downstream persistence code must NOT re-validate
    with ``validate_subdomain`` because a generator-produced slug is
    guaranteed to match ``SUBDOMAIN_REGEX``.
    """
    # Step 1-3: normalize and slugify. Collapse any run of non-
    # alphanumerics into a single hyphen (covers whitespace, punctuation,
    # unicode letters we don't accept) without needing a full Unicode-
    # aware slugify library.
    base = (name or "").lower()
    base = re.sub(r'[^a-z0-9]+', '-', base).strip('-')
    # Step 4: enforce the 30-char upper bound up front so suffix math has
    # room. Re-strip trailing hyphens in case truncation landed mid-run.
    base = base[:30].strip('-')

    # Fallback for empty/too-short bases (e.g., name was "!!!" or a
    # single character). The fallback form is always regex-valid
    # (``tenant-`` prefix + 6 hex chars = 13 chars, all lowercase
    # alphanumeric/hyphen, no leading/trailing hyphen).
    if len(base) < 3 or SUBDOMAIN_REGEX.fullmatch(base) is None:
        base = f"tenant-{uuid.uuid4().hex[:6]}"

    # Step 5: resolve collisions. Try the bare base first, then `-2`,
    # `-3`, ... The suffix starts at 2 per Requirement 9.8. We use a
    # sentinel ``suffix == 1`` to mean "no suffix yet".
    suffix = 1
    while True:
        if suffix == 1:
            candidate = base
        else:
            suffix_str = f"-{suffix}"
            # Ensure base + suffix fits in 30 chars; trim the base if
            # needed and re-strip trailing hyphen.
            max_base_len = 30 - len(suffix_str)
            trimmed_base = base[:max_base_len].rstrip('-')
            if not trimmed_base:
                # Extreme edge case: suffix_str alone is ~30 chars (would
                # require N with ~28 digits). Fall through to a random
                # base so we terminate.
                trimmed_base = f"tenant-{uuid.uuid4().hex[:6]}"[:max_base_len]
            candidate = f"{trimmed_base}{suffix_str}"

        if (
            SUBDOMAIN_REGEX.fullmatch(candidate) is not None
            and candidate not in RESERVED_SUBDOMAINS
        ):
            existing = await db_.companies.find_one(
                {"subdomain": candidate},
                {"_id": 1},
            )
            if existing is None:
                return candidate

        suffix = 2 if suffix == 1 else suffix + 1
        # Guard against unbounded loops under pathological states. After
        # 1000 tries, fall back to a random suffix which has effectively
        # zero collision probability.
        if suffix > 1000:
            return f"tenant-{uuid.uuid4().hex[:8]}"


def _subdomain_error_to_http(exc: SubdomainValidationError) -> HTTPException:
    """Translate a ``SubdomainValidationError`` into the spec HTTP code.

    Per Requirements 9.9, 9.10, 9.11:
    * ``reserved`` → 400 with a body identifying the subdomain as reserved
    * ``malformed`` → 400 with a body describing the required format
    * ``taken`` → 409

    Unknown codes fall through to a generic 400 so any future subclass
    that forgets to map cleanly still fails closed rather than 500ing.
    """
    status_code = 409 if exc.code == "taken" else 400
    return HTTPException(
        status_code=status_code,
        detail={
            "error": "invalid_subdomain",
            "code": exc.code,
            "subdomain": exc.subdomain,
            "message": str(exc),
        },
    )


# ============== Pydantic Models ==============

class UserRole:
    SUPER_ADMIN = "super_admin"
    ADMIN = "admin"
    DRIVER = "driver"
    PLATFORM_OWNER = "platform_owner"


# Allowed values for the JWT ``role`` claim (Requirement 12.2). Any future
# role addition must be reflected here and in the UserRole pseudo-enum.
ALLOWED_JWT_ROLES: frozenset[str] = frozenset({
    UserRole.SUPER_ADMIN,
    UserRole.ADMIN,
    UserRole.DRIVER,
    UserRole.PLATFORM_OWNER,
})

class VehicleStatus:
    ACTIVE = "active"
    UNDER_MAINTENANCE = "under_maintenance"
    REGO_EXPIRED = "rego_expired"
    SAFETY_INSPECTION_DUE = "safety_inspection_due"

class InspectionType:
    PRESTART = "prestart"
    END_SHIFT = "end_shift"

class ChecklistItemStatus:
    OK = "ok"
    ISSUE = "issue"
    NOT_APPLICABLE = "not_applicable"

class AIDamageStatus:
    NO_DAMAGE = "no_damage"
    POSSIBLE_DAMAGE = "possible_damage"
    CONFIRMED_DAMAGE = "confirmed_damage"

# Auth Models
class CustomDocumentInput(BaseModel):
    """A repeatable, owner-defined driver document.

    Replaces the hardcoded medical / first_aid / forklift / dangerous_goods
    / MSIC / other slots. The front/back files are accepted as base64 and
    persisted to the `compliance` MinIO bucket; `front_object_key` /
    `back_object_key` are written by the backend and surfaced as presigned
    URLs on read.
    """
    label: str
    number: Optional[str] = None
    issue: Optional[str] = None    # DD/MM/YYYY
    expiry: Optional[str] = None   # DD/MM/YYYY or "NA"
    front_base64: Optional[str] = None    # data URL or raw base64; image OR pdf
    back_base64: Optional[str] = None
    front_object_key: Optional[str] = None  # set by server; keep on update
    back_object_key: Optional[str] = None
    # Optional client-side hint about which type of file the photo is.
    # Server still validates magic bytes — this is purely a UI affordance.
    front_kind: Optional[str] = None  # "image" | "pdf"
    back_kind: Optional[str] = None


class UserRegister(BaseModel):
    email: Optional[EmailStr] = None  # Optional - can login with username instead
    password: str
    name: str
    username: Optional[str] = None  # Auto-generated if not provided
    phone: Optional[str] = None
    role: str = UserRole.DRIVER
    company_id: Optional[str] = None
    # Optional tenant subdomain (Requirements 9.1, 9.3). When provided the
    # value is validated + uniqueness-checked; when omitted the register
    # handler calls slug_generator() to derive one from the company name.
    # Mobile clients should NOT set this — they carry tenant context via
    # JWT claims (Requirement 18.2) — but the web signup form may.
    subdomain: Optional[str] = None
    # Driver license and training details
    license_number: Optional[str] = None
    license_class: Optional[str] = None
    license_issue_date: Optional[str] = None  # DD/MM/YYYY
    license_expiry: Optional[str] = None  # DD/MM/YYYY or "NA"
    medical_certificate_number: Optional[str] = None
    medical_certificate_issue: Optional[str] = None
    medical_certificate_expiry: Optional[str] = None
    first_aid_number: Optional[str] = None
    first_aid_issue: Optional[str] = None
    first_aid_expiry: Optional[str] = None
    forklift_license_number: Optional[str] = None
    forklift_license_issue: Optional[str] = None
    forklift_license_expiry: Optional[str] = None
    dangerous_goods_number: Optional[str] = None
    dangerous_goods_issue: Optional[str] = None
    dangerous_goods_expiry: Optional[str] = None
    # MSIC (Maritime Security Identification Card) — Phase 2.1
    msic_number: Optional[str] = None
    msic_issue: Optional[str] = None
    msic_expiry: Optional[str] = None
    # Free-form "Other document" slot. The label is owner-supplied so any
    # cert the fixed types don't cover can still be tracked.
    other_doc_label: Optional[str] = None
    other_doc_number: Optional[str] = None
    other_doc_issue: Optional[str] = None
    other_doc_expiry: Optional[str] = None
    # 2026-05-19 — `custom_documents` is the new owner-driven, repeatable
    # document list. Replaces the hardcoded forklift/dangerous_goods/MSIC/
    # other slots above. Each entry has an arbitrary label + number +
    # issue + expiry and optional front/back uploads (image OR PDF).
    custom_documents: Optional[List["CustomDocumentInput"]] = None
    # 2026-05-19 — `pin` is a 4-digit numeric PIN used by drivers to sign
    # in on the mobile app. When supplied it replaces the `password` field
    # (no policy enforcement). For drivers we require PIN; for admins we
    # require password.
    pin: Optional[str] = None

class DriverUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    license_number: Optional[str] = None
    license_class: Optional[str] = None
    license_issue_date: Optional[str] = None
    license_expiry: Optional[str] = None
    medical_certificate_number: Optional[str] = None
    medical_certificate_issue: Optional[str] = None
    medical_certificate_expiry: Optional[str] = None
    first_aid_number: Optional[str] = None
    first_aid_issue: Optional[str] = None
    first_aid_expiry: Optional[str] = None
    forklift_license_number: Optional[str] = None
    forklift_license_issue: Optional[str] = None
    forklift_license_expiry: Optional[str] = None
    dangerous_goods_number: Optional[str] = None
    dangerous_goods_issue: Optional[str] = None
    dangerous_goods_expiry: Optional[str] = None
    # MSIC (Maritime Security Identification Card) — Phase 2.1
    msic_number: Optional[str] = None
    msic_issue: Optional[str] = None
    msic_expiry: Optional[str] = None
    # Free-form "Other document" slot. The label is owner-supplied so any
    # cert the fixed types don't cover can still be tracked.
    other_doc_label: Optional[str] = None
    other_doc_number: Optional[str] = None
    other_doc_issue: Optional[str] = None
    other_doc_expiry: Optional[str] = None
    # 2026-05-19 — `custom_documents` mirrors UserRegister. Full replace on
    # update (the entire list is sent back from the UI).
    custom_documents: Optional[List["CustomDocumentInput"]] = None
    # 2026-05-19 — optional PIN reset. When supplied, must be 4 digits.
    pin: Optional[str] = None


class UserLogin(BaseModel):
    email: Optional[str] = None  # Can be email or username
    username: Optional[str] = None  # Alternative to email
    password: str
    remember_me: bool = False  # Keep logged in option
    # Optional tenant subdomain (Requirements 13.1, 14.2). When the web
    # client is on a tenant host like ``acme.fleetshield365.com`` it
    # includes ``tenant_subdomain: "acme"`` so the backend enforces that
    # the authenticated user belongs to that tenant (401 on mismatch,
    # 401 on unknown slug). When omitted — apex / www / mobile — the
    # login proceeds and the apex path returns a ``redirect_to`` pointing
    # at the user's own tenant dashboard.
    tenant_subdomain: Optional[str] = None

# Fuel Submission Models
class FuelSubmission(BaseModel):
    vehicle_id: str
    amount: float  # Dollar amount
    liters: float
    receipt_photo_base64: Optional[str] = None
    odometer: Optional[int] = None
    fuel_station: Optional[str] = None
    notes: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    location_address: Optional[str] = None
    timestamp: Optional[str] = None  # ISO timestamp from mobile app (for offline submissions)
    idempotency_key: Optional[str] = None  # see PrestartCreate

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict

# Company Models
class CompanyCreate(BaseModel):
    name: str
    logo_base64: Optional[str] = None
    timezone: Optional[str] = "Australia/Sydney"

class CompanyUpdate(BaseModel):
    name: Optional[str] = None
    logo_base64: Optional[str] = None
    subscription_plan: Optional[str] = None
    timezone: Optional[str] = None
    # Phase 5 — region preferences. The Settings panel writes these so
    # the owner can override what we auto-detected at signup.
    country: Optional[str] = None
    preferred_currency: Optional[str] = None
    units_system: Optional[str] = None
    locale: Optional[str] = None
    # 2026-05-19 — default workshop email used to pre-fill the "Email
    # Defects to Workshop" modal. Persisted per tenant so each admin
    # doesn't retype it. Empty string clears the default.
    workshop_email_default: Optional[str] = None
    workshop_name_default: Optional[str] = None

# Vehicle Models
class VehicleCustomField(BaseModel):
    label: str
    value: str = ""
    # 2026-05-19 — owner request: optional issue + expiry per custom
    # field, so things like "Trailer rego" can carry their own dates
    # and feed into /expiry-summary the same way as license/insurance/
    # COI do. DD/MM/YYYY format (parse_date_flexible handles parsing).
    issue: Optional[str] = None
    expiry: Optional[str] = None

class VehicleCreate(BaseModel):
    name: str
    registration_number: str
    trailer_attached: Optional[str] = None
    status: str = VehicleStatus.ACTIVE
    type: Optional[str] = "truck"
    rego_expiry: Optional[str] = None
    insurance_expiry: Optional[str] = None
    safety_certificate_expiry: Optional[str] = None
    coi_expiry: Optional[str] = None
    service_due_km: Optional[int] = None
    current_odometer: Optional[int] = 0
    assigned_driver_ids: Optional[List[str]] = None
    # Phase 2.2 — optional image, free-text notes, owner-defined extras.
    image_base64: Optional[str] = None
    notes: Optional[str] = None
    custom_fields: Optional[List[VehicleCustomField]] = None
    # 2026-05-20 — image OR PDF for each of the four expiry documents.
    # The form already collects the EXPIRY DATE; this adds the supporting
    # paperwork (rego cert, insurance policy, etc) so the owner can pull
    # the file at audit time. UPLOAD_FORMAT_GROUP['vehicle_doc']=image_or_pdf.
    rego_doc_base64: Optional[str] = None
    insurance_doc_base64: Optional[str] = None
    safety_cert_doc_base64: Optional[str] = None
    coi_doc_base64: Optional[str] = None

class VehicleUpdate(BaseModel):
    name: Optional[str] = None
    registration_number: Optional[str] = None
    trailer_attached: Optional[str] = None
    status: Optional[str] = None
    type: Optional[str] = None
    rego_expiry: Optional[str] = None
    insurance_expiry: Optional[str] = None
    safety_certificate_expiry: Optional[str] = None
    coi_expiry: Optional[str] = None
    service_due_km: Optional[int] = None
    current_odometer: Optional[int] = None
    assigned_driver_ids: Optional[List[str]] = None
    image_base64: Optional[str] = None
    notes: Optional[str] = None
    custom_fields: Optional[List[VehicleCustomField]] = None
    rego_doc_base64: Optional[str] = None
    insurance_doc_base64: Optional[str] = None
    safety_cert_doc_base64: Optional[str] = None
    coi_doc_base64: Optional[str] = None

# Checklist Models
class ChecklistItem(BaseModel):
    name: str
    section: str
    status: str = ChecklistItemStatus.OK
    comment: Optional[str] = None

class InspectionPhoto(BaseModel):
    """Inspection photo payload accepted by /inspections/prestart and
    /inspections/end-shift.

    Two upload patterns are supported (Phase 2 of STORAGE-PLAN.txt):

    * Legacy: ``base64_data`` carries the photo bytes inline. The handler
      decodes + uploads to MinIO at submit time.
    * Preferred: ``photo_id`` references a row in ``temp_photos`` from a
      prior POST /photos/upload-multipart call — the bytes already live
      in MinIO. The handler just links the existing object_key to the
      new inspection and marks the temp row as used. Saves the JSON
      body roughly 33% (no base64 overhead on the wire) and lets the
      mobile app upload photos as the driver captures them instead of
      one giant payload at submit time.
    """
    photo_type: str  # front, rear, left, right, cabin, odometer, damage
    base64_data: Optional[str] = None
    photo_id: Optional[str] = None
    timestamp: str
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    location_address: Optional[str] = None
    ai_damage_status: str = AIDamageStatus.NO_DAMAGE

# Inspection Models
class DigitalAgreement(BaseModel):
    driver_name: str
    driver_id: Optional[str] = None
    agreed_at: str  # ISO timestamp
    declaration_text: str
    device_info: Optional[str] = None

class PrestartCreate(BaseModel):
    vehicle_id: str
    odometer: int
    checklist_items: List[ChecklistItem]
    photos: List[InspectionPhoto]
    signature_base64: Optional[str] = None  # Now optional - replaced by digital agreement
    digital_agreement: Optional[DigitalAgreement] = None  # New digital consent
    declaration_confirmed: bool = True
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    location_address: Optional[str] = None
    timestamp: Optional[str] = None  # ISO timestamp from mobile app (for offline submissions)
    # 2026-05-19 — client-supplied idempotency key. Set to the mobile
    # uploadQueue's `dedupHash` so a queue retry after a network blip
    # hits the same key and the server returns the existing inspection
    # instead of inserting a second copy. Owner-reported duplicates in
    # WhatsApp screenshot.
    idempotency_key: Optional[str] = None

class EndShiftCreate(BaseModel):
    vehicle_id: str
    odometer: int
    fuel_level: str
    new_damage: bool = False
    incident_today: bool = False
    cleanliness: str  # clean, average, dirty
    damage_comment: Optional[str] = None
    incident_comment: Optional[str] = None
    photos: Optional[List[InspectionPhoto]] = []
    signature_base64: Optional[str] = None  # Now optional
    digital_agreement: Optional[DigitalAgreement] = None  # New digital consent
    declaration_confirmed: bool = True
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    location_address: Optional[str] = None
    timestamp: Optional[str] = None  # ISO timestamp from mobile app (for offline submissions)
    idempotency_key: Optional[str] = None  # see PrestartCreate

# Maintenance Models
class MaintenanceLogCreate(BaseModel):
    vehicle_id: str
    service_date: str
    service_type: str
    cost: float
    workshop_name: str
    invoice_base64: Optional[str] = None
    notes: Optional[str] = None

# ============== Service Record Models ==============

class ServiceType(str, Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    WARRANTY = "warranty"
    OTHER = "other"

class ServiceRecordCreate(BaseModel):
    vehicle_id: str
    service_date: str  # YYYY-MM-DD
    service_type: ServiceType
    service_type_other: Optional[str] = None  # For "other" type
    description: str
    cost: Optional[float] = None
    odometer_reading: Optional[int] = None
    technician_name: Optional[str] = None
    workshop_name: Optional[str] = None
    next_service_date: Optional[str] = None  # Scheduled next service
    next_service_odometer: Optional[int] = None  # Or at this odometer
    attachments: Optional[List[str]] = []  # Base64 encoded photos/docs
    warranty_until: Optional[str] = None  # Warranty expiry date
    warranty_notes: Optional[str] = None  # Warranty details

class ServiceRecordUpdate(BaseModel):
    service_date: Optional[str] = None
    service_type: Optional[ServiceType] = None
    service_type_other: Optional[str] = None
    description: Optional[str] = None
    cost: Optional[float] = None
    odometer_reading: Optional[int] = None
    technician_name: Optional[str] = None
    workshop_name: Optional[str] = None
    next_service_date: Optional[str] = None
    next_service_odometer: Optional[int] = None
    attachments: Optional[List[str]] = None
    warranty_until: Optional[str] = None
    warranty_notes: Optional[str] = None

# Alert Models
class AlertCreate(BaseModel):
    type: str  # unsafe_vehicle, repeated_issues, expiry_warning, vehicle_offline
    message: str
    vehicle_id: Optional[str] = None
    driver_id: Optional[str] = None



# ============== Support Request Models ==============

class SupportRequestCategory(str, Enum):
    GENERAL = "general"
    TECHNICAL = "technical"
    BILLING = "billing"
    FEATURE_REQUEST = "feature_request"
    BUG_REPORT = "bug_report"

class SupportRequestStatus(str, Enum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    CLOSED = "closed"

class SupportRequestCreate(BaseModel):
    subject: str
    message: str
    category: SupportRequestCategory = SupportRequestCategory.GENERAL

class SupportRequestUpdate(BaseModel):
    status: Optional[SupportRequestStatus] = None
    admin_response: Optional[str] = None


# ============== Incident Report Models ==============

class IncidentSeverity:
    MINOR = "minor"
    MODERATE = "moderate"
    SEVERE = "severe"

def _validate_au_phone(v: Optional[str]) -> Optional[str]:
    """Owner request 2026-05-23: phone numbers on the incident form
    must be exactly 10 digits (Australian convention, e.g. 0412345678).
    Accept blank/None as a no-op so the field stays optional, but
    reject any partial-typed value. Strips spaces / dashes / brackets
    so '04 1234 5678' or '(04) 1234-5678' are accepted as 10 digits.
    """
    if v is None:
        return None
    cleaned = ''.join(ch for ch in v if ch.isdigit())
    if cleaned == '':
        return None
    if len(cleaned) != 10:
        raise ValueError('Phone number must be exactly 10 digits.')
    return cleaned


class OtherPartyDetails(BaseModel):
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    vehicle_rego: Optional[str] = None
    insurance_company: Optional[str] = None
    insurance_policy: Optional[str] = None

    @field_validator('phone')
    @classmethod
    def _phone_must_be_10_digits(cls, v):
        return _validate_au_phone(v)


class WitnessDetails(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    statement: Optional[str] = None

    @field_validator('phone')
    @classmethod
    def _phone_must_be_10_digits(cls, v):
        return _validate_au_phone(v)

class IncidentCreate(BaseModel):
    # Owner request 2026-05-27 — admins can record incidents for vehicles
    # NOT in the fleet (e.g. a hired vehicle, another party's vehicle).
    # When vehicle_id is omitted, vehicle_other_label MUST be supplied
    # and gets stored as the display name (no MinIO key prefix issues
    # because there's no vehicle row to reference).
    vehicle_id: Optional[str] = None
    vehicle_other_label: Optional[str] = None
    description: str
    severity: str = IncidentSeverity.MODERATE  # minor, moderate, severe
    location_address: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    other_party: OtherPartyDetails
    witnesses: Optional[List[WitnessDetails]] = []
    police_report_number: Optional[str] = None
    injuries_occurred: bool = False
    injury_description: Optional[str] = None
    damage_photos: List[str] = []  # Base64 encoded photos
    other_vehicle_photos: List[str] = []  # Base64 encoded photos
    scene_photos: List[str] = []  # Base64 encoded photos
    timestamp: Optional[str] = None  # ISO timestamp from mobile app (for offline submissions)
    idempotency_key: Optional[str] = None  # see PrestartCreate

class IncidentUpdate(BaseModel):
    status: Optional[str] = None  # reported, under_review, resolved, closed
    admin_notes: Optional[str] = None
    insurance_claim_number: Optional[str] = None
    resolution_details: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    location_address: Optional[str] = None
    police_report_number: Optional[str] = None
    # Additional photos - will be appended to existing
    additional_photos: Optional[List[str]] = None
    # PDF attachments (base64 encoded)
    pdf_attachments: Optional[List[dict]] = None  # [{name: str, data: str}]

# Driver Assignment
class DriverAssignment(BaseModel):
    driver_ids: List[str]

# Company Registration Model
class CompanyRegister(BaseModel):
    company_name: str
    name: str
    email: EmailStr
    password: str
    vehicle_count: int = 5
    origin_url: Optional[str] = None
    role: Optional[str] = None  # 'super_admin' for Company Owner, 'admin' for Admin
    timezone: Optional[str] = "Australia/Sydney"  # Company timezone for timestamps
    # Phase 5 — region preferences detected at signup. All optional;
    # missing values fall through to sensible defaults (AU/AUD/metric).
    country: Optional[str] = None         # ISO 3166-1 alpha-2
    preferred_currency: Optional[str] = None  # ISO 4217
    units_system: Optional[str] = None    # "metric" | "imperial"
    locale: Optional[str] = None          # BCP-47 (e.g. "en-AU")
    # Optional tenant subdomain (Requirements 9.1, 9.3). When provided the
    # value is validated + uniqueness-checked; when omitted the register
    # handler calls slug_generator() to derive one from ``company_name``.
    subdomain: Optional[str] = None

# Pricing configuration — fallback defaults. Live values are stored in
# the ``platform_config`` collection under ``_id == "pricing"`` and are
# read via ``get_pricing()``. The legacy PRICING dict is retained as a
# fallback so a fresh install (or a missing config doc) still serves
# sensible prices.
PRICING = {
    "base_price": 29,
    "per_vehicle": 3,
    "trial_days": 15,  # Phase 4.2 — bumped default to 15 days
    "trial_enabled": True,  # Phase 4.2 — owner-controlled global flag
    "trial_max_vehicles": None,  # None = unlimited during trial
    "currency": "AUD",
    "cadence": "monthly",
}


async def get_pricing() -> dict:
    """Read live pricing config from ``platform_config``.

    Falls back to the module-level ``PRICING`` defaults on miss. Returns
    a dict with keys ``base_price``, ``per_vehicle``, ``vehicle_price``
    (alias for backwards compat), ``trial_days``, ``currency``,
    ``cadence``, plus any ``stripe`` sub-dict the owner panel wrote.
    """
    try:
        doc = await db.platform_config.find_one({"_id": "pricing"})
    except Exception:
        doc = None
    base = dict(PRICING)
    if doc:
        for k in (
            "base_price", "per_vehicle", "trial_days", "currency", "cadence",
            "trial_enabled", "trial_max_vehicles",
        ):
            v = doc.get(k)
            if v is not None:
                base[k] = v
        if doc.get("stripe"):
            base["stripe"] = doc["stripe"]
        if doc.get("updated_at"):
            base["updated_at"] = doc["updated_at"].isoformat() if isinstance(doc["updated_at"], datetime) else doc["updated_at"]
    # Backwards-compat alias so older callers reading ``vehicle_price``
    # still work.
    base["vehicle_price"] = base["per_vehicle"]
    return base

# ============== Trial Status Helpers ==============

async def get_trial_status(company_id: str) -> dict:
    """Check trial/subscription status for a company"""
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    if not company:
        return {"status": "unknown", "is_active": False}
    
    subscription_status = company.get("subscription_status", "trialing")
    trial_end_str = company.get("trial_end")
    
    # If already paid/active subscription
    if subscription_status == "active":
        return {
            "status": "active",
            "is_active": True,
            "plan": company.get("subscription_plan", "pro"),
            "message": "Active subscription"
        }
    
    # Check trial status
    if trial_end_str:
        try:
            trial_end = datetime.fromisoformat(trial_end_str.replace('Z', '+00:00'))
            if isinstance(trial_end, datetime) and trial_end.tzinfo is None:
                trial_end = trial_end.replace(tzinfo=None)
            now = utcnow()
            
            days_left = (trial_end - now).days
            
            if days_left > 0:
                return {
                    "status": "trialing",
                    "is_active": True,
                    "days_left": days_left,
                    "trial_end": trial_end_str,
                    "message": f"Trial: {days_left} days remaining"
                }
            else:
                return {
                    "status": "trial_expired",
                    "is_active": False,
                    "days_left": 0,
                    "trial_end": trial_end_str,
                    "message": "Trial expired - Please upgrade to continue"
                }
        except Exception as e:
            logger.error(f"Error parsing trial_end: {e}")
    
    # Default to expired if no valid trial info
    return {
        "status": "trial_expired", 
        "is_active": False,
        "message": "Trial expired - Please upgrade to continue"
    }

async def check_trial_active(company_id: str) -> bool:
    """Quick check if trial/subscription is active"""
    status = await get_trial_status(company_id)
    return status.get("is_active", False)

# ============== PDF Generation ==============

async def _resolve_inspection_photo(
    photo,
    company_id: str,
    inspection_id: str,
    *,
    inspection_type_label: str,
) -> tuple:
    """Materialize one inspection photo and return (object_key, source_bucket).

    Phase 2 of STORAGE-PLAN.txt — dual-path support:

    * ``photo.photo_id`` set → the bytes were pre-uploaded via the
      multipart endpoint and live in the ``photos`` bucket under a
      tenant-scoped key. We look up the temp_photos row, validate the
      tenant prefix, mark the row as used (TTL still cleans up the
      pointer after 24h), and reference the same object_key from the
      new inspection_photos doc — no re-upload, no second copy.
    * ``photo.base64_data`` set → legacy single-shot submit. Decode +
      validate + upload to inspection-photos bucket as before.

    Always returns ``(object_key, source_bucket)``. Callers persist
    ``source_bucket`` on the inspection_photos doc so the read-path
    serializer can sign URLs against the right bucket.

    Raises HTTPException 400 if neither field is set, 404 if the
    referenced temp_photos row is missing or belongs to another
    tenant.
    """
    if photo.photo_id:
        temp_row = await db.temp_photos.find_one({
            "_id": ObjectId(photo.photo_id),
            "company_id": company_id,
        })
        if not temp_row:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"photos[{photo.photo_type}].photo_id "
                    f"{photo.photo_id!r} not found in temp_photos"
                ),
            )
        object_key = temp_row.get("object_key")
        if not object_key:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"photos[{photo.photo_type}].photo_id row is missing "
                    f"object_key — was it uploaded via the base64 path?"
                ),
            )
        # Mark the temp row as consumed by this inspection so the TTL
        # cleanup leaves it alone for the next 24h (audit trail) and
        # so we can detect double-usage if it ever happens.
        await db.temp_photos.update_one(
            {"_id": temp_row["_id"]},
            {"$set": {
                "used": True,
                "used_by_inspection_id": inspection_id,
                "used_at": utcnow(),
                "used_for": inspection_type_label,
            }},
        )
        return object_key, "photos"

    # Legacy base64 fallback. Allocate the destination key, validate +
    # upload + thumbnail in one go.
    if not photo.base64_data:
        raise HTTPException(
            status_code=400,
            detail=(
                f"photos[{photo.photo_type}]: must include either "
                f"photo_id (pre-uploaded multipart) or base64_data (legacy)"
            ),
        )
    object_key = f"{company_id}/{inspection_id}/{uuid.uuid4().hex}.jpg"
    _upload_base64_or_400(
        "inspection-photos",
        object_key,
        photo.base64_data,
        "jpg",
        f"photos[{photo.photo_type}].base64_data",
        expected_company_id=company_id,
        type_key="inspection",
    )
    return object_key, "inspection-photos"


def _pdf_company_header(company: dict, styles: dict, title: str) -> list:
    """Return a uniform PDF header (logo + company name + title) used by
    every export. Owner reported 2026-05-27 — the logo on inspection
    PDFs was low-res because we forced a 2:1 aspect that squashed
    square logos, and most other PDFs had no logo at all. This helper
    reads the logo bytes once from MinIO (or legacy base64), uses PIL
    to compute the natural aspect ratio, and embeds at a sharp 1.8"
    width. Falls back to plain company-name banner when no logo set.
    """
    elements: list = []

    logo_bytes: Optional[bytes] = None
    if company and company.get('logo_object_key'):
        try:
            logo_bytes = object_store.get_bytes("logos", company['logo_object_key'])
        except Exception as exc:
            logger.warning(
                "PDF header: could not fetch logo %s from MinIO: %s",
                company.get('logo_object_key'), exc,
            )
    if logo_bytes is None and company and company.get('logo_base64'):
        try:
            raw_b64 = company['logo_base64']
            logo_bytes = base64.b64decode(
                raw_b64.split(',')[-1] if ',' in raw_b64 else raw_b64
            )
        except Exception:
            logo_bytes = None

    if logo_bytes:
        try:
            from PIL import Image as PILImage
            with PILImage.open(BytesIO(logo_bytes)) as pim:
                w, h = pim.size
            ratio = (h / w) if w else 0.5
            target_w = 1.8 * inch
            target_h = target_w * ratio
            # cap on extremely tall logos so they don't push content off the page
            target_h = min(target_h, 1.4 * inch)
            logo_img = RLImage(BytesIO(logo_bytes), width=target_w, height=target_h)
            logo_img.hAlign = 'CENTER'
            elements.append(logo_img)
            elements.append(Spacer(1, 6))
        except Exception as exc:
            logger.warning("PDF header: failed to embed company logo: %s", exc)

    company_name = (company or {}).get("name", "FleetShield365")
    name_style = ParagraphStyle(
        'PdfCompanyName',
        parent=styles['Normal'],
        fontSize=11,
        alignment=1,
        textColor=colors.HexColor('#475569'),
        spaceAfter=4,
    )
    elements.append(Paragraph(company_name, name_style))

    title_style = ParagraphStyle(
        'PdfTitle',
        parent=styles['Heading1'],
        fontSize=18,
        alignment=1,
        textColor=colors.HexColor('#1e3a5f'),
        spaceAfter=14,
    )
    elements.append(Paragraph(title, title_style))
    return elements


async def generate_inspection_pdf_bytes(inspection: dict, vehicle: dict, driver: dict, company: dict) -> bytes:
    """Generate the inspection PDF and return raw bytes.

    Phase 2 of STORAGE-PLAN.txt — PDFs land in MinIO under
    inspection-photos/<company_id>/<inspection_id>/report.pdf, not
    inlined into Mongo. Callers either upload directly via
    _store_inspection_pdf() (write path) or stream to the client
    (download path).
    """
    pdf_b64 = await generate_inspection_pdf(inspection, vehicle, driver, company)
    return base64.b64decode(pdf_b64)


async def _store_inspection_pdf(
    inspection_id: str, company_id: str, pdf_bytes: bytes,
) -> str:
    """Upload PDF bytes to MinIO and return the tenant-scoped object key.

    Bucket is the existing ``inspection-photos`` (avoids needing a brand-
    new bucket provisioned on every MinIO box). The key shape
    ``<company_id>/<inspection_id>/report.pdf`` keeps the tenant prefix
    validation working and gives the file a clear name in the admin
    console.
    """
    key = f"{company_id}/{inspection_id}/report.pdf"
    object_store.upload_bytes(
        "inspection-photos",
        key,
        pdf_bytes,
        "application/pdf",
        expected_company_id=company_id,
    )
    return key


async def generate_inspection_pdf(inspection: dict, vehicle: dict, driver: dict, company: dict) -> str:
    """Generate PDF report for inspection and return base64 encoded string"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    styles = getSampleStyleSheet()
    
    # Get company timezone
    company_tz = company.get('timezone', DEFAULT_TIMEZONE) if company else DEFAULT_TIMEZONE
    
    # Title Style
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#1a365d'), spaceAfter=12)
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#2d3748'), spaceAfter=8)
    normal_style = ParagraphStyle('Normal', parent=styles['Normal'], fontSize=10, spaceAfter=6)
    
    # Uniform header — owner request 2026-05-27 (logo at natural ratio,
    # company name, then page title). Same helper is used by every PDF
    # generator below so branding is consistent.
    inspection_type = "Prestart Inspection Report" if inspection['type'] == 'prestart' else "End of Shift Report"
    elements.extend(_pdf_company_header(company, styles, inspection_type))
    
    # Get timezone display name
    tz_display = company_tz.split('/')[-1].replace('_', ' ')
    
    # Basic Info Table
    info_data = [
        ['Date/Time:', f"{format_timestamp(inspection.get('timestamp', 'N/A'), company_tz)} ({tz_display})"],
        ['Vehicle:', f"{vehicle.get('name', 'N/A')} ({vehicle.get('registration_number', 'N/A')})"],
        ['Driver:', driver.get('name', 'N/A')],
        ['Odometer:', f"{inspection.get('odometer', 'N/A')} km"],
    ]
    
    # Show address if available, otherwise show GPS coordinates
    if inspection.get('location_address'):
        info_data.append(['Location:', inspection['location_address']])
    elif inspection.get('gps_latitude') and inspection.get('gps_longitude'):
        info_data.append(['GPS Location:', f"{inspection['gps_latitude']:.6f}, {inspection['gps_longitude']:.6f}"])
    
    info_table = Table(info_data, colWidths=[100, 350])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e2e8f0')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#2d3748')),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cbd5e0')),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))
    
    # Checklist (for prestart)
    if inspection['type'] == 'prestart' and inspection.get('checklist_items'):
        elements.append(Paragraph("Inspection Checklist", heading_style))
        
        checklist_data = [['Item', 'Section', 'Status', 'Comment']]
        for item in inspection['checklist_items']:
            status_color = '✓' if item['status'] == 'ok' else ('⚠' if item['status'] == 'issue' else 'N/A')
            checklist_data.append([
                item['name'],
                item['section'],
                status_color,
                item.get('comment', '')[:50] if item.get('comment') else ''
            ])
        
        checklist_table = Table(checklist_data, colWidths=[150, 100, 60, 140])
        checklist_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2d3748')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cbd5e0')),
            ('ALIGN', (2, 0), (2, -1), 'CENTER'),
        ]))
        elements.append(checklist_table)
        elements.append(Spacer(1, 20))
    
    # End Shift specific info
    if inspection['type'] == 'end_shift':
        elements.append(Paragraph("End of Shift Details", heading_style))
        shift_data = [
            ['Fuel Level:', inspection.get('fuel_level', 'N/A')],
            ['Cleanliness:', inspection.get('cleanliness', 'N/A')],
            ['New Damage:', 'Yes' if inspection.get('new_damage') else 'No'],
            ['Incident Today:', 'Yes' if inspection.get('incident_today') else 'No'],
        ]
        if inspection.get('damage_comment'):
            shift_data.append(['Damage Comment:', inspection['damage_comment'][:100]])
        if inspection.get('incident_comment'):
            shift_data.append(['Incident Comment:', inspection['incident_comment'][:100]])
        
        shift_table = Table(shift_data, colWidths=[120, 330])
        shift_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e2e8f0')),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cbd5e0')),
        ]))
        elements.append(shift_table)
        elements.append(Spacer(1, 20))
    
    # Photos section (with actual images)
    # Task 5.4: photos are now referenced by object_key on the
    # inspection_photos collection; fetch each via object_store.get_bytes
    # so the PDF embeds the actual image. Pre-migration rows that still
    # carry base64_data inline are handled by the fallback branch.
    if inspection.get('photos') and len(inspection['photos']) > 0:
        elements.append(Paragraph("Inspection Photos", heading_style))
        elements.append(Spacer(1, 10))
        
        rendered_any = False
        # Add photos one by one. Cap matches MAX_PHOTOS_PER_INSPECTION
        # (was 6, which silently truncated end-shift damage + cabin/
        # odometer extras and made the PDF look incomplete).
        for i, photo in enumerate(inspection['photos'][:MAX_PHOTOS_PER_INSPECTION]):
            try:
                photo_bytes: Optional[bytes] = None
                photo_key = photo.get('object_key')
                # Phase 2 — photos can live in either bucket; respect the
                # source_bucket hint stamped by fetch_inspection_photos.
                # Hardcoding "inspection-photos" used to silently fail for
                # every multipart-uploaded photo and the PDF came out
                # without images.
                photo_bucket = photo.get('source_bucket') or 'inspection-photos'
                if photo_key:
                    try:
                        photo_bytes = object_store.get_bytes(
                            photo_bucket, photo_key
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to fetch inspection photo %s from %s: %s",
                            photo_key, photo_bucket, exc,
                        )
                        photo_bytes = None
                if photo_bytes is None:
                    photo_base64 = photo.get('base64_data', '') or ''
                    if photo_base64:
                        if ',' in photo_base64:
                            photo_base64 = photo_base64.split(',')[-1]
                        try:
                            photo_bytes = base64.b64decode(photo_base64)
                        except Exception:
                            photo_bytes = None
                if photo_bytes:
                    photo_img = RLImage(BytesIO(photo_bytes), width=3*inch, height=2.5*inch)

                    # Add photo type label
                    photo_type = photo.get('photo_type', 'Photo').replace('_', ' ').title()
                    elements.append(Paragraph(f"<b>{photo_type}</b>", normal_style))
                    elements.append(Spacer(1, 5))
                    elements.append(photo_img)
                    elements.append(Spacer(1, 15))
                    rendered_any = True
            except Exception as e:
                logger.error(f"Failed to add photo to PDF: {e}")
                continue
        
        if not rendered_any:
            elements.append(Paragraph("Photos on file (unable to render)", normal_style))
        
        elements.append(Spacer(1, 20))
    
    # Signature
    # Task 5.4: signature now lives in MinIO at
    # inspection.signature_object_key. Fetch the bytes via
    # object_store.get_bytes; pre-migration rows still carrying
    # signature_base64 fall through to the legacy branch.
    sig_bytes: Optional[bytes] = None
    if inspection.get('signature_object_key'):
        try:
            sig_bytes = object_store.get_bytes(
                "signatures", inspection['signature_object_key']
            )
        except Exception as exc:
            logger.warning(
                "Failed to fetch inspection signature %s: %s",
                inspection.get('signature_object_key'), exc,
            )
            sig_bytes = None
    if sig_bytes is None and inspection.get('signature_base64'):
        sig_data = inspection['signature_base64']
        if ',' in sig_data:
            sig_data = sig_data.split(',')[-1]
        try:
            sig_bytes = base64.b64decode(sig_data)
        except Exception:
            sig_bytes = None
    if sig_bytes is not None:
        elements.append(Paragraph("Driver Signature", heading_style))
        try:
            sig_img = RLImage(BytesIO(sig_bytes), width=2*inch, height=0.75*inch)
            elements.append(sig_img)
        except Exception as e:
            logger.warning("Failed to embed signature: %s", e)
            elements.append(Paragraph("Signature on file", normal_style))
        elements.append(Spacer(1, 12))
    
    # Declaration
    elements.append(Paragraph("Declaration", heading_style))
    if inspection['type'] == 'prestart':
        declaration_text = "I confirm this vehicle is safe to operate."
    else:
        declaration_text = "I confirm this report is accurate."
    elements.append(Paragraph(f"✓ {declaration_text}", normal_style))
    
    # Footer
    elements.append(Spacer(1, 30))
    footer_text = f"Generated by FleetShield365 | Report ID: {str(inspection.get('_id', 'N/A'))[:8]}"
    elements.append(Paragraph(footer_text, ParagraphStyle('Footer', fontSize=8, textColor=colors.gray)))
    
    doc.build(elements)
    pdf_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    return pdf_base64

# ============== Email Service (SMTP-backed, with DB logging) ==============

class EmailService:
    """SMTP email service for sending notifications, with DB logging."""

    @staticmethod
    async def send_email(to_email: str, subject: str, body: str, company_id: str = None, is_html: bool = True, sender: str = "alerts"):
        """
        Send email via SMTP (Namecheap PrivateEmail). Falls back to mock-log when the mailbox
        password is not configured. `sender` selects which mailbox: "alerts" or "noreply".
        """
        password_configured = (SMTP_NOREPLY_PASSWORD if sender == "noreply" else SMTP_PASSWORD)
        email_log = {
            "to_email":   to_email,
            "subject":    subject,
            "body":       body[:500],
            "company_id": company_id,
            "sent_at":    utcnow(),
            "status":     "pending",
            "provider":   f"smtp:{sender}" if password_configured else "mock",
        }

        if password_configured:
            html_body = body if is_html else f"<pre>{body}</pre>"
            ok = await _send_via_smtp(to_email, subject, html_body, sender=sender)
            email_log["status"] = "sent" if ok else "failed"
        else:
            logger.info(f"[MOCK EMAIL:{sender}] To: {to_email} | Subject: {subject}")
            email_log["status"] = "mocked"

        await db.email_logs.insert_one(email_log)
        return email_log["status"] in ["sent", "mocked"]
    
    @staticmethod
    async def send_alert_email(alert_type: str, message: str, admin_emails: list, company_id: str):
        """Send alert notification to admins with styled HTML"""
        subject_map = {
            "unsafe_vehicle": "URGENT: Vehicle Marked Unsafe",
            "repeated_issues": "Alert: Repeated Vehicle Issues",
            "expiry_warning": "Reminder: Upcoming Vehicle Expiry",
            "expiry_critical": "CRITICAL: Document Has Expired",
            "driver_expiry_warning": "Reminder: Driver Document Expiring",
            "driver_expiry_critical": "CRITICAL: Driver Document Expired",
            "vehicle_offline": "Vehicle Status: Offline",
        }
        subject = subject_map.get(alert_type, "FleetShield365 Alert")
        
        # Determine alert color based on type
        alert_color = "#EF4444" if "critical" in alert_type or "unsafe" in alert_type else "#F59E0B"
        
        # Create HTML email body
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #F8FAFC; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <div style="background-color: {alert_color}; color: white; padding: 20px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">{subject}</h1>
                </div>
                <div style="padding: 30px;">
                    <p style="font-size: 16px; color: #334155; line-height: 1.6;">
                        {message}
                    </p>
                    <hr style="border: none; border-top: 1px solid #E2E8F0; margin: 20px 0;">
                    <p style="font-size: 14px; color: #64748B;">
                        This is an automated notification from FleetShield365.
                        Please log in to your dashboard to take action.
                    </p>
                </div>
                <div style="background-color: #F1F5F9; padding: 15px; text-align: center;">
                    <p style="margin: 0; font-size: 12px; color: #94A3B8;">
                        FleetShield365 - Vehicle Inspection Management
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        for email in admin_emails:
            await EmailService.send_email(email, subject, html_body, company_id, is_html=True)

email_service = EmailService()

# ============== Alert System ==============

async def check_and_create_expiry_alerts(vehicle: dict, company_id: str):
    """Check vehicle expiry dates and create alerts at 60, 30, 14, 7 day intervals"""
    now = utcnow()
    vehicle_name = f"{vehicle['name']} ({vehicle['registration_number']})"
    vehicle_id = str(vehicle['_id'])
    
    # Reminder intervals: 60, 30, 14, 7 days
    REMINDER_DAYS = [60, 30, 14, 7]
    
    expiry_fields = [
        ('rego_expiry', 'Registration'),
        ('insurance_expiry', 'Insurance'),
        ('safety_certificate_expiry', 'Safety Certificate'),
        ('coi_expiry', 'COI (Certificate of Inspection)')
    ]
    
    for field, label in expiry_fields:
        expiry_date_str = vehicle.get(field)
        if expiry_date_str:
            try:
                # Use flexible date parser (handles both DD/MM/YYYY and YYYY-MM-DD)
                expiry_date = parse_date_flexible(expiry_date_str)
                if not expiry_date:
                    continue
                    
                days_until_expiry = (expiry_date - now).days
                display_date = format_date_display(expiry_date_str)
                
                # Already expired
                if days_until_expiry < 0:
                    existing_alert = await db.alerts.find_one({
                        "vehicle_id": vehicle_id,
                        "type": "expiry_critical",
                        "message": {"$regex": f"{label}.*EXPIRED"}
                    })

                    if not existing_alert:
                        message = f"{label} for {vehicle_name} has EXPIRED (was due {display_date})"
                        await create_alert(company_id, "expiry_critical", message, vehicle_id, reminder_window="expired")

                # Check each reminder interval
                else:
                    for reminder_day in REMINDER_DAYS:
                        if days_until_expiry <= reminder_day:
                            # Determine severity based on days remaining
                            if days_until_expiry <= 7:
                                alert_type = "expiry_critical"
                                urgency = "CRITICAL"
                            elif days_until_expiry <= 14:
                                alert_type = "expiry_warning"
                                urgency = "URGENT"
                            elif days_until_expiry <= 30:
                                alert_type = "expiry_warning"
                                urgency = "ACTION NEEDED"
                            else:  # 60 days
                                alert_type = "expiry_warning"
                                urgency = "HEADS UP"

                            # Check if alert already exists for this specific reminder
                            existing_alert = await db.alerts.find_one({
                                "vehicle_id": vehicle_id,
                                "type": alert_type,
                                "message": {"$regex": f"{label}.*{vehicle_name}.*{reminder_day}"}
                            })

                            if not existing_alert:
                                message = f"[{urgency}] {label} for {vehicle_name} expires in {days_until_expiry} days ({display_date})"
                                await create_alert(company_id, alert_type, message, vehicle_id, reminder_window=str(reminder_day))

                            break  # Only create alert for the most urgent matching interval
                            
            except Exception:
                pass  # Invalid date format, skip

async def create_alert(company_id: str, alert_type: str, message: str, vehicle_id: str = None, driver_id: str = None, reminder_window: str = None):
    """Create alert and send notification.

    Each admin's `notification_preferences` is consulted before adding
    them to the email recipient list. Mapping:
      - alert_type contains "expiry" → master = `expiry_alerts`, per-window
        = `expiry_alert_{reminder_window}d` for 60/30/14/7, or
        `expiry_alert_expired` for already-expired vehicles. Drivers'
        document expiry uses the same per-window keys.
      - repeated_issues / vehicle_offline → `issue_alerts`.
      - unsafe_vehicle → no email here (handled by notify_admins_with_photos).
    Missing pref docs default to all-on so older tenants keep emails.
    """
    alert = {
        "_id": ObjectId(),
        "company_id": company_id,
        "type": alert_type,
        "message": message,
        "vehicle_id": vehicle_id,
        "driver_id": driver_id,
        "is_read": False,
        "email_sent": False,
        "created_at": utcnow()
    }
    await db.alerts.insert_one(alert)

    if alert_type == "unsafe_vehicle":
        return alert

    is_expiry = "expiry" in alert_type
    master_key = "expiry_alerts" if is_expiry else "issue_alerts"
    window_key = None
    if is_expiry and reminder_window:
        window_key = "expiry_alert_expired" if reminder_window == "expired" else f"expiry_alert_{reminder_window}d"

    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": [UserRole.SUPER_ADMIN, UserRole.ADMIN]}, "deleted_at": None
    }).to_list(100)

    admin_emails: list = []
    for admin in admins:
        if not admin.get("email"):
            continue
        prefs = await db.notification_preferences.find_one({"user_id": str(admin["_id"])}) or {}
        if not prefs.get("email_enabled", True):
            continue
        if not prefs.get(master_key, True):
            continue
        if window_key and not prefs.get(window_key, True):
            continue
        admin_emails.append(admin["email"])

    if admin_emails:
        await email_service.send_alert_email(alert_type, message, admin_emails, company_id)
        await db.alerts.update_one({"_id": alert["_id"]}, {"$set": {"email_sent": True}})

    return alert

async def log_audit_trail(user_id: str, action: str, entity_type: str, entity_id: str, ip_address: str, changes: dict = None):
    """Log audit trail entry"""
    await db.audit_trail.insert_one({
        "user_id": user_id,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "timestamp": utcnow(),
        "ip_address": ip_address,
        "changes": changes or {}
    })

# ============== Auth Routes ==============

@api_router.post("/auth/register")
async def register(user: UserRegister, request: Request):
    # Phase 3 — enforce platform password policy at every set-password
    # site so register/reset/invite/admin-set all agree on the rules.
    validate_password_policy(user.password)
    # Check if email exists
    existing = await db.users.find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create company for super_admin
    company_id = user.company_id
    if user.role == UserRole.SUPER_ADMIN and not company_id:
        # Resolve the tenant subdomain (Requirements 9.1, 9.3, 9.7).
        # When the caller supplied a subdomain we validate + uniqueness-
        # check it; otherwise we derive one from the company name via
        # slug_generator. Validation errors surface as 400/409 per the
        # _subdomain_error_to_http mapping.
        company_name = f"{user.name}'s Company"
        try:
            if user.subdomain is not None:
                resolved_subdomain = validate_subdomain(user.subdomain)
                await ensure_subdomain_unique(resolved_subdomain, db)
            else:
                resolved_subdomain = await slug_generator(company_name, db)
        except SubdomainValidationError as exc:
            raise _subdomain_error_to_http(exc)

        company = {
            "_id": ObjectId(),
            "name": company_name,
            "subdomain": resolved_subdomain,
            "logo_object_key": None,
            "subscription_plan": "basic",
            "active_vehicles_count": 0,
            "billing_history": [],
            "created_at": utcnow()
        }
        await db.companies.insert_one(company)
        company_id = str(company["_id"])
    
    # Create user
    user_doc = {
        "_id": ObjectId(),
        "email": user.email,
        "password_hash": get_password_hash(user.password),
        "name": user.name,
        "phone": user.phone,
        "role": user.role,
        "company_id": company_id,
        "assigned_vehicles": [],
        "created_at": utcnow(),
        "ip_address": request.client.host if request.client else "unknown"
    }
    await db.users.insert_one(user_doc)
    
    # Mint a full-claims JWT (Req 12.1-12.3). ``user_doc`` carries the
    # newly persisted role / company_id; ``_mint_access_token`` looks up
    # the current companies.subdomain so the ``subdomain`` claim always
    # reflects the slug we just resolved.
    token = await _mint_access_token(
        str(user_doc["_id"]),
        user_doc=user_doc,
        company_id=company_id,
        role=user_doc["role"],
    )
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": serialize_doc(user_doc)
    }

@api_router.post("/auth/login")
@limiter.limit("5/minute")
async def login(credentials: UserLogin, request: Request):
    # Support login with email OR username
    login_identifier = credentials.email or credentials.username
    if not login_identifier:
        raise HTTPException(status_code=400, detail="Email or username is required")

    login_identifier = login_identifier.lower().strip()

    # Try to find user by email or username
    user = await db.users.find_one({
        "$or": [
            {"email": login_identifier},
            {"username": login_identifier}
        ]
    })

    # Phase 3 — account lockout. Check BEFORE password verify so a
    # locked account doesn't burn CPU on bcrypt. Returns 423 so the
    # client can render a "try again in X minutes" message.
    if user:
        locked_until = _account_locked_until(user)
        if locked_until:
            seconds_remaining = int((locked_until - utcnow()).total_seconds())
            raise HTTPException(
                status_code=423,
                detail=(
                    f"Too many failed login attempts. Account locked for "
                    f"another {max(1, seconds_remaining // 60)} minute(s)."
                ),
            )

    # Some legacy user docs persist the hash as `hashed_password`
    # rather than `password_hash`, and invite-pending users have
    # neither field set until they accept the invite. Resolve the hash
    # defensively so login returns a clean 401 instead of 500.
    stored_hash = (user or {}).get("password_hash") or (user or {}).get("hashed_password")
    if not user or not stored_hash or not verify_password(credentials.password, stored_hash):
        # Phase 3 — record the failure (if the user exists) so the
        # lockout counter can kick in. Don't reveal whether the
        # account exists: response stays "Invalid credentials".
        if user:
            await _record_failed_login(user)
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Reject soft-deleted (trashed) accounts. The credentials are valid
    # at this point, so the person owns the account — a clear message is
    # safe (no enumeration concern). Owner request 2026-05-28: a user in
    # Trash must not be able to sign in. Restore from Trash to re-enable.
    if user.get("deleted_at"):
        raise HTTPException(
            status_code=403,
            detail="This account has been removed. Contact your administrator to restore access.",
        )

    # Successful auth — reset the failure counter.
    await _clear_failed_logins(user["_id"])

    # Tenant-scoped login enforcement (Req 13.1, 13.2, 13.3, 13.4). When
    # the client passed a ``tenant_subdomain`` (always true on the web
    # client when it is on a tenant host), look up the company attached
    # to the authenticated user and reject with 401 if:
    #   * the submitted slug does not correspond to any company, or
    #   * the user's ``company_id`` does not map to that company.
    # 401 is deliberate — a 403 would leak that the creds are valid but
    # on the wrong tenant, which is useful information to an attacker
    # enumerating which orgs a given email belongs to.
    company_doc: Optional[dict] = None
    user_company_id = user.get("company_id")
    if credentials.tenant_subdomain:
        try:
            submitted_slug = validate_subdomain(credentials.tenant_subdomain)
        except SubdomainValidationError:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        scoped_company = await db.companies.find_one(
            {"subdomain": submitted_slug},
            {"_id": 1, "subdomain": 1, "timezone": 1, "name": 1},
        )
        if not scoped_company:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not user_company_id or str(scoped_company["_id"]) != str(user_company_id):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        company_doc = scoped_company

    # Update last login
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"last_login": utcnow(), "ip_address": request.client.host if request.client else "unknown"}}
    )
    
    # Token expiry based on "remember me" option
    # Always 30-day sessions now — user requested. ``remember_me`` kept
    # in the request schema for backward compat but no longer changes
    # the TTL. Phase 3 revocation list ends a session early on logout.
    expires_delta = timedelta(days=365)
    token = await _mint_access_token(
        str(user["_id"]),
        user_doc=user,
        company_id=user_company_id,
        expires_delta=expires_delta,
    )

    # Get company timezone + subdomain if we did not already fetch the
    # company document during tenant scoping.
    company_timezone = DEFAULT_TIMEZONE
    company_subdomain: Optional[str] = None
    if user_company_id:
        if company_doc is None:
            company_doc = await db.companies.find_one(
                {"_id": ObjectId(user_company_id)},
                {"timezone": 1, "subdomain": 1, "name": 1},
            )
        if company_doc:
            company_timezone = company_doc.get("timezone", DEFAULT_TIMEZONE)
            company_subdomain = company_doc.get("subdomain")

    # Flip the invitation status to 'accepted' the first time a driver
    # successfully signs in after being invited. Idempotent — only
    # writes if the current status is 'invited'.
    try:
        if user.get("invite_status") == "invited":
            await db.users.update_one(
                {"_id": user["_id"], "invite_status": "invited"},
                {"$set": {
                    "invite_status": "accepted",
                    "invite_accepted_at": utcnow(),
                }},
            )
    except Exception:
        pass

    # Add company timezone to user data. Phase 3 — sanitize so
    # password_hash never reaches the wire.
    user_data = sanitize_user_doc(serialize_doc(user))
    user_data["company_timezone"] = company_timezone

    response: dict = {
        "access_token": token,
        "token_type": "bearer",
        "user": user_data,
    }

    # Apex / www login redirect (Req 14.3). When the caller did NOT pass
    # a ``tenant_subdomain`` (i.e. they logged in from the marketing
    # apex, www, or owner host) and the user belongs to a company with
    # a resolvable subdomain, surface a ``redirect_to`` URL so the web
    # client can bounce the session to the branded tenant host. Phase 3:
    # we construct the URL server-side from the tenant slug, then run
    # it through validate_redirect_url so a malformed slug can never
    # cause an off-domain redirect even by accident.
    if not credentials.tenant_subdomain and company_subdomain:
        candidate = f"https://{company_subdomain}.fleetshield365.com/dashboard"
        safe_redirect = validate_redirect_url(candidate)
        if safe_redirect:
            response["redirect_to"] = safe_redirect

    return response


@api_router.post("/auth/refresh")
async def refresh_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    current_user: dict = Depends(get_current_user),
):
    """Issue a fresh access token from a valid (non-expired) one.

    Phase 3 of TODO.md — used by mobile to silently renew before the
    access token would otherwise expire, so drivers mid-inspection
    don't get bounced to the login screen. The old token's jti is
    revoked so a leaked old token cannot continue to be used in
    parallel with the new one.
    """
    try:
        old_payload = jwt.decode(
            credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM]
        )
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = str(current_user["_id"])
    new_token = await _mint_access_token(
        user_id,
        user_doc=current_user,
        company_id=current_user.get("company_id"),
        expires_delta=timedelta(days=365),
    )

    # Revoke the old jti so it cannot be reused alongside the new one.
    old_jti = old_payload.get("jti")
    old_exp = old_payload.get("exp")
    if old_jti and old_exp:
        try:
            expires_at = datetime.utcfromtimestamp(int(old_exp))
        except (ValueError, TypeError, OSError):
            expires_at = utcnow() + timedelta(days=365)
        await db.revoked_tokens.update_one(
            {"_id": old_jti},
            {
                "$setOnInsert": {
                    "_id": old_jti,
                    "user_id": user_id,
                    "expires_at": expires_at,
                    "revoked_at": utcnow(),
                    "reason": "refreshed",
                },
            },
            upsert=True,
        )

    return {"access_token": new_token, "token_type": "bearer"}


@api_router.post("/auth/logout")
async def logout(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    current_user: dict = Depends(get_current_user),
):
    """Revoke the bearer token.

    Phase 3 of TODO.md. The token's ``jti`` is written to the
    ``revoked_tokens`` collection with TTL = the token's ``exp``, so
    the collection self-cleans without manual maintenance. Subsequent
    requests carrying the same jti are rejected with HTTP 401 by
    get_current_user.

    Tokens minted before jti existed (legacy) have no jti to revoke —
    we accept the logout but it is cosmetic for those callers. New
    tokens always carry jti.
    """
    try:
        payload = jwt.decode(
            credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM]
        )
    except Exception:
        # Token is invalid or expired — already effectively logged out.
        return {"status": "logged_out"}

    jti = payload.get("jti")
    exp = payload.get("exp")
    if jti and exp:
        try:
            expires_at = datetime.utcfromtimestamp(int(exp))
        except (ValueError, TypeError, OSError):
            expires_at = utcnow() + timedelta(days=365)
        await db.revoked_tokens.update_one(
            {"_id": jti},
            {
                "$setOnInsert": {
                    "_id": jti,
                    "user_id": payload.get("sub"),
                    "expires_at": expires_at,
                    "revoked_at": utcnow(),
                },
            },
            upsert=True,
        )

    return {"status": "logged_out"}

@api_router.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    # Get company info if user has a company
    company = None
    company_id = current_user.get("company_id")
    if company_id:
        # Fetch company and dynamic vehicle count in parallel
        company_doc, vehicle_count = await asyncio.gather(
            db.companies.find_one({"_id": ObjectId(company_id)}),
            db.vehicles.count_documents({"company_id": company_id})
        )
        if company_doc:
            company = serialize_doc(company_doc)
            # Override with dynamic vehicle count
            company["vehicle_count"] = vehicle_count
            company["active_vehicles_count"] = vehicle_count
            # Task 5.4: expose presigned logo URL alongside logo_object_key
            # (Requirements 21.12, 21.13).
            company["logo_url"] = _presign_if_key(
                "logos", company.get("logo_object_key")
            )
    
    return {
        "user": sanitize_user_doc(serialize_doc(current_user)),
        "company": company
    }

# ============== Password Reset ==============

class ForgotPasswordRequest(BaseModel):
    email: str
    origin_url: str = DEFAULT_ORIGIN_URL

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

@api_router.post("/auth/forgot-password")
@limiter.limit("3/minute")
async def forgot_password(request: Request, payload: ForgotPasswordRequest):
    """Send password reset email.

    Parameter ordering matters: slowapi looks for a parameter named
    ``request`` that is a ``starlette.requests.Request`` to derive the
    rate-limit key. Using the same name for a Pydantic body model
    causes slowapi to raise inside the wrapper and the endpoint 500s
    before any code runs (which surfaces to the browser as a CORS
    error because FastAPI's exception handler short-circuits the
    CORS middleware). So we keep ``request`` for the HTTP request
    and rename the body to ``payload``.
    """
    # Case-insensitive email lookup. Exclude trashed accounts — a user
    # in Trash must not receive any mail (owner request 2026-05-28) and
    # can't log in anyway.
    email_lower = payload.email.lower()
    user = await db.users.find_one({"email": email_lower, "deleted_at": None})

    # Always return success to prevent email enumeration
    if not user:
        return {"message": "If an account exists with this email, you will receive a password reset link."}

    # Generate reset token
    import secrets
    reset_token = secrets.token_urlsafe(32)
    expires_at = utcnow() + timedelta(hours=1)

    # Store reset token
    await db.password_resets.update_one(
        {"user_id": str(user["_id"])},
        {"$set": {
            "user_id": str(user["_id"]),
            "token": reset_token,
            "expires_at": expires_at,
            "created_at": utcnow()
        }},
        upsert=True
    )

    # Send reset email
    # Validate the submitted origin against the CORS allow list / tenant
    # subdomain regex to prevent an open-redirect via the password-reset
    # email (Requirement 6.3). On mismatch we silently fall back to
    # DEFAULT_ORIGIN_URL — this is a trust boundary, not user-facing input
    # validation, so surfacing a 4xx would only help an attacker probe.
    submitted_origin = payload.origin_url
    if _is_allowed_origin(submitted_origin):
        validated_origin = submitted_origin.strip().rstrip('/')
    else:
        logger.warning(
            "forgot_password: rejecting disallowed origin_url=%r; "
            "falling back to DEFAULT_ORIGIN_URL",
            submitted_origin,
        )
        validated_origin = DEFAULT_ORIGIN_URL.rstrip('/')

    reset_url = f"{validated_origin}/reset-password?token={reset_token}"
    # Phase 3 — _safe_html escapes the user-controlled name so a
    # crafted display name can't inject HTML / script tags into the
    # reset email body (XSS-via-email defence).
    body = (
        f"<p>Hi {_safe_html(user.get('name', 'there'))},</p>"
        f"<p>We received a request to reset your FleetShield365 password. "
        f"Click the button below to set a new password.</p>"
        f"<p style='color:#94a3b8; font-size:13px;'>This link expires in 1 hour. "
        f"If you didn't request a reset, you can safely ignore this email.</p>"
    )
    html_content = _email_template_branded(
        heading="Reset your password",
        body_html=body,
        button_label="Reset Password",
        button_url=reset_url,
    )

    try:
        await send_system_email(
            payload.email,
            "[FleetShield365] Reset your password",
            html_content,
        )
    except Exception as e:
        # Mail send failures are not user-facing — we already saved the
        # reset token. Log and return the same enumeration-safe 200 so
        # an attacker can't distinguish "mailbox unreachable" from
        # "user does not exist".
        logger.error(f"forgot_password: mail send failed for {payload.email}: {e}")

    return {"message": "If an account exists with this email, you will receive a password reset link."}


def _mask_email(email: str) -> str:
    """Return a UI-safe email hint like ``r***@gmail.com`` for the
    forgot-by-username response. We never leak the full address back to
    a caller who only supplied a username."""
    if not email or "@" not in email:
        return "your email"
    local, _, domain = email.partition("@")
    if len(local) <= 1:
        return f"{local}***@{domain}"
    return f"{local[0]}{'*' * max(2, len(local) - 1)}@{domain}"


class ForgotByUsernameRequest(BaseModel):
    username: str
    origin_url: str = DEFAULT_ORIGIN_URL


@api_router.post("/auth/forgot-by-username")
@limiter.limit("3/minute")
async def forgot_password_by_username(request: Request, payload: ForgotByUsernameRequest):
    """Mobile-friendly forgot-password: look up account by username first,
    then fall back to the email-based reset flow when an email is on file.

    The mobile app collects username first because drivers don't always
    remember the email their admin used. We return one of three signals
    so the client can guide the user:

      * ``status="email_sent"`` — username matched and an email is on file
      * ``status="no_email"``   — username matched but no email; ask admin
      * ``status="not_found"``  — username didn't match any account

    The user-not-found case still returns HTTP 200 to stay consistent
    with the email-based endpoint (no enumeration), but with a distinct
    status code so the UI can show the right next step.
    """
    username = (payload.username or "").strip().lower()
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    user = await db.users.find_one({"username": username})
    if not user:
        return {"status": "not_found", "message": "No account found with that username."}

    email = (user.get("email") or "").strip()
    if not email:
        return {
            "status": "no_email",
            "message": "This account doesn't have an email on file. Please ask your admin to reset your PIN.",
        }

    # Reuse the email-based flow: mint a reset token + send the branded
    # reset email. We don't call the forgot_password endpoint directly
    # so the rate limit on that path doesn't double-charge this caller.
    import secrets
    reset_token = secrets.token_urlsafe(32)
    expires_at = utcnow() + timedelta(hours=1)
    await db.password_resets.update_one(
        {"user_id": str(user["_id"])},
        {"$set": {
            "user_id": str(user["_id"]),
            "token": reset_token,
            "expires_at": expires_at,
            "created_at": utcnow(),
        }},
        upsert=True,
    )

    submitted_origin = payload.origin_url
    if _is_allowed_origin(submitted_origin):
        validated_origin = submitted_origin.strip().rstrip("/")
    else:
        validated_origin = DEFAULT_ORIGIN_URL.rstrip("/")
    reset_url = f"{validated_origin}/reset-password?token={reset_token}"

    body = (
        f"<p>Hi {_safe_html(user.get('name', 'there'))},</p>"
        f"<p>We received a request to reset the FleetShield365 PIN for "
        f"<strong>{_safe_html(username)}</strong>. Click the button below to set a new credential.</p>"
        f"<p style='color:#94a3b8; font-size:13px;'>This link expires in 1 hour. "
        f"If you didn't request a reset, you can safely ignore this email.</p>"
    )
    html_content = _email_template_branded(
        heading="Reset your PIN",
        body_html=body,
        button_label="Reset PIN",
        button_url=reset_url,
    )

    # Mask the email when echoing back so we don't leak the full address
    # to whoever has just the username.
    masked = _mask_email(email)
    try:
        await send_system_email(
            email,
            "[FleetShield365] Reset your PIN",
            html_content,
        )
    except Exception as e:
        logger.error(f"forgot_by_username: mail send failed for {email}: {e}")

    return {"status": "email_sent", "email_hint": masked,
            "message": f"A reset link was sent to {masked}."}


# 2026-05-19 — mobile PIN-reset flow.
#
# The forgot-by-username flow above emails a long token + opens a web
# page. That's fine for admin password resets, but drivers using the
# mobile app should never have to leave the app. These two endpoints
# implement a self-contained OTP flow:
#
#   1) POST /auth/request-pin-reset {username}
#      → emails a 6-digit OTP to the user's email on file
#   2) POST /auth/reset-pin {username, otp, new_pin}
#      → validates OTP, bcrypt-hashes the new 4-digit PIN, updates
#        password_hash, clears the OTP. Driver returns to the login
#        screen and signs in with the new PIN.
#
# OTP TTL: 15 minutes. Max 5 wrong-OTP attempts per code (then the
# driver must request a fresh code). Username lookup is enumeration-
# safe (same {not_found, no_email, otp_sent} discriminated status
# shape as forgot-by-username).


class RequestPinResetRequest(BaseModel):
    username: str


@api_router.post("/auth/request-pin-reset")
@limiter.limit("3/minute")
async def request_pin_reset(request: Request, payload: RequestPinResetRequest):
    """Send a 6-digit OTP to a driver's email for in-app PIN reset."""
    username = (payload.username or "").strip().lower()
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")

    user = await db.users.find_one({"username": username})
    if not user:
        return {"status": "not_found",
                "message": "No account found with that username."}

    email = (user.get("email") or "").strip()
    if not email:
        return {
            "status": "no_email",
            "message": "This account doesn't have an email on file. Please ask your admin to reset your PIN.",
        }

    # Generate 6-digit numeric OTP. secrets.randbelow gives us uniform
    # distribution across [0, 1_000_000) which is what we want — 6
    # digits, no leading-zero stripping (we format-string-pad).
    import secrets as _secrets
    otp = f"{_secrets.randbelow(1_000_000):06d}"
    expires_at = utcnow() + timedelta(minutes=15)

    # password_resets is keyed by user_id; we reuse it so a fresh OTP
    # always replaces the previous one (per Phase 3 of the existing
    # forgot-password flow).
    #
    # The collection has a unique index on `token` (the web flow's reset
    # link). PIN reset doesn't use a token — only an OTP. We explicitly
    # $unset token so the sparse-unique index skips this doc instead of
    # treating it as token=null (which would collide with every other
    # PIN-reset doc, see 2026-05-20 incident).
    await db.password_resets.update_one(
        {"user_id": str(user["_id"])},
        {
            "$set": {
                "user_id": str(user["_id"]),
                "otp": otp,
                "otp_attempts": 0,
                "expires_at": expires_at,
                "created_at": utcnow(),
            },
            "$unset": {"token": ""},
        },
        upsert=True,
    )

    body = (
        f"<p>Hi {_safe_html(user.get('name', 'there'))},</p>"
        f"<p>Your FleetShield365 PIN-reset code is:</p>"
        f"<div style='font-size:32px; font-weight:700; letter-spacing:10px; "
        f"color:#0d9488; padding:18px 28px; background:#f0fdfa; "
        f"border-radius:10px; display:inline-block; font-family:monospace;'>"
        f"{otp}"
        f"</div>"
        f"<p style='color:#94a3b8; font-size:13px; margin-top:24px;'>"
        f"Open the FleetShield365 app and type this code on the "
        f"<em>Forgot PIN</em> screen. This code expires in 15 minutes. "
        f"If you didn't request a reset, you can safely ignore this email."
        f"</p>"
    )
    html_content = _email_template_branded(
        heading="Your PIN reset code",
        body_html=body,
        button_label=None,
        button_url=None,
    )

    masked = _mask_email(email)
    try:
        await send_system_email(
            email,
            "[FleetShield365] Your PIN reset code",
            html_content,
        )
    except Exception as e:
        logger.error(f"request_pin_reset: mail send failed for {email}: {e}")

    return {
        "status": "otp_sent",
        "email_hint": masked,
        "message": f"A 6-digit code was sent to {masked}.",
    }


class ResetPinRequest(BaseModel):
    username: str
    otp: str
    new_pin: str


@api_router.post("/auth/reset-pin")
@limiter.limit("5/minute")
async def reset_pin(request: Request, payload: ResetPinRequest):
    """Verify the OTP + set a new 4-digit PIN."""
    username = (payload.username or "").strip().lower()
    otp = (payload.otp or "").strip()
    new_pin = (payload.new_pin or "").strip()

    if not username or not otp:
        raise HTTPException(status_code=400, detail="Username and code are required")

    # Enforce 4-digit PIN policy (matches create_driver + update_driver).
    validate_driver_pin(new_pin)

    user = await db.users.find_one({"username": username})
    # Don't leak which step failed — both unknown-user and missing-OTP
    # collapse to a generic "Invalid code". Brute-force protection is
    # the OTP itself (1-in-1M) + the per-record attempt cap below.
    if not user:
        raise HTTPException(status_code=400, detail="Invalid code")

    record = await db.password_resets.find_one({"user_id": str(user["_id"])})
    if not record or not record.get("otp"):
        raise HTTPException(
            status_code=400,
            detail="No reset code on file. Request a new one.",
        )

    # Expiry check.
    exp = record.get("expires_at")
    if not exp or utcnow() > exp:
        raise HTTPException(
            status_code=400,
            detail="Code has expired. Request a new one.",
        )

    # Attempt cap.
    attempts = record.get("otp_attempts", 0)
    if attempts >= 5:
        raise HTTPException(
            status_code=429,
            detail="Too many incorrect attempts. Request a new code.",
        )

    if record["otp"] != otp:
        await db.password_resets.update_one(
            {"_id": record["_id"]},
            {"$inc": {"otp_attempts": 1}},
        )
        raise HTTPException(status_code=400, detail="Invalid code")

    # OK — update password_hash to the new PIN. Clear lockout state so
    # the user can sign in immediately. Drop the OTP record so it can't
    # be reused.
    await db.users.update_one(
        {"_id": user["_id"]},
        {
            "$set": {
                "password_hash": get_password_hash(new_pin),
                "auth_mode": "pin",
                "pin_reset_at": utcnow(),
                "failed_login_attempts": 0,
            },
            "$unset": {"locked_until": ""},
        },
    )
    await db.password_resets.delete_one({"_id": record["_id"]})

    return {
        "status": "ok",
        "message": "PIN updated. You can now sign in with your new PIN.",
    }


@api_router.post("/auth/reset-password")
async def reset_password(request: ResetPasswordRequest):
    """Reset password using token"""
    reset_record = await db.password_resets.find_one({"token": request.token})

    if not reset_record:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    # Check expiration
    if utcnow() > reset_record["expires_at"]:
        await db.password_resets.delete_one({"token": request.token})
        raise HTTPException(status_code=400, detail="Reset token has expired")

    # Phase 3 — uniform password policy (was 6-char min — too weak).
    validate_password_policy(request.new_password)

    # Block reusing the current password — forces a meaningful reset.
    target_user = await db.users.find_one(
        {"_id": ObjectId(reset_record["user_id"])},
        {"password_hash": 1},
    )
    reject_same_password(request.new_password, (target_user or {}).get("password_hash"))

    # Update password + clear any lingering lockout (a successful reset
    # implies the legitimate user is back in control).
    await db.users.update_one(
        {"_id": ObjectId(reset_record["user_id"])},
        {
            "$set": {"password_hash": get_password_hash(request.new_password)},
            "$unset": {"failed_login_attempts": "", "locked_until": ""},
        }
    )

    # Delete used token
    await db.password_resets.delete_one({"token": request.token})

    return {"message": "Password reset successfully. You can now log in with your new password."}


# ============== Email Verification + Invite Flow ==============

class VerifyEmailRequest(BaseModel):
    token: str

class ResendVerificationRequest(BaseModel):
    email: str
    origin_url: str = DEFAULT_ORIGIN_URL

class InviteUserRequest(BaseModel):
    email: str
    name: str
    role: str  # "admin" or "driver" (super_admin can also invite admin; admin can invite driver)
    origin_url: str = DEFAULT_ORIGIN_URL

class AcceptInviteRequest(BaseModel):
    token: str
    new_password: str


async def _issue_email_token(user_id: str, token_type: str, ttl_hours: int = 24) -> str:
    """Create a fresh single-use token of `token_type` ("verify" or "invite") for `user_id`."""
    import secrets
    token = secrets.token_urlsafe(32)
    await db.email_tokens.insert_one({
        "token": token,
        "user_id": user_id,
        "type": token_type,
        "expires_at": utcnow() + timedelta(hours=ttl_hours),
        "created_at": utcnow(),
    })
    return token


async def send_verification_email(user_email: str, user_name: str, token: str, origin_url: str) -> bool:
    """Send a branded email-verification email via the noreply mailbox."""
    if not _is_allowed_origin(origin_url):
        origin_url = DEFAULT_ORIGIN_URL
    verify_url = f"{origin_url.rstrip('/')}/verify-email?token={token}"
    # Phase 3 — escape user-controlled display name (XSS-via-email defence).
    body = (
        f"<p>Hi {_safe_html(user_name) or 'there'},</p>"
        f"<p>Welcome to <strong>FleetShield365</strong>. Please confirm this email "
        f"address so we know it's really you. Click the button below to verify "
        f"your account.</p>"
        f"<p style='color:#94a3b8; font-size:13px;'>This link expires in 24 hours.</p>"
    )
    html = _email_template_branded(
        heading="Verify your email",
        body_html=body,
        button_label="Verify Email",
        button_url=verify_url,
    )
    return await send_system_email(user_email, "[FleetShield365] Verify your email address", html)


async def send_invite_email(user_email: str, user_name: str, inviter_name: str,
                            company_name: str, role: str, token: str, origin_url: str) -> bool:
    """Send a branded invite email with a set-password link via the noreply mailbox."""
    if not _is_allowed_origin(origin_url):
        origin_url = DEFAULT_ORIGIN_URL
    setup_url = f"{origin_url.rstrip('/')}/set-password?token={token}"
    role_label = {"super_admin": "Company Owner", "admin": "Admin", "driver": "Operator"}.get(role, role)
    # Phase 3 — escape user-controlled names (inviter / invitee / company).
    body = (
        f"<p>Hi {_safe_html(user_name) or 'there'},</p>"
        f"<p><strong>{_safe_html(inviter_name)}</strong> has invited you to join "
        f"<strong>{_safe_html(company_name)}</strong> on FleetShield365 as a <strong>{_safe_html(role_label)}</strong>.</p>"
        f"<p>Click the button below to set your password and activate your account.</p>"
        f"<p style='color:#94a3b8; font-size:13px;'>This invite link expires in 7 days.</p>"
    )
    html = _email_template_branded(
        heading="You've been invited to FleetShield365",
        body_html=body,
        button_label="Set Password & Sign In",
        button_url=setup_url,
    )
    return await send_system_email(user_email, f"[FleetShield365] You've been invited to {company_name}", html)


@api_router.post("/auth/verify-email")
async def verify_email(request: VerifyEmailRequest):
    """Mark the user's email as verified using a token from the verification email."""
    token_record = await db.email_tokens.find_one({"token": request.token, "type": "verify"})
    if not token_record:
        raise HTTPException(status_code=400, detail="Invalid or already-used verification link")
    if utcnow() > token_record["expires_at"]:
        await db.email_tokens.delete_one({"_id": token_record["_id"]})
        raise HTTPException(status_code=400, detail="Verification link has expired. Please request a new one.")

    await db.users.update_one(
        {"_id": ObjectId(token_record["user_id"])},
        {"$set": {"email_verified": True, "email_verified_at": utcnow()}}
    )
    await db.email_tokens.delete_one({"_id": token_record["_id"]})
    return {"message": "Email verified successfully. You can now use all features of your account."}


@api_router.post("/auth/resend-verification")
@limiter.limit("2/minute")
async def resend_verification(request: Request, payload: ResendVerificationRequest):
    """Re-send the verification email. Always returns the same response to avoid email enumeration.

    Parameter order is intentional: slowapi requires ``request`` to be
    a starlette.Request to derive the rate-limit key. See forgot_password
    for the full explanation.
    """
    user = await db.users.find_one({"email": payload.email.lower()})
    if not user:
        return {"message": "If an account with this email exists, a verification email has been sent."}
    if user.get("email_verified"):
        return {"message": "This email is already verified. You can sign in."}

    # Invalidate any prior verify tokens for this user.
    await db.email_tokens.delete_many({"user_id": str(user["_id"]), "type": "verify"})
    token = await _issue_email_token(str(user["_id"]), "verify", ttl_hours=24)
    try:
        await send_verification_email(user["email"], user.get("name", ""), token, payload.origin_url)
    except Exception as e:
        logger.error(f"resend_verification: mail send failed for {payload.email}: {e}")
    return {"message": "If an account with this email exists, a verification email has been sent."}


@api_router.post("/users/invite")
async def invite_user(request: InviteUserRequest, current_user: dict = Depends(get_current_user)):
    """Invite an admin or driver to the current user's company.

    - super_admin can invite admin OR driver.
    - admin can invite driver only (not another admin).
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized to invite users")

    target_role = request.role.strip().lower()
    if target_role not in {"admin", "driver"}:
        raise HTTPException(status_code=400, detail="Role must be 'admin' or 'driver'")
    if target_role == "admin" and current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only the Company Owner can invite other admins")

    email_lower = request.email.strip().lower()
    if not email_lower or "@" not in email_lower:
        raise HTTPException(status_code=400, detail="Valid email is required")

    # Reject if a user with this email already exists (any company)
    existing = await db.users.find_one({"email": email_lower})
    if existing:
        raise HTTPException(status_code=409, detail="A user with this email already exists")

    # Look up company for the email body
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    company_name = company.get("name", "your team") if company else "your team"

    # Create the invited user in a pending state (no usable password yet).
    user_doc = {
        "email": email_lower,
        "password_hash": "",  # set when invite accepted
        "name": request.name.strip() or email_lower.split("@")[0],
        "role": target_role,
        "company_id": current_user["company_id"],
        "email_verified": False,
        "invite_pending": True,
        "invited_by": current_user["id"],
        "created_at": utcnow().isoformat(),
    }
    user_result = await db.users.insert_one(user_doc)
    user_id = str(user_result.inserted_id)

    token = await _issue_email_token(user_id, "invite", ttl_hours=24 * 7)
    sent = await send_invite_email(
        email_lower,
        user_doc["name"],
        current_user.get("name", "Your admin"),
        company_name,
        target_role,
        token,
        request.origin_url,
    )

    return {
        "message": f"Invite sent to {email_lower}",
        "user_id": user_id,
        "email_sent": sent,
    }


@api_router.post("/auth/accept-invite")
async def accept_invite(request: AcceptInviteRequest):
    """Set the password for an invited user using the invite token. Marks email_verified=True."""
    token_record = await db.email_tokens.find_one({"token": request.token, "type": "invite"})
    if not token_record:
        raise HTTPException(status_code=400, detail="Invalid or already-used invite link")
    if utcnow() > token_record["expires_at"]:
        await db.email_tokens.delete_one({"_id": token_record["_id"]})
        raise HTTPException(status_code=400, detail="Invite link has expired. Please ask your admin to re-invite you.")
    # Phase 3 — uniform password policy.
    validate_password_policy(request.new_password)

    user_id = token_record["user_id"]
    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {
            "password_hash": get_password_hash(request.new_password),
            "email_verified": True,
            "email_verified_at": utcnow(),
            "invite_pending": False,
            "invite_accepted_at": utcnow(),
        }}
    )
    await db.email_tokens.delete_one({"_id": token_record["_id"]})

    user = await db.users.find_one({"_id": ObjectId(user_id)})
    return {
        "message": "Account activated. You can now sign in.",
        "email": user.get("email"),
    }

# ============== Email Test Route ==============

class TestEmailRequest(BaseModel):
    to_email: str

@api_router.post("/test-email")
async def test_email(request: TestEmailRequest, current_user: dict = Depends(get_current_user)):
    """Send a test email to verify SMTP integration"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    html_content = """
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #0D9488;">FleetShield365 Email Test</h2>
        <p>This is a test email from FleetShield365.</p>
        <p>If you're receiving this, email notifications are working correctly!</p>
        <hr style="border: none; border-top: 1px solid #E5E7EB; margin: 20px 0;">
        <p style="color: #6B7280; font-size: 12px;">
            FleetShield365 - Vehicle Inspection Management
        </p>
    </body>
    </html>
    """
    
    success = await send_email_notification(
        request.to_email,
        "[FleetShield365] Test Email - Notifications Working!",
        html_content
    )
    
    if success:
        return {"status": "success", "message": f"Test email sent to {request.to_email}"}
    else:
        raise HTTPException(status_code=500, detail="Failed to send email. Check SMTP configuration and sender mailbox.")

@api_router.post("/trigger-weekly-summary")
async def trigger_weekly_summary(current_user: dict = Depends(get_current_user)):
    """Manually trigger weekly summary email (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await generate_weekly_summary()
    return {"status": "success", "message": "Weekly summary emails sent to all company admins"}

# ============== Contact Form (Public) ==============

class ContactFormRequest(BaseModel):
    name: str
    email: str
    company: Optional[str] = ""
    phone: Optional[str] = ""
    message: str

CONTACT_RECIPIENT = os.environ.get('CONTACT_RECIPIENT_EMAIL', 'contact@fleetshield365.com')

@api_router.post("/contact")
@limiter.limit("3/minute")
async def submit_contact_form(request: Request, payload: ContactFormRequest):
    """Public endpoint: submits a contact request from the website landing page.

    Parameter ordering: slowapi requires ``request`` to be a
    starlette.Request — see forgot_password for the full explanation.
    """
    # Basic validation
    name = (payload.name or "").strip()
    email = (payload.email or "").strip()
    message = (payload.message or "").strip()
    company = (payload.company or "").strip()
    phone = (payload.phone or "").strip()

    if not name or not email or not message:
        raise HTTPException(status_code=400, detail="Name, email and message are required.")
    if "@" not in email or len(email) > 200:
        raise HTTPException(status_code=400, detail="Please provide a valid email address.")
    if len(message) > 5000:
        raise HTTPException(status_code=400, detail="Message is too long (max 5000 characters).")

    submitted_at = datetime.now(SYDNEY_TZ).strftime("%d/%m/%Y %I:%M %p AEST")

    # Phase 3 — every user-supplied value runs through _safe_html before
    # being embedded. The contact form is a public unauthenticated
    # endpoint so it's the highest-risk XSS-via-email surface.
    name_safe = _safe_html(name)
    email_safe = _safe_html(email)
    company_safe = _safe_html(company or '-')
    phone_safe = _safe_html(phone or '-')
    message_safe = _safe_html(message)

    # Email to admin
    admin_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; color:#0f172a;">
        <h2 style="color:#0891b2; margin-bottom:8px;">New Contact Form Submission</h2>
        <p style="color:#64748b; margin-top:0;">Received on {submitted_at}</p>
        <table style="border-collapse:collapse; width:100%; max-width:600px; margin-top:16px;">
            <tr><td style="padding:8px; border-bottom:1px solid #e2e8f0;"><b>Name</b></td><td style="padding:8px; border-bottom:1px solid #e2e8f0;">{name_safe}</td></tr>
            <tr><td style="padding:8px; border-bottom:1px solid #e2e8f0;"><b>Email</b></td><td style="padding:8px; border-bottom:1px solid #e2e8f0;"><a href="mailto:{email_safe}">{email_safe}</a></td></tr>
            <tr><td style="padding:8px; border-bottom:1px solid #e2e8f0;"><b>Company</b></td><td style="padding:8px; border-bottom:1px solid #e2e8f0;">{company_safe}</td></tr>
            <tr><td style="padding:8px; border-bottom:1px solid #e2e8f0;"><b>Phone</b></td><td style="padding:8px; border-bottom:1px solid #e2e8f0;">{phone_safe}</td></tr>
        </table>
        <h3 style="margin-top:24px; color:#0f172a;">Message</h3>
        <div style="background:#f1f5f9; padding:16px; border-radius:8px; white-space:pre-wrap; line-height:1.5;">{message_safe}</div>
        <p style="color:#64748b; font-size:12px; margin-top:24px;">Reply directly to this lead at <a href="mailto:{email_safe}">{email_safe}</a>.</p>
    </body>
    </html>
    """

    admin_sent = await send_email_notification(
        CONTACT_RECIPIENT,
        f"[FleetShield365] New Contact: {name_safe}",
        admin_html
    )

    # Auto-reply confirmation to the submitter
    confirm_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; color:#0f172a;">
        <h2 style="color:#0891b2;">Thanks for reaching out, {name_safe}!</h2>
        <p>We've received your message and a member of the FleetShield365 team will get back to you within 24 hours.</p>
        <p style="margin-top:16px;"><b>Your message:</b></p>
        <div style="background:#f1f5f9; padding:16px; border-radius:8px; white-space:pre-wrap; line-height:1.5;">{message_safe}</div>
        <p style="margin-top:24px;">Need urgent help? Email us directly at <a href="mailto:{CONTACT_RECIPIENT}">{CONTACT_RECIPIENT}</a>.</p>
        <hr style="border:none; border-top:1px solid #e2e8f0; margin:24px 0;">
        <p style="color:#64748b; font-size:12px;">FleetShield365 — A product of Prime Mover Rentals Pty Ltd<br>This is an automated confirmation. Please do not reply directly.</p>
    </body>
    </html>
    """

    # Auto-reply to the submitter is a transactional "do not reply" confirmation,
    # so it goes out from the noreply@ mailbox (not the alerts@ mailbox).
    try:
        await send_system_email(
            email,
            "[FleetShield365] We've received your message",
            confirm_html
        )
    except Exception as e:
        logger.error(f"[CONTACT] Failed to send confirmation to {email}: {e}")

    # Persist for record-keeping
    try:
        await db.contact_submissions.insert_one({
            "id": str(uuid.uuid4()),
            "name": name,
            "email": email,
            "company": company,
            "phone": phone,
            "message": message,
            "admin_email_sent": bool(admin_sent),
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        logger.error(f"[CONTACT] Failed to persist contact submission: {e}")

    if not admin_sent:
        # Email send failed (SMTP not configured or transport error).
        # The contact submission is persisted above; we surface a 202
        # Accepted so the website UI can still acknowledge receipt
        # instead of showing a hard 5xx.
        logger.warning(
            "/api/contact: email not delivered (SMTP not configured or send failed); "
            "submission persisted for manual follow-up."
        )
        return {
            "status": "pending",
            "message": "Message received. We'll be in touch soon.",
        }

    return {"status": "success", "message": "Message received. We'll be in touch soon."}

# ============== Company Routes ==============

@api_router.get("/company")
async def get_company(current_user: dict = Depends(get_current_user)):
    if not current_user.get("company_id"):
        raise HTTPException(status_code=404, detail="No company associated")
    company_id = current_user["company_id"]
    
    # Fetch company and dynamically calculate vehicle count
    company, vehicle_count = await asyncio.gather(
        db.companies.find_one({"_id": ObjectId(company_id)}),
        db.vehicles.count_documents({"company_id": company_id})
    )
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Serialize and override vehicle_count with actual count
    result = serialize_doc(company)
    result["vehicle_count"] = vehicle_count
    result["active_vehicles_count"] = vehicle_count
    # Ensure timezone has a default value
    result["timezone"] = result.get("timezone", DEFAULT_TIMEZONE)
    # Task 5.4: expose a presigned GET URL alongside the logo_object_key so
    # the frontend renders the asset directly through Nginx_Proxy without
    # pulling bytes through the API (Requirements 21.12, 21.13).
    result["logo_url"] = _presign_if_key("logos", result.get("logo_object_key"))
    return result

@api_router.get("/timezones")
async def get_timezones():
    """Get list of supported timezones for company settings"""
    return {
        "timezones": SUPPORTED_TIMEZONES,
        "default": DEFAULT_TIMEZONE
    }

@api_router.put("/company")
async def update_company(update: CompanyUpdate, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Task 5.3: strip logo_base64 off the payload and route it through MinIO.
    # The stored document must not contain the raw base64 (Req 21.11); we
    # upload under logos/<company_id>/logo.png and persist logo_object_key.
    company_id = current_user["company_id"]
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    logo_b64 = update_data.pop("logo_base64", None)
    if logo_b64:
        logo_key = f"{company_id}/logo.png"
        _upload_base64_or_400(
            "logos", logo_key, logo_b64, "png", "logo_base64",
            expected_company_id=company_id,
            type_key="logo",
        )
        update_data["logo_object_key"] = logo_key

    if update_data:
        await db.companies.update_one(
            {"_id": ObjectId(company_id)},
            {"$set": update_data}
        )
    
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    result = serialize_doc(company)
    # Task 5.4: surface logo_url alongside logo_object_key on the updated
    # response so the frontend can refresh its preview immediately after a
    # logo change (Requirements 21.12, 21.13).
    if isinstance(result, dict):
        result["logo_url"] = _presign_if_key(
            "logos", result.get("logo_object_key")
        )
    return result

@api_router.post("/company/logo")
async def upload_company_logo(logo: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    """Upload company logo for branding on PDF reports"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    # Read file (capped by Content-Length validation below). The client-
    # supplied content_type is ignored — only magic bytes decide format.
    contents = await logo.read()

    # Per-type size + magic-byte validation (Phase 1 of STORAGE-PLAN.txt).
    # Stamps the right Content-Type on the MinIO object based on detected
    # format and also generates a thumbnail as a side effect.
    detected_format = _validate_upload_or_400(contents, "logo", "logo")
    content_type = _FORMAT_TO_CONTENT_TYPE[detected_format]

    # Task 5.3: store bytes in MinIO under the tenant-scoped logos bucket and
    # persist only logo_object_key on the company document (Req 21.10, 21.11).
    company_id = current_user["company_id"]
    logo_key = f"{company_id}/logo.png"
    try:
        _upload_with_thumbnail(
            "logos", logo_key, contents, content_type,
            expected_company_id=company_id,
        )
    except object_store.TenantPrefixViolation as exc:
        # Task 5.5 / Req 21.14: the computed key does not begin with the
        # caller's company_id. This is defensive — the key is derived
        # from current_user above so this path should be unreachable
        # under normal auth, but we fail closed with 403 rather than
        # 500 if the invariant is ever violated.
        logger.error(
            f"Logo upload blocked by tenant prefix check for "
            f"company {company_id}: {exc}"
        )
        raise HTTPException(status_code=403, detail="Forbidden Object_Key")
    except Exception as exc:
        logger.error(f"Logo upload failed for company {company_id}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to upload logo")
    
    # Update company with new object key; remove the legacy base64 field so
    # the stored document never retains the bytes.
    await db.companies.update_one(
        {"_id": ObjectId(company_id)},
        {
            "$set": {"logo_object_key": logo_key},
            "$unset": {"logo_base64": "", "logo_url": ""},
        },
    )
    
    return {
        "message": "Logo uploaded successfully",
        "logo_object_key": logo_key,
        # Task 5.4: include the presigned URL so the client can refresh
        # its logo preview immediately without another round-trip to
        # /api/company (Requirements 21.12, 21.13).
        "logo_url": _presign_if_key("logos", logo_key),
    }

# ============== User Management Routes ==============

class UserCreate(BaseModel):
    email: EmailStr
    full_name: str
    password: str
    role: str = "admin"

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None
    # Optional password reset from the Users panel — owner request
    # 2026-05-29 so a super_admin can set a new password for an admin
    # without the admin having to run the forgot-password flow.
    password: Optional[str] = None

@api_router.get("/users")
async def get_users(current_user: dict = Depends(get_current_user)):
    """Get all users in the company (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    # Phase 4 — exclude soft-deleted users.
    users = await db.users.find({
        **_soft_delete_filter(),
        "company_id": current_user["company_id"],
    }).to_list(100)
    # Phase 3 — sanitize ALL secret fields, not just hashed_password.
    return sanitize_user_doc(serialize_doc(users))

@api_router.post("/users")
async def create_user(user_data: UserCreate, current_user: dict = Depends(get_current_user)):
    """Create a new admin/driver user for the company.

    Role gating (2026-05-27 — owner reported the Add User panel was
    surfacing a generic error when the email was already in use):
      - super_admin can create admin OR driver
      - admin can create driver only
      - Nobody can create another super_admin or platform_owner through
        this endpoint (those roles have their own out-of-band setup).
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    allowed_roles = {UserRole.ADMIN, UserRole.DRIVER}
    if user_data.role not in allowed_roles:
        raise HTTPException(
            status_code=403,
            detail="Can only create admin or driver users from this panel",
        )
    if user_data.role == UserRole.ADMIN and current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(
            status_code=403,
            detail="Only the Company Owner can invite admins",
        )

    # Friendly duplicate-email message — owner reported it was hidden
    # behind a generic 'Could not save user' toast.
    existing = await db.users.find_one({"email": user_data.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already in use")

    validate_password_policy(user_data.password)

    new_user = {
        "email": user_data.email,
        "full_name": user_data.full_name,
        # Field name MUST match what the login lookup reads (password_hash).
        # An older revision wrote 'hashed_password' here, so users created
        # via this endpoint silently failed login.
        "password_hash": get_password_hash(user_data.password),
        "role": user_data.role,
        "company_id": current_user["company_id"],
        "created_at": datetime.now(timezone.utc),
    }

    result = await db.users.insert_one(new_user)
    new_user["id"] = str(result.inserted_id)
    new_user.pop("_id", None)
    return sanitize_user_doc(new_user)

@api_router.put("/users/{user_id}")
async def update_user(user_id: str, user_data: UserUpdate, current_user: dict = Depends(get_current_user)):
    """Update a user (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Verify user belongs to same company
    user = await db.users.find_one({
        "_id": ObjectId(user_id),
        "company_id": current_user["company_id"]
    })
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    update_data = {k: v for k, v in user_data.dict().items() if v is not None}

    # Password reset path — only the Company Owner (super_admin) may set
    # another user's password. Hash it into password_hash (never store
    # the raw value) and run the same policy check as everywhere else.
    new_password = update_data.pop("password", None)
    if new_password:
        if current_user["role"] != UserRole.SUPER_ADMIN:
            raise HTTPException(status_code=403, detail="Only the Company Owner can reset another user's password")
        validate_password_policy(new_password)
        update_data["password_hash"] = get_password_hash(new_password)
        # Clear any legacy hash field so login can't read a stale value.
        update_data["hashed_password"] = None

    if update_data:
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})

    updated_user = await db.users.find_one({"_id": ObjectId(user_id)})
    # Phase 3 — single sanitizer covers all secret fields.
    return sanitize_user_doc(serialize_doc(updated_user))

@api_router.delete("/users/{user_id}")
async def delete_user(user_id: str, current_user: dict = Depends(get_current_user)):
    """Soft-delete a user (admin only, cannot delete self).

    Phase 4 — sets ``deleted_at`` instead of removing the row. Admin can
    restore from the Trash view within 30 days; the manual purge button
    permanently removes anything older than that.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    if str(current_user["_id"]) == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")

    # Verify user belongs to same company AND is not already tombstoned.
    user = await db.users.find_one({
        **_soft_delete_filter(),
        "_id": ObjectId(user_id),
        "company_id": current_user["company_id"],
    })
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        _soft_delete_update(current_user.get("_id")),
    )
    invalidate_cache("drivers", current_user["company_id"])
    return {"message": "User deleted"}

@api_router.post("/auth/export-my-data")
async def export_my_data(current_user: dict = Depends(get_current_user)):
    """User data export — Phase 10 of TODO.md.

    Returns a ZIP containing JSON dumps of everything the platform
    holds against the authenticated user: profile, every inspection,
    fuel submission, incident, plus presigned URLs for their stored
    photos. Streamed so a long-tenured driver doesn't OOM the box.

    Required by privacy laws in many jurisdictions (GDPR Article 20,
    Australian Privacy Act APP 12). Self-serve eliminates support
    requests for routine data-export questions.
    """
    import io
    import zipfile
    from starlette.responses import StreamingResponse

    user_id = str(current_user["_id"])
    company_id = current_user.get("company_id")

    # Build the manifest in memory — for a single driver the payload
    # is small (~MB). For owners of large fleets this scales with
    # their personal records only (we don't export tenant-wide rows
    # — that's a separate endpoint).
    inspections = await db.inspections.find({"driver_id": user_id}).to_list(10000)
    fuel = await db.fuel_submissions.find({"driver_id": user_id}).to_list(10000)
    incidents = await db.incidents.find({"driver_id": user_id}).to_list(10000)

    # Profile — sanitize so the export never contains the password
    # hash etc. The user is the data subject, but the bcrypt hash
    # is operationally sensitive (offline cracking risk).
    profile = sanitize_user_doc(serialize_doc(current_user))

    def _attach_photo_urls(rows: list, bucket: str, key_field: str = "object_key"):
        for r in rows or []:
            k = r.get(key_field)
            if k:
                r[f"{key_field.replace('_key', '_url')}"] = _presign_if_key(bucket, k)
        return rows

    _attach_photo_urls(inspections, "inspection-photos", "signature_object_key")
    for ins in inspections:
        for ph in ins.get("photo_refs") or []:
            if ph.get("object_key"):
                ph["object_url"] = _presign_if_key("inspection-photos", ph["object_key"])
    _attach_photo_urls(fuel, "fuel-receipts", "receipt_object_key")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("profile.json", json.dumps(profile, indent=2, default=str))
        zf.writestr("inspections.json", json.dumps(serialize_doc(inspections), indent=2, default=str))
        zf.writestr("fuel.json", json.dumps(serialize_doc(fuel), indent=2, default=str))
        zf.writestr("incidents.json", json.dumps(serialize_doc(incidents), indent=2, default=str))
        zf.writestr(
            "README.txt",
            "FleetShield365 data export\n"
            f"User: {profile.get('email') or profile.get('username')}\n"
            f"Generated: {datetime.now(timezone.utc).isoformat()}\n"
            f"Company: {company_id}\n\n"
            "Files:\n"
            "  profile.json    — your user record (password hash and\n"
            "                    other operational secrets removed).\n"
            "  inspections.json— every inspection you have submitted.\n"
            "  fuel.json       — every fuel log you have submitted.\n"
            "  incidents.json  — every incident report you have filed.\n\n"
            "Photo URLs in the exports are presigned and expire after 1\n"
            "hour. Re-export to refresh them.\n",
        )

    buf.seek(0)
    filename = (
        f"fleetshield365-export-{user_id}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d')}.zip"
    )

    async def _stream():
        # Single yield is fine — ZIP is already built in memory; the
        # streaming wrapper here only matters for very large exports
        # that we'd refactor to true streaming later.
        yield buf.getvalue()

    return StreamingResponse(
        _stream(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@api_router.post("/account/delete-request")
async def request_account_deletion(current_user: dict = Depends(get_current_user)):
    """User requests their own account deletion - anonymizes data per NHVR compliance"""
    user_id = str(current_user["_id"])
    company_id = current_user.get("company_id")
    user_name = current_user.get("name", current_user.get("username", "Unknown"))
    user_email = current_user.get("email", "")
    
    # Anonymize inspection records (keep for NHVR 3-year compliance, remove personal data)
    await db.inspections.update_many(
        {"driver_id": user_id},
        {"$set": {"driver_name": "Deleted User", "driver_id": f"deleted_{user_id[:8]}"}}
    )
    
    # Anonymize fuel submissions
    await db.fuel_submissions.update_many(
        {"driver_id": user_id},
        {"$set": {"driver_name": "Deleted User", "driver_id": f"deleted_{user_id[:8]}"}}
    )
    
    # Anonymize incidents
    await db.incidents.update_many(
        {"driver_id": user_id},
        {"$set": {"driver_name": "Deleted User", "driver_id": f"deleted_{user_id[:8]}"}}
    )
    
    # Remove push tokens
    await db.push_tokens.delete_many({"user_id": user_id})
    
    # Remove user account completely
    await db.users.delete_one({"_id": ObjectId(user_id)})
    
    # Send confirmation email
    try:
        if user_email:
            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; padding: 20px; max-width: 600px; margin: 0 auto;">
                <div style="background-color: #0891B2; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0;">
                    <h2 style="margin: 0;">FleetShield365 - Account Deleted</h2>
                </div>
                <div style="border: 1px solid #E5E7EB; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
                    <p>Hi {user_name},</p>
                    <p>Your FleetShield365 account has been successfully deleted.</p>
                    <p><strong>What was removed:</strong></p>
                    <ul>
                        <li>Your account credentials and profile</li>
                        <li>Your personal information (name, email, phone)</li>
                        <li>Push notification tokens</li>
                    </ul>
                    <p><strong>What was retained (anonymized):</strong></p>
                    <ul>
                        <li>Inspection records — anonymized as "Deleted User" (retained for NHVR compliance, 3-year requirement)</li>
                        <li>Fuel logs — anonymized as "Deleted User"</li>
                        <li>Incident reports — anonymized as "Deleted User"</li>
                    </ul>
                    <p>These records are required by Australian National Heavy Vehicle Regulator (NHVR) for a minimum of 3 years. Your personal identity has been completely removed from these records.</p>
                    <p>If you have any questions, contact us at alerts@fleetshield365.com</p>
                    <p style="color: #9CA3AF; font-size: 12px; margin-top: 20px;">FleetShield365 — A product of Prime Mover Rentals Pty Ltd.</p>
                </div>
            </body>
            </html>
            """
            await send_email_notification(user_email, "[FleetShield365] Account Deletion Confirmation", html_content)
    except Exception:
        pass  # Don't block deletion if email fails
    
    # Notify company admins
    try:
        admins = await db.users.find({"company_id": company_id, "role": {"$in": ["super_admin", "admin"]}, "deleted_at": None}).to_list(10)
        for admin in admins:
            if admin.get("email"):
                await send_email_notification(
                    admin["email"],
                    f"[FleetShield365] User Account Deleted - {user_name}",
                    f"<p>Driver <strong>{user_name}</strong> has deleted their account. Their inspection and fuel records have been anonymized and retained for compliance.</p>"
                )
    except Exception:
        pass
    
    return {"message": "Account deleted successfully. A confirmation email has been sent."}

# ============== Vehicle Routes ==============

@api_router.post("/vehicles")
async def create_vehicle(vehicle: VehicleCreate, request: Request, current_user: dict = Depends(require_active_tenant)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]

    # Phase 12 of TODO.md — plan limit enforcement. Each company has a
    # max_vehicles cap (null = unlimited on the pro plan). HTTP 402
    # (Payment Required) with an upgrade-CTA detail is the correct
    # signal here — distinct from 403 ("you don't have permission") so
    # the client UI can render an upgrade modal instead of a generic
    # "not authorised" error.
    company_doc = await db.companies.find_one(
        {"_id": ObjectId(company_id)},
        {"max_vehicles": 1, "subscription_plan": 1},
    )
    if company_doc:
        max_vehicles = company_doc.get("max_vehicles")
        # max_vehicles=None means unlimited (pro plan). 0 means free
        # plan, no vehicles. Positive = hard cap.
        if max_vehicles is not None:
            current_count = await db.vehicles.count_documents({
                **_soft_delete_filter(),
                "company_id": company_id,
            })
            if current_count >= max_vehicles:
                raise HTTPException(
                    status_code=402,
                    detail=(
                        f"Your {company_doc.get('subscription_plan', 'current')} plan "
                        f"allows up to {max_vehicles} vehicle(s). Upgrade your plan to "
                        f"add more vehicles."
                    ),
                )

    vehicle_id = ObjectId()
    vehicle_doc = {
        "_id": vehicle_id,
        "company_id": company_id,
        "name": vehicle.name,
        "registration_number": vehicle.registration_number,
        "trailer_attached": vehicle.trailer_attached,
        "status": vehicle.status,
        "type": vehicle.type or "truck",
        "rego_expiry": vehicle.rego_expiry,
        "insurance_expiry": vehicle.insurance_expiry,
        "safety_certificate_expiry": vehicle.safety_certificate_expiry,
        "coi_expiry": vehicle.coi_expiry,
        "service_due_km": vehicle.service_due_km,
        "current_odometer": vehicle.current_odometer or 0,
        "assigned_driver_ids": vehicle.assigned_driver_ids or [],
        # Phase 2.2 — owner-supplied "anything else" fields. Notes and
        # custom_fields are persisted directly; the photo goes to MinIO
        # and only the object key is stored on the doc.
        "notes": (vehicle.notes or "").strip()[:2000] or None,
        "custom_fields": (
            [cf.dict() for cf in (vehicle.custom_fields or [])][:10] or None
        ),
        "created_at": utcnow()
    }
    if vehicle.image_base64:
        image_key = f"{company_id}/vehicles/{vehicle_id}.jpg"
        _upload_base64_or_400(
            "photos",
            image_key,
            vehicle.image_base64,
            "jpg",
            "image_base64",
            expected_company_id=company_id,
            type_key="profile",
        )
        vehicle_doc["image_object_key"] = image_key

    # 2026-05-20 — optional supporting docs for each expiry. Image OR PDF.
    # Lives in the `compliance` bucket under `vehicle-docs/<company_id>/...`
    # so the per-tenant prefix validator passes (it accepts company_id at
    # segment index 1 in the compliance bucket — see object_store.py).
    for field_name, doc_key in [
        ("rego_doc_base64", "rego_doc_object_key"),
        ("insurance_doc_base64", "insurance_doc_object_key"),
        ("safety_cert_doc_base64", "safety_cert_doc_object_key"),
        ("coi_doc_base64", "coi_doc_object_key"),
    ]:
        b64 = getattr(vehicle, field_name, None)
        if not b64:
            continue
        # Detect ext from base64 magic bytes — _upload_base64_or_400 stamps
        # Content-Type from detected format, but the key extension is purely
        # cosmetic. Default .bin if we can't infer; .jpg/.png/.pdf otherwise.
        try:
            raw = base64.b64decode(object_store._DATA_URL_PREFIX_RE.sub("", b64).strip(), validate=True)
            detected = _detect_format(raw)
            ext = {"jpeg": "jpg", "png": "png", "webp": "webp", "pdf": "pdf"}.get(detected, "bin")
        except Exception:
            ext = "bin"
        doc_slug = doc_key.replace("_doc_object_key", "")  # rego / insurance / safety_cert / coi
        target = f"vehicle-docs/{company_id}/{vehicle_id}/{doc_slug}.{ext}"
        _upload_base64_or_400(
            "compliance",
            target,
            b64,
            ext,
            field_name,
            expected_company_id=company_id,
            type_key="vehicle_doc",
        )
        vehicle_doc[doc_key] = target

    await db.vehicles.insert_one(vehicle_doc)
    
    # Invalidate vehicles cache
    invalidate_cache("vehicles", company_id)
    invalidate_cache("dashboard", company_id)
    
    # Check for upcoming expiries and create alerts
    await check_and_create_expiry_alerts(vehicle_doc, company_id)
    
    # Update company vehicle count
    await db.companies.update_one(
        {"_id": ObjectId(company_id)},
        {"$inc": {"active_vehicles_count": 1}}
    )
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "vehicle", str(vehicle_doc["_id"]),
        request.client.host if request.client else "unknown"
    )

    # Stripe: bump the per-vehicle line item by 1 (no-op if the tenant
    # has no Stripe subscription yet or Stripe isn't configured).
    try:
        await _sync_vehicle_quantity_to_stripe(company_id)
    except Exception:
        pass

    return _attach_vehicle_image_url(serialize_doc(vehicle_doc))

@api_router.get("/vehicles")
async def get_vehicles(
    limit: int = 200,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """List vehicles for the tenant. Paginated (Phase 2 of STORAGE-PLAN.txt).

    Default ``limit=200`` matches the historical implicit cap and keeps
    the cached fast path identical to the pre-pagination behaviour.
    Hard-capped at 500 to bound the worst-case response. ``offset`` for
    pagination beyond that.
    """
    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)
    company_id = current_user["company_id"]

    # For drivers, don't use cache (they see filtered list).
    if current_user["role"] == UserRole.DRIVER:
        # Phase 4 — exclude soft-deleted vehicles from driver view.
        query = {
            **_soft_delete_filter(),
            "company_id": company_id,
            "assigned_driver_ids": str(current_user["_id"]),
        }
        vehicles = await db.vehicles.find(query).skip(actual_offset).limit(actual_limit).to_list(actual_limit)
        result = serialize_doc(vehicles)
        return [_attach_vehicle_image_url(v) for v in result] if isinstance(result, list) else result

    # Cache only the default page. Bypass when caller paginates.
    use_cache = actual_offset == 0 and actual_limit >= 200
    if use_cache:
        cached = get_cached("vehicles", company_id)
        if cached:
            # Re-attach presigned URLs on cache hit so they don't expire.
            return [_attach_vehicle_image_url(dict(v)) for v in cached] if isinstance(cached, list) else cached

    # Phase 4 — exclude soft-deleted vehicles from the admin list.
    query = {**_soft_delete_filter(), "company_id": company_id}
    vehicles = await db.vehicles.find(query).skip(actual_offset).limit(actual_limit).to_list(actual_limit)
    result = serialize_doc(vehicles)
    if isinstance(result, list):
        result = [_attach_vehicle_image_url(v) for v in result]

    if use_cache:
        # Cache without presigned URLs (they expire). Presigning is cheap,
        # so re-attach on every cache hit instead of caching short-lived
        # links that go stale.
        cacheable = [
            {k: v for k, v in vd.items() if k != "image_url"}
            for vd in (result if isinstance(result, list) else [])
        ]
        set_cached("vehicles", company_id, cacheable)
    return result

@api_router.get("/vehicles/active-today")
async def get_active_vehicles_today(
    current_user: dict = Depends(get_current_user),
    tz_offset: int = 0  # Kept for backwards compatibility, but ignored
):
    """Lightweight endpoint to get just the IDs of vehicles that had inspections today"""
    company_id = current_user["company_id"]
    
    # Use shared Sydney timezone helper (same as dashboard)
    today_utc, _ = get_sydney_today_range()
    
    # Get active vehicle IDs
    active_ids = await db.inspections.distinct("vehicle_id", {
        "company_id": company_id,
        "timestamp": {"$gte": today_utc}
    })
    
    return {"active_vehicle_ids": active_ids, "count": len(active_ids)}

async def _compute_storage_categories(company_filter: Optional[dict] = None) -> List[dict]:
    """Phase 4.4 — content breakdown of storage by user-facing category.

    Counts come from Mongo (cheap aggregations on indexed fields). Bytes
    are an estimate (count × avg per-category size); see CLAUDE.md §13
    note. Pass ``company_filter`` to scope to one tenant.
    """
    filt: dict = dict(company_filter or {})

    def f(extra: Optional[dict] = None) -> dict:
        out = dict(filt)
        if extra:
            out.update(extra)
        return out

    # Inspection photo counts. photo_refs is a small array on each
    # inspection doc; counting array elements requires an aggregate.
    async def _photo_count(coll, match: dict, array_field: str) -> int:
        pipeline = [
            {"$match": match},
            {"$project": {"n": {"$size": {"$ifNull": [f"${array_field}", []]}}}},
            {"$group": {"_id": None, "total": {"$sum": "$n"}}},
        ]
        result = await coll.aggregate(pipeline).to_list(1)
        return result[0]["total"] if result else 0

    soft = _soft_delete_filter()
    # Owner review 2026-05-19: the inspections collection uses field
    # name `type` ("prestart" / "end_shift"), NOT `inspection_type`.
    # The previous version of this helper returned 0 for both classes
    # of inspection photos because the match never hit a single doc —
    # so the Storage page's "Prestart inspection photos" + "End-shift
    # inspection photos" rows always read 0.
    prestart_photos = await _photo_count(
        db.inspections, f({**soft, "type": "prestart"}), "photo_refs")
    endshift_photos = await _photo_count(
        db.inspections, f({**soft, "type": "end_shift"}), "photo_refs")
    incident_damage = await _photo_count(
        db.incidents, f({**soft}), "damage_photos")
    incident_scene = await _photo_count(
        db.incidents, f({**soft}), "scene_photos")
    incident_other = await _photo_count(
        db.incidents, f({**soft}), "other_vehicle_photos")

    # Simple count_documents for object-key-or-not. The presence of the
    # key implies a stored file.
    signatures = await db.inspections.count_documents(
        f({**soft, "signature_object_key": {"$exists": True, "$ne": None}}))
    fuel_receipts = await db.fuel_submissions.count_documents(
        f({"receipt_object_key": {"$exists": True, "$ne": None}}))
    # Service-record attachments live under two field names: the new
    # `attachment_object_keys` array (post-Phase-2) and the legacy
    # `attachments` array (pre-migration rows). Count both.
    service_attachments = await _photo_count(
        db.service_records, f({**soft}), "attachment_object_keys")
    service_attachments += await _photo_count(
        db.service_records, f({**soft, "attachment_object_keys": {"$exists": False}}), "attachments")
    maintenance_invoices = await db.maintenance_logs.count_documents(
        f({**soft, "invoice_object_key": {"$exists": True, "$ne": None}}))
    logos = await db.companies.count_documents(
        f({**soft, "logo_object_key": {"$exists": True, "$ne": None}}))

    # Driver compliance docs — front + back per doc type.
    doc_fields = [
        "license_front_object_key", "license_back_object_key",
        "medical_cert_front_object_key", "medical_cert_back_object_key",
        "first_aid_front_object_key", "first_aid_back_object_key",
        "forklift_front_object_key", "forklift_back_object_key",
        "dangerous_goods_front_object_key", "dangerous_goods_back_object_key",
        "msic_front_object_key", "msic_back_object_key",
        "other_doc_front_object_key", "other_doc_back_object_key",
    ]
    driver_doc_count = 0
    for field in doc_fields:
        driver_doc_count += await db.users.count_documents(
            f({**soft, field: {"$exists": True, "$ne": None}}))

    # Vehicle photos (Phase 2.2).
    vehicle_images = await db.vehicles.count_documents(
        f({**soft, "image_object_key": {"$exists": True, "$ne": None}}))

    # Per-category KB estimates (post-compression sizes from CLAUDE.md
    # §11 Phase-1 numbers: thumbnails 50KB, photos 250KB, PDFs 200KB).
    def _kb(n: int, kb: int) -> int:
        return n * kb * 1024

    return [
        {"key": "prestart_photos", "label": "Prestart inspection photos",
         "count": prestart_photos, "bytes_estimate": _kb(prestart_photos, 250)},
        {"key": "endshift_photos", "label": "End-shift inspection photos",
         "count": endshift_photos, "bytes_estimate": _kb(endshift_photos, 250)},
        {"key": "inspection_signatures", "label": "Inspection signatures",
         "count": signatures, "bytes_estimate": _kb(signatures, 50)},
        {"key": "incident_damage_photos", "label": "Incident damage photos",
         "count": incident_damage, "bytes_estimate": _kb(incident_damage, 250)},
        {"key": "incident_scene_photos", "label": "Incident scene photos",
         "count": incident_scene, "bytes_estimate": _kb(incident_scene, 250)},
        {"key": "incident_other_photos", "label": "Incident other-vehicle photos",
         "count": incident_other, "bytes_estimate": _kb(incident_other, 250)},
        {"key": "fuel_receipts", "label": "Fuel receipts",
         "count": fuel_receipts, "bytes_estimate": _kb(fuel_receipts, 200)},
        {"key": "service_attachments", "label": "Service record attachments",
         "count": service_attachments, "bytes_estimate": _kb(service_attachments, 300)},
        {"key": "maintenance_invoices", "label": "Maintenance invoices",
         "count": maintenance_invoices, "bytes_estimate": _kb(maintenance_invoices, 300)},
        {"key": "driver_documents", "label": "Driver compliance documents",
         "count": driver_doc_count, "bytes_estimate": _kb(driver_doc_count, 200)},
        {"key": "company_logos", "label": "Company logos",
         "count": logos, "bytes_estimate": _kb(logos, 100)},
        {"key": "vehicle_images", "label": "Vehicle photos",
         "count": vehicle_images, "bytes_estimate": _kb(vehicle_images, 200)},
    ]


def _attach_vehicle_image_url(vehicle_doc: dict) -> dict:
    """Phase 2.2 — render-time helper. Tacks a short-lived presigned URL
    onto vehicles that carry an image_object_key so the frontend can
    render the photo without a separate fetch. The key never leaves the
    backend in its raw form for non-developers; the URL is the only
    public-facing pointer.

    2026-05-20 — also presigns the four vehicle-doc keys (rego /
    insurance / safety_cert / coi) so the Edit Vehicle modal can show
    a "Document attached — open" link straight away.
    """
    if not isinstance(vehicle_doc, dict):
        return vehicle_doc
    object_key = vehicle_doc.get("image_object_key")
    if object_key:
        try:
            vehicle_doc["image_url"] = _presign_if_key("photos", object_key)
        except Exception:
            vehicle_doc["image_url"] = None
    for ok_field, url_field in [
        ("rego_doc_object_key",         "rego_doc_url"),
        ("insurance_doc_object_key",    "insurance_doc_url"),
        ("safety_cert_doc_object_key",  "safety_cert_doc_url"),
        ("coi_doc_object_key",          "coi_doc_url"),
    ]:
        key = vehicle_doc.get(ok_field)
        if key:
            try:
                vehicle_doc[url_field] = _presign_if_key("compliance", key)
            except Exception:
                vehicle_doc[url_field] = None
    return vehicle_doc


# Declared BEFORE the dynamic /vehicles/{vehicle_id} route so FastAPI's
# in-order matcher hits this for /vehicles/documents/download — otherwise
# the dynamic route would try ObjectId("documents") and 500. Matches the
# same pattern used for /incidents/export/pdf elsewhere in this file.
@api_router.get("/vehicles/documents/download")
async def export_vehicle_documents_zip(
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,   # CSV "id1,id2,..."
    doc_types: Optional[str] = None,     # CSV subset of rego/insurance/safety_cert/coi (default: all)
    current_user: dict = Depends(get_current_user),
):
    """Streams a ZIP of every vehicle's rego/insurance/safety-cert/COI docs.

    Folder layout inside the ZIP: ``<vehicle name (rego)>/<doc_type>.<ext>``.
    Skips vehicles with no attached docs entirely. Manifest at the root
    lists what was included and what was skipped (missing files, read
    errors, vehicles without docs).
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    from starlette.responses import StreamingResponse

    company_id = current_user["company_id"]
    query: dict = {**_soft_delete_filter(), "company_id": company_id}
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["_id"] = {"$in": [ObjectId(v) for v in vid_list if ObjectId.is_valid(v)]}
    elif vehicle_id and ObjectId.is_valid(vehicle_id):
        query["_id"] = ObjectId(vehicle_id)

    allowed_types = {"rego", "insurance", "safety_cert", "coi"}
    requested_types = (
        {t.strip() for t in doc_types.split(",") if t.strip()}
        if doc_types else allowed_types
    )
    requested_types &= allowed_types
    if not requested_types:
        raise HTTPException(status_code=400, detail="No valid doc_types selected")

    vehicles = await db.vehicles.find(
        query,
        {"name": 1, "registration_number": 1,
         "rego_doc_object_key": 1, "insurance_doc_object_key": 1,
         "safety_cert_doc_object_key": 1, "coi_doc_object_key": 1},
    ).to_list(2000)

    zip_buffer = BytesIO()
    manifest_lines = [
        "FleetShield365 Vehicle Document Export",
        f"Generated: {utcnow().isoformat()}",
        f"Vehicles in scope: {len(vehicles)}",
        f"Doc types: {sorted(requested_types)}",
        "",
    ]
    written = 0
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for v in vehicles:
            vname = v.get("registration_number") or v.get("name") or str(v["_id"])
            folder_safe = "".join(c if c.isalnum() or c in "-_ " else "_" for c in vname)
            for doc_type in sorted(requested_types):
                key = v.get(f"{doc_type}_doc_object_key")
                if not key:
                    continue
                try:
                    raw = object_store.get_bytes("compliance", key)
                except Exception as exc:
                    logger.warning(f"Vehicle doc {key} unreadable: {exc}")
                    manifest_lines.append(f"- SKIPPED {folder_safe}/{doc_type} (read error)")
                    continue
                ext = ".pdf" if raw[:4] == b"%PDF" else ".jpg"
                fname = f"{folder_safe}/{doc_type}{ext}"
                zf.writestr(fname, raw)
                manifest_lines.append(f"- {fname}")
                written += 1
        zf.writestr("manifest.txt", "\n".join(manifest_lines))

    if written == 0:
        raise HTTPException(status_code=404, detail="No vehicle documents match the selected filters")

    zip_buffer.seek(0)
    out_name = f"vehicle_documents_{utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={out_name}"},
    )


@api_router.get("/vehicles/{vehicle_id}")
async def get_vehicle(vehicle_id: str, current_user: dict = Depends(get_current_user)):
    if not ObjectId.is_valid(vehicle_id):
        raise HTTPException(status_code=404, detail="Vehicle not found")
    vehicle = await db.vehicles.find_one({
        "_id": ObjectId(vehicle_id),
        "company_id": current_user["company_id"]
    })
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return _attach_vehicle_image_url(serialize_doc(vehicle))


@api_router.get("/vehicles/{vehicle_id}/history")
async def get_vehicle_history(
    vehicle_id: str,
    limit: int = 10,
    current_user: dict = Depends(get_current_user),
):
    """Return a per-vehicle activity feed.

    Aggregates the most recent inspections (pre-start + end-shift),
    fuel logs, incidents, and service records into one response so the
    UI can render a single "Vehicle History" panel without 4 parallel
    requests. Each row carries the driver/operator name and timestamp.

    Drivers can only fetch history for vehicles they're assigned to.
    Admins + super_admins see any vehicle in their tenant.
    """
    if current_user["role"] not in [
        UserRole.SUPER_ADMIN, UserRole.ADMIN, UserRole.DRIVER,
    ]:
        raise HTTPException(status_code=403, detail="Not authorized")

    # 2026-05-19 — guard against bad vehicle_id slugs in the URL.
    # Without this, ObjectId() raises bson.errors.InvalidId which
    # bubbles up as a 500 instead of the expected 400/404.
    if not ObjectId.is_valid(vehicle_id):
        raise HTTPException(status_code=400, detail="Invalid vehicle id")

    company_id = current_user["company_id"]
    vehicle = await db.vehicles.find_one({
        **_soft_delete_filter(),
        "_id": ObjectId(vehicle_id),
        "company_id": company_id,
    })
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    if current_user["role"] == UserRole.DRIVER:
        # A driver can only view history for vehicles they're assigned to.
        assigned_ids = vehicle.get("assigned_driver_ids") or []
        if str(current_user["_id"]) not in assigned_ids:
            raise HTTPException(status_code=403, detail="Not authorized")

    actual_limit = max(1, min(int(limit), 50))

    # Resolve all driver names referenced across the rows in one shot
    # so we avoid N+1 queries per row.
    async def _fetch_inspections(itype: Optional[str] = None) -> List[dict]:
        q: dict = {
            **_soft_delete_filter(),
            "company_id": company_id,
            "vehicle_id": vehicle_id,
        }
        if itype:
            q["type"] = itype
        cursor = db.inspections.find(q).sort("timestamp", -1).limit(actual_limit)
        return await cursor.to_list(actual_limit)

    prestart_rows, endshift_rows, fuel_rows, incident_rows, service_rows = await asyncio.gather(
        _fetch_inspections("prestart"),
        _fetch_inspections("end_shift"),
        db.fuel_submissions.find({
            **_soft_delete_filter(),
            "company_id": company_id,
            "vehicle_id": vehicle_id,
        }).sort("timestamp", -1).limit(actual_limit).to_list(actual_limit),
        db.incidents.find({
            **_soft_delete_filter(),
            "company_id": company_id,
            "vehicle_id": vehicle_id,
        }).sort("created_at", -1).limit(actual_limit).to_list(actual_limit),
        db.service_records.find({
            **_soft_delete_filter(),
            "company_id": company_id,
            "vehicle_id": vehicle_id,
        }).sort("service_date", -1).limit(actual_limit).to_list(actual_limit),
    )

    # Collect driver IDs we need to resolve to names.
    driver_ids: set = set()
    for row in prestart_rows + endshift_rows + fuel_rows + incident_rows:
        did = row.get("driver_id")
        if did:
            driver_ids.add(did)
    driver_lookup: dict = {}
    if driver_ids:
        try:
            cursor = db.users.find(
                {"_id": {"$in": [ObjectId(d) for d in driver_ids if d]}},
                {"name": 1, "username": 1},
            )
            async for u in cursor:
                driver_lookup[str(u["_id"])] = (
                    u.get("name") or u.get("username") or "Operator"
                )
        except Exception:
            # ObjectId parsing fail (e.g. legacy string ids) — fall back
            # to row-level driver_name if present.
            driver_lookup = {}

    def _name(row: dict) -> str:
        did = row.get("driver_id")
        if did and did in driver_lookup:
            return driver_lookup[did]
        return row.get("driver_name") or "Unknown operator"

    def _serial(rows: List[dict], extra: callable) -> List[dict]:
        out = []
        for r in rows:
            r = serialize_doc(r)
            out.append(extra(r))
        return out

    response = {
        "vehicle": _attach_vehicle_image_url(serialize_doc(vehicle)),
        "prestart_inspections": _serial(prestart_rows, lambda r: {
            "id": r.get("id"),
            "timestamp": r.get("timestamp"),
            "odometer": r.get("odometer"),
            "driver_name": _name(r),
            "is_safe": r.get("is_safe"),
            "issue_count": sum(
                1 for c in (r.get("checklist_items") or [])
                if c.get("status") == "issue"
            ),
        }),
        "endshift_inspections": _serial(endshift_rows, lambda r: {
            "id": r.get("id"),
            "timestamp": r.get("timestamp"),
            "odometer": r.get("odometer"),
            "driver_name": _name(r),
            "new_damage": bool(r.get("new_damage")),
            "incident_today": bool(r.get("incident_today")),
            "cleanliness": r.get("cleanliness"),
            "damage_comment": r.get("damage_comment"),
        }),
        "fuel_logs": _serial(fuel_rows, lambda r: {
            "id": r.get("id"),
            "timestamp": r.get("timestamp") or r.get("created_at"),
            "amount": r.get("amount"),
            "liters": r.get("liters"),
            "odometer": r.get("odometer"),
            "fuel_station": r.get("fuel_station"),
            "driver_name": _name(r),
        }),
        "incidents": _serial(incident_rows, lambda r: {
            "id": r.get("id"),
            "timestamp": r.get("created_at") or r.get("incident_date"),
            "severity": r.get("severity"),
            "description": r.get("description") or r.get("notes"),
            "driver_name": _name(r),
        }),
        "service_records": _serial(service_rows, lambda r: {
            "id": r.get("id"),
            "service_date": r.get("service_date"),
            "service_type": r.get("service_type"),
            "cost": r.get("cost"),
            "odometer": r.get("odometer"),
            "workshop": r.get("workshop"),
        }),
    }
    return response


@api_router.put("/vehicles/{vehicle_id}")
async def update_vehicle(vehicle_id: str, update: VehicleUpdate, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]
    update_data = {k: v for k, v in update.dict().items() if v is not None}

    # Phase 2.2 — image_base64 is an upload, not a storable field. Push
    # the bytes to MinIO under photos/<company>/vehicles/<id>.jpg and
    # swap in the object key instead.
    image_b64 = update_data.pop("image_base64", None)
    if image_b64:
        image_key = f"{company_id}/vehicles/{vehicle_id}.jpg"
        _upload_base64_or_400(
            "photos",
            image_key,
            image_b64,
            "jpg",
            "image_base64",
            expected_company_id=company_id,
            type_key="profile",
        )
        update_data["image_object_key"] = image_key

    # 2026-05-20 — same vehicle-doc upload flow as create. Caller passes
    # a base64 string per field they want to replace; missing fields are
    # left untouched. Empty-string explicitly clears the doc reference.
    for field_name, doc_key in [
        ("rego_doc_base64", "rego_doc_object_key"),
        ("insurance_doc_base64", "insurance_doc_object_key"),
        ("safety_cert_doc_base64", "safety_cert_doc_object_key"),
        ("coi_doc_base64", "coi_doc_object_key"),
    ]:
        if field_name not in update_data:
            continue
        b64 = update_data.pop(field_name)
        if not b64:
            update_data[doc_key] = None
            continue
        try:
            raw = base64.b64decode(object_store._DATA_URL_PREFIX_RE.sub("", b64).strip(), validate=True)
            detected = _detect_format(raw)
            ext = {"jpeg": "jpg", "png": "png", "webp": "webp", "pdf": "pdf"}.get(detected, "bin")
        except Exception:
            ext = "bin"
        doc_slug = doc_key.replace("_doc_object_key", "")
        target = f"vehicle-docs/{company_id}/{vehicle_id}/{doc_slug}.{ext}"
        _upload_base64_or_400(
            "compliance",
            target,
            b64,
            ext,
            field_name,
            expected_company_id=company_id,
            type_key="vehicle_doc",
        )
        update_data[doc_key] = target

    # Cap notes + custom_fields the same way create does. Pydantic gives
    # us the list back as VehicleCustomField objects when present.
    if "notes" in update_data and update_data["notes"] is not None:
        update_data["notes"] = update_data["notes"].strip()[:2000] or None
    if "custom_fields" in update_data and update_data["custom_fields"] is not None:
        update_data["custom_fields"] = (
            [cf.dict() if hasattr(cf, "dict") else cf for cf in update_data["custom_fields"]][:10]
            or None
        )

    if update_data:
        await db.vehicles.update_one(
            {"_id": ObjectId(vehicle_id), "company_id": company_id},
            {"$set": update_data}
        )

    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    serialized = _attach_vehicle_image_url(serialize_doc(vehicle))

    # Invalidate cache
    invalidate_cache("vehicles", current_user["company_id"])
    invalidate_cache("dashboard", current_user["company_id"])

    await log_audit_trail(
        str(current_user["_id"]), "update", "vehicle", vehicle_id,
        request.client.host if request.client else "unknown", update_data
    )

    return serialized

@api_router.delete("/vehicles/{vehicle_id}")
async def delete_vehicle(
    vehicle_id: str,
    request: Request,
    cascade: bool = False,
    current_user: dict = Depends(get_current_user),
):
    """Soft-delete a vehicle with referential integrity check (Phase 4).

    Default behaviour: if the vehicle has any non-deleted children
    (inspections, fuel, incidents, service records, maintenance logs)
    the request returns HTTP 409 with a count breakdown so the admin
    can see what's still attached. Passing ``?cascade=true`` soft-
    deletes the vehicle AND its non-immutable children (service +
    maintenance) — inspections, fuel, incidents stay (NHVR records).
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]

    vehicle = await db.vehicles.find_one({
        **_soft_delete_filter(),
        "_id": ObjectId(vehicle_id),
        "company_id": company_id,
    })
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    # Phase 4 referential integrity. Count non-deleted children that
    # would be orphaned. inspections/fuel/incidents are NHVR compliance
    # records — we count them for the admin's awareness but never
    # touch them, even on cascade.
    child_counts: dict = {}
    for coll_name, soft in [
        ("service_records", True),
        ("maintenance_logs", True),
        ("inspections", False),
        ("fuel_submissions", False),
        ("incidents", False),
    ]:
        filt = {"vehicle_id": vehicle_id, "company_id": company_id}
        if soft:
            filt.update(_soft_delete_filter())
        count = await db[coll_name].count_documents(filt)
        if count:
            child_counts[coll_name] = count

    if child_counts and not cascade:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    "Vehicle has linked records. Delete those first, "
                    "or re-send with ?cascade=true to soft-delete "
                    "service + maintenance rows (NHVR records "
                    "[inspections/fuel/incidents] are kept regardless)."
                ),
                "children": child_counts,
            },
        )

    # Soft-delete the vehicle itself.
    await db.vehicles.update_one(
        {"_id": ObjectId(vehicle_id)},
        _soft_delete_update(current_user.get("_id")),
    )

    # On cascade: soft-delete the two non-immutable child collections.
    cascade_counts: dict = {}
    if cascade:
        for coll_name in ("service_records", "maintenance_logs"):
            res = await db[coll_name].update_many(
                {
                    **_soft_delete_filter(),
                    "vehicle_id": vehicle_id,
                    "company_id": company_id,
                },
                _soft_delete_update(current_user.get("_id")),
            )
            if res.modified_count:
                cascade_counts[coll_name] = res.modified_count

    invalidate_cache("vehicles", company_id)
    invalidate_cache("dashboard", company_id)

    await db.companies.update_one(
        {"_id": ObjectId(company_id)},
        {"$inc": {"active_vehicles_count": -1}}
    )

    await log_audit_trail(
        str(current_user["_id"]), "delete", "vehicle", vehicle_id,
        request.client.host if request.client else "unknown"
    )

    # Stripe: drop the per-vehicle line item by 1 (proration credit
    # applies on the next invoice). No-op if tenant has no Stripe sub.
    try:
        await _sync_vehicle_quantity_to_stripe(company_id)
    except Exception:
        pass

    return {
        "message": "Vehicle deleted",
        "cascade": cascade_counts,
    }

@api_router.post("/vehicles/{vehicle_id}/assign")
async def assign_drivers(vehicle_id: str, assignment: DriverAssignment, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.vehicles.update_one(
        {"_id": ObjectId(vehicle_id), "company_id": current_user["company_id"]},
        {"$set": {"assigned_driver_ids": assignment.driver_ids}}
    )
    
    # Update drivers' assigned vehicles
    for driver_id in assignment.driver_ids:
        await db.users.update_one(
            {"_id": ObjectId(driver_id)},
            {"$addToSet": {"assigned_vehicles": vehicle_id}}
        )
    
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    return serialize_doc(vehicle)

# ============== Driver Routes ==============

@api_router.get("/drivers/generate-username")
async def generate_username_preview(name: str, current_user: dict = Depends(get_current_user)):
    """Generate a globally unique username preview for the frontend"""
    if not name or not name.strip():
        return {"username": "user"}
    
    # Use the same logic as create_driver
    username = await generate_unique_username(name, current_user["company_id"])
    return {"username": username}

@api_router.get("/drivers")
async def get_drivers(
    limit: int = 200,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)
    company_id = current_user["company_id"]

    # Cache only the default page (Phase 2 of STORAGE-PLAN.txt).
    use_cache = actual_offset == 0 and actual_limit >= 200
    if use_cache:
        cached = get_cached("drivers", company_id)
        if cached:
            return cached

    # Phase 4 — exclude soft-deleted users.
    drivers = await db.users.find({
        **_soft_delete_filter(),
        "company_id": company_id,
        "role": UserRole.DRIVER,
    }).skip(actual_offset).limit(actual_limit).to_list(actual_limit)

    # Also get admins who are enabled as operators (always full list —
    # tiny by definition, never the pagination bottleneck).
    admin_operators = await db.users.find({
        **_soft_delete_filter(),
        "company_id": company_id,
        "role": {"$in": [UserRole.ADMIN, UserRole.SUPER_ADMIN]},
        "is_also_operator": True,
    }).to_list(100)

    # Combine lists
    all_operators = drivers + admin_operators
    result = serialize_doc(all_operators)

    # Attach presigned URLs for custom_documents so the UI can render
    # thumbnails without a second round-trip. Skipped when the list is
    # empty so caching cost stays the same as before.
    if isinstance(result, list):
        for entry in result:
            _attach_custom_document_urls(entry)

    # Cache the result
    set_cached("drivers", company_id, result)
    return result

@api_router.post("/drivers")
async def create_driver(user: UserRegister, request: Request, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    # 2026-05-19 — owner request: driver email is OPTIONAL. Drivers sign
    # in with username + 4-digit PIN on the mobile app, so an email isn't
    # required at create time. When omitted, `email_verified` defaults to
    # false and the credentials email is skipped client-side.
    if user.phone:
        phone_digits = "".join(ch for ch in user.phone if ch.isdigit())
        if len(phone_digits) != 10:
            raise HTTPException(
                status_code=400,
                detail="Driver phone must be exactly 10 digits",
            )

    # 2026-05-19 — accept either a 4-digit PIN (preferred for drivers) or
    # a free-form password. PIN wins when both are supplied. If neither
    # is supplied we reject — the credential is what makes the account
    # usable.
    pin = (user.pin or "").strip()
    password = user.password or ""
    if pin:
        validate_driver_pin(pin)
        credential_hash = get_password_hash(pin)
    elif password:
        credential_hash = get_password_hash(password)
    else:
        raise HTTPException(status_code=400, detail="PIN or password is required")

    # Check if email already exists
    email_lower = (user.email or "").strip().lower() or None
    if email_lower:
        existing = await db.users.find_one({"email": email_lower})

        # Check if email belongs to an admin in the same company
        if existing:
            # If it's an admin from the same company, enable them as operator too
            if existing.get("role") in [UserRole.ADMIN, UserRole.SUPER_ADMIN] and existing.get("company_id") == current_user["company_id"]:
                # Add is_also_operator flag to the existing admin account
                await db.users.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {
                        "is_also_operator": True,
                        "operator_enabled_at": utcnow()
                    }}
                )
                # Return the updated user
                updated_user = await db.users.find_one({"_id": existing["_id"]})
                return serialize_doc(updated_user)
            else:
                raise HTTPException(status_code=400, detail="Email already registered")

    # Generate unique username
    username = user.username or await generate_unique_username(user.name, current_user["company_id"])

    # Check if username already exists GLOBALLY
    if await db.users.find_one({"username": username}):
        username = await generate_unique_username(user.name, current_user["company_id"])

    driver_id = ObjectId()
    company_id = current_user["company_id"]

    # 2026-05-19 — persist owner-defined custom documents (repeatable
    # label/number/issue/expiry with optional front/back upload). The
    # uploads land in the `compliance` bucket scoped to this driver_id.
    custom_documents = _persist_custom_documents(
        user.custom_documents, company_id, str(driver_id),
    )

    driver_doc = {
        "_id": driver_id,
        "username": username,
        "password_hash": credential_hash,
        "auth_mode": "pin" if pin else "password",
        "name": user.name,
        # Normalise empty-string phone to None so the sparse unique
        # index treats "no phone" as missing (not a real value). Owner
        # reported 2026-05-25: second driver-create with phone='' was
        # firing the unique-constraint and surfaced the misleading
        # "phone already used" error.
        "phone": (user.phone or "").strip() or None,
        "role": UserRole.DRIVER,
        "company_id": company_id,
        "assigned_vehicles": [],
        "created_at": utcnow(),
        "ip_address": request.client.host if request.client else "unknown",
        # License and training details
        "license_number": user.license_number,
        "license_class": user.license_class,
        "license_issue_date": user.license_issue_date,
        "license_expiry": user.license_expiry,
        "medical_certificate_number": user.medical_certificate_number,
        "medical_certificate_issue": user.medical_certificate_issue,
        "medical_certificate_expiry": user.medical_certificate_expiry,
        "first_aid_number": user.first_aid_number,
        "first_aid_issue": user.first_aid_issue,
        "first_aid_expiry": user.first_aid_expiry,
        "forklift_license_number": user.forklift_license_number,
        "forklift_license_issue": user.forklift_license_issue,
        "forklift_license_expiry": user.forklift_license_expiry,
        "dangerous_goods_number": user.dangerous_goods_number,
        "dangerous_goods_issue": user.dangerous_goods_issue,
        "dangerous_goods_expiry": user.dangerous_goods_expiry,
        # Phase 2.1 — MSIC and free-form "Other" document. Photo uploads
        # land via /drivers/{id}/documents/{doc_type} after create.
        "msic_number": user.msic_number,
        "msic_issue": user.msic_issue,
        "msic_expiry": user.msic_expiry,
        "other_doc_label": user.other_doc_label,
        "other_doc_number": user.other_doc_number,
        "other_doc_issue": user.other_doc_issue,
        "other_doc_expiry": user.other_doc_expiry,
        # 2026-05-19 — repeatable custom docs (replaces the fixed slots
        # for new drivers; legacy slots still write/read for older accts).
        "custom_documents": custom_documents,
    }
    # Only add email if provided (sparse index doesn't like None values)
    if email_lower:
        driver_doc["email"] = email_lower

    # 2026-05-19 — catch unique-index violations (phone, email, username)
    # and surface a friendly 400 instead of a 500 + ugly stack trace.
    # The sparse `phone_1` index is the most common offender — admins
    # often paste a number that's already on another operator's record.
    try:
        await db.users.insert_one(driver_doc)
    except DuplicateKeyError as exc:
        msg = str(exc)
        if "phone_1" in msg or "phone:" in msg:
            raise HTTPException(
                status_code=400,
                detail="That phone number is already used by another operator on this platform.",
            )
        if "email_1" in msg or "email:" in msg:
            raise HTTPException(
                status_code=400,
                detail="That email is already registered.",
            )
        if "username_1" in msg or "username:" in msg:
            raise HTTPException(
                status_code=400,
                detail="That username is already taken. Try a different name.",
            )
        raise HTTPException(
            status_code=400,
            detail="A unique-key conflict prevented driver creation. Please change the value and try again.",
        )

    # Invalidate cache
    invalidate_cache("drivers", company_id)
    invalidate_cache("dashboard", company_id)

    return _attach_custom_document_urls(serialize_doc(driver_doc))

@api_router.delete("/drivers/{driver_id}")
async def delete_driver(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Soft-delete a driver (Phase 4)."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]

    driver = await db.users.find_one({
        **_soft_delete_filter(),
        "_id": ObjectId(driver_id),
        "company_id": company_id,
        "role": UserRole.DRIVER,
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    await db.users.update_one(
        {"_id": ObjectId(driver_id)},
        _soft_delete_update(current_user.get("_id")),
    )

    # Invalidate cache
    invalidate_cache("drivers", company_id)
    invalidate_cache("dashboard", company_id)

    return {"message": "Driver deleted"}

# ============== License Photo Routes (Owner Only) ==============

class LicensePhotoUpload(BaseModel):
    front_photo_base64: Optional[str] = None
    back_photo_base64: Optional[str] = None

class PasswordVerification(BaseModel):
    password: str

class DocumentDownloadRequest(BaseModel):
    operator_ids: List[str]
    document_types: List[str]  # driver_license, medical, first_aid, forklift, dangerous_goods
    password: str

@api_router.post("/drivers/download-documents")
async def download_operator_documents(request: DocumentDownloadRequest, current_user: dict = Depends(get_current_user)):
    """Download operator documents as ZIP - Owner (super_admin) only"""
    from fastapi.responses import StreamingResponse
    import base64
    import re
    
    # Only super_admin can download documents
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can download documents")
    
    # Verify password. Return 400 (not 401) so the web axios interceptor
    # treats it as an inline form error instead of an expired session.
    if not verify_password(request.password, current_user.get("password_hash") or current_user.get("hashed_password") or ""):
        raise HTTPException(status_code=400, detail="Incorrect password")

    # Fetch selected operators
    operator_ids = [ObjectId(oid) for oid in request.operator_ids]
    operators = await db.users.find({
        "_id": {"$in": operator_ids},
        "company_id": current_user["company_id"]
    }).to_list(100)
    
    if not operators:
        raise HTTPException(status_code=404, detail="No operators found")
    
    # Create ZIP file in memory
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        manifest_lines = ["FleetShield365 Document Export", f"Generated: {datetime.now(timezone.utc).isoformat()}", ""]
        
        for operator in operators:
            op_name = operator.get("name", "Unknown").replace("/", "_").replace("\\", "_")
            folder_name = re.sub(r'[^\w\s-]', '', op_name).strip().replace(' ', '_')
            manifest_lines.append(f"\n{op_name}:")
            
            # Document type mappings
            # 2026-05-21 — license field names were mis-mapped: stored as
            # `license_front_object_key` / `license_back_object_key` (no
            # `_photo_` infix), so the bulk ZIP never included license files.
            doc_mappings = {
                "driver_license": [
                    ("license_front", "driver_license_front.jpg"),
                    ("license_back", "driver_license_back.jpg")
                ],
                "medical": [
                    ("medical_cert_front", "medical_certificate_front.jpg"),
                    ("medical_cert_back", "medical_certificate_back.jpg")
                ],
                "first_aid": [
                    ("first_aid_front", "first_aid_front.jpg"),
                    ("first_aid_back", "first_aid_back.jpg")
                ],
                "forklift": [
                    ("forklift_front", "forklift_license_front.jpg"),
                    ("forklift_back", "forklift_license_back.jpg")
                ],
                "dangerous_goods": [
                    ("dangerous_goods_front", "dangerous_goods_front.jpg"),
                    ("dangerous_goods_back", "dangerous_goods_back.jpg")
                ],
                "msic": [
                    ("msic_front", "msic_front.jpg"),
                    ("msic_back", "msic_back.jpg")
                ],
                "other": [
                    ("other_doc_front", "other_document_front.jpg"),
                    ("other_doc_back", "other_document_back.jpg")
                ],
            }
            
            for doc_type in request.document_types:
                # 2026-05-19 — owner request: include the new
                # `custom_documents` (Additional Documents) in the
                # download ZIP. The client sends doc_type ==
                # "custom_documents" (or "additional"); each entry's
                # front/back object_key is fetched from MinIO and
                # bundled under a per-doc-label folder.
                if doc_type in ("custom_documents", "additional"):
                    for cd in (operator.get("custom_documents") or []):
                        if not isinstance(cd, dict):
                            continue
                        label = (cd.get("label") or "additional").strip()
                        safe_label = re.sub(r"[^A-Za-z0-9_-]+", "_", label)[:40] or "additional"
                        for side, ext in (("front", "bin"), ("back", "bin")):
                            key = cd.get(f"{side}_object_key")
                            if not key:
                                continue
                            try:
                                image_bytes = object_store.get_bytes("compliance", key)
                            except Exception as e:
                                logger.error(f"Failed to fetch {op_name}/{safe_label}-{side}: {e}")
                                manifest_lines.append(
                                    f"  - additional/{safe_label}_{side}.bin (ERROR: Could not retrieve)"
                                )
                                continue
                            # Sniff the file extension from magic bytes so the
                            # download has a sensible suffix in the ZIP.
                            ext_real = "bin"
                            if image_bytes[:3] == b"\xFF\xD8\xFF":
                                ext_real = "jpg"
                            elif image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
                                ext_real = "png"
                            elif image_bytes[:5] == b"%PDF-":
                                ext_real = "pdf"
                            zip_file.writestr(
                                f"{folder_name}/additional/{safe_label}_{side}.{ext_real}",
                                image_bytes,
                            )
                            manifest_lines.append(
                                f"  - additional/{safe_label}_{side}.{ext_real}"
                            )
                    continue
                if doc_type not in doc_mappings:
                    continue

                # Task 5.4: prefer the MinIO-backed object keys. Fetch each
                # file via object_store.get_bytes from the compliance bucket
                # and write to the ZIP. Fall back to the legacy inline
                # base64 field for pre-migration rows (Req 21.10, 21.11).
                for field_name, file_name in doc_mappings[doc_type]:
                    image_bytes: Optional[bytes] = None
                    key_field = f"{field_name}_object_key"
                    object_key = operator.get(key_field)
                    if object_key:
                        try:
                            image_bytes = object_store.get_bytes(
                                "compliance", object_key
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to fetch {op_name}/{file_name} from MinIO: {e}"
                            )
                            image_bytes = None

                    if image_bytes is None:
                        photo_data = operator.get(field_name)
                        if photo_data:
                            if photo_data.startswith("data:"):
                                base64_data = (
                                    photo_data.split(",", 1)[1]
                                    if "," in photo_data
                                    else photo_data
                                )
                            else:
                                base64_data = photo_data
                            try:
                                image_bytes = base64.b64decode(base64_data)
                            except Exception as e:
                                logger.error(
                                    f"Failed to decode image for {op_name}/{file_name}: {e}"
                                )
                                image_bytes = None

                    if image_bytes is not None:
                        # 2026-05-21 — drivers now upload PDFs (license,
                        # medical, etc.) per the image-or-pdf format
                        # widening. Sniff magic bytes so the file in
                        # the ZIP ends in .pdf when it's a PDF.
                        actual_name = file_name
                        if image_bytes[:5] == b"%PDF-":
                            actual_name = file_name.rsplit(".", 1)[0] + ".pdf"
                        elif image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
                            actual_name = file_name.rsplit(".", 1)[0] + ".png"
                        zip_file.writestr(
                            f"{folder_name}/{actual_name}", image_bytes
                        )
                        manifest_lines.append(f"  - {file_name}")
                    elif object_key or operator.get(field_name):
                        manifest_lines.append(
                            f"  - {file_name} (ERROR: Could not retrieve)"
                        )
        
        # Add manifest
        zip_file.writestr("manifest.txt", "\n".join(manifest_lines))
    
    zip_buffer.seek(0)
    
    # Generate filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"FleetShield_Documents_{timestamp}.zip"
    
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@api_router.post("/drivers/{driver_id}/license-photos")
async def upload_license_photos(driver_id: str, photos: LicensePhotoUpload, current_user: dict = Depends(get_current_user)):
    """Upload license photos for a driver - Owner (super_admin) only"""
    # Only super_admin can upload license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can upload license photos")
    
    # Verify driver exists and belongs to the same company
    company_id = current_user["company_id"]
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": company_id
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    # Task 5.3: license photos land in the compliance bucket under the
    # tenant+driver namespace; Mongo stores only the object keys
    # (Req 21.10, 21.11, 21.14). Field names follow design Section 4.18.
    update_data = {}
    if photos.front_photo_base64:
        front_key = (
            f"driver-docs/{company_id}/{driver_id}/license-front.jpg"
        )
        _upload_base64_or_400(
            "compliance",
            front_key,
            photos.front_photo_base64,
            "jpg",
            "front_photo_base64",
            expected_company_id=company_id,
            type_key="license",
        )
        update_data["license_front_object_key"] = front_key
    if photos.back_photo_base64:
        back_key = (
            f"driver-docs/{company_id}/{driver_id}/license-back.jpg"
        )
        _upload_base64_or_400(
            "compliance",
            back_key,
            photos.back_photo_base64,
            "jpg",
            "back_photo_base64",
            expected_company_id=company_id,
            type_key="license",
        )
        update_data["license_back_object_key"] = back_key
    
    if update_data:
        update_data["license_photos_updated_at"] = utcnow()
        update_data["license_photos_uploaded_by"] = str(current_user["_id"])
        await db.users.update_one(
            {"_id": ObjectId(driver_id)},
            {
                "$set": update_data,
                # Remove legacy inline base64 fields so the stored document
                # never retains the bytes alongside the new object keys.
                "$unset": {
                    "license_photo_front": "",
                    "license_photo_back": "",
                },
            }
        )
    
    return {"message": "License photos uploaded successfully", "updated_fields": list(update_data.keys())}

@api_router.post("/drivers/{driver_id}/license-photos/view")
async def view_license_photos(driver_id: str, verification: PasswordVerification, current_user: dict = Depends(get_current_user)):
    """View license photos with password re-authentication - Owner (super_admin) only"""
    # Only super_admin can view license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can view license photos")
    
    # Verify password. Return 400 (not 401) so the web axios interceptor
    # treats it as an inline form error instead of an expired session.
    if not verify_password(verification.password, current_user.get("password_hash") or current_user.get("hashed_password") or ""):
        raise HTTPException(status_code=400, detail="Incorrect password")

    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    return {
        "driver_id": driver_id,
        "driver_name": driver.get("name"),
        # Task 5.3: object keys are the canonical pointer; legacy inline
        # base64 is returned for backward compat with pre-migration rows.
        "license_front_object_key": driver.get("license_front_object_key"),
        "license_back_object_key": driver.get("license_back_object_key"),
        # Task 5.4: presigned URLs let the frontend render the license
        # photos directly from Nginx_Proxy without pulling bytes through
        # the API (Requirements 21.12, 21.13).
        "license_front_url": _presign_if_key(
            "compliance", driver.get("license_front_object_key")
        ),
        "license_back_url": _presign_if_key(
            "compliance", driver.get("license_back_object_key")
        ),
        "front_photo": driver.get("license_photo_front"),
        "back_photo": driver.get("license_photo_back"),
        "uploaded_at": driver.get("license_photos_updated_at")
    }

@api_router.delete("/drivers/{driver_id}/license-photos")
async def delete_license_photos(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Delete license photos for a driver - Owner (super_admin) only"""
    # Only super_admin can delete license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can delete license photos")
    
    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    await db.users.update_one(
        {"_id": ObjectId(driver_id)},
        {"$unset": {
            "license_photo_front": "",
            "license_photo_back": "",
            "license_front_object_key": "",
            "license_back_object_key": "",
            "license_photos_updated_at": "",
            "license_photos_uploaded_by": ""
        }}
    )
    
    return {"message": "License photos deleted successfully"}

@api_router.get("/drivers/{driver_id}/has-license-photos")
async def check_license_photos(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Check if driver has license photos - Owner (super_admin) only"""
    # Only super_admin can check license photos
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can access license photo information")
    
    # Verify driver exists and belongs to the same company
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    return {
        "has_front_photo": bool(
            driver.get("license_front_object_key")
            or driver.get("license_photo_front")
        ),
        "has_back_photo": bool(
            driver.get("license_back_object_key")
            or driver.get("license_photo_back")
        ),
        "uploaded_at": driver.get("license_photos_updated_at")
    }

# Generic document upload for all certificate types
class DocumentUpload(BaseModel):
    front_photo_base64: Optional[str] = None
    back_photo_base64: Optional[str] = None

@api_router.post("/drivers/{driver_id}/documents/{doc_type}")
async def upload_driver_documents(driver_id: str, doc_type: str, photos: DocumentUpload, current_user: dict = Depends(get_current_user)):
    """Upload documents for a driver - Owner (super_admin) only"""
    # Only super_admin can upload
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can upload documents")
    
    # Validate document type
    valid_doc_types = {
        "medical": ("medical_cert_front", "medical_cert_back", "medical-cert"),
        "first_aid": ("first_aid_front", "first_aid_back", "first-aid"),
        "forklift": ("forklift_front", "forklift_back", "forklift"),
        "dangerous_goods": ("dangerous_goods_front", "dangerous_goods_back", "dangerous-goods"),
        "msic": ("msic_front", "msic_back", "msic"),
        "other": ("other_doc_front", "other_doc_back", "other-doc"),
    }

    if doc_type not in valid_doc_types:
        raise HTTPException(status_code=400, detail=f"Invalid document type. Valid types: {list(valid_doc_types.keys())}")
    
    front_field, back_field, slug = valid_doc_types[doc_type]
    front_key_field = f"{front_field}_object_key"
    back_key_field = f"{back_field}_object_key"
    
    # Verify driver exists and belongs to the same company
    company_id = current_user["company_id"]
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": company_id
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    # Task 5.3: driver certificates go to the compliance bucket and Mongo
    # keeps only the tenant-scoped object keys (Req 21.10, 21.11, 21.14).
    update_data: dict = {}
    if photos.front_photo_base64:
        key = (
            f"driver-docs/{company_id}/{driver_id}/{slug}-front.jpg"
        )
        _upload_base64_or_400(
            "compliance",
            key,
            photos.front_photo_base64,
            "jpg",
            "front_photo_base64",
            expected_company_id=company_id,
            type_key="driver_doc",
        )
        update_data[front_key_field] = key
    if photos.back_photo_base64:
        key = (
            f"driver-docs/{company_id}/{driver_id}/{slug}-back.jpg"
        )
        _upload_base64_or_400(
            "compliance",
            key,
            photos.back_photo_base64,
            "jpg",
            "back_photo_base64",
            expected_company_id=company_id,
            type_key="driver_doc",
        )
        update_data[back_key_field] = key
    
    if update_data:
        update_data[f"{doc_type}_updated_at"] = utcnow()
        update_data[f"{doc_type}_uploaded_by"] = str(current_user["_id"])
        await db.users.update_one(
            {"_id": ObjectId(driver_id)},
            {
                "$set": update_data,
                # Drop the legacy inline-base64 fields so the stored doc
                # never carries the bytes alongside the new object keys.
                "$unset": {
                    front_field: "",
                    back_field: "",
                },
            }
        )
    
    return {"message": f"{doc_type.replace('_', ' ').title()} uploaded successfully", "updated_fields": list(update_data.keys())}

@api_router.get("/drivers/{driver_id}/documents/{doc_type}")
async def get_driver_documents(driver_id: str, doc_type: str, current_user: dict = Depends(get_current_user)):
    """Check if driver has documents uploaded - Owner (super_admin) only"""
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can access documents")
    
    valid_doc_types = {
        "medical": ("medical_cert_front", "medical_cert_back"),
        "first_aid": ("first_aid_front", "first_aid_back"),
        "forklift": ("forklift_front", "forklift_back"),
        "dangerous_goods": ("dangerous_goods_front", "dangerous_goods_back"),
        "msic": ("msic_front", "msic_back"),
        "other": ("other_doc_front", "other_doc_back"),
    }

    if doc_type not in valid_doc_types:
        raise HTTPException(status_code=400, detail=f"Invalid document type")

    front_field, back_field = valid_doc_types[doc_type]

    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    # Task 5.3: post-migration we check the new _object_key fields; legacy
    # inline-base64 fields are also consulted so pre-migration rows still
    # report has_front / has_back correctly.
    return {
        "has_front": bool(
            driver.get(f"{front_field}_object_key") or driver.get(front_field)
        ),
        "has_back": bool(
            driver.get(f"{back_field}_object_key") or driver.get(back_field)
        ),
        "uploaded_at": driver.get(f"{doc_type}_updated_at"),
        # For "other" the label is a user-supplied free-text string stored
        # alongside the photo keys; expose it so the UI can render the
        # right card heading.
        "label": driver.get("other_doc_label") if doc_type == "other" else None,
        "number": driver.get(f"{doc_type}_number"),
        "issue_date": driver.get(f"{doc_type}_issue_date"),
        "expiry_date": driver.get(f"{doc_type}_expiry_date"),
    }

@api_router.post("/drivers/{driver_id}/documents/{doc_type}/view")
async def view_driver_documents(driver_id: str, doc_type: str, verification: PasswordVerification, current_user: dict = Depends(get_current_user)):
    """View driver documents with password re-authentication - Owner only"""
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Company Owners can view documents")
    
    if not verify_password(verification.password, current_user.get("password_hash") or current_user.get("hashed_password") or ""):
        raise HTTPException(status_code=400, detail="Incorrect password")

    valid_doc_types = {
        "medical": ("medical_cert_front", "medical_cert_back"),
        "first_aid": ("first_aid_front", "first_aid_back"),
        "forklift": ("forklift_front", "forklift_back"),
        "dangerous_goods": ("dangerous_goods_front", "dangerous_goods_back"),
        "msic": ("msic_front", "msic_back"),
        "other": ("other_doc_front", "other_doc_back"),
    }
    
    if doc_type not in valid_doc_types:
        raise HTTPException(status_code=400, detail=f"Invalid document type")
    
    front_field, back_field = valid_doc_types[doc_type]
    
    driver = await db.users.find_one({
        "_id": ObjectId(driver_id),
        "company_id": current_user["company_id"]
    })
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    return {
        "driver_id": driver_id,
        "driver_name": driver.get("name"),
        "doc_type": doc_type,
        # Task 5.3: object keys are the canonical pointer; legacy inline
        # base64 is returned for backward compat with pre-migration rows.
        f"{front_field}_object_key": driver.get(f"{front_field}_object_key"),
        f"{back_field}_object_key": driver.get(f"{back_field}_object_key"),
        # Task 5.4: presigned URLs let the frontend render the document
        # directly via Nginx_Proxy (Requirements 21.12, 21.13). Driver
        # compliance documents live in the ``compliance`` bucket.
        f"{front_field}_url": _presign_if_key(
            "compliance", driver.get(f"{front_field}_object_key")
        ),
        f"{back_field}_url": _presign_if_key(
            "compliance", driver.get(f"{back_field}_object_key")
        ),
        "front_photo": driver.get(front_field),
        "back_photo": driver.get(back_field),
        "uploaded_at": driver.get(f"{doc_type}_updated_at")
    }

# ============== Photo Upload Routes (Upload during capture) ==============

class PhotoUploadRequest(BaseModel):
    base64_data: str
    photo_type: str
    vehicle_id: str
    timestamp: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None

@api_router.post("/photos/upload")
async def upload_photo(photo: PhotoUploadRequest, current_user: dict = Depends(get_current_user)):
    """
    Upload a single photo immediately after capture.
    Returns a photo_id that can be referenced in inspection submission.
    Photos are stored in temp_photos collection with 24h TTL.
    """
    try:
        photo_id = ObjectId()
        # Task 5.3: photo bytes live in MinIO, never in MongoDB (Req 21.10,
        # 21.11). The key is tenant-scoped per Req 21.14.
        company_id = current_user["company_id"]
        user_id = current_user.get("id") or str(current_user.get("_id"))
        object_key = f"{company_id}/{user_id}/{uuid.uuid4().hex}.jpg"
        _upload_base64_or_400(
            "photos", object_key, photo.base64_data, "jpg", "base64_data",
            expected_company_id=company_id,
            type_key="profile",
        )

        photo_doc = {
            "_id": photo_id,
            "company_id": company_id,
            "uploaded_by": user_id,
            "vehicle_id": photo.vehicle_id,
            "photo_type": photo.photo_type,
            "object_key": object_key,
            "timestamp": photo.timestamp or utcnow().isoformat(),
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
            "created_at": utcnow(),
            "expires_at": utcnow() + timedelta(hours=24),  # Auto-delete after 24h if not used
            "used": False  # Will be set to True when linked to an inspection
        }
        
        await db.temp_photos.insert_one(photo_doc)
        
        return {
            "success": True,
            "photo_id": str(photo_id),
            # Task 5.4: return the object key + presigned URL so the
            # mobile/web client can reference the uploaded photo without a
            # follow-up GET /api/photos/{photo_id} (Req 21.12, 21.13).
            "object_key": object_key,
            "object_url": _presign_if_key("photos", object_key),
            "message": "Photo uploaded successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Photo upload failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload photo")

@api_router.post("/photos/upload-multipart")
async def upload_photo_multipart(
    photo_type: str = Form(...),
    vehicle_id: Optional[str] = Form(None),
    timestamp: Optional[str] = Form(None),
    gps_latitude: Optional[float] = Form(None),
    gps_longitude: Optional[float] = Form(None),
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    """Multipart variant of /photos/upload (Phase 2 of STORAGE-PLAN.txt).

    Same behaviour as POST /photos/upload — writes to temp_photos with a
    24-hour TTL, returns a photo_id usable in subsequent inspection-
    submit calls. The only difference is the wire format: this endpoint
    accepts a real multipart/form-data file part instead of a base64
    string in JSON, saving ~33% bandwidth on the upload payload. The
    old base64 endpoint stays alive so older app builds keep working.
    """
    company_id = current_user["company_id"]
    user_id = current_user.get("id") or str(current_user.get("_id"))

    contents = await file.read()
    detected = _validate_upload_or_400(contents, "inspection", "file")
    content_type = _FORMAT_TO_CONTENT_TYPE[detected]
    ext = _FORMAT_TO_EXT[detected]

    photo_id = ObjectId()
    object_key = f"{company_id}/{user_id}/{uuid.uuid4().hex}.{ext}"
    try:
        _upload_with_thumbnail(
            "photos", object_key, contents, content_type,
            expected_company_id=company_id,
        )
    except object_store.TenantPrefixViolation as exc:
        raise HTTPException(status_code=403, detail=f"Forbidden Object_Key: {exc}")

    photo_doc = {
        "_id": photo_id,
        "company_id": company_id,
        "uploaded_by": user_id,
        "vehicle_id": vehicle_id,
        "photo_type": photo_type,
        "object_key": object_key,
        "timestamp": timestamp or utcnow().isoformat(),
        "gps_latitude": gps_latitude,
        "gps_longitude": gps_longitude,
        "created_at": utcnow(),
        "expires_at": utcnow() + timedelta(hours=24),
        "used": False,
    }
    await db.temp_photos.insert_one(photo_doc)

    return {
        "photo_id": str(photo_id),
        "object_key": object_key,
        "object_url": _presign_if_key("photos", object_key),
    }


@api_router.get("/photos/{photo_id}")
async def get_photo(photo_id: str, current_user: dict = Depends(get_current_user)):
    """Get a previously uploaded photo by ID"""
    try:
        photo = await db.temp_photos.find_one({
            "_id": ObjectId(photo_id),
            "company_id": current_user["company_id"]
        })
        
        if not photo:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        # Task 5.3: the stored document references MinIO via object_key.
        # Task 5.4: emit a sibling object_url so the frontend renders the
        # photo directly from Nginx_Proxy (Requirements 21.12, 21.13).
        return {
            "photo_id": str(photo["_id"]),
            "photo_type": photo["photo_type"],
            "object_key": photo.get("object_key"),
            "object_url": _presign_if_key(
                "photos", photo.get("object_key")
            ),
            "timestamp": photo.get("timestamp"),
            "gps_latitude": photo.get("gps_latitude"),
            "gps_longitude": photo.get("gps_longitude")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get photo failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to get photo")

# ============== Inspection Routes ==============

async def _vehicle_for_inspection(vehicle_id: str, current_user: dict) -> dict:
    """Tenant-scoped vehicle fetch with assignment enforcement.

    Owner request 2026-05-21: drivers must not be able to start an
    inspection on a vehicle they are not assigned to. The /vehicles list
    endpoint already filters by assigned_driver_ids for drivers, but the
    POST endpoints used to do a bare find_one with no company_id — a
    driver who knew (or guessed) a vehicle id could submit an inspection
    against any vehicle (including cross-tenant). This helper closes
    both holes.
    """
    company_id = current_user["company_id"]
    try:
        oid = ObjectId(vehicle_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    vehicle = await db.vehicles.find_one({"_id": oid, "company_id": company_id})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    if current_user.get("role") == UserRole.DRIVER:
        assigned = vehicle.get("assigned_driver_ids") or []
        if str(current_user["_id"]) not in assigned:
            raise HTTPException(
                status_code=403,
                detail="You are not assigned to this vehicle. Ask your admin to assign you before inspecting.",
            )
    return vehicle


@api_router.post("/inspections/prestart")
async def create_prestart(inspection: PrestartCreate, request: Request, current_user: dict = Depends(get_current_user)):
    # 2026-05-19 — idempotency. Queue retries from the mobile app resend
    # the same payload with the same dedupHash → idempotency_key. Return
    # the existing record instead of creating a duplicate.
    existing = await _check_idempotency(
        db.inspections, current_user["company_id"], inspection.idempotency_key,
    )
    if existing:
        return serialize_doc(existing)

    vehicle = await _vehicle_for_inspection(inspection.vehicle_id, current_user)
    
    # Check mandatory photos
    required_photos = {'front', 'rear', 'left', 'right', 'cabin', 'odometer'}
    provided_photos = {p.photo_type for p in inspection.photos}
    if not required_photos.issubset(provided_photos):
        missing = required_photos - provided_photos
        raise HTTPException(status_code=400, detail=f"Missing required photos: {missing}")
    
    # Check for issues requiring damage photos
    has_issues = any(item.status == ChecklistItemStatus.ISSUE for item in inspection.checklist_items)
    if has_issues and 'damage' not in provided_photos:
        raise HTTPException(status_code=400, detail="Damage photo required when issues are reported")
    
    inspection_id = ObjectId()
    
    # Store photos separately using bulk insert for better performance.
    # Task 5.3: each photo's base64 bytes go to MinIO under
    # inspection-photos/<company_id>/<inspection_id>/<uuid>.jpg. Mongo
    # holds only the tenant-scoped object_key (Req 21.10, 21.11, 21.14).
    company_id = current_user["company_id"]
    _enforce_count_or_413(
        inspection.photos, MAX_PHOTOS_PER_INSPECTION, "photos",
    )
    photo_refs = []
    photo_docs = []
    for photo in inspection.photos:
        photo_id = ObjectId()
        # Phase 2 of STORAGE-PLAN.txt — prefer the pre-uploaded
        # multipart path when the client supplied a photo_id. Falls
        # back to the legacy base64 path so old mobile builds continue
        # to work.
        photo_object_key, source_bucket = await _resolve_inspection_photo(
            photo, company_id, str(inspection_id),
            inspection_type_label="prestart",
        )
        photo_docs.append({
            "_id": photo_id,
            "inspection_id": str(inspection_id),
            "vehicle_id": inspection.vehicle_id,
            "photo_type": photo.photo_type,
            "object_key": photo_object_key,
            "source_bucket": source_bucket,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
            "ai_damage_status": photo.ai_damage_status,
            "inspection_type": InspectionType.PRESTART,
            "created_at": utcnow()
        })
        photo_refs.append({
            "photo_id": str(photo_id),
            "photo_type": photo.photo_type,
            "object_key": photo_object_key,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
        })
    
    # Bulk insert all photos at once (much faster than individual inserts)
    if photo_docs:
        await db.inspection_photos.insert_many(photo_docs)
    
    # Parse timestamp from mobile app (for offline submissions)
    if inspection.timestamp:
        try:
            inspection_timestamp = datetime.fromisoformat(inspection.timestamp.replace('Z', '+00:00'))
            # Convert to UTC if needed
            if inspection_timestamp.tzinfo:
                inspection_timestamp = inspection_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            inspection_timestamp = utcnow()
    else:
        inspection_timestamp = utcnow()
    
    # Task 5.3: signature bytes go to MinIO under
    # signatures/<company_id>/<inspection_id>.png. Mongo stores only
    # signature_object_key (Req 21.10, 21.11, 21.14).
    signature_object_key: Optional[str] = None
    if inspection.signature_base64:
        signature_object_key = f"{company_id}/{str(inspection_id)}.png"
        _upload_base64_or_400(
            "signatures",
            signature_object_key,
            inspection.signature_base64,
            "png",
            "signature_base64",
            expected_company_id=company_id,
            type_key="signature",
        )

    inspection_doc = {
        "_id": inspection_id,
        "vehicle_id": inspection.vehicle_id,
        "vehicle_name": vehicle.get("name"),
        "driver_id": str(current_user["_id"]),
        "driver_name": current_user.get("name") or current_user.get("username") or "Operator",
        "company_id": company_id,
        "type": InspectionType.PRESTART,
        "odometer": inspection.odometer,
        "checklist_items": [item.dict() for item in inspection.checklist_items],
        "photo_refs": photo_refs,  # References to inspection_photos docs (which point at MinIO)
        "signature_object_key": signature_object_key,
        # Store digital agreement if provided (new checkbox-based consent)
        "digital_agreement": inspection.digital_agreement.dict() if inspection.digital_agreement else None,
        "declaration_confirmed": inspection.declaration_confirmed,
        "gps_latitude": inspection.gps_latitude,
        "gps_longitude": inspection.gps_longitude,
        "location_address": inspection.location_address,
        "timestamp": inspection_timestamp,
        "ip_address": request.client.host if request.client else "unknown",
        "pdf_base64": None,
        "is_safe": not has_issues,
        "idempotency_key": (inspection.idempotency_key or None),
    }

    await db.inspections.insert_one(inspection_doc)
    
    # Update vehicle odometer
    await db.vehicles.update_one(
        {"_id": ObjectId(inspection.vehicle_id)},
        {"$set": {"current_odometer": inspection.odometer}}
    )
    
    # Generate PDF lazily on download. Phase 3.1 (2026-05-18 plan):
    # PDFs are no longer stored on the server — they're rebuilt and
    # streamed from /inspections/{id}/pdf on demand. No bytes leave
    # the request lifecycle, no MinIO objects accumulate.
    driver = await db.users.find_one({"_id": current_user["_id"]})

    # Create alert if vehicle marked unsafe
    if has_issues:
        issue_items = [item.name for item in inspection.checklist_items if item.status == ChecklistItemStatus.ISSUE]
        await create_alert(
            current_user["company_id"],
            "unsafe_vehicle",
            f"Vehicle {vehicle['name']} ({vehicle['registration_number']}) has issues: {', '.join(issue_items)}",
            inspection.vehicle_id,
            str(current_user["_id"])
        )
        
        # Fetch photos for the email alert (get damage photo + a few others)
        photos_for_email = []
        for photo in inspection.photos:
            photos_for_email.append({
                "photo_type": photo.photo_type,
                "base64_data": photo.base64_data
            })
        
        # Send notifications to admins WITH PHOTOS
        issue_comments = {item.name: item.comment for item in inspection.checklist_items if item.status == ChecklistItemStatus.ISSUE and item.comment}
        await notify_admins_with_photos(
            current_user["company_id"],
            vehicle['name'],
            current_user.get('name', current_user.get('full_name', 'Driver')),
            ', '.join(issue_items),
            "Pre-start",
            photos_for_email,
            str(inspection_id),
            {
                "odometer": inspection.odometer,
                "checklist_issues": issue_items,
                "checklist_comments": issue_comments,
                "total_items": len(inspection.checklist_items),
                "failed_items": len(issue_items)
            }
        )
    
    # Check for repeated issues (3+ in 7 days)
    seven_days_ago = utcnow() - timedelta(days=7)
    recent_inspections = await db.inspections.find({
        "vehicle_id": inspection.vehicle_id,
        "is_safe": False,
        "timestamp": {"$gte": seven_days_ago}
    }).sort("timestamp", -1).to_list(20)
    
    if len(recent_inspections) >= 3:
        await create_alert(
            current_user["company_id"],
            "repeated_issues",
            f"Vehicle {vehicle['name']} has had {len(recent_inspections)} issues in the last 7 days",
            inspection.vehicle_id
        )
        # Send detailed repeated issues email
        await send_repeated_issues_email(
            current_user["company_id"],
            vehicle['name'],
            recent_inspections
        )
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "inspection", str(inspection_doc["_id"]),
        request.client.host if request.client else "unknown"
    )

    # Per-activity opt-in email — defaults off so we don't spam admins.
    try:
        driver_display = current_user.get("name") or current_user.get("full_name") or "Driver"
        status_word = "with defects" if has_issues else "all clear"
        await send_activity_email(
            company_id,
            "prestart_email",
            f"[Pre-start] {vehicle.get('name', 'Vehicle')} — {status_word}",
            f"<p>{driver_display} completed a pre-start check for "
            f"<b>{vehicle.get('name', 'Vehicle')} ({vehicle.get('registration_number', 'N/A')})</b>.</p>"
            f"<p>Status: <b>{status_word}</b>. Odometer: {inspection.odometer}.</p>"
            f"<p><a href=\"https://www.fleetshield365.com/reports\">View in dashboard</a></p>",
        )
    except Exception:
        pass

    return serialize_doc(inspection_doc)

@api_router.post("/inspections/end-shift")
async def create_end_shift(inspection: EndShiftCreate, request: Request, current_user: dict = Depends(get_current_user)):
    # 2026-05-19 — see prestart for context.
    existing = await _check_idempotency(
        db.inspections, current_user["company_id"], inspection.idempotency_key,
    )
    if existing:
        return serialize_doc(existing)

    vehicle = await _vehicle_for_inspection(inspection.vehicle_id, current_user)

    # Validate damage/incident photos
    if inspection.new_damage and not any(p.photo_type == 'damage' for p in (inspection.photos or [])):
        raise HTTPException(status_code=400, detail="Damage photo required when new damage reported")
    
    inspection_id = ObjectId()
    
    # Store photos using bulk insert for better performance.
    # Task 5.3: each photo's base64 bytes go to MinIO under
    # inspection-photos/<company_id>/<inspection_id>/<uuid>.jpg. Mongo
    # holds only the tenant-scoped object_key (Req 21.10, 21.11, 21.14).
    company_id = current_user["company_id"]
    _enforce_count_or_413(
        inspection.photos, MAX_PHOTOS_PER_INSPECTION, "photos",
    )
    photo_refs = []
    photo_docs = []
    for photo in (inspection.photos or []):
        photo_id = ObjectId()
        # Phase 2 of STORAGE-PLAN.txt — dual-path (multipart photo_id
        # preferred, base64 legacy fallback). See create_prestart.
        photo_object_key, source_bucket = await _resolve_inspection_photo(
            photo, company_id, str(inspection_id),
            inspection_type_label="end_shift",
        )
        photo_docs.append({
            "_id": photo_id,
            "inspection_id": str(inspection_id),
            "vehicle_id": inspection.vehicle_id,
            "photo_type": photo.photo_type,
            "object_key": photo_object_key,
            "source_bucket": source_bucket,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
            "ai_damage_status": photo.ai_damage_status,
            "inspection_type": InspectionType.END_SHIFT,
            "created_at": utcnow()
        })
        photo_refs.append({
            "photo_id": str(photo_id),
            "photo_type": photo.photo_type,
            "object_key": photo_object_key,
            "timestamp": photo.timestamp,
            "gps_latitude": photo.gps_latitude,
            "gps_longitude": photo.gps_longitude,
        })
    
    # Bulk insert all photos at once
    if photo_docs:
        await db.inspection_photos.insert_many(photo_docs)
    
    # Parse timestamp from mobile app (for offline submissions)
    if inspection.timestamp:
        try:
            inspection_timestamp = datetime.fromisoformat(inspection.timestamp.replace('Z', '+00:00'))
            # Convert to UTC if needed
            if inspection_timestamp.tzinfo:
                inspection_timestamp = inspection_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            inspection_timestamp = utcnow()
    else:
        inspection_timestamp = utcnow()
    
    # Task 5.3: signature bytes go to MinIO under
    # signatures/<company_id>/<inspection_id>.png. Mongo stores only
    # signature_object_key (Req 21.10, 21.11, 21.14).
    signature_object_key: Optional[str] = None
    if inspection.signature_base64:
        signature_object_key = f"{company_id}/{str(inspection_id)}.png"
        _upload_base64_or_400(
            "signatures",
            signature_object_key,
            inspection.signature_base64,
            "png",
            "signature_base64",
            expected_company_id=company_id,
            type_key="signature",
        )

    inspection_doc = {
        "_id": inspection_id,
        "vehicle_id": inspection.vehicle_id,
        "vehicle_name": vehicle.get("name"),
        "driver_id": str(current_user["_id"]),
        "driver_name": current_user.get("name") or current_user.get("username") or "Operator",
        "company_id": company_id,
        "type": InspectionType.END_SHIFT,
        "odometer": inspection.odometer,
        "fuel_level": inspection.fuel_level,
        "new_damage": inspection.new_damage,
        "incident_today": inspection.incident_today,
        "cleanliness": inspection.cleanliness,
        "damage_comment": inspection.damage_comment,
        "incident_comment": inspection.incident_comment,
        "photo_refs": photo_refs,  # References to inspection_photos docs (which point at MinIO)
        "signature_object_key": signature_object_key,
        # Store digital agreement if provided (new checkbox-based consent)
        "digital_agreement": inspection.digital_agreement.dict() if inspection.digital_agreement else None,
        "declaration_confirmed": inspection.declaration_confirmed,
        "gps_latitude": inspection.gps_latitude,
        "gps_longitude": inspection.gps_longitude,
        "location_address": inspection.location_address,
        "timestamp": inspection_timestamp,
        "ip_address": request.client.host if request.client else "unknown",
        "pdf_base64": None,
        "is_safe": not (inspection.new_damage or inspection.incident_today),
        "idempotency_key": (inspection.idempotency_key or None),
    }

    await db.inspections.insert_one(inspection_doc)
    
    # Update vehicle odometer
    await db.vehicles.update_one(
        {"_id": ObjectId(inspection.vehicle_id)},
        {"$set": {"current_odometer": inspection.odometer}}
    )
    
    # Generate PDF lazily on download. Phase 3.1 (2026-05-18 plan):
    # PDFs are no longer persisted server-side — rebuilt + streamed
    # from /inspections/{id}/pdf on demand.
    driver = await db.users.find_one({"_id": current_user["_id"]})

    # Create alert if damage or incident
    if inspection.new_damage:
        await create_alert(
            current_user["company_id"],
            "unsafe_vehicle",
            f"New damage reported on {vehicle['name']}: {inspection.damage_comment or 'No details'}",
            inspection.vehicle_id,
            str(current_user["_id"])
        )
        
        # Send instant alert with photos
        photos_for_email = []
        for photo in (inspection.photos or []):
            photos_for_email.append({
                "photo_type": photo.photo_type,
                "base64_data": photo.base64_data
            })
        
        issue_summary = f"New damage: {inspection.damage_comment or 'See photos'}"
        if inspection.incident_today:
            issue_summary += f" | Incident: {inspection.incident_comment or 'See photos'}"
        
        await notify_admins_with_photos(
            current_user["company_id"],
            vehicle['name'],
            current_user.get('name', current_user.get('full_name', 'Driver')),
            issue_summary,
            "End-of-shift",
            photos_for_email,
            str(inspection_id),
            {
                "odometer": inspection.odometer,
                "fuel_level": inspection.fuel_level,
                "cleanliness": inspection.cleanliness,
                "incident_today": inspection.incident_today,
                "incident_comment": inspection.incident_comment
            }
        )
    elif inspection.incident_today:
        # Also alert for incidents without damage
        await create_alert(
            current_user["company_id"],
            "incident",
            f"Incident reported for {vehicle['name']}: {inspection.incident_comment or 'No details'}",
            inspection.vehicle_id,
            str(current_user["_id"])
        )
        
        photos_for_email = []
        for photo in (inspection.photos or []):
            photos_for_email.append({
                "photo_type": photo.photo_type,
                "base64_data": photo.base64_data
            })
        
        await notify_admins_with_photos(
            current_user["company_id"],
            vehicle['name'],
            current_user.get('name', current_user.get('full_name', 'Driver')),
            f"Incident reported: {inspection.incident_comment or 'See photos'}",
            "End-of-shift",
            photos_for_email,
            str(inspection_id)
        )

    await log_audit_trail(
        str(current_user["_id"]), "create", "inspection", str(inspection_doc["_id"]),
        request.client.host if request.client else "unknown"
    )

    try:
        driver_display = current_user.get("name") or current_user.get("full_name") or "Driver"
        flags = []
        if inspection.new_damage:    flags.append("new damage")
        if inspection.incident_today: flags.append("incident")
        status_word = ", ".join(flags) if flags else "no issues"
        await send_activity_email(
            current_user["company_id"],
            "endshift_email",
            f"[End-shift] {vehicle.get('name', 'Vehicle')} — {status_word}",
            f"<p>{driver_display} completed an end-of-shift report for "
            f"<b>{vehicle.get('name', 'Vehicle')} ({vehicle.get('registration_number', 'N/A')})</b>.</p>"
            f"<p>Status: <b>{status_word}</b>. Odometer: {inspection.odometer}.</p>"
            f"<p><a href=\"https://www.fleetshield365.com/reports\">View in dashboard</a></p>",
        )
    except Exception:
        pass

    return serialize_doc(inspection_doc)

@api_router.get("/inspections")
async def get_inspections(
    vehicle_id: Optional[str] = None,
    driver_id: Optional[str] = None,
    inspection_type: Optional[str] = None,
    has_issues: Optional[bool] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_photos: Optional[bool] = False,
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    query = {"company_id": current_user["company_id"]}
    
    # Drivers only see their own inspections
    if current_user["role"] == UserRole.DRIVER:
        query["driver_id"] = str(current_user["_id"])
    elif driver_id:
        query["driver_id"] = driver_id
    
    if vehicle_id:
        query["vehicle_id"] = vehicle_id
    if inspection_type:
        query["type"] = inspection_type
    if has_issues is not None:
        if has_issues:
            # Show inspections with issues: is_safe=False OR new_damage=True OR incident_today=True
            query["$or"] = [
                {"is_safe": False},
                {"new_damage": True},
                {"incident_today": True}
            ]
        else:
            # Show safe inspections only
            query["is_safe"] = True
            query["new_damage"] = {"$ne": True}
            query["incident_today"] = {"$ne": True}
    
    # Use Sydney timezone for date filtering (same as dashboard)
    if start_date:
        start_utc = get_sydney_date_as_utc(start_date, is_end_of_day=False)
        query["timestamp"] = {"$gte": start_utc}
    if end_date:
        end_utc = get_sydney_date_as_utc(end_date, is_end_of_day=True)
        if "timestamp" in query:
            query["timestamp"]["$lte"] = end_utc
        else:
            query["timestamp"] = {"$lte": end_utc}
    
    # Cap at 500 for performance
    actual_limit = min(limit, 500)
    
    # Exclude large base64 data from list query for performance
    projection = {"signature_base64": 0, "photos": 0, "pdf_base64": 0, "photo_refs": 0}
    inspections = await db.inspections.find(query, projection).sort("timestamp", -1).to_list(actual_limit)
    
    # Batch fetch driver and vehicle names for all inspections
    if inspections:
        driver_ids = list(set(i.get("driver_id") for i in inspections if i.get("driver_id")))
        vehicle_ids = list(set(i.get("vehicle_id") for i in inspections if i.get("vehicle_id")))
        
        drivers_task = db.users.find({"_id": {"$in": [ObjectId(did) for did in driver_ids if did]}}).to_list(500)
        vehicles_task = db.vehicles.find({"_id": {"$in": [ObjectId(vid) for vid in vehicle_ids if vid]}}).to_list(500)
        
        drivers, vehicles = await asyncio.gather(drivers_task, vehicles_task)
        
        driver_map = {str(d["_id"]): d for d in drivers}
        vehicle_map = {str(v["_id"]): v for v in vehicles}
        
        # Enrich inspections with driver and vehicle info
        for inspection in inspections:
            driver = driver_map.get(inspection.get("driver_id"))
            vehicle = vehicle_map.get(inspection.get("vehicle_id"))
            d_name = driver.get("name", driver.get("full_name", "Unknown")) if driver else "Unknown"
            d_user = driver.get("username", "") if driver else ""
            inspection["driver_name"] = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
            v_name = vehicle.get("name", "Unknown") if vehicle else "Unknown"
            v_rego = vehicle.get("registration_number", "") if vehicle else ""
            inspection["vehicle_name"] = f"{v_name} ({v_rego})" if v_rego else v_name
            inspection["vehicle_rego"] = v_rego or "N/A"
    
    # Phase 2: every inspection in the list response gets a pdf_url when
    # the PDF lives in MinIO. Cheap (just signs the URL — no body fetch).
    for inspection in inspections:
        if inspection.get("pdf_object_key"):
            inspection["pdf_url"] = _presign_if_key(
                "inspection-photos", inspection["pdf_object_key"]
            )

    # Optionally include photos (only when viewing single inspection detail)
    if include_photos:
        for inspection in inspections:
            photos = await fetch_inspection_photos(str(inspection["_id"]))
            inspection["photos"] = photos
            # Task 5.4: expose signature_url when photos are requested so
            # the inspection detail view can render the driver signature
            # without a separate round-trip (Req 21.12, 21.13).
            inspection["signature_url"] = _presign_if_key(
                "signatures", inspection.get("signature_object_key")
            )

    return serialize_doc(inspections)

async def fetch_inspection_photos(inspection_id: str) -> List[dict]:
    """Fetch photos for an inspection from the separate collection"""
    # First try to find photos by report_id
    photos = await db.inspection_photos.find({"report_id": inspection_id}).to_list(20)
    
    if not photos:
        # If no photos found by report_id, check if inspection has photo_refs
        inspection = await db.inspections.find_one({"_id": ObjectId(inspection_id)})
        if inspection and inspection.get("photo_refs"):
            photo_ids = [ref.get("photo_id") for ref in inspection["photo_refs"] if ref.get("photo_id")]
            if photo_ids:
                # Fetch photos by their IDs
                photos = await db.inspection_photos.find({
                    "_id": {"$in": [ObjectId(pid) for pid in photo_ids]}
                }).to_list(20)
    
    # Phase 2 of STORAGE-PLAN.txt — photos can live in either bucket:
    #   * ``inspection-photos`` (legacy + base64-path uploads)
    #   * ``photos`` (multipart pre-uploads, referenced by photo_id)
    # The handler persists ``source_bucket`` on the inspection_photos
    # doc so we can sign the right URL on read. Defaults to
    # ``inspection-photos`` for pre-source_bucket docs (every row
    # written before this change).
    return [
        {
            "photo_type": p.get("photo_type"),
            "object_key": p.get("object_key"),
            # Phase 2 — photos uploaded via multipart live in the "photos"
            # bucket; legacy base64 uploads live in "inspection-photos".
            # The PDF generator needs this hint to fetch bytes from the
            # right bucket — without it, multipart photos 404 and never
            # render in the PDF.
            "source_bucket": p.get("source_bucket") or "inspection-photos",
            "object_url": _presign_if_key(
                p.get("source_bucket") or "inspection-photos",
                p.get("object_key"),
            ),
            "base64_data": p.get("base64_data"),
        }
        for p in photos
    ]

@api_router.get("/inspections/{inspection_id}")
async def get_inspection(inspection_id: str, current_user: dict = Depends(get_current_user)):
    inspection = await db.inspections.find_one({
        "_id": ObjectId(inspection_id),
        "company_id": current_user["company_id"]
    })
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    
    # Fetch photos from separate collection
    photos = await fetch_inspection_photos(inspection_id)
    inspection["photos"] = photos

    # Task 5.4: expose a presigned signature URL alongside the stored
    # signature_object_key so the frontend can render the driver's
    # signature image without a second round-trip (Req 21.12, 21.13).
    inspection["signature_url"] = _presign_if_key(
        "signatures", inspection.get("signature_object_key")
    )

    # Phase 2: emit pdf_url for inspections whose PDF is in MinIO.
    # Legacy rows still carrying pdf_base64 are unaffected — their
    # bytes are returned via the dedicated /pdf endpoint.
    if inspection.get("pdf_object_key"):
        inspection["pdf_url"] = _presign_if_key(
            "inspection-photos", inspection["pdf_object_key"]
        )

    # Add driver and vehicle info
    if inspection.get("driver_id"):
        driver = await db.users.find_one({"_id": ObjectId(inspection["driver_id"])})
        d_name = driver.get("name", driver.get("full_name", "Unknown")) if driver else "Unknown"
        d_user = driver.get("username", "") if driver else ""
        inspection["driver_name"] = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
    else:
        inspection["driver_name"] = "Unknown"
    
    if inspection.get("vehicle_id"):
        vehicle = await db.vehicles.find_one({"_id": ObjectId(inspection["vehicle_id"])})
        v_name = vehicle.get("name", "Unknown") if vehicle else "Unknown"
        v_rego = vehicle.get("registration_number", "") if vehicle else ""
        inspection["vehicle_name"] = f"{v_name} ({v_rego})" if v_rego else v_name
        inspection["vehicle_rego"] = v_rego or "N/A"
    else:
        inspection["vehicle_name"] = "Unknown"
        inspection["vehicle_rego"] = "N/A"
    
    return serialize_doc(inspection)

@api_router.get("/inspections/{inspection_id}/pdf")
async def get_inspection_pdf(
    inspection_id: str,
    regenerate: bool = False,  # legacy param — generation is now always ephemeral
    current_user: dict = Depends(get_current_user),
):
    """Stream the inspection PDF on demand. Phase 3.1 (Plan 2026-05-18):

    Every PDF is now generated in-memory and streamed to the caller.
    Nothing is persisted to MinIO and ``pdf_base64`` is no longer
    written. Legacy rows that still carry ``pdf_base64`` are returned
    in the old JSON shape for backwards compatibility with the mobile
    app's existing download path. Legacy ``pdf_object_key`` rows pull
    the bytes from MinIO and stream them through; the persisted object
    is left in place for the migration script to clean up.

    ``regenerate`` is accepted (and ignored) so any clients passing it
    don't 422; behaviour is identical with or without it.
    """
    company_id = current_user["company_id"]
    inspection = await db.inspections.find_one({
        "_id": ObjectId(inspection_id),
        "company_id": company_id,
    })
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    has_minio_pdf = bool(inspection.get("pdf_object_key"))
    has_legacy_b64 = bool(inspection.get("pdf_base64"))

    # Helper to wrap raw bytes in a streaming response that triggers a
    # browser download. Filename is anchored on the inspection ID so the
    # caller doesn't get an opaque "report.pdf" in their downloads list.
    def _stream(pdf_bytes: bytes):
        from fastapi.responses import StreamingResponse
        from io import BytesIO

        filename = f"inspection_{inspection_id}.pdf"
        return StreamingResponse(
            BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    # Legacy base64 row — keep the old JSON-with-pdf_base64 shape so the
    # mobile app's existing handler doesn't have to change. The web
    # admin handler (ReportsPage) checks pdf_base64 first too.
    if has_legacy_b64 and not regenerate:
        return {"pdf_base64": inspection["pdf_base64"]}

    # Legacy MinIO row — pull the persisted bytes once and stream them.
    if has_minio_pdf and not regenerate:
        try:
            stored = object_store.get_bytes(
                "inspection-photos", inspection["pdf_object_key"]
            )
            if stored:
                return _stream(stored)
        except Exception as exc:
            logger.warning(
                f"Stored PDF fetch failed for inspection {inspection_id}: {exc}; "
                f"regenerating ephemerally"
            )

    # Default + regenerate path — build fresh in memory, stream out,
    # never touch storage. The DB document keeps no PDF artifact.
    photos = await fetch_inspection_photos(inspection_id)
    inspection["photos"] = photos
    vehicle = await db.vehicles.find_one({"_id": ObjectId(inspection["vehicle_id"])})
    driver = await db.users.find_one({"_id": ObjectId(inspection["driver_id"])})
    company = await db.companies.find_one({"_id": ObjectId(inspection["company_id"])})

    pdf_bytes = await generate_inspection_pdf_bytes(inspection, vehicle, driver, company)
    return _stream(pdf_bytes)

# ============== Fuel Submission Routes ==============

@api_router.post("/fuel")
async def create_fuel_submission(fuel: FuelSubmission, request: Request, current_user: dict = Depends(get_current_user)):
    """Driver submits fuel receipt"""
    # 2026-05-19 — idempotency. See PrestartCreate for context.
    existing = await _check_idempotency(
        db.fuel_submissions, current_user["company_id"], fuel.idempotency_key,
    )
    if existing:
        return serialize_doc(existing)

    vehicle = await db.vehicles.find_one({"_id": ObjectId(fuel.vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    # Parse timestamp from mobile app (for offline submissions)
    if fuel.timestamp:
        try:
            fuel_timestamp = datetime.fromisoformat(fuel.timestamp.replace('Z', '+00:00'))
            if fuel_timestamp.tzinfo:
                fuel_timestamp = fuel_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            fuel_timestamp = utcnow()
    else:
        fuel_timestamp = utcnow()
    
    # Task 5.3: upload the receipt bytes to MinIO and persist only the key
    # on the fuel submission document (Req 21.10, 21.11, 21.14).
    fuel_id = ObjectId()
    company_id = current_user["company_id"]
    receipt_object_key: Optional[str] = None
    if fuel.receipt_photo_base64:
        receipt_object_key = f"{company_id}/{str(fuel_id)}.jpg"
        _upload_base64_or_400(
            "fuel-receipts",
            receipt_object_key,
            fuel.receipt_photo_base64,
            "jpg",
            "receipt_photo_base64",
            expected_company_id=company_id,
            type_key="fuel",
        )

    fuel_doc = {
        "_id": fuel_id,
        "company_id": company_id,
        "vehicle_id": fuel.vehicle_id,
        "driver_id": str(current_user["_id"]),
        "amount": fuel.amount,
        "liters": fuel.liters,
        "price_per_liter": round(fuel.amount / fuel.liters, 2) if fuel.liters > 0 else 0,
        "receipt_object_key": receipt_object_key,
        "odometer": fuel.odometer,
        "fuel_station": fuel.fuel_station,
        "notes": fuel.notes,
        "gps_latitude": fuel.gps_latitude,
        "gps_longitude": fuel.gps_longitude,
        "location_address": fuel.location_address,
        "timestamp": fuel_timestamp,
        "ip_address": request.client.host if request.client else "unknown",
        "idempotency_key": (fuel.idempotency_key or None),
    }

    await db.fuel_submissions.insert_one(fuel_doc)

    try:
        driver_display = current_user.get("name") or current_user.get("full_name") or "Driver"
        await send_activity_email(
            company_id,
            "fuel_email",
            f"[Fuel] {vehicle.get('name', 'Vehicle')} — ${fuel.amount:.2f}",
            f"<p>{driver_display} logged a fuel entry for "
            f"<b>{vehicle.get('name', 'Vehicle')} ({vehicle.get('registration_number', 'N/A')})</b>.</p>"
            f"<p>Amount: <b>${fuel.amount:.2f}</b> · Litres: <b>{fuel.liters:.2f}</b> · "
            f"Station: {fuel.fuel_station or 'N/A'}.</p>"
            f"<p><a href=\"https://www.fleetshield365.com/fuel-logs\">View in dashboard</a></p>",
        )
    except Exception:
        pass

    return {"id": str(fuel_doc["_id"]), "message": "Fuel submission recorded successfully"}

@api_router.get("/fuel")
async def get_fuel_submissions(
    vehicle_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """Get fuel submissions for company (paginated, Phase 2)."""
    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)
    query = {"company_id": current_user["company_id"]}
    if vehicle_id:
        query["vehicle_id"] = vehicle_id

    pipeline = [
        {"$match": query},
        {"$sort": {"timestamp": -1}},
        {"$skip": actual_offset},
        {"$limit": actual_limit},
        {"$addFields": {"has_receipt": {"$cond": [
            {"$or": [
                {"$ifNull": ["$receipt_object_key", False]},
                {"$ifNull": ["$receipt_photo_base64", False]},
            ]},
            True,
            False,
        ]}}},
        {"$project": {"receipt_photo_base64": 0}}
    ]
    submissions = await db.fuel_submissions.aggregate(pipeline).to_list(actual_limit)
    
    # Get vehicle names
    vehicle_ids = list(set(s["vehicle_id"] for s in submissions))
    vehicles = await db.vehicles.find({"_id": {"$in": [ObjectId(vid) for vid in vehicle_ids]}}).to_list(100)
    vehicle_map = {str(v["_id"]): f"{v['name']} ({v.get('registration_number', '')})" if v.get('registration_number') else v['name'] for v in vehicles}
    
    # Get driver names
    driver_ids = list(set(s["driver_id"] for s in submissions))
    drivers = await db.users.find({"_id": {"$in": [ObjectId(did) for did in driver_ids]}}).to_list(100)
    driver_map = {str(d["_id"]): d for d in drivers}
    
    for s in submissions:
        s["id"] = str(s.pop("_id"))
        s["vehicle_name"] = vehicle_map.get(s["vehicle_id"], "Unknown")
        d = driver_map.get(s["driver_id"])
        if d:
            d_name = d.get("name", "Unknown")
            d_user = d.get("username", "")
            s["driver_name"] = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
        else:
            s["driver_name"] = "Unknown"
        s["has_receipt"] = s.get("has_receipt", False)
        # Task 5.4: surface a presigned receipt URL alongside
        # receipt_object_key so the admin web UI can render the thumbnail
        # inline without a follow-up GET (Requirements 21.12, 21.13).
        s["receipt_url"] = _presign_if_key(
            "fuel-receipts", s.get("receipt_object_key")
        )
    
    return submissions

@api_router.get("/fuel/{fuel_id}/receipt")
async def get_fuel_receipt(fuel_id: str, current_user: dict = Depends(get_current_user)):
    """Get receipt photo for a specific fuel submission"""
    submission = await db.fuel_submissions.find_one(
        {"_id": ObjectId(fuel_id), "company_id": current_user["company_id"]},
        {"receipt_photo_base64": 1, "receipt_object_key": 1}
    )
    if not submission:
        raise HTTPException(status_code=404, detail="Fuel submission not found")
    
    # Task 5.3: new rows carry receipt_object_key; we expose it here so the
    # frontend can render via the presigned-URL path added in Task 5.4.
    # Pre-migration rows may still carry legacy receipt_photo_base64.
    object_key = submission.get("receipt_object_key")
    receipt = submission.get("receipt_photo_base64")
    if not object_key and not receipt:
        raise HTTPException(status_code=404, detail="No receipt photo for this submission")
    
    return {
        "receipt_object_key": object_key,
        # Task 5.4: presigned receipt URL so the frontend can render the
        # receipt image directly from Nginx_Proxy without pulling bytes
        # through the API (Requirements 21.12, 21.13).
        "receipt_url": _presign_if_key("fuel-receipts", object_key),
        "receipt_photo_base64": receipt,
    }

@api_router.get("/fuel/receipts/download")
async def export_fuel_receipts_zip(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """Owner review 2026-05-18: stream all fuel receipt photos as a ZIP
    with the same filters as the CSV export. Photos pulled from the
    fuel-receipts MinIO bucket (or legacy inline base64 fallback).
    In-memory ZIP, no /tmp file."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    from starlette.responses import StreamingResponse

    company_id = current_user["company_id"]
    query: dict = {"company_id": company_id}
    if date_from or date_to:
        if not (date_from and date_to):
            raise HTTPException(
                status_code=400,
                detail="Specify both date_from and date_to, or neither",
            )
        # `timestamp` on fuel_submissions is stored as a Python datetime
        # (see create handler). String comparison against ISO yyyy-mm-dd
        # never matches a Mongo Date — 2026-05-20 bug. Parse the bounds
        # into datetimes so BSON does a proper date-to-date comparison.
        try:
            ts_from = datetime.fromisoformat(date_from)
            ts_to = datetime.fromisoformat(date_to + "T23:59:59")
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be ISO yyyy-mm-dd")
        query["timestamp"] = {"$gte": ts_from, "$lte": ts_to}
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id

    # Vehicle name map for folder naming inside the ZIP.
    vehicles = await db.vehicles.find(
        {"company_id": company_id},
        {"name": 1, "registration_number": 1},
    ).to_list(2000)
    vehicle_map = {
        str(v["_id"]): (v.get("registration_number") or v.get("name") or str(v["_id"]))
        for v in vehicles
    }

    # Cap at 2000 receipts per export to bound memory.
    submissions = await db.fuel_submissions.find(
        query, {"vehicle_id": 1, "timestamp": 1, "receipt_object_key": 1,
                "receipt_photo_base64": 1, "fuel_station": 1}
    ).sort("timestamp", -1).to_list(2000)

    if not submissions:
        raise HTTPException(status_code=404, detail="No fuel receipts match the selected filters")

    zip_buffer = BytesIO()
    manifest_lines = [
        "FleetShield365 Fuel Receipt Export",
        f"Generated: {utcnow().isoformat()}",
        f"Filters: vehicles={vid_list or [vehicle_id] if vehicle_id else 'all'} dates={date_from}..{date_to}",
        f"Receipts: {len(submissions)}",
        "",
    ]
    written = 0
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for sub in submissions:
            vname = vehicle_map.get(sub.get("vehicle_id", ""), "unknown")
            ts = str(sub.get("timestamp", ""))[:19].replace(":", "-").replace("T", "_")
            station = (sub.get("fuel_station") or "").replace("/", "_").replace("\\", "_")[:40]
            filename = f"{vname}/{ts}{('_' + station) if station else ''}.jpg"
            image_bytes: Optional[bytes] = None
            key = sub.get("receipt_object_key")
            if key:
                try:
                    image_bytes = object_store.get_bytes("fuel-receipts", key)
                except Exception as exc:
                    logger.warning(f"Receipt {key} unreadable: {exc}")
            if image_bytes is None and sub.get("receipt_photo_base64"):
                raw = sub["receipt_photo_base64"]
                if raw.startswith("data:"):
                    raw = raw.split(",", 1)[1] if "," in raw else raw
                try:
                    image_bytes = base64.b64decode(raw)
                except Exception:
                    image_bytes = None
            if image_bytes is None:
                manifest_lines.append(f"- SKIPPED {filename} (no readable receipt)")
                continue
            zf.writestr(filename, image_bytes)
            manifest_lines.append(f"- {filename}")
            written += 1
        zf.writestr("manifest.txt", "\n".join(manifest_lines))

    zip_buffer.seek(0)
    out_name = f"fuel_receipts_{utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={out_name}"},
    )


@api_router.get("/fuel/export/csv")
async def export_fuel_csv(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,  # CSV "id1,id2,..." for multi-vehicle filter
    current_user: dict = Depends(get_current_user)
):
    """Export fuel logs to CSV, streamed row-by-row from a Mongo cursor.

    Phase 3.2 (2026-05-18 plan):
    * date_from / date_to are optional. When both are omitted the
      export covers all time. The 365-day cap only applies when a
      bounded range is supplied — "all time" intentionally bypasses
      it because the stream is memory-bounded already.
    * vehicle_ids (comma-separated) lets the caller pick several
      vehicles at once. Single vehicle_id stays supported for older
      clients. Neither set = all vehicles.

    Nothing is persisted server-side: rows stream straight to the
    client from a Mongo cursor.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    import csv
    from io import StringIO
    from starlette.responses import StreamingResponse
    from datetime import datetime as dt

    df = None
    dtt = None
    if date_from or date_to:
        if not (date_from and date_to):
            raise HTTPException(
                status_code=400,
                detail="Specify both date_from and date_to, or neither (for all time)",
            )
        try:
            df = dt.fromisoformat(date_from)
            dtt = dt.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="date_from and date_to must be YYYY-MM-DD",
            )
        if dtt < df:
            raise HTTPException(
                status_code=400,
                detail="date_to must be >= date_from",
            )
        if (dtt - df).days > 365:
            raise HTTPException(
                status_code=400,
                detail="Export window is capped at 365 days; please narrow the range or omit dates for an all-time export",
            )

    company_id = current_user["company_id"]
    query: dict = {"company_id": company_id}
    if df is not None and dtt is not None:
        # fuel_submissions.timestamp is stored as a Python datetime (see
        # create handler), so string $gte/$lte never matched and the CSV
        # came down with header-only when a date range was given. Parse
        # the inputs to datetime bounds — same pattern as
        # /incidents/export/csv at ~10647.
        try:
            ts_from = datetime.fromisoformat(date_from)
            ts_to = datetime.fromisoformat(date_to + "T23:59:59")
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="date_from/date_to must be ISO yyyy-mm-dd",
            )
        query["timestamp"] = {"$gte": ts_from, "$lte": ts_to}

    # vehicle_ids takes precedence over vehicle_id (multi-select case).
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id

    # Lookup maps — small (vehicles, drivers) so keep in memory once.
    vehicles = await db.vehicles.find(
        {"company_id": company_id},
        {"name": 1, "registration_number": 1},
    ).to_list(2000)
    vehicle_map = {
        str(v["_id"]): f"{v.get('name', 'Unknown')} ({v.get('registration_number', 'N/A')})"
        for v in vehicles
    }
    drivers = await db.users.find(
        {"company_id": company_id},
        {"name": 1, "username": 1},
    ).to_list(2000)
    driver_map: dict = {}
    for d in drivers:
        name = d.get("name", "Unknown")
        user = d.get("username", "")
        driver_map[str(d["_id"])] = f"{name} ({user})" if user and user != name else name

    HEADER = [
        "Date", "Time", "Driver", "Vehicle", "Notes", "Litres",
        "Cost ($)", "Price/L ($)", "Odometer (km)", "Station", "Has Receipt",
    ]

    def _format_row(s: dict) -> list:
        ts = s.get("timestamp", "")
        if ts:
            try:
                parsed = dt.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else ts
                date_str = parsed.strftime("%Y-%m-%d")
                time_str = parsed.strftime("%H:%M")
            except Exception:
                date_str = str(ts)[:10]
                time_str = str(ts)[11:16]
        else:
            date_str = ""
            time_str = ""

        return [
            date_str,
            time_str,
            driver_map.get(s.get("driver_id", ""), "Unknown"),
            vehicle_map.get(s.get("vehicle_id", ""), "Unknown"),
            s.get("notes", ""),
            s.get("liters", s.get("litres", "")),
            s.get("amount", s.get("total_cost", "")),
            s.get("price_per_liter", ""),
            s.get("odometer", ""),
            s.get("fuel_station", ""),
            "Yes" if (
                s.get("receipt_object_key") is not None
                or s.get("receipt_photo_base64") is not None
                or s.get("has_receipt")
            ) else "No",
        ]

    async def _row_iter():
        """Yield CSV chunks as the Mongo cursor produces rows.

        We re-use a single StringIO per chunk so the csv writer can
        format quoting/escaping properly. After each row we hand the
        buffer's contents to the response and reset.
        """
        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow(HEADER)
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        cursor = db.fuel_submissions.find(
            query, {"receipt_photo_base64": 0},
        ).sort("timestamp", -1)
        async for s in cursor:
            writer.writerow(_format_row(s))
            chunk = buf.getvalue()
            if chunk:
                yield chunk
                buf.seek(0)
                buf.truncate(0)

    filename = (
        f"fuel_logs_from_{date_from}_to_{date_to}"
        f"_{utcnow().strftime('%Y%m%d')}.csv"
    )
    return StreamingResponse(
        _row_iter(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ============== Driver Update Routes ==============

class AdminResetPasswordRequest(BaseModel):
    new_password: str

@api_router.post("/drivers/{driver_id}/reset-password")
async def admin_reset_driver_password(driver_id: str, request: AdminResetPasswordRequest, current_user: dict = Depends(get_current_user)):
    """Admin can reset a driver's sign-in PIN.

    Drivers authenticate on the mobile app with a 4-digit PIN, not a
    password, so the admin reset modal now collects a PIN (the field
    name `new_password` is preserved for backward compat with older
    clients but the value MUST be 4 digits).
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    validate_driver_pin(request.new_password)

    driver = await db.users.find_one({"_id": ObjectId(driver_id), "company_id": current_user["company_id"]})
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    hashed = get_password_hash(request.new_password.strip())

    await db.users.update_one(
        {"_id": ObjectId(driver_id)},
        {"$set": {
            "password_hash": hashed,
            "auth_mode": "pin",
        }}
    )

    return {"message": f"PIN reset successfully for {driver.get('name', 'driver')}"}

@api_router.put("/drivers/{driver_id}")
async def update_driver(driver_id: str, update: DriverUpdate, request: Request, current_user: dict = Depends(get_current_user)):
    """Update driver details including license and training"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    driver = await db.users.find_one({"_id": ObjectId(driver_id), "company_id": current_user["company_id"]})
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    # Phone must be 10 digits when provided (same rule as create).
    if update.phone is not None:
        phone_digits = "".join(ch for ch in update.phone if ch.isdigit())
        if update.phone.strip() and len(phone_digits) != 10:
            raise HTTPException(
                status_code=400,
                detail="Driver phone must be exactly 10 digits",
            )

    update_dict = update.dict()

    # 2026-05-19 — handle PIN reset (separate from password). When the
    # admin enters a new 4-digit PIN we rehash and flip auth_mode to
    # "pin". We never write the raw PIN to the doc.
    new_pin = (update_dict.pop("pin", None) or "").strip()
    if new_pin:
        validate_driver_pin(new_pin)
        update_dict["password_hash"] = get_password_hash(new_pin)
        update_dict["auth_mode"] = "pin"

    # 2026-05-19 — custom_documents is a full-replace list. Persist any
    # new front/back uploads to MinIO and store the resulting object
    # keys; preserved entries pass through unchanged.
    if update_dict.get("custom_documents") is not None:
        update_dict["custom_documents"] = _persist_custom_documents(
            update_dict["custom_documents"],
            current_user["company_id"],
            driver_id,
        )

    update_data = {k: v for k, v in update_dict.items() if v is not None}
    if update_data:
        await db.users.update_one({"_id": ObjectId(driver_id)}, {"$set": update_data})

        # Check for expiring documents in background (don't block response)
        asyncio.create_task(check_driver_expiry_alerts(driver_id, current_user["company_id"]))

    return {"message": "Driver updated successfully"}

@api_router.post("/drivers/{driver_id}/send-credentials")
async def send_driver_credentials(driver_id: str, current_user: dict = Depends(get_current_user)):
    """Send login credentials to driver via email"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    driver = await db.users.find_one({"_id": ObjectId(driver_id), "company_id": current_user["company_id"]})
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
    company_name = company.get("name", "FleetShield365") if company else "FleetShield365"
    
    driver_email = driver.get("email")
    driver_name = driver.get("name", "Operator")
    
    if not driver_email:
        raise HTTPException(status_code=400, detail="Driver has no email address")
    
    # Create welcome email with login instructions
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px; background-color: #f8fafc;">
        <div style="max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px;">
            <h2 style="color: #0f172a; margin-bottom: 20px;">Welcome to FleetShield365!</h2>
            <p style="color: #475569;">Hi {driver_name},</p>
            <p style="color: #475569;">You've been added as an operator for <strong>{company_name}</strong>. You can now access the FleetShield365 mobile app to complete equipment inspections.</p>
            
            <div style="background-color: #f1f5f9; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <h3 style="color: #0f172a; margin-top: 0;">Your Login Details:</h3>
                <p style="color: #475569; margin: 5px 0;"><strong>Email:</strong> {driver_email}</p>
                <p style="color: #475569; margin: 5px 0;"><strong>Password:</strong> (set by your admin)</p>
            </div>
            
            <p style="color: #475569;">If you don't know your password, please contact your administrator.</p>
            
            <div style="text-align: center; margin: 30px 0;">
                <a href="https://fleetshield365.com" style="background-color: #0d9488; color: white; padding: 12px 30px; text-decoration: none; border-radius: 8px; font-weight: bold;">Open FleetShield365</a>
            </div>
            
            <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
            <p style="color: #94a3b8; font-size: 12px;">FleetShield365 - Equipment Inspection Management</p>
        </div>
    </body>
    </html>
    """
    
    success = await send_email_notification(
        driver_email,
        f"[FleetShield365] Your Login Credentials for {company_name}",
        html_content
    )

    if not success:
        # SMTP returned False — either the mailbox isn't configured yet
        # (dev / pre-rollout) or the upstream MTA refused the recipient
        # (invalid email, mailbox full, MX issues). Either way this is
        # a data / config problem the admin can act on, not a backend
        # crash — surface a 400 with the offending address so the UI
        # toast reads like "Could not email Sarah Wilson at
        # sarah@…: please check the address".
        raise HTTPException(
            status_code=400,
            detail=(
                f"Could not deliver email to {driver_email}. "
                "Check that the address is valid and reachable, or use Copy to share the PIN another way."
            ),
        )

    # Mark the invitation as sent. The web Drivers panel reads this to
    # show "Invitation sent on …" badges instead of the user wondering
    # whether they already invited the driver. invite_status flips to
    # 'accepted' the first time the driver successfully logs in (set
    # in the login endpoint, see below).
    now_ts = utcnow()
    await db.users.update_one(
        {"_id": ObjectId(driver_id)},
        {"$set": {
            "invite_sent_at": now_ts,
            "invite_status": "invited",
            "last_invite_email": driver_email,
        }},
    )

    return {"message": f"Credentials sent to {driver_email}", "invite_sent_at": now_ts.isoformat()}

async def check_driver_expiry_alerts(driver_id: str, company_id: str):
    """Check driver document expiry dates and create alerts at 60, 30, 14, 7 day intervals"""
    driver = await db.users.find_one({"_id": ObjectId(driver_id)})
    if not driver:
        return
    
    driver_name = driver.get("name", "Unknown Driver")
    driver_username = driver.get("username", "")
    display_name = f"{driver_name} ({driver_username})" if driver_username and driver_username != driver_name else driver_name
    now = utcnow()
    
    # Reminder intervals: 60, 30, 14, 7 days
    REMINDER_DAYS = [60, 30, 14, 7]
    
    expiry_fields = [
        ('license_expiry', 'Driver License'),
        ('medical_certificate_expiry', 'Medical Certificate'),
        ('first_aid_expiry', 'First Aid Certificate'),
        ('forklift_license_expiry', 'Forklift License'),
        ('dangerous_goods_expiry', 'Dangerous Goods Training'),
    ]
    
    for field, label in expiry_fields:
        expiry_str = driver.get(field)
        if expiry_str and expiry_str.upper() != "NA":
            try:
                # Use flexible date parser (handles both DD/MM/YYYY and YYYY-MM-DD)
                expiry_date = parse_date_flexible(expiry_str)
                if not expiry_date:
                    continue
                    
                days_until = (expiry_date - now).days
                display_date = format_date_display(expiry_str)
                
                # Already expired
                if days_until < 0:
                    existing = await db.alerts.find_one({
                        "driver_id": driver_id,
                        "type": "driver_expiry_critical",
                        "message": {"$regex": f"{label}.*EXPIRED"}
                    })
                    if not existing:
                        message = f"{label} for {display_name} has EXPIRED (was due {display_date})"
                        await create_alert(company_id, "driver_expiry_critical", message, driver_id=driver_id, reminder_window="expired")
                        await send_driver_expiry_email(company_id, display_name, label, days_until, display_date, expired=True)

                # Check each reminder interval
                else:
                    for reminder_day in REMINDER_DAYS:
                        if days_until <= reminder_day:
                            # Determine severity based on days remaining
                            if days_until <= 7:
                                alert_type = "driver_expiry_critical"
                                urgency = "CRITICAL"
                            elif days_until <= 14:
                                alert_type = "driver_expiry_warning"
                                urgency = "URGENT"
                            elif days_until <= 30:
                                alert_type = "driver_expiry_warning"
                                urgency = "ACTION NEEDED"
                            else:  # 60 days
                                alert_type = "driver_expiry_warning"
                                urgency = "HEADS UP"

                            # Check if alert already exists for this specific reminder
                            existing = await db.alerts.find_one({
                                "driver_id": driver_id,
                                "type": alert_type,
                                "message": {"$regex": f"{label}.*{reminder_day}"}
                            })

                            if not existing:
                                message = f"[{urgency}] {label} for {display_name} expires in {days_until} days ({display_date})"
                                await create_alert(company_id, alert_type, message, driver_id=driver_id, reminder_window=str(reminder_day))
                                await send_driver_expiry_email(company_id, display_name, label, days_until, display_date)

                            break  # Only create alert for the most urgent matching interval
                            
            except Exception:
                pass

async def send_driver_expiry_email(company_id: str, driver_name: str, document_type: str, days_until: int, expiry_date: str, expired: bool = False):
    """Send email notification about driver document expiry"""
    admins = await db.users.find({
        "company_id": company_id,
        "role": {"$in": [UserRole.SUPER_ADMIN, UserRole.ADMIN]}, "deleted_at": None
    }).to_list(100)
    
    for admin in admins:
        if expired:
            subject = f"URGENT: {driver_name}'s {document_type} has EXPIRED"
            body = f"URGENT: {driver_name}'s {document_type} expired on {expiry_date}.\n\nPlease ensure this is updated immediately to maintain compliance."
        else:
            subject = f"Reminder: {driver_name}'s {document_type} expires in {days_until} days"
            body = f"{driver_name}'s {document_type} will expire on {expiry_date} ({days_until} days remaining).\n\nPlease arrange renewal before expiry."
        
        await send_email_notification(admin.get("email"), subject, body)

# ============== Maintenance Routes ==============

@api_router.post("/maintenance")
async def create_maintenance(log: MaintenanceLogCreate, request: Request, background_tasks: BackgroundTasks, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Task 5.3: invoice bytes go to MinIO; Mongo stores only
    # invoice_object_key under maintenance/<company_id>/<log_id>/invoice.pdf
    # (Req 21.10, 21.11, 21.14).
    log_id = ObjectId()
    company_id = current_user["company_id"]
    invoice_object_key: Optional[str] = None
    if log.invoice_base64:
        invoice_object_key = f"{company_id}/{str(log_id)}/invoice.pdf"
        _upload_base64_or_400(
            "maintenance",
            invoice_object_key,
            log.invoice_base64,
            "pdf",
            "invoice_base64",
            expected_company_id=company_id,
            type_key="maintenance",
            background_tasks=background_tasks,
        )

    maintenance_doc = {
        "_id": log_id,
        "company_id": company_id,
        "vehicle_id": log.vehicle_id,
        "service_date": log.service_date,
        "service_type": log.service_type,
        "cost": log.cost,
        "workshop_name": log.workshop_name,
        "invoice_object_key": invoice_object_key,
        "notes": log.notes,
        "created_by": str(current_user["_id"]),
        "created_at": utcnow()
    }
    await db.maintenance_logs.insert_one(maintenance_doc)
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "maintenance", str(maintenance_doc["_id"]),
        request.client.host if request.client else "unknown"
    )
    
    return serialize_doc(maintenance_doc)

@api_router.get("/maintenance")
async def get_maintenance_logs(
    vehicle_id: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """Paginated maintenance log list (Phase 2 of STORAGE-PLAN.txt)."""
    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)
    # Phase 4 — exclude soft-deleted rows by default.
    query = {**_soft_delete_filter(), "company_id": current_user["company_id"]}
    if vehicle_id:
        query["vehicle_id"] = vehicle_id

    logs = await db.maintenance_logs.find(query).sort("service_date", -1) \
        .skip(actual_offset).limit(actual_limit).to_list(actual_limit)
    # Task 5.4: expose invoice_url alongside invoice_object_key so the
    # admin UI can download/preview the PDF directly from Nginx_Proxy
    # (Requirements 21.12, 21.13).
    serialized = serialize_doc(logs)
    if isinstance(serialized, list):
        for log in serialized:
            if isinstance(log, dict):
                log["invoice_url"] = _presign_if_key(
                    "maintenance", log.get("invoice_object_key")
                )
    return serialized

@api_router.get("/maintenance/stats/{vehicle_id}")
async def get_maintenance_stats(vehicle_id: str, current_user: dict = Depends(get_current_user)):
    logs = await db.maintenance_logs.find({
        "company_id": current_user["company_id"],
        "vehicle_id": vehicle_id
    }).to_list(1000)
    
    total_cost = sum(log.get("cost", 0) for log in logs)
    service_count = len(logs)
    
    # Task 5.4: enrich each log with invoice_url so any UI reusing this
    # stats endpoint can render the PDF without a separate fetch
    # (Requirements 21.12, 21.13).
    serialized_logs = serialize_doc(logs)
    if isinstance(serialized_logs, list):
        for log in serialized_logs:
            if isinstance(log, dict):
                log["invoice_url"] = _presign_if_key(
                    "maintenance", log.get("invoice_object_key")
                )

    return {
        "vehicle_id": vehicle_id,
        "total_cost": total_cost,
        "service_count": service_count,
        "logs": serialized_logs
    }

# ============== Service Records Routes ==============

@api_router.post("/service-records")
async def create_service_record(record: ServiceRecordCreate, request: Request, background_tasks: BackgroundTasks, current_user: dict = Depends(get_current_user)):
    """Create a new service record for a vehicle"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    company_id = current_user["company_id"]
    
    # Verify vehicle exists and belongs to company
    vehicle = await db.vehicles.find_one({
        "_id": ObjectId(record.vehicle_id),
        "company_id": company_id
    })
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    # Task 5.3: attachments arrive as base64 strings; upload each to MinIO
    # under service-records/<company_id>/<record_id>/<uuid>.pdf and store
    # only the tenant-scoped object keys (Req 21.10, 21.11, 21.14).
    record_id = ObjectId()
    _enforce_count_or_413(
        record.attachments, MAX_SERVICE_ATTACHMENTS, "attachments",
    )
    attachment_keys: List[str] = []
    for idx, b64 in enumerate(record.attachments or []):
        if not b64:
            continue
        key = (
            f"{company_id}/{str(record_id)}/{uuid.uuid4().hex}.pdf"
        )
        _upload_base64_or_400(
            "service-records",
            key,
            b64,
            "pdf",
            f"attachments[{idx}]",
            expected_company_id=company_id,
            type_key="service",
            background_tasks=background_tasks,
        )
        attachment_keys.append(key)

    # Create service record
    service_doc = {
        "_id": record_id,
        "company_id": company_id,
        "vehicle_id": record.vehicle_id,
        "service_date": record.service_date,
        "service_type": record.service_type.value,
        "service_type_other": record.service_type_other if record.service_type == ServiceType.OTHER else None,
        "description": record.description,
        "cost": record.cost,
        "odometer_reading": record.odometer_reading,
        "technician_name": record.technician_name,
        "workshop_name": record.workshop_name,
        "next_service_date": record.next_service_date,
        "next_service_odometer": record.next_service_odometer,
        "attachments": attachment_keys,
        "warranty_until": record.warranty_until,
        "warranty_notes": record.warranty_notes,
        "created_by": str(current_user["_id"]),
        "created_at": utcnow(),
        "updated_at": utcnow()
    }
    
    await db.service_records.insert_one(service_doc)
    
    # If odometer reading provided, update vehicle's current odometer
    if record.odometer_reading and record.odometer_reading > (vehicle.get("current_odometer") or 0):
        await db.vehicles.update_one(
            {"_id": ObjectId(record.vehicle_id)},
            {"$set": {"current_odometer": record.odometer_reading}}
        )
    
    # Invalidate cache
    invalidate_cache("service_records", company_id)
    
    await log_audit_trail(
        str(current_user["_id"]), "create", "service_record", str(service_doc["_id"]),
        request.client.host if request.client else "unknown"
    )
    
    return serialize_doc(service_doc)

@api_router.get("/service-records")
async def get_service_records(
    vehicle_id: Optional[str] = None,
    service_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    skip: int = 0,
    current_user: dict = Depends(get_current_user)
):
    """Get all service records with optional filtering"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    company_id = current_user["company_id"]
    # Phase 4 — exclude soft-deleted rows by default.
    query = {**_soft_delete_filter(), "company_id": company_id}

    if vehicle_id:
        query["vehicle_id"] = vehicle_id

    if service_type:
        query["service_type"] = service_type

    if search:
        query["$or"] = [
            {"description": {"$regex": search, "$options": "i"}},
            {"technician_name": {"$regex": search, "$options": "i"}},
            {"workshop_name": {"$regex": search, "$options": "i"}},
            {"service_type_other": {"$regex": search, "$options": "i"}}
        ]
    
    # Get total count for pagination
    total = await db.service_records.count_documents(query)
    
    # Get records sorted by service date (newest first)
    records = await db.service_records.find(query).sort("service_date", -1).skip(skip).limit(limit).to_list(limit)
    
    # Task 5.4: enrich each record with a parallel attachment_urls list so
    # the frontend can render or download each attachment directly through
    # Nginx_Proxy (Requirements 21.12, 21.13). Per-record attachments is a
    # List[str] of object keys; attachment_urls preserves positional
    # alignment one-for-one.
    serialized = serialize_doc(records)
    if isinstance(serialized, list):
        for record in serialized:
            if isinstance(record, dict):
                record["attachment_urls"] = _presign_keys(
                    "service-records", record.get("attachments")
                )

    return {
        "data": serialized,
        "total": total,
        "limit": limit,
        "skip": skip
    }

@api_router.get("/service-records/summary")
async def get_service_records_summary(current_user: dict = Depends(get_current_user)):
    """Get summary statistics for service records"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    company_id = current_user["company_id"]
    
    # Get all records for this company
    records = await db.service_records.find({"company_id": company_id}).to_list(10000)
    
    total_cost = sum(r.get("cost", 0) or 0 for r in records)
    total_records = len(records)
    
    # Count by service type
    by_type = {}
    for r in records:
        st = r.get("service_type", "unknown")
        by_type[st] = by_type.get(st, 0) + 1
    
    # Get records from this month
    now = utcnow()
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    this_month_records = [r for r in records if r.get("created_at") and r["created_at"] >= this_month_start]
    this_month_cost = sum(r.get("cost", 0) or 0 for r in this_month_records)
    
    return {
        "total_records": total_records,
        "total_cost": total_cost,
        "this_month_records": len(this_month_records),
        "this_month_cost": this_month_cost,
        "by_type": by_type
    }

@api_router.get("/service-records/export/csv")
async def export_service_records_csv(
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,  # CSV multi-vehicle filter
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """Export service records to CSV. Phase 6 (2026-05-18): adds
    vehicle multi-select + date range filters."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    import csv
    from io import StringIO
    from starlette.responses import StreamingResponse

    company_id = current_user["company_id"]
    query: dict = {**_soft_delete_filter(), "company_id": company_id}

    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id
    if date_from or date_to:
        date_filter: dict = {}
        if date_from:
            date_filter["$gte"] = date_from
        if date_to:
            date_filter["$lte"] = date_to + "T23:59:59"
        query["service_date"] = date_filter

    records = await db.service_records.find(query).sort("service_date", -1).to_list(10000)
    
    # Get vehicles for names
    vehicles = await db.vehicles.find({"company_id": company_id}).to_list(1000)
    vehicle_map = {str(v["_id"]): f"{v.get('name', 'Unknown')} ({v.get('registration_number', 'N/A')})" for v in vehicles}
    
    # Create CSV
    output = StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow([
        "Date", "Equipment", "Service Type", "Description", "Cost ($)",
        "Odometer", "Technician", "Workshop", "Next Service Date", "Next Service KM", "Warranty Until", "Warranty Notes"
    ])
    
    # Data rows
    for r in records:
        service_type = r.get("service_type", "").title()
        if r.get("service_type_other"):
            service_type = f"Other: {r.get('service_type_other')}"
        
        writer.writerow([
            r.get("service_date", ""),
            vehicle_map.get(r.get("vehicle_id", ""), "Unknown"),
            service_type,
            r.get("description", ""),
            r.get("cost", ""),
            r.get("odometer_reading", ""),
            r.get("technician_name", ""),
            r.get("workshop_name", ""),
            r.get("next_service_date", ""),
            r.get("next_service_odometer", ""),
            r.get("warranty_until", ""),
            r.get("warranty_notes", "")
        ])
    
    output.seek(0)
    
    filename = f"service_records_{utcnow().strftime('%Y%m%d')}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@api_router.get("/service-records/attachments/download")
async def export_service_record_attachments_zip(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """Owner request 2026-05-20 — mirror the fuel receipts ZIP for
    service-record attachments (workshop invoices, photos, certs). In-memory
    ZIP, no /tmp file. Cap at 2000 attachments per export to bound memory.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    from starlette.responses import StreamingResponse

    company_id = current_user["company_id"]
    query: dict = {**_soft_delete_filter(), "company_id": company_id}
    if date_from or date_to:
        if not (date_from and date_to):
            raise HTTPException(status_code=400, detail="Specify both date_from and date_to, or neither")
        query["service_date"] = {"$gte": date_from, "$lte": date_to}
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id

    vehicles = await db.vehicles.find(
        {"company_id": company_id},
        {"name": 1, "registration_number": 1},
    ).to_list(2000)
    vehicle_map = {
        str(v["_id"]): (v.get("registration_number") or v.get("name") or str(v["_id"]))
        for v in vehicles
    }

    records = await db.service_records.find(
        query,
        {"vehicle_id": 1, "service_date": 1, "service_type": 1,
         "attachments": 1, "attachment_object_keys": 1},
    ).sort("service_date", -1).to_list(2000)

    if not records:
        raise HTTPException(status_code=404, detail="No service records match the selected filters")

    zip_buffer = BytesIO()
    manifest_lines = [
        "FleetShield365 Service-Record Attachments Export",
        f"Generated: {utcnow().isoformat()}",
        f"Filters: vehicles={vid_list or [vehicle_id] if vehicle_id else 'all'} dates={date_from}..{date_to}",
        f"Service records: {len(records)}",
        "",
    ]
    written = 0
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for rec in records:
            vname = vehicle_map.get(str(rec.get("vehicle_id", "")), "unknown")
            svc_date = str(rec.get("service_date", ""))[:10]
            svc_type = (rec.get("service_type") or "service").replace("/", "_")
            folder = f"{vname}/{svc_date}_{svc_type}"
            # 2026-05-21 — the create handler stores MinIO object keys
            # under the field name `attachments` (the legacy name; despite
            # the storage-breakdown helper calling it `attachment_object_keys`).
            # Read both for safety, deduplicating by string identity.
            object_keys: list = []
            for source_field in ("attachments", "attachment_object_keys"):
                for k in (rec.get(source_field) or []):
                    if isinstance(k, str) and k and "/" in k and k not in object_keys:
                        object_keys.append(k)
            for idx, key in enumerate(object_keys, start=1):
                try:
                    raw = object_store.get_bytes("service-records", key)
                except Exception as exc:
                    logger.warning(f"Service attachment {key} unreadable: {exc}")
                    manifest_lines.append(f"- SKIPPED {folder}/attachment-{idx} (read error)")
                    continue
                ext = ".pdf" if raw[:4] == b"%PDF" else ".jpg"
                fname = f"{folder}/attachment-{idx}{ext}"
                zf.writestr(fname, raw)
                manifest_lines.append(f"- {fname}")
                written += 1
            # Legacy: some very old rows may store inline base64 strings
            # in `attachments`. Detect by content (data: prefix OR not
            # a valid object key path) and decode if present.
            for idx, payload in enumerate(rec.get("attachments") or [], start=1):
                if not isinstance(payload, str) or "/" in payload:
                    continue  # already handled above (was an object key)
                raw_str = payload
                if raw_str.startswith("data:"):
                    raw_str = raw_str.split(",", 1)[1] if "," in raw_str else raw_str
                try:
                    raw_bytes = base64.b64decode(raw_str)
                except Exception:
                    continue
                ext = ".pdf" if raw_bytes[:4] == b"%PDF" else ".jpg"
                fname = f"{folder}/legacy-attachment-{idx}{ext}"
                zf.writestr(fname, raw_bytes)
                manifest_lines.append(f"- {fname}")
                written += 1
        zf.writestr("manifest.txt", "\n".join(manifest_lines))

    zip_buffer.seek(0)
    out_name = f"service_attachments_{utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={out_name}"},
    )


@api_router.get("/service-records/{record_id}/pdf")
async def get_service_record_pdf(
    record_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Stream a single service record as a one-page PDF. Phase 6
    (2026-05-18). Generated in-memory; no MinIO write, no /tmp file."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    from starlette.responses import StreamingResponse

    company_id = current_user["company_id"]
    record = await db.service_records.find_one({
        "_id": ObjectId(record_id),
        "company_id": company_id,
    })
    if not record:
        raise HTTPException(status_code=404, detail="Service record not found")

    vehicle = await db.vehicles.find_one({"_id": ObjectId(record.get("vehicle_id", ""))}) if record.get("vehicle_id") else None
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = (company or {}).get("name", "FleetShield365")
    company_tz = (company or {}).get("timezone", DEFAULT_TIMEZONE)
    tz_display = company_tz.split('/')[-1].replace('_', ' ')
    vehicle_name = (vehicle or {}).get("name", "Unknown")
    vehicle_rego = (vehicle or {}).get("registration_number", "N/A")

    service_type = (record.get("service_type") or "").title()
    if record.get("service_type_other"):
        service_type = f"Other: {record['service_type_other']}"

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=30, bottomMargin=30, leftMargin=40, rightMargin=40)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#1e3a5f'), spaceAfter=20)
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, textColor=colors.gray)
    elements: list = []

    elements.extend(_pdf_company_header(company, styles, "Service Record"))

    data = [
        ["Record ID:", str(record.get("_id", ""))[:8] + "..."],
        ["Service date:", record.get("service_date", "")],
        ["Vehicle:", f"{vehicle_name} ({vehicle_rego})"],
        ["Service type:", service_type],
        ["Cost:", f"${record.get('cost', 0):,.2f}" if record.get('cost') is not None else "—"],
        ["Odometer:", str(record.get("odometer_reading", "—"))],
        ["Technician:", record.get("technician_name", "—")],
        ["Workshop:", record.get("workshop_name", "—")],
        ["Next service date:", record.get("next_service_date", "—")],
        ["Next service km:", str(record.get("next_service_odometer", "—"))],
        ["Warranty until:", record.get("warranty_until", "—")],
    ]
    table = Table(data, colWidths=[140, 350])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 14))

    if record.get("description"):
        elements.append(Paragraph("<b>Description:</b>", styles['Normal']))
        elements.append(Paragraph(record.get("description", ""), styles['Normal']))
        elements.append(Spacer(1, 10))

    if record.get("warranty_notes"):
        elements.append(Paragraph("<b>Warranty notes:</b>", styles['Normal']))
        elements.append(Paragraph(record["warranty_notes"], styles['Normal']))
        elements.append(Spacer(1, 10))

    attachments = record.get("attachments") or record.get("attachment_object_keys") or []
    if attachments:
        elements.append(Paragraph(
            f"<b>Attachments:</b> {len(attachments)} file(s) — view in admin panel",
            styles['Normal']))

    elements.append(Spacer(1, 20))
    elements.append(Paragraph(
        f"Generated: {datetime.now(get_timezone(company_tz)).strftime('%d/%m/%Y %H:%M')} ({tz_display})",
        footer_style))

    doc.build(elements)
    buffer.seek(0)
    filename = f"service_record_{record_id}.pdf"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@api_router.get("/service-records/{record_id}")
async def get_service_record(record_id: str, current_user: dict = Depends(get_current_user)):
    """Get a single service record by ID"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    record = await db.service_records.find_one({
        "_id": ObjectId(record_id),
        "company_id": current_user["company_id"]
    })

    if not record:
        raise HTTPException(status_code=404, detail="Service record not found")
    
    # Task 5.4: expose attachment_urls parallel to attachments so the
    # frontend can render PDF/image attachments via Nginx_Proxy without a
    # separate fetch (Requirements 21.12, 21.13).
    result = serialize_doc(record)
    if isinstance(result, dict):
        result["attachment_urls"] = _presign_keys(
            "service-records", result.get("attachments")
        )
    return result

@api_router.put("/service-records/{record_id}")
async def update_service_record(
    record_id: str,
    update: ServiceRecordUpdate,
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Update a service record"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    company_id = current_user["company_id"]
    
    # Check record exists
    existing = await db.service_records.find_one({
        "_id": ObjectId(record_id),
        "company_id": company_id
    })
    
    if not existing:
        raise HTTPException(status_code=404, detail="Service record not found")
    
    # Build update data
    update_data = {}
    for key, value in update.dict().items():
        if value is not None:
            if key == "service_type":
                update_data[key] = value.value
            elif key == "attachments":
                # Task 5.3: incoming attachments are base64 strings; upload
                # each to MinIO and replace with tenant-scoped object keys
                # (Req 21.10, 21.11, 21.14). The caller is expected to send
                # the full desired list on update, same as the pre-MinIO
                # behaviour of replacing the array.
                _enforce_count_or_413(
                    value, MAX_SERVICE_ATTACHMENTS, "attachments",
                )
                attachment_keys: List[str] = []
                for idx, b64 in enumerate(value or []):
                    if not b64:
                        continue
                    # If caller resent an already-migrated key (looks like a
                    # path, not base64), pass it through unchanged.
                    if isinstance(b64, str) and b64.startswith(
                        f"{company_id}/{record_id}/"
                    ):
                        attachment_keys.append(b64)
                        continue
                    key_path = (
                        f"{company_id}/{record_id}/"
                        f"{uuid.uuid4().hex}.pdf"
                    )
                    _upload_base64_or_400(
                        "service-records",
                        key_path,
                        b64,
                        "pdf",
                        f"attachments[{idx}]",
                        expected_company_id=company_id,
                        type_key="service",
                        background_tasks=background_tasks,
                    )
                    attachment_keys.append(key_path)
                update_data[key] = attachment_keys
            else:
                update_data[key] = value
    
    if update_data:
        update_data["updated_at"] = utcnow()
        await db.service_records.update_one(
            {"_id": ObjectId(record_id)},
            {"$set": update_data}
        )
    
    # Invalidate cache
    invalidate_cache("service_records", company_id)
    
    await log_audit_trail(
        str(current_user["_id"]), "update", "service_record", record_id,
        request.client.host if request.client else "unknown", update_data
    )
    
    updated_record = await db.service_records.find_one({"_id": ObjectId(record_id)})
    # Task 5.4: enrich the updated response with attachment_urls so the UI
    # can immediately render any newly uploaded attachments
    # (Requirements 21.12, 21.13).
    result = serialize_doc(updated_record)
    if isinstance(result, dict):
        result["attachment_urls"] = _presign_keys(
            "service-records", result.get("attachments")
        )
    return result

@api_router.delete("/service-records/{record_id}")
async def delete_service_record(record_id: str, request: Request, current_user: dict = Depends(get_current_user)):
    """Soft-delete a service record (Phase 4)."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]

    record = await db.service_records.find_one({
        **_soft_delete_filter(),
        "_id": ObjectId(record_id),
        "company_id": company_id,
    })
    if not record:
        raise HTTPException(status_code=404, detail="Service record not found")

    await db.service_records.update_one(
        {"_id": ObjectId(record_id)},
        _soft_delete_update(current_user.get("_id")),
    )

    # Invalidate cache
    invalidate_cache("service_records", company_id)

    await log_audit_trail(
        str(current_user["_id"]), "delete", "service_record", record_id,
        request.client.host if request.client else "unknown"
    )

    return {"message": "Service record deleted successfully"}

@api_router.get("/service-records/{record_id}/pdf")
async def get_service_record_pdf(record_id: str, current_user: dict = Depends(get_current_user)):
    """Generate and return PDF for a service record"""
    company_id = current_user["company_id"]
    
    record = await db.service_records.find_one({
        "_id": ObjectId(record_id),
        "company_id": company_id
    })
    
    if not record:
        raise HTTPException(status_code=404, detail="Service record not found")
    
    # Get vehicle info
    vehicle = await db.vehicles.find_one({"_id": ObjectId(record["vehicle_id"])})
    vehicle_name = vehicle.get("name", "Unknown") if vehicle else "Unknown"
    vehicle_rego = vehicle.get("registration_number", "N/A") if vehicle else "N/A"
    
    # Get company info
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = company.get("name", "FleetShield365") if company else "FleetShield365"
    company_tz = company.get("timezone", DEFAULT_TIMEZONE) if company else DEFAULT_TIMEZONE
    tz_display = company_tz.split('/')[-1].replace('_', ' ')
    
    # Generate PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=30, bottomMargin=30, leftMargin=40, rightMargin=40)
    styles = getSampleStyleSheet()
    elements = []
    
    # Uniform branded header — replaces the bare title-only top.
    elements.extend(_pdf_company_header(company, styles, f"Service Record — {vehicle_name}"))

    # Service date in Australian format
    service_date = record.get("service_date", "")
    if service_date:
        try:
            date_obj = datetime.fromisoformat(service_date.replace("Z", "+00:00")) if "T" in service_date else datetime.strptime(service_date, "%Y-%m-%d")
            service_date = date_obj.strftime("%d/%m/%Y")
        except Exception:
            pass
    
    # Details table
    data = [
        ["Vehicle:", vehicle_name],
        ["Registration:", vehicle_rego],
        ["Service Date:", service_date],
        ["Service Type:", record.get("service_type", "N/A").replace("_", " ").title()],
        ["Odometer:", f"{record.get('odometer_reading', 'N/A')} km"],
        ["Cost:", f"${record.get('cost', 0):.2f}"],
        ["Workshop:", record.get("workshop_name", "N/A")],
        ["Technician:", record.get("technician_name", "N/A")],
    ]
    
    table = Table(data, colWidths=[120, 350])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 20))
    
    # Description
    if record.get("description"):
        elements.append(Paragraph("<b>Description:</b>", styles['Normal']))
        elements.append(Paragraph(record.get("description", ""), styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Next service info
    if record.get("next_service_date") or record.get("next_service_odometer"):
        elements.append(Paragraph("<b>Next Service Due:</b>", styles['Normal']))
        if record.get("next_service_date"):
            next_date = record.get("next_service_date", "")
            try:
                date_obj = datetime.fromisoformat(next_date.replace("Z", "+00:00")) if "T" in next_date else datetime.strptime(next_date, "%Y-%m-%d")
                next_date = date_obj.strftime("%d/%m/%Y")
            except Exception:
                pass
            elements.append(Paragraph(f"Date: {next_date}", styles['Normal']))
        if record.get("next_service_odometer"):
            elements.append(Paragraph(f"Odometer: {record.get('next_service_odometer')} km", styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Footer
    elements.append(Spacer(1, 30))
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, textColor=colors.gray)
    elements.append(Paragraph(f"Generated by {company_name} via FleetShield365", footer_style))
    elements.append(Paragraph(f"Report generated: {datetime.now(get_timezone(company_tz)).strftime('%d/%m/%Y %H:%M')} ({tz_display})", footer_style))
    
    doc.build(elements)
    pdf_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    
    return {
        "pdf_base64": pdf_base64,
        "filename": f"service_record_{vehicle_rego}_{service_date.replace('/', '-')}.pdf"
    }

# ============== Alert Routes ==============

@api_router.get("/expiry-summary")
async def get_expiry_summary(
    days: int = 60,
    current_user: dict = Depends(get_current_user),
):
    """Live expiry feed for the Dashboard 'Expiry Alerts' panel.

    Computes upcoming + already-expired expiries from the current
    vehicles + drivers collections — does NOT depend on the `alerts`
    table being populated. The old dashboard only filtered
    `alerts.type == 'expiry_warning'`, but expiry alerts are only
    inserted on vehicle/driver create + occasional crons; if a fresh
    tenant or an admin who never re-edited a vehicle never triggered
    that path, the panel showed "All Clear" even when registration
    had expired weeks ago. This endpoint always reflects current state.

    Returns each item with: kind ('vehicle' | 'driver'), label
    (Registration / Insurance / License / etc), name (vehicle name or
    driver name), expiry_date (DD/MM/YYYY), days_until_expiry (negative
    if already expired), severity ('expired' | 'critical' | 'warning'
    | 'heads_up'), entity_id (vehicle/driver id for deep-link).
    Capped at 200 to keep the response bounded.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]
    window_days = max(7, min(int(days), 365))
    now = utcnow()

    def _classify(days_left: int) -> str:
        if days_left < 0:
            return "expired"
        if days_left <= 7:
            return "critical"
        if days_left <= 30:
            return "warning"
        return "heads_up"

    items: List[dict] = []

    # --- Vehicle expiries ---------------------------------------------
    vehicle_fields = [
        ("rego_expiry", "Registration"),
        ("insurance_expiry", "Insurance"),
        ("safety_certificate_expiry", "Safety Certificate"),
        ("coi_expiry", "COI"),
    ]
    vehicles_cursor = db.vehicles.find(
        {**_soft_delete_filter(), "company_id": company_id},
        {
            "name": 1, "registration_number": 1,
            "rego_expiry": 1, "insurance_expiry": 1,
            "safety_certificate_expiry": 1, "coi_expiry": 1,
            "custom_fields": 1,
        },
    )
    async for v in vehicles_cursor:
        v_id = str(v["_id"])
        v_label = f"{v.get('name', '')} ({v.get('registration_number', '')})"
        for field, label in vehicle_fields:
            raw = v.get(field)
            if not raw:
                continue
            dt = parse_date_flexible(raw)
            if not dt:
                continue
            days_left = (dt - now).days
            if days_left > window_days:
                continue
            items.append({
                "kind": "vehicle",
                "entity_id": v_id,
                "label": label,
                "name": v_label,
                "expiry_date": format_date_display(raw),
                "days_until_expiry": days_left,
                "severity": _classify(days_left),
            })
        # Custom-field expiries (2026-05-19 owner request).
        for cf in (v.get("custom_fields") or []):
            if not isinstance(cf, dict):
                continue
            raw = (cf.get("expiry") or "").strip()
            if not raw:
                continue
            dt = parse_date_flexible(raw)
            if not dt:
                continue
            days_left = (dt - now).days
            if days_left > window_days:
                continue
            items.append({
                "kind": "vehicle",
                "entity_id": v_id,
                "label": (cf.get("label") or "Custom field"),
                "name": v_label,
                "expiry_date": format_date_display(raw),
                "days_until_expiry": days_left,
                "severity": _classify(days_left),
            })

    # --- Driver doc expiries ------------------------------------------
    driver_fields = [
        ("license_expiry", "Driver License"),
        ("medical_certificate_expiry", "Medical Certificate"),
        ("first_aid_expiry", "First Aid"),
        ("forklift_license_expiry", "Forklift License"),
        ("dangerous_goods_expiry", "Dangerous Goods"),
        ("msic_expiry", "MSIC"),
        ("other_doc_expiry", "Other Document"),
    ]
    drivers_cursor = db.users.find(
        {
            **_soft_delete_filter(),
            "company_id": company_id,
            "role": UserRole.DRIVER,
        },
        {
            "name": 1, "username": 1,
            "license_expiry": 1, "medical_certificate_expiry": 1,
            "first_aid_expiry": 1, "forklift_license_expiry": 1,
            "dangerous_goods_expiry": 1, "msic_expiry": 1,
            "other_doc_expiry": 1, "custom_documents": 1,
        },
    )
    async for u in drivers_cursor:
        u_id = str(u["_id"])
        u_name = u.get("name") or u.get("username") or "Operator"
        for field, label in driver_fields:
            raw = u.get(field)
            if not raw:
                continue
            dt = parse_date_flexible(raw)
            if not dt:
                continue
            days_left = (dt - now).days
            if days_left > window_days:
                continue
            items.append({
                "kind": "driver",
                "entity_id": u_id,
                "label": label,
                "name": u_name,
                "expiry_date": format_date_display(raw),
                "days_until_expiry": days_left,
                "severity": _classify(days_left),
            })
        # Custom driver documents (Additional Documents).
        for cd in (u.get("custom_documents") or []):
            if not isinstance(cd, dict):
                continue
            raw = (cd.get("expiry") or "").strip()
            if not raw:
                continue
            dt = parse_date_flexible(raw)
            if not dt:
                continue
            days_left = (dt - now).days
            if days_left > window_days:
                continue
            items.append({
                "kind": "driver",
                "entity_id": u_id,
                "label": (cd.get("label") or "Custom document"),
                "name": u_name,
                "expiry_date": format_date_display(raw),
                "days_until_expiry": days_left,
                "severity": _classify(days_left),
            })

    # Sort: most urgent first (most-expired → most-pending).
    items.sort(key=lambda it: it["days_until_expiry"])
    return {
        "items": items[:200],
        "count": len(items),
        "window_days": window_days,
        "generated_at": now.isoformat(),
    }


@api_router.get("/alerts")
async def get_alerts(
    unread_only: bool = False,
    limit: int = 200,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)
    query = {"company_id": current_user["company_id"]}
    if unread_only:
        query["is_read"] = False

    alerts = await db.alerts.find(query).sort("created_at", -1) \
        .skip(actual_offset).limit(actual_limit).to_list(actual_limit)
    return serialize_doc(alerts)

@api_router.put("/alerts/{alert_id}/read")
async def mark_alert_read(alert_id: str, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.alerts.update_one(
        {"_id": ObjectId(alert_id), "company_id": current_user["company_id"]},
        {"$set": {"is_read": True}}
    )
    return {"message": "Alert marked as read"}

# ============== Incident Reports ==============

async def send_incident_alert_email(admin_email: str, company_name: str, incident: dict, vehicle_name: str, driver_name: str):
    """Send incident alert email to admin"""
    severity_colors = {
        "minor": "#EAB308",
        "moderate": "#F97316", 
        "severe": "#DC2626"
    }
    severity_bg = {
        "minor": "#FEFCE8",
        "moderate": "#FFF7ED",
        "severe": "#FEF2F2"
    }
    severity_border = {
        "minor": "#FDE68A",
        "moderate": "#FDBA74",
        "severe": "#FECACA"
    }
    sev = incident.get("severity", "moderate")
    severity_color = severity_colors.get(sev, "#F97316")
    bg_color = severity_bg.get(sev, "#FFF7ED")
    border_color = severity_border.get(sev, "#FDBA74")
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <div style="background-color: {severity_color}; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0;">
            <h2 style="margin: 0;">INCIDENT REPORT — {sev.upper()}</h2>
        </div>
        
        <div style="border: 1px solid {border_color}; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
        <p>Hi {company_name} Admin,</p>
        <p><strong>An incident has been reported and requires your immediate attention.</strong></p>
        
        <div style="background-color: {bg_color}; border: 1px solid {border_color}; padding: 16px; border-radius: 8px; margin: 20px 0;">
            <p><strong>Vehicle:</strong> {vehicle_name}</p>
            <p><strong>Driver:</strong> {driver_name}</p>
            <p><strong>Date/Time:</strong> {format_timestamp_sydney(incident.get('created_at', 'N/A'))}</p>
            <p><strong>Severity:</strong> <span style="color: {severity_color}; font-weight: bold;">{sev.upper()}</span></p>
            <p><strong>Location:</strong> {incident.get('location_address', 'GPS coordinates available')}</p>
            <p><strong>Injuries:</strong> {'Yes - ' + incident.get('injury_description', '') if incident.get('injuries_occurred') else 'No injuries reported'}</p>
        </div>
        
        <h3>Description:</h3>
        <p style="background-color: #F8FAFC; padding: 12px; border-radius: 4px;">{incident.get('description', 'No description provided')}</p>
        
        <h3>Other Party Details:</h3>
        <p><strong>Name:</strong> {incident.get('other_party', {}).get('name', 'N/A')}</p>
        <p><strong>Phone:</strong> {incident.get('other_party', {}).get('phone', 'N/A')}</p>
        <p><strong>Vehicle Rego:</strong> {incident.get('other_party', {}).get('vehicle_rego', 'N/A')}</p>
        
        <div style="margin-top: 25px; text-align: center;">
            <a href="https://www.fleetshield365.com/dashboard" style="background-color: {severity_color}; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: bold;">View Full Incident Report</a>
        </div>
        
        <p style="color: #64748B; font-size: 12px; margin-top: 20px;">This is an automated alert from FleetShield365.</p>
        </div>
    </body>
    </html>
    """
    return await send_email_notification(admin_email, f"[URGENT] Incident Report: {vehicle_name} - {incident.get('severity', 'moderate').upper()}", html_content)

@api_router.post("/incidents")
async def create_incident(
    incident: IncidentCreate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Create a new incident report"""
    company_id = current_user["company_id"]

    # 2026-05-19 — idempotency. See PrestartCreate for context.
    existing = await _check_idempotency(
        db.incidents, company_id, incident.idempotency_key,
    )
    if existing:
        return serialize_doc(existing)

    # Get vehicle info — owner 2026-05-27 allowed "Other" vehicles
    # that aren't in the fleet. When vehicle_id is empty we expect a
    # vehicle_other_label instead and skip the lookup.
    other_label = (incident.vehicle_other_label or "").strip()
    vehicle: dict = {}
    if incident.vehicle_id:
        try:
            vehicle = await db.vehicles.find_one({"_id": ObjectId(incident.vehicle_id), "company_id": company_id}) or {}
        except Exception:
            vehicle = {}
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
    elif not other_label:
        raise HTTPException(status_code=400, detail="Pick a vehicle or describe the other vehicle in the 'Other vehicle' field")
    
    # Parse timestamp from mobile app (for offline submissions)
    if incident.timestamp:
        try:
            incident_timestamp = datetime.fromisoformat(incident.timestamp.replace('Z', '+00:00'))
            if incident_timestamp.tzinfo:
                incident_timestamp = incident_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            incident_timestamp = datetime.now(timezone.utc)
    else:
        incident_timestamp = datetime.now(timezone.utc)
    
    # Task 5.3: upload each incident photo array to MinIO and store only
    # object keys. Keys are scoped to
    # incident-photos/<company_id>/<incident_id>/<kind>/<uuid>.jpg so every
    # photo sits inside the tenant's namespace (Req 21.10, 21.11, 21.14).
    incident_id = ObjectId()

    def _upload_incident_photos(b64_list: List[str], kind: str) -> List[str]:
        _enforce_count_or_413(
            b64_list, MAX_INCIDENT_PHOTOS_PER_CATEGORY, f"{kind}_photos",
        )
        keys: List[str] = []
        for idx, b64 in enumerate(b64_list or []):
            if not b64:
                continue
            key = (
                f"{company_id}/{str(incident_id)}/{kind}/"
                f"{uuid.uuid4().hex}.jpg"
            )
            _upload_base64_or_400(
                "incident-photos",
                key,
                b64,
                "jpg",
                f"{kind}_photos[{idx}]",
                expected_company_id=company_id,
                type_key="incident_photo",
            )
            keys.append(key)
        return keys

    damage_photo_keys = _upload_incident_photos(incident.damage_photos, "damage")
    other_vehicle_photo_keys = _upload_incident_photos(
        incident.other_vehicle_photos, "other-vehicle"
    )
    scene_photo_keys = _upload_incident_photos(incident.scene_photos, "scene")

    incident_doc = {
        "_id": incident_id,
        "company_id": company_id,
        # vehicle_id is None for "Other" vehicles; vehicle_other_label
        # carries the free-text description in that case.
        "vehicle_id": incident.vehicle_id or None,
        "vehicle_other_label": other_label or None,
        "driver_id": str(current_user["_id"]),
        "description": incident.description,
        "severity": incident.severity,
        "location_address": incident.location_address,
        "gps_latitude": incident.gps_latitude,
        "gps_longitude": incident.gps_longitude,
        "other_party": incident.other_party.dict(),
        "witnesses": [w.dict() for w in incident.witnesses] if incident.witnesses else [],
        "police_report_number": incident.police_report_number,
        "injuries_occurred": incident.injuries_occurred,
        "injury_description": incident.injury_description,
        "damage_photos": damage_photo_keys,
        "other_vehicle_photos": other_vehicle_photo_keys,
        "scene_photos": scene_photo_keys,
        "status": "reported",
        "created_at": incident_timestamp,
        "updated_at": datetime.now(timezone.utc),
        "idempotency_key": (incident.idempotency_key or None),
    }

    result = await db.incidents.insert_one(incident_doc)
    incident_doc["id"] = str(result.inserted_id)
    
    # Create alert for admin. For an "Other" vehicle the label is what
    # the admin typed; for an in-fleet vehicle it's the vehicle name.
    vehicle_label = vehicle.get('name', '') if vehicle else f"Other: {other_label}" if other_label else 'Unknown'
    alert_doc = {
        "company_id": company_id,
        "type": "incident_report",
        "severity": "critical" if incident.severity == "severe" else "warning",
        "message": f"Incident reported: {vehicle_label} - {incident.severity.upper()} - {incident.description[:100]}",
        "vehicle_id": incident.vehicle_id or None,
        "driver_id": str(current_user["_id"]),
        "incident_id": str(result.inserted_id),
        "is_read": False,
        "created_at": datetime.now(timezone.utc),
    }
    await db.alerts.insert_one(alert_doc)
    
    # Send email notification to admins - OPTIMIZED: batch fetch notification preferences
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    admin_users = await db.users.find({
        "company_id": company_id,
        "role": {"$in": [UserRole.ADMIN, UserRole.SUPER_ADMIN]}
    }).to_list(100)
    
    vehicle_name = f"{vehicle.get('name', 'Unknown')} ({vehicle.get('registration_number', 'N/A')})"
    driver_name = current_user.get("name", current_user.get("email", "Unknown"))
    
    # Batch fetch all notification preferences and push tokens
    admin_ids = [str(admin["_id"]) for admin in admin_users]
    all_prefs = await db.notification_preferences.find({"user_id": {"$in": admin_ids}}).to_list(100)
    prefs_map = {p["user_id"]: p for p in all_prefs}
    
    all_tokens = await db.push_tokens.find({"user_id": {"$in": admin_ids}}).to_list(100)
    tokens_map = {}
    for t in all_tokens:
        if t["user_id"] not in tokens_map:
            tokens_map[t["user_id"]] = []
        tokens_map[t["user_id"]].append(t["token"])
    
    for admin in admin_users:
        prefs = prefs_map.get(str(admin["_id"]), {})
        # Respect both the master email toggle and the per-activity
        # incident_email switch. Default for incidents is on (the most
        # important class of event) so blank/missing prefs still alert.
        if prefs.get("email_enabled", True) and prefs.get("incident_email", True):
            background_tasks.add_task(
                send_incident_alert_email,
                admin["email"],
                company.get("name", "Your Company") if company else "Your Company",
                incident_doc,
                vehicle_name,
                driver_name
            )
    
    # Send push notification to admins
    push_tokens = []
    for admin_id in admin_ids:
        push_tokens.extend(tokens_map.get(admin_id, []))
    
    if push_tokens:
        background_tasks.add_task(
            send_push_notification,
            push_tokens,
            f"Incident Report — {incident.severity.upper()}",
            f"{vehicle_name}: {incident.description[:100]}",
            {"type": "incident", "incident_id": str(result.inserted_id)}
        )
    
    return serialize_doc(incident_doc)

@api_router.get("/incidents")
async def get_incidents(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = None,
    severity: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    limit: int = 50,
    skip: int = 0
):
    """Get all incidents for the company - OPTIMIZED"""
    company_id = current_user["company_id"]

    # Phase 4 — exclude soft-deleted incidents by default.
    query = {**_soft_delete_filter(), "company_id": company_id}
    if status:
        query["status"] = status
    if severity:
        query["severity"] = severity
    if vehicle_id:
        query["vehicle_id"] = vehicle_id
    
    # Exclude large base64 data from list query for performance
    projection = {
        "photos": 0, 
        "pdf_attachments": 0,
        "damage_photos": 0,
        "scene_photos": 0,
        "other_vehicle_photos": 0
    }
    incidents = await db.incidents.find(query, projection).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    if not incidents:
        return []
    
    # Batch fetch vehicles and drivers (2 queries instead of N*2)
    vehicle_ids = list(set(i.get("vehicle_id") for i in incidents if i.get("vehicle_id")))
    driver_ids = list(set(i.get("driver_id") for i in incidents if i.get("driver_id")))
    
    vehicles_task = db.vehicles.find({"_id": {"$in": [ObjectId(vid) for vid in vehicle_ids if vid]}}).to_list(100)
    drivers_task = db.users.find({"_id": {"$in": [ObjectId(did) for did in driver_ids if did]}}).to_list(100)
    
    vehicles, drivers = await asyncio.gather(vehicles_task, drivers_task)
    
    vehicle_map = {str(v["_id"]): v for v in vehicles}
    driver_map = {str(d["_id"]): d for d in drivers}
    
    # Enrich with vehicle and driver info
    for incident in incidents:
        vehicle = vehicle_map.get(incident.get("vehicle_id"))
        driver = driver_map.get(incident.get("driver_id"))
        v_name = vehicle.get("name", "Unknown") if vehicle else "Unknown"
        v_rego = vehicle.get("registration_number", "") if vehicle else ""
        incident["vehicle_name"] = f"{v_name} ({v_rego})" if v_rego else v_name
        incident["vehicle_rego"] = v_rego or "N/A"
        d_name = driver.get("name", driver.get("email", "Unknown")) if driver else "Unknown"
        d_user = driver.get("username", "") if driver else ""
        incident["driver_name"] = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
    
    return serialize_doc(incidents)

@api_router.get("/incidents/export/csv")
async def export_incidents_csv(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,  # CSV "id1,id2,..." for multi-vehicle filter
    current_user: dict = Depends(get_current_user)
):
    """Export incidents to CSV. Phase 3.3: adds vehicle filtering."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    import csv
    from io import StringIO
    from starlette.responses import StreamingResponse

    company_id = current_user["company_id"]
    query: dict = {**_soft_delete_filter(), "company_id": company_id}

    if status:
        query["status"] = status
    if severity:
        query["severity"] = severity
    if date_from or date_to:
        # incidents.created_at is stored as a Python datetime (see create
        # handler ~line 10206). Compare against datetime bounds, not
        # strings — a string $gte against a Mongo Date never matches.
        date_filter: dict = {}
        try:
            if date_from:
                date_filter["$gte"] = datetime.fromisoformat(date_from)
            if date_to:
                date_filter["$lte"] = datetime.fromisoformat(date_to + "T23:59:59")
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be ISO yyyy-mm-dd")
        query["created_at"] = date_filter
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id

    incidents = await db.incidents.find(query, {"_id": 0, "damage_photos": 0, "scene_photos": 0, "other_vehicle_photos": 0}).sort("created_at", -1).to_list(10000)
    
    # Maps
    vehicle_ids = list(set(i.get("vehicle_id") for i in incidents if i.get("vehicle_id")))
    driver_ids = list(set(i.get("driver_id") for i in incidents if i.get("driver_id")))
    vehicles = await db.vehicles.find({"_id": {"$in": [ObjectId(v) for v in vehicle_ids]}}).to_list(1000) if vehicle_ids else []
    drivers = await db.users.find({"_id": {"$in": [ObjectId(d) for d in driver_ids]}}).to_list(1000) if driver_ids else []
    
    vehicle_map = {}
    for v in vehicles:
        v_name = v.get("name", "Unknown")
        v_rego = v.get("registration_number", "")
        vehicle_map[str(v["_id"])] = f"{v_name} ({v_rego})" if v_rego else v_name
    
    driver_map = {}
    for d in drivers:
        d_name = d.get("name", "Unknown")
        d_user = d.get("username", "")
        driver_map[str(d["_id"])] = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
    
    output = StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        "Date", "Time", "Driver", "Vehicle", "Severity", "Status",
        "Description", "Location", "Police Report #", "Insurance Claim #",
        "Injuries", "Injury Description", "Other Party Name", "Other Party Phone",
        "Other Party Rego", "Damage Photos", "Scene Photos", "Other Vehicle Photos"
    ])
    
    for inc in incidents:
        ts = inc.get("created_at", "")
        try:
            from datetime import datetime as dt
            parsed = dt.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else ts
            date_str = parsed.strftime("%Y-%m-%d")
            time_str = parsed.strftime("%H:%M")
        except Exception:
            date_str = str(ts)[:10]
            time_str = str(ts)[11:16]
        
        writer.writerow([
            date_str,
            time_str,
            driver_map.get(inc.get("driver_id", ""), "Unknown"),
            vehicle_map.get(inc.get("vehicle_id", ""), "Unknown"),
            inc.get("severity", "").title(),
            inc.get("status", "").replace("_", " ").title(),
            inc.get("description", ""),
            inc.get("location_address", ""),
            inc.get("police_report_number", ""),
            inc.get("insurance_claim_number", ""),
            "Yes" if inc.get("injuries_occurred") else "No",
            inc.get("injury_description", ""),
            inc.get("other_party_name", ""),
            inc.get("other_party_phone", ""),
            inc.get("other_party_rego", ""),
            len(inc.get("damage_photos", [])) if "damage_photos" in inc else 0,
            len(inc.get("scene_photos", [])) if "scene_photos" in inc else 0,
            len(inc.get("other_vehicle_photos", [])) if "other_vehicle_photos" in inc else 0,
        ])
    
    output.seek(0)
    filename = f"incidents_{utcnow().strftime('%Y%m%d')}.csv"
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@api_router.get("/incidents/{incident_id}")
async def get_incident(incident_id: str, current_user: dict = Depends(get_current_user)):
    """Get a specific incident by ID"""
    if not ObjectId.is_valid(incident_id):
        raise HTTPException(status_code=404, detail="Incident not found")
    company_id = current_user["company_id"]

    incident = await db.incidents.find_one({
        "_id": ObjectId(incident_id),
        "company_id": company_id
    })
    
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    
    # Enrich with vehicle and driver info
    vehicle = await db.vehicles.find_one({"_id": ObjectId(incident["vehicle_id"])})
    driver = await db.users.find_one({"_id": ObjectId(incident["driver_id"])})
    incident["vehicle_name"] = vehicle.get("name", "Unknown") if vehicle else "Unknown"
    incident["vehicle_rego"] = vehicle.get("registration_number", "N/A") if vehicle else "N/A"
    incident["driver_name"] = driver.get("name", driver.get("email", "Unknown")) if driver else "Unknown"

    # Task 5.4: emit parallel URL lists for each stored photo-key array and
    # a url field on each pdf_attachments entry so the frontend can render
    # every asset via Nginx_Proxy without pulling bytes through the API
    # (Requirements 21.12, 21.13). Per-photo keys live in the
    # ``incident-photos`` bucket; PDF attachments in ``incident-attachments``.
    incident["damage_photo_urls"] = _presign_keys(
        "incident-photos", incident.get("damage_photos")
    )
    incident["other_vehicle_photo_urls"] = _presign_keys(
        "incident-photos", incident.get("other_vehicle_photos")
    )
    incident["scene_photo_urls"] = _presign_keys(
        "incident-photos", incident.get("scene_photos")
    )
    incident["pdf_attachments"] = _presign_photos(
        incident.get("pdf_attachments") or [],
        "incident-attachments",
        url_field="url",
        key_field="object_key",
    )
    
    return serialize_doc(incident)


# IMPORTANT — declared before /incidents/{incident_id}/pdf so FastAPI's
# in-order path matcher doesn't route /incidents/export/pdf to the
# dynamic route and try ObjectId("export"). See defensive guards below.
@api_router.get("/incidents/export/pdf")
async def export_incidents_pdf(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    vehicle_ids: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """Bulk-export incidents as a single PDF (one incident per page).

    Streamed in-memory — nothing persists server-side. Filters mirror
    /incidents/export/csv exactly.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    from starlette.responses import StreamingResponse
    from reportlab.platypus import PageBreak

    company_id = current_user["company_id"]
    query: dict = {**_soft_delete_filter(), "company_id": company_id}
    if status:
        query["status"] = status
    if severity:
        query["severity"] = severity
    if date_from or date_to:
        # incidents.created_at is stored as a Python datetime (see create
        # handler ~line 10206). Compare against datetime bounds, not
        # strings — a string $gte against a Mongo Date never matches.
        date_filter: dict = {}
        try:
            if date_from:
                date_filter["$gte"] = datetime.fromisoformat(date_from)
            if date_to:
                date_filter["$lte"] = datetime.fromisoformat(date_to + "T23:59:59")
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be ISO yyyy-mm-dd")
        query["created_at"] = date_filter
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id

    incidents = await db.incidents.find(query).sort("created_at", -1).to_list(500)

    if not incidents:
        raise HTTPException(status_code=404, detail="No incidents match the selected filters")

    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = (company or {}).get("name", "FleetShield365")
    company_tz = (company or {}).get("timezone", DEFAULT_TIMEZONE)
    tz_display = company_tz.split('/')[-1].replace('_', ' ')

    vehicle_id_set = {str(i.get("vehicle_id")) for i in incidents if i.get("vehicle_id")}
    driver_id_set = {str(i.get("driver_id")) for i in incidents if i.get("driver_id")}
    vehicles = await db.vehicles.find(
        {"_id": {"$in": [ObjectId(v) for v in vehicle_id_set]}}
    ).to_list(len(vehicle_id_set)) if vehicle_id_set else []
    drivers = await db.users.find(
        {"_id": {"$in": [ObjectId(d) for d in driver_id_set]}}
    ).to_list(len(driver_id_set)) if driver_id_set else []
    vehicle_map = {str(v["_id"]): v for v in vehicles}
    driver_map = {str(d["_id"]): d for d in drivers}

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=30, bottomMargin=30, leftMargin=40, rightMargin=40)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#1e3a5f'), spaceAfter=20)
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, textColor=colors.gray)
    elements: list = []

    elements.extend(_pdf_company_header(company, styles, "Incident Report"))
    elements.append(Paragraph(f"{len(incidents)} incident(s) included.", styles['Normal']))
    if date_from or date_to:
        elements.append(Paragraph(
            f"Date range: {date_from or 'beginning'} → {date_to or 'today'}",
            styles['Normal']))
    if severity:
        elements.append(Paragraph(f"Severity filter: {severity}", styles['Normal']))
    if status:
        elements.append(Paragraph(f"Status filter: {status}", styles['Normal']))
    if vid_list or vehicle_id:
        picked = vid_list or [vehicle_id]
        names = ", ".join(
            f"{(vehicle_map.get(v) or {}).get('name', v)}" for v in picked
        )
        elements.append(Paragraph(f"Vehicle filter: {names}", styles['Normal']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(
        f"Generated: {datetime.now(get_timezone(company_tz)).strftime('%d/%m/%Y %H:%M')} ({tz_display})",
        footer_style))
    elements.append(PageBreak())

    severity_bg = {"minor": "#fef3c7", "moderate": "#fed7aa", "major": "#fecaca", "critical": "#fecdd3"}
    severity_text = {"minor": "#92400e", "moderate": "#c2410c", "major": "#dc2626", "critical": "#9f1239"}
    status_colors = {"reported": "#dc2626", "under_review": "#d97706", "resolved": "#16a34a", "closed": "#6b7280"}

    for idx, incident in enumerate(incidents):
        vehicle = vehicle_map.get(str(incident.get("vehicle_id"))) or {}
        driver = driver_map.get(str(incident.get("driver_id"))) or {}
        vehicle_name = vehicle.get("name", "Unknown")
        vehicle_rego = vehicle.get("registration_number", "N/A")
        d_name = driver.get("name", driver.get("email", "Unknown"))
        d_user = driver.get("username", "")
        driver_name = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
        sev = incident.get("severity", "unknown")
        incident_date = format_timestamp(incident.get("created_at", ""), company_tz)

        elements.append(Paragraph(
            f"Incident #{idx + 1} — {sev.upper()}", title_style))

        data = [
            ["Incident ID:", str(incident.get("_id", ""))[:8] + "..."],
            ["Date/Time:", f"{incident_date} ({tz_display})"],
            ["Vehicle:", f"{vehicle_name} ({vehicle_rego})"],
            ["Driver:", driver_name],
            ["Severity:", sev.title()],
            ["Status:", incident.get("status", "pending").replace("_", " ").title()],
            ["Location:", incident.get("location_address", "N/A")],
        ]
        table = Table(data, colWidths=[120, 350])
        sev_bg = severity_bg.get(sev, "#f3f4f6")
        sev_txt = severity_text.get(sev, "#1f2937")
        status_color = status_colors.get(incident.get("status", ""), "#6b7280")
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BACKGROUND', (1, 4), (1, 4), colors.HexColor(sev_bg)),
            ('TEXTCOLOR', (1, 4), (1, 4), colors.HexColor(sev_txt)),
            ('FONTNAME', (1, 4), (1, 4), 'Helvetica-Bold'),
            ('TEXTCOLOR', (1, 5), (1, 5), colors.HexColor(status_color)),
            ('FONTNAME', (1, 5), (1, 5), 'Helvetica-Bold'),
        ]))
        elements.append(table)
        elements.append(Spacer(1, 12))

        if incident.get("description"):
            elements.append(Paragraph("<b>Description:</b>", styles['Normal']))
            elements.append(Paragraph(incident.get("description", ""), styles['Normal']))
            elements.append(Spacer(1, 10))

        if incident.get("injuries_occurred"):
            injury_style = ParagraphStyle('Injury', parent=styles['Normal'], textColor=colors.red)
            elements.append(Paragraph("<b>⚠ INJURIES REPORTED</b>", injury_style))
            elements.append(Paragraph(incident.get("injury_description", "No details provided"), styles['Normal']))
            elements.append(Spacer(1, 10))

        photo_counts = [
            ("Damage", len(incident.get("damage_photos", []) or [])),
            ("Scene", len(incident.get("scene_photos", []) or [])),
            ("Other vehicle", len(incident.get("other_vehicle_photos", []) or [])),
        ]
        attached = ", ".join(f"{n} {label}" for label, n in photo_counts if n)
        if attached:
            elements.append(Paragraph(f"<b>Photos attached:</b> {attached}", styles['Normal']))

        if idx < len(incidents) - 1:
            elements.append(PageBreak())

    doc.build(elements)
    buffer.seek(0)
    filename = f"incidents_{utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@api_router.get("/incidents/{incident_id}/pdf")
async def get_incident_pdf(incident_id: str, current_user: dict = Depends(get_current_user)):
    """Generate and return PDF for an incident report"""
    # Defensive: this dynamic route is registered before /incidents/export/pdf
    # in source order, so a request for /incidents/export/pdf would otherwise
    # try ObjectId("export") and 500. Reject non-hex IDs early.
    if not ObjectId.is_valid(incident_id):
        raise HTTPException(status_code=404, detail="Incident not found")
    company_id = current_user["company_id"]

    incident = await db.incidents.find_one({
        "_id": ObjectId(incident_id),
        "company_id": company_id
    })
    
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    
    # Get related info
    vehicle = await db.vehicles.find_one({"_id": ObjectId(incident["vehicle_id"])})
    driver = await db.users.find_one({"_id": ObjectId(incident["driver_id"])})
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    
    vehicle_name = vehicle.get("name", "Unknown") if vehicle else "Unknown"
    vehicle_rego = vehicle.get("registration_number", "N/A") if vehicle else "N/A"
    d_name = driver.get("name", driver.get("email", "Unknown")) if driver else "Unknown"
    d_user = driver.get("username", "") if driver else ""
    driver_name = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
    company_name = company.get("name", "FleetShield365") if company else "FleetShield365"
    company_tz = company.get("timezone", DEFAULT_TIMEZONE) if company else DEFAULT_TIMEZONE
    tz_display = company_tz.split('/')[-1].replace('_', ' ')
    
    # Generate PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=30, bottomMargin=30, leftMargin=40, rightMargin=40)
    styles = getSampleStyleSheet()
    elements = []
    
    # Severity colour reference (used by the severity badge below).
    severity_colors = {"minor": "#f59e0b", "moderate": "#ef4444", "major": "#dc2626", "critical": "#7f1d1d"}
    severity = incident.get("severity", "unknown")
    # Branded header on every PDF — owner request 2026-05-27.
    elements.extend(_pdf_company_header(company, styles, f"Incident Report — {severity.upper()}"))
    
    # Incident date in company timezone
    incident_date = format_timestamp(incident.get("created_at", ""), company_tz)
    
    # Color maps
    severity_bg = {"minor": "#fef3c7", "moderate": "#fed7aa", "major": "#fecaca", "critical": "#fecdd3"}
    severity_text = {"minor": "#92400e", "moderate": "#c2410c", "major": "#dc2626", "critical": "#9f1239"}
    status_colors = {"reported": "#dc2626", "under_review": "#d97706", "resolved": "#16a34a", "closed": "#6b7280"}
    
    status_val = incident.get("status", "pending").replace("_", " ").title()
    status_color = status_colors.get(incident.get("status", ""), "#6b7280")
    sev_bg = severity_bg.get(severity, "#f3f4f6")
    sev_txt = severity_text.get(severity, "#1f2937")
    
    # Details table
    data = [
        ["Incident ID:", str(incident.get("_id", ""))[:8] + "..."],
        ["Date/Time:", f"{incident_date} ({tz_display})"],
        ["Vehicle:", f"{vehicle_name} ({vehicle_rego})"],
        ["Driver:", driver_name],
        ["Severity:", severity.title()],
        ["Status:", status_val],
        ["Location:", incident.get("location_address", "N/A")],
    ]
    
    table = Table(data, colWidths=[120, 350])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        # Severity row (row index 4) - colored background
        ('BACKGROUND', (1, 4), (1, 4), colors.HexColor(sev_bg)),
        ('TEXTCOLOR', (1, 4), (1, 4), colors.HexColor(sev_txt)),
        ('FONTNAME', (1, 4), (1, 4), 'Helvetica-Bold'),
        # Status row (row index 5) - colored text
        ('TEXTCOLOR', (1, 5), (1, 5), colors.HexColor(status_color)),
        ('FONTNAME', (1, 5), (1, 5), 'Helvetica-Bold'),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 20))
    
    # Description
    if incident.get("description"):
        elements.append(Paragraph("<b>Description:</b>", styles['Normal']))
        elements.append(Paragraph(incident.get("description", ""), styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Other party info
    other_party = incident.get("other_party", {})
    if other_party and any(other_party.values()):
        elements.append(Paragraph("<b>Other Party Information:</b>", styles['Normal']))
        if other_party.get("name"):
            elements.append(Paragraph(f"Name: {other_party.get('name')}", styles['Normal']))
        if other_party.get("phone"):
            elements.append(Paragraph(f"Phone: {other_party.get('phone')}", styles['Normal']))
        if other_party.get("vehicle_rego"):
            elements.append(Paragraph(f"Vehicle Rego: {other_party.get('vehicle_rego')}", styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Admin notes
    if incident.get("admin_notes"):
        elements.append(Paragraph("<b>Admin Notes:</b>", styles['Normal']))
        elements.append(Paragraph(incident.get("admin_notes", ""), styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Police report
    if incident.get("police_report_number"):
        elements.append(Paragraph(f"<b>Police Report #:</b> {incident.get('police_report_number')}", styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Insurance info
    if incident.get("insurance_claim_number"):
        elements.append(Paragraph(f"<b>Insurance Claim #:</b> {incident.get('insurance_claim_number')}", styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Injuries
    if incident.get("injuries_occurred"):
        injury_style = ParagraphStyle('Injury', parent=styles['Normal'], textColor=colors.red)
        elements.append(Paragraph("<b>⚠ INJURIES REPORTED</b>", injury_style))
        elements.append(Paragraph(incident.get("injury_description", "No details provided"), styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Resolution details
    if incident.get("resolution_details"):
        elements.append(Paragraph("<b>Resolution Details:</b>", styles['Normal']))
        elements.append(Paragraph(incident.get("resolution_details", ""), styles['Normal']))
        elements.append(Spacer(1, 15))
    
    # Photos
    # Task 5.4: stored values are now MinIO object keys rather than base64
    # strings (see Task 5.3). Fetch each via object_store.get_bytes from
    # the incident-photos bucket; pre-migration rows carrying inline
    # base64 fall through to the legacy decode branch.
    photo_sections = [
        ("Damage Photos", incident.get("damage_photos", [])),
        ("Other Vehicle Photos", incident.get("other_vehicle_photos", [])),
        ("Scene Photos", incident.get("scene_photos", [])),
    ]
    
    has_any_photos = any(photos for _, photos in photo_sections)
    if has_any_photos:
        elements.append(Paragraph("<b>Photo Evidence:</b>", styles['Normal']))
        elements.append(Spacer(1, 8))
        
        for section_name, photos in photo_sections:
            if photos:
                elements.append(Paragraph(f"<i>{section_name} ({len(photos)})</i>", styles['Normal']))
                elements.append(Spacer(1, 5))
                for i, photo_data in enumerate(photos):
                    try:
                        img_bytes: Optional[bytes] = None
                        if isinstance(photo_data, str) and photo_data.startswith('data:'):
                            img_data = photo_data.split(',', 1)[1]
                            try:
                                img_bytes = base64.b64decode(img_data)
                            except Exception:
                                img_bytes = None
                        elif isinstance(photo_data, str):
                            # Post-migration values are object keys of the
                            # form "<company_id>/<incident_id>/<kind>/<uuid>.jpg"
                            # inside the incident-photos bucket. Legacy
                            # rows can still carry a raw base64 string; we
                            # try the MinIO lookup first and fall back to
                            # base64 decode on failure.
                            try:
                                img_bytes = object_store.get_bytes(
                                    "incident-photos", photo_data
                                )
                            except Exception:
                                try:
                                    img_bytes = base64.b64decode(photo_data)
                                except Exception:
                                    img_bytes = None
                        else:
                            continue

                        if img_bytes is None:
                            elements.append(
                                Paragraph(
                                    f"[Photo {i+1} could not be rendered]",
                                    styles['Normal'],
                                )
                            )
                            elements.append(Spacer(1, 5))
                            continue

                        img_buffer = BytesIO(img_bytes)
                        img = RLImage(img_buffer, width=250, height=180)
                        elements.append(img)
                        elements.append(Spacer(1, 10))
                    except Exception as e:
                        elements.append(Paragraph(f"[Photo {i+1} could not be rendered]", styles['Normal']))
                        elements.append(Spacer(1, 5))
        
        elements.append(Spacer(1, 15))
    
    # Footer
    elements.append(Spacer(1, 30))
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, textColor=colors.gray)
    elements.append(Paragraph(f"Generated by {company_name} via FleetShield365", footer_style))
    elements.append(Paragraph(f"Report generated: {datetime.now(get_timezone(company_tz)).strftime('%d/%m/%Y %H:%M')} ({tz_display})", footer_style))
    
    doc.build(elements)
    pdf_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

    return {
        "pdf_base64": pdf_base64,
        "filename": f"incident_report_{vehicle_rego}_{incident_date.replace('/', '-').replace(':', '-').replace(' ', '_')}.pdf"
    }


# export_incidents_pdf is registered ~line 10166 (must be declared before
# the dynamic /incidents/{incident_id}/pdf route so FastAPI's in-order
# path matcher doesn't try ObjectId("export")).
async def _export_incidents_pdf_unused() -> None:
    """Original body lives at the registered handler above."""
    return None
    # The lines below are unreachable dead code kept only so this Edit
    # didn't churn 170 lines. They never execute. Will be removed in the
    # next refactor.
    query: dict = {**_soft_delete_filter(), "company_id": ""}
    if status:
        query["status"] = status
    if severity:
        query["severity"] = severity
    if date_from or date_to:
        # incidents.created_at is stored as a Python datetime (see create
        # handler ~line 10206). Compare against datetime bounds, not
        # strings — a string $gte against a Mongo Date never matches.
        date_filter: dict = {}
        try:
            if date_from:
                date_filter["$gte"] = datetime.fromisoformat(date_from)
            if date_to:
                date_filter["$lte"] = datetime.fromisoformat(date_to + "T23:59:59")
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be ISO yyyy-mm-dd")
        query["created_at"] = date_filter
    vid_list = [v.strip() for v in (vehicle_ids or "").split(",") if v.strip()]
    if vid_list:
        query["vehicle_id"] = {"$in": vid_list}
    elif vehicle_id:
        query["vehicle_id"] = vehicle_id

    # Cap at 500 incidents per export — that's already a ~500-page PDF.
    # Anyone needing more should narrow filters or use CSV.
    incidents = await db.incidents.find(query).sort("created_at", -1).to_list(500)

    if not incidents:
        raise HTTPException(status_code=404, detail="No incidents match the selected filters")

    # Pre-fetch vehicle/driver/company maps so the per-incident loop is
    # entirely from-memory rather than N×3 round-trips.
    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    company_name = (company or {}).get("name", "FleetShield365")
    company_tz = (company or {}).get("timezone", DEFAULT_TIMEZONE)
    tz_display = company_tz.split('/')[-1].replace('_', ' ')

    vehicle_id_set = {str(i.get("vehicle_id")) for i in incidents if i.get("vehicle_id")}
    driver_id_set = {str(i.get("driver_id")) for i in incidents if i.get("driver_id")}
    vehicles = await db.vehicles.find(
        {"_id": {"$in": [ObjectId(v) for v in vehicle_id_set]}}
    ).to_list(len(vehicle_id_set)) if vehicle_id_set else []
    drivers = await db.users.find(
        {"_id": {"$in": [ObjectId(d) for d in driver_id_set]}}
    ).to_list(len(driver_id_set)) if driver_id_set else []
    vehicle_map = {str(v["_id"]): v for v in vehicles}
    driver_map = {str(d["_id"]): d for d in drivers}

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=30, bottomMargin=30, leftMargin=40, rightMargin=40)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#1e3a5f'), spaceAfter=20)
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, textColor=colors.gray)
    elements: list = []

    # Cover page — branded header
    elements.extend(_pdf_company_header(company, styles, "Incident Report"))
    elements.append(Paragraph(f"{len(incidents)} incident(s) included.", styles['Normal']))
    if date_from or date_to:
        elements.append(Paragraph(
            f"Date range: {date_from or 'beginning'} → {date_to or 'today'}",
            styles['Normal']))
    if severity:
        elements.append(Paragraph(f"Severity filter: {severity}", styles['Normal']))
    if status:
        elements.append(Paragraph(f"Status filter: {status}", styles['Normal']))
    if vid_list or vehicle_id:
        picked = vid_list or [vehicle_id]
        names = ", ".join(
            f"{(vehicle_map.get(v) or {}).get('name', v)}" for v in picked
        )
        elements.append(Paragraph(f"Vehicle filter: {names}", styles['Normal']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(
        f"Generated: {datetime.now(get_timezone(company_tz)).strftime('%d/%m/%Y %H:%M')} ({tz_display})",
        footer_style))
    elements.append(PageBreak())

    severity_bg = {"minor": "#fef3c7", "moderate": "#fed7aa", "major": "#fecaca", "critical": "#fecdd3"}
    severity_text = {"minor": "#92400e", "moderate": "#c2410c", "major": "#dc2626", "critical": "#9f1239"}
    status_colors = {"reported": "#dc2626", "under_review": "#d97706", "resolved": "#16a34a", "closed": "#6b7280"}

    for idx, incident in enumerate(incidents):
        vehicle = vehicle_map.get(str(incident.get("vehicle_id"))) or {}
        driver = driver_map.get(str(incident.get("driver_id"))) or {}
        vehicle_name = vehicle.get("name", "Unknown")
        vehicle_rego = vehicle.get("registration_number", "N/A")
        d_name = driver.get("name", driver.get("email", "Unknown"))
        d_user = driver.get("username", "")
        driver_name = f"{d_name} ({d_user})" if d_user and d_user != d_name else d_name
        sev = incident.get("severity", "unknown")
        incident_date = format_timestamp(incident.get("created_at", ""), company_tz)

        elements.append(Paragraph(
            f"Incident #{idx + 1} — {sev.upper()}", title_style))

        data = [
            ["Incident ID:", str(incident.get("_id", ""))[:8] + "..."],
            ["Date/Time:", f"{incident_date} ({tz_display})"],
            ["Vehicle:", f"{vehicle_name} ({vehicle_rego})"],
            ["Driver:", driver_name],
            ["Severity:", sev.title()],
            ["Status:", incident.get("status", "pending").replace("_", " ").title()],
            ["Location:", incident.get("location_address", "N/A")],
        ]
        table = Table(data, colWidths=[120, 350])
        sev_bg = severity_bg.get(sev, "#f3f4f6")
        sev_txt = severity_text.get(sev, "#1f2937")
        status_color = status_colors.get(incident.get("status", ""), "#6b7280")
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BACKGROUND', (1, 4), (1, 4), colors.HexColor(sev_bg)),
            ('TEXTCOLOR', (1, 4), (1, 4), colors.HexColor(sev_txt)),
            ('FONTNAME', (1, 4), (1, 4), 'Helvetica-Bold'),
            ('TEXTCOLOR', (1, 5), (1, 5), colors.HexColor(status_color)),
            ('FONTNAME', (1, 5), (1, 5), 'Helvetica-Bold'),
        ]))
        elements.append(table)
        elements.append(Spacer(1, 12))

        if incident.get("description"):
            elements.append(Paragraph("<b>Description:</b>", styles['Normal']))
            elements.append(Paragraph(incident.get("description", ""), styles['Normal']))
            elements.append(Spacer(1, 10))

        if incident.get("injuries_occurred"):
            injury_style = ParagraphStyle('Injury', parent=styles['Normal'], textColor=colors.red)
            elements.append(Paragraph("<b>⚠ INJURIES REPORTED</b>", injury_style))
            elements.append(Paragraph(incident.get("injury_description", "No details provided"), styles['Normal']))
            elements.append(Spacer(1, 10))

        # Photo counts only — embedding every photo across 500 incidents
        # would blow up the PDF. Owners can drill into the per-incident
        # PDF endpoint if they need photo evidence.
        photo_counts = [
            ("Damage", len(incident.get("damage_photos", []) or [])),
            ("Scene", len(incident.get("scene_photos", []) or [])),
            ("Other vehicle", len(incident.get("other_vehicle_photos", []) or [])),
        ]
        attached = ", ".join(f"{n} {label}" for label, n in photo_counts if n)
        if attached:
            elements.append(Paragraph(f"<b>Photos attached:</b> {attached}", styles['Normal']))

        if idx < len(incidents) - 1:
            elements.append(PageBreak())

    doc.build(elements)
    buffer.seek(0)
    filename = f"incidents_{utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )



@api_router.put("/incidents/{incident_id}")
async def update_incident(
    incident_id: str,
    update: IncidentUpdate,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    """Update an incident (admin only)"""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    company_id = current_user["company_id"]
    
    # Get existing incident to handle photo appending
    existing = await db.incidents.find_one({"_id": ObjectId(incident_id), "company_id": company_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Incident not found")
    
    update_data = {}
    
    # Handle simple field updates
    for field in ["status", "admin_notes", "insurance_claim_number", "resolution_details", 
                  "description", "severity", "location_address", "police_report_number"]:
        value = getattr(update, field, None)
        if value is not None:
            update_data[field] = value
    
    # Task 5.3: additional photos supplied as base64 are uploaded to MinIO
    # and appended as object keys on the incident. The stored document
    # never contains the raw base64 (Req 21.10, 21.11, 21.14).
    if update.additional_photos:
        _enforce_count_or_413(
            update.additional_photos,
            MAX_INCIDENT_PHOTOS_PER_CATEGORY,
            "additional_photos",
        )
        existing_photos = existing.get("damage_photos", [])
        new_keys: List[str] = []
        for idx, b64 in enumerate(update.additional_photos):
            if not b64:
                continue
            key = (
                f"{company_id}/{str(existing['_id'])}/damage/"
                f"{uuid.uuid4().hex}.jpg"
            )
            _upload_base64_or_400(
                "incident-photos",
                key,
                b64,
                "jpg",
                f"additional_photos[{idx}]",
                expected_company_id=company_id,
                type_key="incident_photo",
            )
            new_keys.append(key)
        update_data["damage_photos"] = existing_photos + new_keys
    
    # Task 5.3: PDF attachments supplied as {name, data} get uploaded to
    # MinIO. Mongo stores {name, object_key} entries instead of
    # {name, data} (Req 21.10, 21.11, 21.14).
    if update.pdf_attachments:
        _enforce_count_or_413(
            update.pdf_attachments,
            MAX_SERVICE_ATTACHMENTS,
            "pdf_attachments",
        )
        existing_pdfs = existing.get("pdf_attachments", [])
        new_pdfs: List[dict] = []
        for idx, attachment in enumerate(update.pdf_attachments):
            if not isinstance(attachment, dict):
                continue
            b64 = attachment.get("data")
            if not b64:
                continue
            key = (
                f"{company_id}/{str(existing['_id'])}/"
                f"{uuid.uuid4().hex}.pdf"
            )
            _upload_base64_or_400(
                "incident-attachments",
                key,
                b64,
                "pdf",
                f"pdf_attachments[{idx}].data",
                expected_company_id=company_id,
                type_key="incident_pdf",
                background_tasks=background_tasks,
            )
            new_pdfs.append({
                "name": attachment.get("name"),
                "object_key": key,
            })
        update_data["pdf_attachments"] = existing_pdfs + new_pdfs
    
    update_data["updated_at"] = datetime.now(timezone.utc)
    
    result = await db.incidents.update_one(
        {"_id": ObjectId(incident_id), "company_id": company_id},
        {"$set": update_data}
    )
    
    # Return updated incident
    updated = await db.incidents.find_one({"_id": ObjectId(incident_id)})
    return serialize_doc(updated)

@api_router.get("/incidents/stats/summary")
async def get_incident_stats(current_user: dict = Depends(get_current_user)):
    """Get incident statistics for dashboard"""
    company_id = current_user["company_id"]
    
    # Total incidents
    total = await db.incidents.count_documents({"company_id": company_id})
    
    # This month
    month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    this_month = await db.incidents.count_documents({
        "company_id": company_id,
        "created_at": {"$gte": month_start}
    })
    
    # By severity
    by_severity = {}
    for sev in ["minor", "moderate", "severe"]:
        by_severity[sev] = await db.incidents.count_documents({
            "company_id": company_id,
            "severity": sev
        })
    
    # By status
    by_status = {}
    for status in ["reported", "under_review", "resolved", "closed"]:
        by_status[status] = await db.incidents.count_documents({
            "company_id": company_id,
            "status": status
        })
    
    # Open incidents (not resolved/closed)
    open_incidents = await db.incidents.count_documents({
        "company_id": company_id,
        "status": {"$in": ["reported", "under_review"]}
    })
    
    return {
        "total": total,
        "this_month": this_month,
        "open_incidents": open_incidents,
        "by_severity": by_severity,
        "by_status": by_status
    }

# ============== Dashboard Stats ==============

@api_router.get("/dashboard/stats")
async def get_dashboard_stats(
    current_user: dict = Depends(get_current_user),
    tz_offset: int = 0  # Kept for backwards compatibility, but ignored
):
    company_id = current_user["company_id"]
    
    # NOTE: Cache disabled to ensure fresh "Active Today" counts
    # The 30-second cache was causing mismatches between dashboard cards
    # and filtered pages that make fresh API calls
    
    # Use shared Sydney timezone helper for consistent "today" calculation
    today_utc, _ = get_sydney_today_range()
    
    # Pre-calculate date strings
    thirty_days = (utcnow() + timedelta(days=30)).isoformat()[:10]
    sixty_days = (utcnow() + timedelta(days=60)).isoformat()[:10]
    today_str = utcnow().isoformat()[:10]
    month_start = utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Run all queries in parallel for better performance
    results = await asyncio.gather(
        # Basic counts
        db.vehicles.count_documents({"company_id": company_id}),
        db.inspections.count_documents({"company_id": company_id, "timestamp": {"$gte": today_utc}}),
        db.inspections.distinct("vehicle_id", {"company_id": company_id, "timestamp": {"$gte": today_utc}}),
        # Issues today: is_safe=False OR new_damage=True OR incident_today=True
        db.inspections.count_documents({
            "company_id": company_id, 
            "timestamp": {"$gte": today_utc}, 
            "$or": [
                {"is_safe": False},
                {"new_damage": True},
                {"incident_today": True}
            ]
        }),
        # Vehicles needing attention: expired or expiring within 30 days (any doc type)
        db.vehicles.count_documents({"company_id": company_id, "$or": [
            {"rego_expiry": {"$lte": thirty_days}},
            {"insurance_expiry": {"$lte": thirty_days}},
            {"safety_certificate_expiry": {"$lte": thirty_days}},
            {"coi_expiry": {"$lte": thirty_days}},
        ]}),
        # Expiry counts
        db.vehicles.count_documents({"company_id": company_id, "rego_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        db.vehicles.count_documents({"company_id": company_id, "insurance_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        db.vehicles.count_documents({"company_id": company_id, "safety_certificate_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        db.vehicles.count_documents({"company_id": company_id, "coi_expiry": {"$lte": thirty_days, "$gte": today_str}}),
        # Vehicle names with expiring items
        db.vehicles.find({"company_id": company_id, "rego_expiry": {"$lte": thirty_days, "$gte": today_str}}, {"name": 1, "rego_expiry": 1, "_id": 0}).to_list(10),
        db.vehicles.find({"company_id": company_id, "insurance_expiry": {"$lte": thirty_days, "$gte": today_str}}, {"name": 1, "insurance_expiry": 1, "_id": 0}).to_list(10),
        db.vehicles.find({"company_id": company_id, "coi_expiry": {"$lte": thirty_days, "$gte": today_str}}, {"name": 1, "coi_expiry": 1, "_id": 0}).to_list(10),
        # Fuel and alerts
        db.fuel_submissions.aggregate([{"$match": {"company_id": company_id, "timestamp": {"$gte": month_start}}}, {"$group": {"_id": None, "total": {"$sum": "$amount"}}}]).to_list(1),
        db.alerts.count_documents({"company_id": company_id, "is_read": False}),
        # Drivers
        db.users.find({"company_id": company_id, "role": UserRole.DRIVER}).to_list(1000),
    )
    
    # Unpack results
    total_vehicles, inspections_today, active_today_raw, issues_today, vehicles_needing_attention, \
    upcoming_rego, upcoming_insurance, upcoming_safety_cert, upcoming_coi, \
    rego_expiring_vehicles, insurance_expiring_vehicles, coi_expiring_vehicles, \
    fuel_result, unread_alerts, drivers = results
    
    # Get actual existing vehicle IDs to filter out deleted vehicles from active_today
    existing_vehicle_ids = await db.vehicles.distinct("_id", {"company_id": company_id})
    existing_vehicle_id_strs = [str(vid) for vid in existing_vehicle_ids]
    
    # Filter active_today to only include vehicles that still exist
    active_today = [vid for vid in active_today_raw if vid in existing_vehicle_id_strs]
    
    # Calculate derived values
    inspections_missed = max(0, total_vehicles - len(active_today))
    expiring_soon = upcoming_rego + upcoming_insurance + upcoming_safety_cert + upcoming_coi
    fuel_this_month = fuel_result[0]["total"] if fuel_result else 0
    
    # Process driver expiries
    drivers_license_expiring = 0
    drivers_license_expired = 0
    drivers_training_expiring = 0
    drivers_training_expired = 0
    
    for driver in drivers:
        license_exp = driver.get("license_expiry")
        if license_exp and license_exp.upper() != "NA":
            if license_exp < today_str:
                drivers_license_expired += 1
            elif license_exp <= sixty_days:
                drivers_license_expiring += 1
        
        for field in ["medical_certificate_expiry", "first_aid_expiry", "forklift_license_expiry", "dangerous_goods_expiry"]:
            exp = driver.get(field)
            if exp and exp.upper() != "NA":
                if exp < today_str:
                    drivers_training_expired += 1
                elif exp <= sixty_days:
                    drivers_training_expiring += 1
    
    result = {
        "total_vehicles": total_vehicles,
        "total_drivers": len(drivers),
        "inspections_today": inspections_today,
        "inspections_missed": inspections_missed,
        "issues_today": issues_today,
        "fuel_this_month": round(fuel_this_month, 2),
        "expiring_soon": expiring_soon,
        "active_today": len(active_today),
        "active_today_ids": active_today,
        "vehicles_needing_attention": vehicles_needing_attention,
        "upcoming_rego_expiry": upcoming_rego,
        "upcoming_insurance_expiry": upcoming_insurance,
        "upcoming_safety_cert_expiry": upcoming_safety_cert,
        "upcoming_coi_expiry": upcoming_coi,
        "rego_expiring_vehicles": rego_expiring_vehicles,
        "insurance_expiring_vehicles": insurance_expiring_vehicles,
        "coi_expiring_vehicles": coi_expiring_vehicles,
        "unread_alerts": unread_alerts,
        "drivers_license_expiring": drivers_license_expiring,
        "drivers_license_expired": drivers_license_expired,
        "drivers_training_expiring": drivers_training_expiring,
        "drivers_training_expired": drivers_training_expired,
    }
    
    # NOTE: Caching disabled to ensure fresh data consistency
    # set_cached_stats(company_id, result)
    
    return result


# ============== Fleet Health Score ==============

CRITICAL_DEFECT_KEYWORDS = ["brake", "steering", "tire", "tyre", "light", "horn", "seatbelt", "wiper", "fluid leak"]

def _classify_defect_severity(item_name: str, has_damage: bool, has_incident: bool) -> str:
    """Classify defect severity based on keywords + flags."""
    name_l = (item_name or "").lower()
    if has_incident or any(k in name_l for k in ["brake", "steering", "seatbelt"]):
        return "high"
    if has_damage or any(k in name_l for k in CRITICAL_DEFECT_KEYWORDS):
        return "medium"
    return "low"


def _vehicle_health_score(open_defects: list, expiry_days: list, last_inspection_days: int, has_grounding: bool) -> int:
    """Backwards-compat wrapper — returns just the score integer. Use
    ``_vehicle_health_score_with_reasons`` for the new explainer payload."""
    score, _ = _vehicle_health_score_with_reasons(open_defects, expiry_days, last_inspection_days, has_grounding)
    return score


def _vehicle_health_score_with_reasons(
    open_defects: list,
    expiry_days: list,
    last_inspection_days: int,
    has_grounding: bool,
) -> tuple[int, list[dict]]:
    """Calculate vehicle health score (0-100) plus a human-readable list
    of deductions so the UI can explain "why isn't this 100?".

    ``expiry_days`` accepts either ``[int, ...]`` (legacy) or
    ``[(label, days), ...]`` tuples — when a label is provided the
    reason text names the specific document (e.g. "Rego expires in
    3d") instead of the generic "Expiry within 14 days".

    Returned in priority order so the UI can show the top 1-2 offenders.
    """
    score = 100
    reasons: list[dict] = []
    severity_weights = {"high": 15, "medium": 10, "low": 5}
    for d in open_defects:
        sev = d.get("severity", "low")
        pts = severity_weights.get(sev, 5)
        score -= pts
        reasons.append({"label": f"Open {sev} defect: {d.get('name', 'unnamed')}", "points": -pts})
    for entry in expiry_days:
        if entry is None:
            continue
        if isinstance(entry, tuple):
            label, days = entry
        else:
            label, days = None, entry
        if days is None:
            continue
        if days < 0:
            score -= 15
            reasons.append({
                "label": f"{label} expired {abs(days)}d ago" if label else "Already expired (rego/license/cert)",
                "points": -15,
            })
        elif days <= 14:
            score -= 10
            label_text = (
                f"{label} expires {'today' if days == 0 else f'in {days}d'}"
                if label else f"Expiry within 14 days ({days}d)"
            )
            reasons.append({"label": label_text, "points": -10})
    if last_inspection_days >= 999:
        score -= 10
        reasons.append({"label": "Never inspected", "points": -10})
    elif last_inspection_days > 7:
        score -= 10
        reasons.append({"label": f"Last inspected {last_inspection_days}d ago", "points": -10})
    elif last_inspection_days > 3:
        score -= 5
        reasons.append({"label": f"Last inspected {last_inspection_days}d ago", "points": -5})
    if has_grounding:
        score -= 20
        reasons.append({"label": "Vehicle grounded (driver flagged unsafe)", "points": -20})
    return max(0, min(100, score)), reasons


@api_router.get("/fleet-health")
async def get_fleet_health(current_user: dict = Depends(get_current_user)):
    """
    Returns fleet-wide health score + per-vehicle breakdown.
    Pulls from existing inspection data — no mobile app changes required.
    """
    company_id = current_user["company_id"]
    today_str = utcnow().isoformat()[:10]
    today_dt = utcnow()
    thirty_days_ago = today_dt - timedelta(days=30)

    # 1. Get all vehicles for this company
    vehicles_cursor = db.vehicles.find(
        {"company_id": company_id},
        {"name": 1, "registration_number": 1, "rego_expiry": 1, "insurance_expiry": 1,
         "safety_certificate_expiry": 1, "coi_expiry": 1, "status": 1,
         "current_odometer": 1}
    )
    vehicles = await vehicles_cursor.to_list(1000)

    # 1b. Pull each vehicle's most relevant service-record signals — the
    # next-service reminder (date OR odometer) plus the latest warranty
    # window. Both feed the fleet-health score and the per-vehicle alert
    # strip (owner request 2026-05-22).
    service_cursor = db.service_records.find(
        {"company_id": company_id, **_soft_delete_filter()},
        {"vehicle_id": 1, "next_service_date": 1, "next_service_odometer": 1,
         "warranty_until": 1, "service_date": 1}
    )
    service_rows = await service_cursor.to_list(5000)
    # For each vehicle, keep the SOONEST next_service trigger + latest warranty.
    soonest_service_by_v: dict = {}
    latest_warranty_by_v: dict = {}
    for r in service_rows:
        vid = r.get("vehicle_id")
        if not vid:
            continue
        # Next-service: track the earliest upcoming date AND odometer.
        nsd = r.get("next_service_date")
        nso = r.get("next_service_odometer")
        if nsd or nso:
            existing = soonest_service_by_v.get(vid) or {}
            # Pick the row with the earliest next-service date.
            if nsd and (not existing.get("next_service_date") or nsd < existing["next_service_date"]):
                existing["next_service_date"] = nsd
            if nso and (not existing.get("next_service_odometer") or nso < existing["next_service_odometer"]):
                existing["next_service_odometer"] = nso
            soonest_service_by_v[vid] = existing
        # Warranty: pick the latest (longest-extending) warranty per vehicle.
        wu = r.get("warranty_until")
        if wu:
            existing_w = latest_warranty_by_v.get(vid)
            if not existing_w or wu > existing_w:
                latest_warranty_by_v[vid] = wu
    
    # 2. Get all open defects (last 30 days, not yet marked fixed) for company.
    # Timestamp filter removed from DB query because timestamps are stored as a mix of strings
    # (legacy) and BSON ISODates (new mobile-app data). MongoDB cannot compare across BSON types,
    # so we filter post-fetch in Python below for type-safety.
    inspections_cursor = db.inspections.find(
        {
            "company_id": company_id,
            "$or": [
                {"is_safe": False},
                {"new_damage": True},
                {"incident_today": True},
                {"checklist_items.status": "issue"},
            ],
        },
        {"vehicle_id": 1, "timestamp": 1, "type": 1, "checklist_items": 1,
         "new_damage": 1, "incident_today": 1, "damage_comment": 1, "incident_comment": 1,
         "defect_status": 1}
    )
    inspections = await inspections_cursor.to_list(5000)
    
    # 3. Get last-inspection date per vehicle
    last_insp_cursor = db.inspections.aggregate([
        {"$match": {"company_id": company_id}},
        {"$group": {"_id": "$vehicle_id", "last": {"$max": "$timestamp"}}}
    ])
    last_insp_map = {doc["_id"]: doc["last"] for doc in await last_insp_cursor.to_list(1000)}
    
    # Phase 6 (2026-05-18) — load per-defect status overrides up front so
    # the fleet-health view reflects the same fixed/in-progress state
    # that VehicleDefectsPage shows. Previously this endpoint only
    # honoured the legacy inspection-level ``defect_status`` field, which
    # the override endpoint never writes to, so fixed defects kept
    # showing as open here.
    overrides_cursor = db.defect_overrides.find({"company_id": company_id})
    overrides_by_id: dict = {doc["defect_id"]: doc async for doc in overrides_cursor}

    # Build defects per vehicle
    defects_by_vehicle: dict = {}
    def _ts_to_dt(t):
        if isinstance(t, datetime):
            return t
        if isinstance(t, str):
            try:
                return datetime.fromisoformat(t.replace("Z", "")[:26])
            except Exception:
                return None
        return None

    def _status_for(defect_id: str, insp_doc: dict) -> str:
        override = overrides_by_id.get(defect_id)
        if override:
            return override.get("status", "open")
        # Fall back to the legacy inspection-level field for back-compat.
        return insp_doc.get("defect_status", "open")

    for insp in inspections:
        # Skip inspections older than 30 days (filter in Python — handles both string + datetime timestamps)
        ts_dt = _ts_to_dt(insp.get("timestamp"))
        if not ts_dt or ts_dt < thirty_days_ago:
            continue
        vid = insp.get("vehicle_id")
        if not vid:
            continue
        inspection_id = str(insp.get("_id"))
        bucket = defects_by_vehicle.setdefault(vid, [])
        # Pre-start failed checklist items
        for idx, item in enumerate(insp.get("checklist_items") or []):
            if item.get("status") != "issue":
                continue
            d_id = _make_defect_id(inspection_id, "checklist", idx)
            bucket.append({
                "source": "prestart",
                "name": item.get("name"),
                "comment": item.get("comment"),
                "timestamp": insp.get("timestamp"),
                "severity": _classify_defect_severity(item.get("name", ""), False, False),
                "status": _status_for(d_id, insp),
            })
        # End-shift damage / incident
        if insp.get("new_damage"):
            d_id = _make_defect_id(inspection_id, "endshift", "damage")
            bucket.append({
                "source": "endshift",
                "name": "New damage reported",
                "comment": insp.get("damage_comment"),
                "timestamp": insp.get("timestamp"),
                "severity": "medium",
                "status": _status_for(d_id, insp),
            })
        if insp.get("incident_today"):
            d_id = _make_defect_id(inspection_id, "endshift", "incident")
            bucket.append({
                "source": "endshift",
                "name": "Incident reported",
                "comment": insp.get("incident_comment"),
                "timestamp": insp.get("timestamp"),
                "severity": "high",
                "status": _status_for(d_id, insp),
            })
    
    # Calculate per-vehicle score
    per_vehicle = []
    DOC_LABELS = {
        "rego_expiry": "Registration",
        "insurance_expiry": "Insurance",
        "safety_certificate_expiry": "Safety Certificate",
        "coi_expiry": "Certificate of Inspection",
    }
    for v in vehicles:
        vid = str(v.get("_id"))
        defects = [d for d in defects_by_vehicle.get(vid, []) if d.get("status") != "fixed"]
        # Days-until-expiry for each tracked date. Tuple form
        # (label, days) so the scoring helper can name the specific
        # document in the reasons list ("Rego expires in 3d").
        expiry_days_list: list = []
        expiring_docs = []
        for field, label in DOC_LABELS.items():
            val = v.get(field)
            if val:
                try:
                    exp_dt = datetime.fromisoformat(val[:10])
                    days = (exp_dt - today_dt).days
                    expiry_days_list.append((label, days))
                    if days <= 30:
                        expiring_docs.append({"name": label, "days_until_expiry": days})
                except Exception:
                    pass
        # Last inspection age
        last = last_insp_map.get(vid)
        if last:
            try:
                last_dt = datetime.fromisoformat(last.replace("Z", "")) if isinstance(last, str) else last
                last_inspection_days = (today_dt - last_dt).days
            except Exception:
                last_inspection_days = 999
        else:
            last_inspection_days = 999
        # Service + warranty alerts (owner request 2026-05-22).
        # Next service can trigger on date OR on odometer. Surface as
        # entries in expiring_docs so the UI's existing list renders them.
        svc = soonest_service_by_v.get(vid) or {}
        nsd_str = svc.get("next_service_date")
        nso_val = svc.get("next_service_odometer")
        if nsd_str:
            try:
                nsd_dt = datetime.fromisoformat(nsd_str[:10])
                d_days = (nsd_dt - today_dt).days
                if d_days <= 30:
                    expiry_days_list.append(("Next service", d_days))
                    expiring_docs.append({"name": "Next service due", "days_until_expiry": d_days})
            except Exception:
                pass
        cur_odo = v.get("current_odometer")
        if nso_val and isinstance(cur_odo, (int, float)):
            remaining_km = nso_val - cur_odo
            # Within 500km or already over: surface as an alert. Treat as
            # ~equivalent to a 7-day expiry so the score deduction lines up.
            if remaining_km <= 500:
                expiry_days_list.append((
                    f"Service @ {int(nso_val)}km",
                    0 if remaining_km <= 0 else 7,
                ))
                expiring_docs.append({
                    "name": f"Next service @ {int(nso_val)}km" + (
                        f" (overdue by {abs(int(remaining_km))}km)" if remaining_km <= 0
                        else f" (in {int(remaining_km)}km)"
                    ),
                    "days_until_expiry": 0 if remaining_km <= 0 else 7,
                })
        wu_str = latest_warranty_by_v.get(vid)
        if wu_str:
            try:
                wu_dt = datetime.fromisoformat(wu_str[:10])
                w_days = (wu_dt - today_dt).days
                if w_days <= 30:
                    expiry_days_list.append(("Warranty", w_days))
                    expiring_docs.append({"name": "Warranty", "days_until_expiry": w_days})
            except Exception:
                pass
        # Has grounding defect?
        has_grounding = any(d.get("severity") == "high" for d in defects)
        score, score_reasons = _vehicle_health_score_with_reasons(
            defects, expiry_days_list, last_inspection_days, has_grounding
        )
        per_vehicle.append({
            "vehicle_id": vid,
            "name": v.get("name"),
            "registration_number": v.get("registration_number"),
            "score": score,
            "score_reasons": score_reasons,
            "open_defects": len(defects),
            "high_severity_defects": sum(1 for d in defects if d.get("severity") == "high"),
            "last_inspection_days_ago": last_inspection_days if last_inspection_days < 999 else None,
            "tier": "healthy" if score >= 90 else ("attention" if score >= 60 else "critical"),
            "expiring_docs": expiring_docs,
        })
    
    # Sort: critical first, then attention, then healthy
    tier_order = {"critical": 0, "attention": 1, "healthy": 2}
    per_vehicle.sort(key=lambda x: (tier_order.get(x["tier"], 3), x["score"]))
    
    # Fleet score = average
    fleet_score = round(sum(v["score"] for v in per_vehicle) / len(per_vehicle)) if per_vehicle else 100
    
    # Trend: compare to score 30 days ago (simple — use today's score as baseline if no history)
    # For now, return None for trend; future enhancement can store daily snapshots
    
    return {
        "fleet_score": fleet_score,
        "fleet_tier": "healthy" if fleet_score >= 90 else ("attention" if fleet_score >= 60 else "critical"),
        "vehicle_count": len(per_vehicle),
        "healthy_count": sum(1 for v in per_vehicle if v["tier"] == "healthy"),
        "attention_count": sum(1 for v in per_vehicle if v["tier"] == "attention"),
        "critical_count": sum(1 for v in per_vehicle if v["tier"] == "critical"),
        "total_open_defects": sum(v["open_defects"] for v in per_vehicle),
        "vehicles": per_vehicle,
        "worst_vehicle": per_vehicle[0] if per_vehicle and per_vehicle[0]["tier"] != "healthy" else None,
    }


# ============== Vehicle Defect Hub (Phase 2) ==============

def _make_defect_id(inspection_id: str, source: str, idx) -> str:
    """Stable identifier for a defect derived from an inspection."""
    return f"{inspection_id}::{source}::{idx}"


@api_router.get("/vehicles/{vehicle_id}/defects")
async def get_vehicle_defects(vehicle_id: str, current_user: dict = Depends(get_current_user)):
    """
    Returns all defects for a single vehicle, aggregated from pre-start checklist
    failures + end-shift damage/incident reports across the last 90 days.
    Each defect has a stable ID, severity, source, status (open/assigned/fixed).
    """
    company_id = current_user["company_id"]
    today_dt = utcnow()
    ninety_days_ago = today_dt - timedelta(days=90)

    # Verify vehicle belongs to this company
    vehicle = await db.vehicles.find_one(
        {"_id": ObjectId(vehicle_id), "company_id": company_id},
        {"name": 1, "registration_number": 1, "make": 1, "model": 1, "year": 1, "current_odometer": 1, "vin": 1}
    )
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")

    # Pull inspections that have a problem flag.
    # Timestamp filter applied in Python (post-fetch) because MongoDB cannot compare across
    # the mix of legacy string timestamps and new BSON ISODate timestamps.
    inspections = await db.inspections.find(
        {
            "company_id": company_id,
            "vehicle_id": vehicle_id,
            "$or": [
                {"is_safe": False},
                {"new_damage": True},
                {"incident_today": True},
                {"checklist_items.status": "issue"},
            ],
        },
        {"timestamp": 1, "type": 1, "checklist_items": 1, "new_damage": 1, "incident_today": 1,
         "damage_comment": 1, "incident_comment": 1, "driver_name": 1, "driver_id": 1,
         "photos": 1, "odometer": 1}
    ).sort("timestamp", -1).to_list(500)

    # Type-tolerant 90-day filter
    def _ts_to_dt(t):
        if isinstance(t, datetime):
            return t
        if isinstance(t, str):
            try:
                return datetime.fromisoformat(t.replace("Z", "")[:26])
            except Exception:
                return None
        return None
    inspections = [i for i in inspections if (_d := _ts_to_dt(i.get("timestamp"))) and _d >= ninety_days_ago]

    # Pull status overrides for this vehicle
    status_overrides_cursor = db.defect_overrides.find(
        {"company_id": company_id, "vehicle_id": vehicle_id}
    )
    status_overrides = {doc["defect_id"]: doc async for doc in status_overrides_cursor}

    # Legacy inspections don't carry driver_name (only driver_id). Batch
    # resolve the missing names from the users collection so the defect
    # rows show real names instead of "Unknown driver".
    missing_driver_ids = {
        insp["driver_id"] for insp in inspections
        if not insp.get("driver_name") and insp.get("driver_id")
    }
    driver_name_by_id: dict = {}
    if missing_driver_ids:
        obj_ids = []
        for did in missing_driver_ids:
            try:
                obj_ids.append(ObjectId(did))
            except Exception:
                pass
        if obj_ids:
            users = await db.users.find(
                {"_id": {"$in": obj_ids}, "company_id": company_id},
                {"name": 1, "username": 1},
            ).to_list(None)
            for u in users:
                driver_name_by_id[str(u["_id"])] = u.get("name") or u.get("username") or ""

    # Build defect records
    defects = []
    name_counter: dict = {}  # to detect recurring issues

    for insp in inspections:
        inspection_id = str(insp.get("_id"))
        timestamp = insp.get("timestamp")
        driver_name = (
            insp.get("driver_name")
            or driver_name_by_id.get(insp.get("driver_id", ""))
            or "Unknown driver"
        )

        # Pre-start checklist failures
        for idx, item in enumerate(insp.get("checklist_items") or []):
            if item.get("status") != "issue":
                continue
            name = item.get("name") or "Unnamed item"
            severity = _classify_defect_severity(name, False, False)
            d_id = _make_defect_id(inspection_id, "checklist", idx)
            override = status_overrides.get(d_id, {})
            defects.append({
                "id": d_id,
                "source": "Pre-Start",
                "name": name,
                "comment": item.get("comment") or "",
                "photos": [p for p in (item.get("photos") or []) if p][:3],
                "timestamp": timestamp,
                "driver_name": driver_name,
                "severity": severity,
                "status": override.get("status", "open"),
                "assigned_to": override.get("assigned_to"),
                "fixed_date": override.get("fixed_date"),
                "fixed_cost": override.get("fixed_cost"),
                "notes": override.get("notes"),
            })
            name_counter[name.lower()] = name_counter.get(name.lower(), 0) + 1

        # End-shift damage
        if insp.get("new_damage"):
            d_id = _make_defect_id(inspection_id, "endshift", "damage")
            override = status_overrides.get(d_id, {})
            name = "Damage reported (end of shift)"
            defects.append({
                "id": d_id,
                "source": "End-Shift",
                "name": name,
                "comment": insp.get("damage_comment") or "",
                "photos": (insp.get("photos") or [])[:3],
                "timestamp": timestamp,
                "driver_name": driver_name,
                "severity": "medium",
                "status": override.get("status", "open"),
                "assigned_to": override.get("assigned_to"),
                "fixed_date": override.get("fixed_date"),
                "fixed_cost": override.get("fixed_cost"),
                "notes": override.get("notes"),
            })
            name_counter[name.lower()] = name_counter.get(name.lower(), 0) + 1

        # End-shift incident
        if insp.get("incident_today"):
            d_id = _make_defect_id(inspection_id, "endshift", "incident")
            override = status_overrides.get(d_id, {})
            name = "Incident reported"
            defects.append({
                "id": d_id,
                "source": "End-Shift",
                "name": name,
                "comment": insp.get("incident_comment") or "",
                "photos": (insp.get("photos") or [])[:3],
                "timestamp": timestamp,
                "driver_name": driver_name,
                "severity": "high",
                "status": override.get("status", "open"),
                "assigned_to": override.get("assigned_to"),
                "fixed_date": override.get("fixed_date"),
                "fixed_cost": override.get("fixed_cost"),
                "notes": override.get("notes"),
            })
            name_counter[name.lower()] = name_counter.get(name.lower(), 0) + 1

    # Detect recurring issues (3+ in last 30 days)
    thirty_days_ago_dt = today_dt - timedelta(days=30)
    def _ts_to_dt(t):
        if isinstance(t, datetime):
            return t
        if isinstance(t, str):
            try:
                return datetime.fromisoformat(t.replace("Z", "")[:26])
            except Exception:
                return None
        return None
    recurring_counter: dict = {}
    for d in defects:
        d_dt = _ts_to_dt(d.get("timestamp"))
        if d_dt and d_dt >= thirty_days_ago_dt:
            key = d["name"].lower()
            recurring_counter[key] = recurring_counter.get(key, 0) + 1
    recurring = [{"name": k, "count": v} for k, v in recurring_counter.items() if v >= 3]
    recurring.sort(key=lambda x: x["count"], reverse=True)

    # Mark recurring on each defect
    recurring_keys = {r["name"] for r in recurring}
    for d in defects:
        d["is_recurring"] = d["name"].lower() in recurring_keys

    # Sort: open first (severity desc, then date desc), then assigned, then fixed
    status_order = {"open": 0, "assigned": 1, "in_progress": 1, "fixed": 2}
    severity_order = {"high": 0, "medium": 1, "low": 2}
    def _ts_for_sort(d):
        d_dt = _ts_to_dt(d.get("timestamp"))
        return d_dt.timestamp() if d_dt else 0
    defects.sort(key=lambda d: (
        status_order.get(d["status"], 3),
        severity_order.get(d["severity"], 3),
        -1 * _ts_for_sort(d),
    ))

    # Convert all defect timestamps to ISO strings for JSON serialization
    for d in defects:
        d_dt = _ts_to_dt(d.get("timestamp"))
        if d_dt:
            d["timestamp"] = d_dt.isoformat()

    open_defects = [d for d in defects if d["status"] != "fixed"]
    fixed_defects = [d for d in defects if d["status"] == "fixed"]

    return {
        "vehicle": {
            "id": vehicle_id,
            "name": vehicle.get("name"),
            "registration_number": vehicle.get("registration_number"),
            "make": vehicle.get("make"),
            "model": vehicle.get("model"),
            "year": vehicle.get("year"),
            "vin": vehicle.get("vin"),
            "current_odometer": vehicle.get("current_odometer"),
        },
        "open_count": len(open_defects),
        "fixed_count": len(fixed_defects),
        "high_severity_count": sum(1 for d in open_defects if d["severity"] == "high"),
        "open_defects": open_defects,
        "fixed_defects": fixed_defects[:20],  # cap fixed history at 20
        "recurring_issues": recurring,
    }


class DefectStatusUpdate(BaseModel):
    status: str  # open / assigned / in_progress / fixed
    assigned_to: Optional[str] = None
    fixed_date: Optional[str] = None
    fixed_cost: Optional[float] = None
    notes: Optional[str] = None


@api_router.patch("/defects/{defect_id}/status")
async def update_defect_status(
    defect_id: str,
    update: DefectStatusUpdate,
    current_user: dict = Depends(get_current_user),
):
    """Update the status of a single defect. defect_id is a synthetic ID."""
    if update.status not in ("open", "assigned", "in_progress", "fixed"):
        raise HTTPException(status_code=400, detail="Invalid status")

    # Extract inspection_id from synthetic id ("inspection_id::source::index")
    parts = defect_id.split("::")
    if len(parts) != 3:
        raise HTTPException(status_code=400, detail="Malformed defect_id")
    inspection_id = parts[0]

    # Verify the inspection belongs to this user's company
    try:
        inspection_oid = ObjectId(inspection_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed defect_id")
    insp = await db.inspections.find_one(
        {"_id": inspection_oid, "company_id": current_user["company_id"]},
        {"vehicle_id": 1}
    )
    if not insp:
        raise HTTPException(status_code=404, detail="Defect source inspection not found")

    update_doc = {
        "defect_id": defect_id,
        "company_id": current_user["company_id"],
        "vehicle_id": insp.get("vehicle_id"),
        "status": update.status,
        "assigned_to": update.assigned_to,
        "fixed_date": update.fixed_date or (utcnow().isoformat() if update.status == "fixed" else None),
        "fixed_cost": update.fixed_cost,
        "notes": update.notes,
        "updated_by": current_user.get("user_id"),
        "updated_at": utcnow().isoformat(),
    }
    await db.defect_overrides.update_one(
        {"defect_id": defect_id, "company_id": current_user["company_id"]},
        {"$set": update_doc},
        upsert=True,
    )
    return {"status": "success", "defect_id": defect_id, "new_status": update.status}


class WorkshopExtraItem(BaseModel):
    """Free-form item the user wants on the workshop email that is not
    backed by an existing defect record (oil leak, scheduled oiling,
    "while you're at it" requests, etc)."""
    type: str  # 'oil_leak' | 'oiling' | 'other' | free-form
    note: Optional[str] = None
    severity: Optional[str] = None  # 'low' | 'medium' | 'high'


class WorkshopEmailRequest(BaseModel):
    workshop_email: str
    workshop_name: Optional[str] = None
    message: Optional[str] = None
    defect_ids: Optional[list] = None  # if provided, only these defects; else all open
    extra_items: Optional[List[WorkshopExtraItem]] = None  # add-on requests beyond defects


def _workshop_extras_html(extras: List[WorkshopExtraItem]) -> str:
    """Render the 'Additional requests' block on the workshop email.
    Caller has already filtered to a non-empty list.
    """
    # Type → human label map. Anything outside the known set falls back to title-case.
    LABELS = {
        "oil_leak": "Oil leak",
        "oiling": "Scheduled oiling / lubrication",
        "tyre_check": "Tyre check",
        "brake_check": "Brake inspection",
        "battery": "Battery check",
        "ac_service": "Air-con service",
        "general_service": "General service",
        "other": "Additional item",
    }
    SEV_COLORS = {"high": "#dc2626", "medium": "#f59e0b", "low": "#64748b"}

    rows = ""
    for i, it in enumerate(extras, start=1):
        label = LABELS.get(it.type, it.type.replace("_", " ").title())
        sev_color = SEV_COLORS.get((it.severity or "low"), "#64748b")
        note_html = (
            f"<div style='color:#475569; margin-top:6px;'>{_safe_html(it.note)}</div>"
            if it.note else ""
        )
        sev_badge = (
            f"<span style='display:inline-block; padding:3px 10px; border-radius:999px; "
            f"background:{sev_color}; color:white; font-size:11px; font-weight:600; "
            f"text-transform:uppercase; margin-top:6px;'>{(it.severity or 'low')}</span>"
        )
        rows += f"""
        <tr>
          <td style="padding:14px; border-bottom:1px solid #e2e8f0; vertical-align:top;">
            <div style="font-size:11px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.5px;">+ EXTRA · #{i}</div>
            <div style="font-weight:600; color:#0f172a; font-size:15px; margin-top:4px;">{_safe_html(label)}</div>
            {note_html}
            {sev_badge}
          </td>
        </tr>
        """
    return f"""
    <h2 style="font-size:16px; color:#0f172a; margin:24px 0 12px;">Additional requests ({len(extras)})</h2>
    <table style="width:100%; border-collapse:collapse; background:white; border:1px solid #e2e8f0; border-radius:8px; overflow:hidden;">
      {rows}
    </table>
    """


@api_router.post("/vehicles/{vehicle_id}/email-workshop")
async def email_defects_to_workshop(
    vehicle_id: str,
    request: WorkshopEmailRequest,
    current_user: dict = Depends(get_current_user),
):
    """Send a defect summary email (HTML) to a workshop or mechanic."""
    if "@" not in (request.workshop_email or ""):
        raise HTTPException(status_code=400, detail="Valid workshop email required")

    # Reuse the defect aggregation logic
    defects_payload = await get_vehicle_defects(vehicle_id, current_user=current_user)
    open_defects = defects_payload["open_defects"]
    if request.defect_ids is not None:
        # Empty list = "no defects, only extras" — different from None ("send all open").
        open_defects = [d for d in open_defects if d["id"] in request.defect_ids]

    extras = request.extra_items or []
    if not open_defects and not extras:
        raise HTTPException(status_code=400, detail="Nothing to send — pick at least one defect or add an extra item")

    vehicle = defects_payload["vehicle"]
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])}, {"name": 1})
    company_name = company.get("name", "FleetShield365 Customer") if company else "FleetShield365 Customer"

    # Build HTML
    workshop_name = (request.workshop_name or "Workshop").strip()
    custom_msg = (request.message or "").strip()
    rows_html = ""
    for i, d in enumerate(open_defects, start=1):
        sev_color = {"high": "#dc2626", "medium": "#f59e0b", "low": "#64748b"}.get(d["severity"], "#64748b")
        ts_short = (d.get("timestamp") or "")[:10]
        comment_html = f"<div style='color:#475569; margin-top:6px;'>{d.get('comment') or ''}</div>" if d.get("comment") else ""
        rows_html += f"""
        <tr>
          <td style="padding:14px; border-bottom:1px solid #e2e8f0; vertical-align:top;">
            <div style="font-size:11px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.5px;">#{i} · {d.get('source')} · {ts_short}</div>
            <div style="font-weight:600; color:#0f172a; font-size:15px; margin-top:4px;">{d.get('name')}</div>
            {comment_html}
            <div style="margin-top:6px;">
              <span style="display:inline-block; padding:3px 10px; border-radius:999px; background:{sev_color}; color:white; font-size:11px; font-weight:600; text-transform:uppercase;">{d.get('severity')}</span>
              <span style="color:#64748b; font-size:12px; margin-left:8px;">Reported by {d.get('driver_name')}</span>
            </div>
          </td>
        </tr>
        """

    custom_block = f"""
        <div style="background:#f1f5f9; padding:16px 20px; border-radius:8px; margin-bottom:24px; border-left:4px solid #0891b2;">
          <div style="color:#0f172a; white-space:pre-wrap; line-height:1.6;">{custom_msg}</div>
        </div>
    """ if custom_msg else ""

    html = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding:0; margin:0; background:#f8fafc;">
      <div style="max-width:680px; margin:0 auto; background:white;">
        <div style="background:linear-gradient(135deg, #0d9488, #0891b2); padding:24px 28px; color:white;">
          <h1 style="margin:0; font-size:22px;">Defect Repair Request</h1>
          <p style="margin:4px 0 0 0; opacity:0.9; font-size:14px;">{company_name} · {utcnow().strftime('%d %B %Y')}</p>
        </div>
        <div style="padding:28px;">
          <p style="color:#0f172a; line-height:1.6;">Hi {workshop_name},</p>
          <p style="color:#475569; line-height:1.6;">
            Please find below the open defects on the following vehicle. We'd appreciate your assessment and quote at your earliest convenience.
          </p>
          {custom_block}

          <div style="background:#f8fafc; padding:18px 20px; border-radius:8px; margin-bottom:20px; border:1px solid #e2e8f0;">
            <div style="font-size:12px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.5px;">Vehicle</div>
            <div style="font-size:18px; font-weight:600; color:#0f172a; margin-top:4px;">{vehicle.get('name') or 'Unknown'}</div>
            <table style="margin-top:10px; width:100%; font-size:13px; color:#475569;">
              <tr><td style="padding:2px 0;"><b>Rego:</b></td><td>{vehicle.get('registration_number') or '-'}</td></tr>
              <tr><td style="padding:2px 0;"><b>Make / Model:</b></td><td>{(vehicle.get('make') or '-')} {(vehicle.get('model') or '')}</td></tr>
              <tr><td style="padding:2px 0;"><b>Year:</b></td><td>{vehicle.get('year') or '-'}</td></tr>
              <tr><td style="padding:2px 0;"><b>Odometer:</b></td><td>{vehicle.get('current_odometer') or '-'} km</td></tr>
              <tr><td style="padding:2px 0;"><b>VIN:</b></td><td>{vehicle.get('vin') or '-'}</td></tr>
            </table>
          </div>

          {("<h2 style=\"font-size:16px; color:#0f172a; margin:24px 0 12px;\">Open Defects (" + str(len(open_defects)) + ")</h2><table style=\"width:100%; border-collapse:collapse; background:white; border:1px solid #e2e8f0; border-radius:8px; overflow:hidden;\">" + rows_html + "</table>") if open_defects else ""}

          {_workshop_extras_html(extras) if extras else ""}

          <p style="color:#64748b; font-size:13px; margin-top:28px; line-height:1.6;">
            All defects above include photo evidence and GPS-stamped timestamps in our compliance records.
            Please reply to this email with your quote or call us if you need additional details.
          </p>

          <hr style="border:none; border-top:1px solid #e2e8f0; margin:24px 0;">
          <p style="color:#94a3b8; font-size:11px; text-align:center;">
            Sent via FleetShield365 — Australian Fleet Compliance Platform<br>
            <a href="https://www.fleetshield365.com" style="color:#0891b2;">www.fleetshield365.com</a>
          </p>
        </div>
      </div>
    </body>
    </html>
    """

    subject = f"[{company_name}] Defect Repair Request — {vehicle.get('name')} ({vehicle.get('registration_number')})"
    sent = await send_email_notification(request.workshop_email, subject, html)

    if not sent:
        raise HTTPException(status_code=500, detail="Failed to send email. Please try again.")

    # Persist log
    try:
        await db.workshop_emails.insert_one({
            "id": str(uuid.uuid4()),
            "vehicle_id": vehicle_id,
            "company_id": current_user["company_id"],
            "workshop_email": request.workshop_email,
            "workshop_name": workshop_name,
            "defect_count": len(open_defects),
            "defect_ids": [d["id"] for d in open_defects],
            "sent_by": current_user.get("user_id"),
            "sent_at": utcnow().isoformat(),
        })
    except Exception as e:
        logger.error(f"[WORKSHOP_EMAIL] Failed to persist log: {e}")

    # 2026-05-19 — remember the workshop email/name on the company so the
    # next "Email Defects" modal opens pre-filled. Best-effort: if the
    # company doc isn't writable for some reason we still return success.
    try:
        await db.companies.update_one(
            {"_id": ObjectId(current_user["company_id"])},
            {"$set": {
                "workshop_email_default": request.workshop_email,
                "workshop_name_default": workshop_name,
            }},
        )
    except Exception as e:
        logger.warning(f"[WORKSHOP_EMAIL] Failed to persist default: {e}")

    # Auto-flip status: open -> assigned for every defect we just emailed
    try:
        for d in open_defects:
            if d.get("status") == "open":
                await db.defect_overrides.update_one(
                    {"company_id": current_user["company_id"], "vehicle_id": vehicle_id, "defect_id": d["id"]},
                    {"$set": {
                        "status": "assigned",
                        "assigned_to": workshop_name,
                        "assigned_at": utcnow().isoformat(),
                    }},
                    upsert=True,
                )
    except Exception as e:
        logger.error(f"[WORKSHOP_EMAIL] Failed to auto-flip statuses: {e}")

    return {
        "status": "success",
        "message": f"Sent {len(open_defects)} defect(s) to {request.workshop_email}",
        "defect_count": len(open_defects),
    }


@api_router.get("/dashboard/chart-data")
async def get_dashboard_chart_data(
    current_user: dict = Depends(get_current_user),
    days: int = 7
):
    """Get weekly inspection and issue data for dashboard charts - OPTIMIZED"""
    company_id = current_user["company_id"]
    
    # Check cache first
    cache_key = f"chart_data_{days}"
    cached = get_cached(cache_key, company_id)
    if cached:
        return cached
    
    # Limit to reasonable range
    days = min(max(days, 7), 30)
    
    # Calculate date range
    end_date = utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    start_date = end_date - timedelta(days=days)
    
    # Single aggregation query for inspections grouped by day
    inspection_pipeline = [
        {
            "$match": {
                "company_id": company_id,
                "timestamp": {"$gte": start_date, "$lt": end_date}
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}
                },
                "inspections": {"$sum": 1},
                "issues": {"$sum": {"$cond": [{"$eq": ["$is_safe", False]}, 1, 0]}}
            }
        }
    ]
    
    # Single aggregation for fuel grouped by day
    fuel_pipeline = [
        {
            "$match": {
                "company_id": company_id,
                "timestamp": {"$gte": start_date, "$lt": end_date}
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}
                },
                "fuel": {"$sum": "$amount"}
            }
        }
    ]
    
    # Run both queries in parallel
    inspection_data, fuel_data = await asyncio.gather(
        db.inspections.aggregate(inspection_pipeline).to_list(days),
        db.fuel_submissions.aggregate(fuel_pipeline).to_list(days)
    )
    
    # Convert to lookup dictionaries
    inspection_lookup = {d["_id"]: d for d in inspection_data}
    fuel_lookup = {d["_id"]: d for d in fuel_data}
    
    # Build response with all days
    chart_data = []
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    for i in range(days - 1, -1, -1):
        day_date = utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=i)
        date_str = day_date.strftime("%Y-%m-%d")
        
        insp_data = inspection_lookup.get(date_str, {})
        fuel_amt = fuel_lookup.get(date_str, {}).get("fuel", 0)
        
        chart_data.append({
            "day": day_names[day_date.weekday()],
            "date": date_str,
            "inspections": insp_data.get("inspections", 0),
            "issues": insp_data.get("issues", 0),
            "fuel": round(fuel_amt, 2) if fuel_amt else 0
        })
    
    # Cache for 30 seconds
    cache_key = f"chart_data_{days}"
    set_cached(cache_key, company_id, chart_data)
    
    return chart_data



# ============== Audit Trail ==============

@api_router.get("/audit-trail")
async def get_audit_trail(
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """Filterable audit log query (Phase 4).

    Tenant-scoped (every record fetched is restricted to users that
    belong to the caller's company). Filters compose; missing filters
    are no-op. ``date_from`` / ``date_to`` are YYYY-MM-DD; range
    capped at 365 days to bound the worst-case scan.
    """
    if current_user["role"] != UserRole.SUPER_ADMIN:
        raise HTTPException(status_code=403, detail="Only Super Admin can view audit trail")

    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)

    # Tenant scoping — derive the set of user_ids that belong to this
    # company so audit rows for other tenants never leak in.
    company_users = await db.users.distinct("_id", {"company_id": current_user["company_id"]})
    tenant_user_ids = [str(uid) for uid in company_users]

    if user_id:
        # When the caller filters by a specific user_id, confirm it's
        # one of theirs — otherwise return empty rather than leaking.
        if user_id not in tenant_user_ids:
            return []
        query: dict = {"user_id": user_id}
    else:
        query = {"user_id": {"$in": tenant_user_ids}}

    if entity_type:
        query["entity_type"] = entity_type
    if entity_id:
        query["entity_id"] = entity_id
    if action:
        query["action"] = action

    # Optional date-range filter on the timestamp field.
    if date_from or date_to:
        ts_filter: dict = {}
        if date_from:
            try:
                df = datetime.fromisoformat(date_from)
                ts_filter["$gte"] = df
            except ValueError:
                raise HTTPException(status_code=400, detail="date_from must be YYYY-MM-DD")
        if date_to:
            try:
                dt = datetime.fromisoformat(date_to) + timedelta(days=1)
                ts_filter["$lt"] = dt
            except ValueError:
                raise HTTPException(status_code=400, detail="date_to must be YYYY-MM-DD")
        if "$gte" in ts_filter and "$lt" in ts_filter:
            if (ts_filter["$lt"] - ts_filter["$gte"]).days > 366:
                raise HTTPException(status_code=400, detail="Range capped at 365 days")
        query["timestamp"] = ts_filter

    trail = await db.audit_trail.find(query).sort("timestamp", -1) \
        .skip(actual_offset).limit(actual_limit).to_list(actual_limit)
    return serialize_doc(trail)


# ============== Phase 7 — In-app notifications ==============
#
# Lightweight notification system distinct from email alerts. Each
# row is per-user and carries a deep-link the UI can route to on tap.
# Email + push (when wired) live alongside, not instead of, this in-
# app feed.

class NotificationCreate(BaseModel):
    title: str
    body: str
    deep_link: Optional[str] = None  # e.g. "/vehicles/<id>" or "/incidents/<id>"
    audience: str  # "all_drivers" | "all_admins" | "user"
    user_id: Optional[str] = None  # when audience == "user"


async def _emit_notifications(
    company_id: str,
    *,
    title: str,
    body: str,
    deep_link: Optional[str],
    user_ids: List[str],
    kind: str = "info",
    actor_id: Optional[str] = None,
) -> int:
    """Insert one row per recipient. Returns the count inserted.

    Centralises every site that creates an in-app notification so the
    schema stays consistent across email-triggered (incident, expiry)
    and admin-triggered (broadcast) origins.
    """
    if not user_ids:
        return 0
    now = utcnow()
    docs = [
        {
            "_id": ObjectId(),
            "company_id": company_id,
            "user_id": uid,
            "title": title,
            "body": body,
            "deep_link": deep_link,
            "kind": kind,
            "read": False,
            "created_at": now,
            "created_by": actor_id,
        }
        for uid in set(user_ids)
    ]
    await db.notifications.insert_many(docs)
    return len(docs)


@api_router.get("/notifications")
async def list_notifications(
    unread_only: bool = False,
    limit: int = 50,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """Per-user in-app notification feed (Phase 7 of TODO.md)."""
    actual_limit = max(1, min(limit, 200))
    actual_offset = max(0, offset)
    query: dict = {"user_id": str(current_user["_id"])}
    if unread_only:
        query["read"] = False
    rows = await db.notifications.find(query).sort("created_at", -1) \
        .skip(actual_offset).limit(actual_limit).to_list(actual_limit)
    unread = await db.notifications.count_documents({
        "user_id": str(current_user["_id"]),
        "read": False,
    })
    return {
        "items": serialize_doc(rows),
        "unread": unread,
    }


@api_router.put("/notifications/{notif_id}/read")
async def mark_notification_read(
    notif_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Mark a single notification as read."""
    try:
        oid = ObjectId(notif_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid notification id")
    res = await db.notifications.update_one(
        {"_id": oid, "user_id": str(current_user["_id"])},
        {"$set": {"read": True, "read_at": utcnow()}},
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"status": "read"}


@api_router.put("/notifications/read-all")
async def mark_all_notifications_read(
    current_user: dict = Depends(get_current_user),
):
    """Bulk mark every unread notification for the caller as read."""
    res = await db.notifications.update_many(
        {"user_id": str(current_user["_id"]), "read": False},
        {"$set": {"read": True, "read_at": utcnow()}},
    )
    return {"updated": res.modified_count}


@api_router.post("/notifications/send")
async def admin_send_notification(
    payload: NotificationCreate,
    current_user: dict = Depends(require_active_tenant),
):
    """Admin-triggered broadcast to drivers / admins / a single user.

    Phase 7 of TODO.md. The audience is resolved at send time so a
    "all drivers" broadcast goes to the current driver roster (not a
    frozen snapshot from when the message was composed).
    """
    if current_user.get("role") not in (UserRole.SUPER_ADMIN, UserRole.ADMIN):
        raise HTTPException(status_code=403, detail="Not authorized")

    company_id = current_user["company_id"]
    audience = (payload.audience or "").strip()
    user_ids: List[str] = []

    if audience == "all_drivers":
        rows = await db.users.find({
            **_soft_delete_filter(),
            "company_id": company_id,
            "role": UserRole.DRIVER,
        }, {"_id": 1}).to_list(2000)
        user_ids = [str(r["_id"]) for r in rows]
    elif audience == "all_admins":
        rows = await db.users.find({
            **_soft_delete_filter(),
            "company_id": company_id,
            "role": {"$in": [UserRole.ADMIN, UserRole.SUPER_ADMIN]},
        }, {"_id": 1}).to_list(2000)
        user_ids = [str(r["_id"]) for r in rows]
    elif audience == "user":
        if not payload.user_id:
            raise HTTPException(status_code=400, detail="user_id required for audience=user")
        # Verify the target belongs to the caller's tenant.
        target = await db.users.find_one({
            "_id": ObjectId(payload.user_id),
            "company_id": company_id,
        }, {"_id": 1})
        if not target:
            raise HTTPException(status_code=404, detail="Target user not found in your company")
        user_ids = [str(target["_id"])]
    else:
        raise HTTPException(status_code=400, detail="audience must be all_drivers | all_admins | user")

    count = await _emit_notifications(
        company_id,
        title=payload.title,
        body=payload.body,
        deep_link=payload.deep_link,
        user_ids=user_ids,
        kind="broadcast",
        actor_id=str(current_user["_id"]),
    )
    return {"sent": count, "audience": audience}


# ============== Phase 4 — Trash / Restore / Purge ==============

# All endpoints below require admin role. Tenant-scoped by company_id
# so a company owner can never see/restore/purge another tenant's rows.

_TRASH_COLLECTIONS: tuple = (
    "vehicles",
    "users",
    "service_records",
    "maintenance_logs",
    "incidents",
)


@api_router.get("/admin/recently-deleted")
async def get_recently_deleted(
    collection: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """List soft-deleted rows across collections for the current tenant.

    Phase 4 of TODO.md. The Trash view in the web admin panel reads
    this. Default returns rows from all collections; pass ?collection=
    to scope to one. Each item carries the source ``_collection``
    field so the UI can render type-specific badges.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    actual_limit = max(1, min(limit, 500))
    actual_offset = max(0, offset)
    company_id = current_user["company_id"]

    cols = (collection,) if collection else _TRASH_COLLECTIONS
    if collection and collection not in _TRASH_COLLECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown collection. Allowed: {list(_TRASH_COLLECTIONS)}",
        )

    out: list = []
    for coll_name in cols:
        rows = await db[coll_name].find({
            "deleted_at": {"$ne": None, "$exists": True},
            "company_id": company_id,
        }).sort("deleted_at", -1).to_list(actual_limit)
        for row in rows:
            row["_collection"] = coll_name
        out.extend(rows)

    # Sort across collections by deleted_at desc.
    out.sort(key=lambda r: r.get("deleted_at") or datetime.min, reverse=True)
    out = out[actual_offset : actual_offset + actual_limit]
    return sanitize_user_doc(serialize_doc(out))


@api_router.post("/admin/restore/{collection}/{doc_id}")
async def restore_deleted_doc(
    collection: str,
    doc_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Restore a soft-deleted row (unset deleted_at + deleted_by)."""
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    if collection not in _TRASH_COLLECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown collection. Allowed: {list(_TRASH_COLLECTIONS)}",
        )

    company_id = current_user["company_id"]
    res = await db[collection].update_one(
        {
            "_id": ObjectId(doc_id),
            "company_id": company_id,
            "deleted_at": {"$ne": None, "$exists": True},
        },
        _restore_update(),
    )
    if res.matched_count == 0:
        raise HTTPException(
            status_code=404,
            detail="Deleted document not found (already restored?)",
        )
    # Bust caches that may have memoised the empty-of-this-row state.
    if collection == "vehicles":
        invalidate_cache("vehicles", company_id)
    elif collection == "users":
        invalidate_cache("drivers", company_id)
    return {"message": "Restored", "collection": collection, "id": doc_id}


@api_router.post("/admin/purge-old-deleted")
async def purge_old_deleted(
    confirm: str = "",
    current_user: dict = Depends(get_current_user),
):
    """Permanently remove rows soft-deleted more than SOFT_DELETE_GRACE_DAYS ago.

    Phase 4 of TODO.md — manual button only, no auto-cron. The admin
    must pass ``?confirm=PURGE`` (matches the existing
    ``DELETE_EVERYTHING`` convention on /developer/clear-all) so a
    misclick can't drop 30-day-old recoverable data.

    Returns a per-collection count of rows actually removed.
    """
    if current_user["role"] not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    if confirm != "PURGE":
        raise HTTPException(
            status_code=400,
            detail="Pass ?confirm=PURGE to confirm permanent removal",
        )

    company_id = current_user["company_id"]
    cutoff = utcnow() - timedelta(days=SOFT_DELETE_GRACE_DAYS)

    purged: dict = {}
    for coll_name in _TRASH_COLLECTIONS:
        res = await db[coll_name].delete_many({
            "company_id": company_id,
            "deleted_at": {"$lt": cutoff},
        })
        if res.deleted_count:
            purged[coll_name] = res.deleted_count

    await log_audit_trail(
        str(current_user["_id"]),
        "purge_old_deleted",
        "admin",
        None,
        "",
    )

    return {
        "message": (
            f"Purged rows deleted more than {SOFT_DELETE_GRACE_DAYS} days ago"
        ),
        "cutoff": cutoff.isoformat() + "Z",
        "removed": purged,
    }

# ============== Stripe per-vehicle quantity sync ==============
#
# Vehicle add/remove → Stripe subscription quantity. The registration
# flow creates a two-line-item subscription (base + per_vehicle); this
# helper finds the per_vehicle line and modifies its quantity to match
# `companies.vehicle_count`. Stripe handles proration on the next
# invoice (`proration_behavior='create_prorations'`).
#
# Guarded behind STRIPE_SECRET_KEY presence — no-op when Stripe isn't
# configured (yet) or the tenant has no `stripe_subscription_id`
# (still on trial / cancelled / sign-up never completed checkout).
# Wrapped in try/except so a Stripe outage never blocks vehicle CRUD.

async def _sync_vehicle_quantity_to_stripe(company_id: str) -> None:
    if not stripe.api_key:
        return
    try:
        company = await db.companies.find_one(
            {"_id": ObjectId(company_id)},
            {"stripe_subscription_id": 1},
        )
        if not company:
            return
        sub_id = company.get("stripe_subscription_id")
        if not sub_id:
            return  # trial / not yet on a paid sub

        # Live count of non-deleted vehicles for the tenant — beats
        # the stored counters which can drift across restores/purges.
        vehicle_count = await db.vehicles.count_documents({
            "company_id": company_id,
            "deleted_at": None,
        })
        vehicle_count = max(1, vehicle_count)
        sub = stripe.Subscription.retrieve(sub_id)
        items = (sub.get("items") or {}).get("data") or []
        # Locate the per-vehicle line. We tag it via product name in
        # the registration flow; fall back to the second line item if
        # naming doesn't match (sub created before this change).
        target = None
        for it in items:
            price = it.get("price") or {}
            product = price.get("product")
            # Stripe returns product as either an id or an object —
            # handle both.
            name = ""
            if isinstance(product, dict):
                name = (product.get("name") or "").lower()
            if "per vehicle" in name or "per_vehicle" in name:
                target = it
                break
        if target is None and len(items) >= 2:
            target = items[1]
        if target is None:
            logger.warning("[stripe_sync] no per-vehicle line item for sub %s", sub_id)
            return
        if int(target.get("quantity") or 0) == vehicle_count:
            return  # nothing to do
        stripe.Subscription.modify(
            sub_id,
            items=[{"id": target["id"], "quantity": vehicle_count}],
            proration_behavior="create_prorations",
        )
        logger.info("[stripe_sync] sub %s quantity → %s", sub_id, vehicle_count)
    except Exception as e:
        logger.exception("[stripe_sync] failed for company %s: %s", company_id, e)


# ============== OTP-first company registration (2026-05-25) ==============
#
# New 2-step registration. `register-request` validates the form, sends
# a 6-digit OTP to the email, and parks the pending registration in a
# TTL-cleaned collection. `register-verify` checks the OTP and only
# THEN creates the company + super_admin user. The legacy
# `register-company` endpoint below stays for backward compat with old
# mobile builds, but the web register flow now goes through these two.

class RegisterRequestPayload(CompanyRegister):
    pass


class RegisterVerifyPayload(BaseModel):
    email: EmailStr
    otp: str


def _gen_otp() -> str:
    import secrets as _s
    return f"{_s.randbelow(1_000_000):06d}"


@api_router.post("/auth/register-request")
@limiter.limit("3/minute")
async def register_request(data: RegisterRequestPayload, request: Request):
    """Step 1 of OTP registration. Validates form, sends OTP email."""
    validate_password_policy(data.password)
    email_norm = data.email.lower()

    # Reject if the email is already an active account.
    if await db.users.find_one({"email": email_norm}):
        raise HTTPException(status_code=400, detail="Email already registered")

    # Validate / pre-allocate the subdomain so step 2 can't 409 after the
    # user has already typed the OTP.
    try:
        if data.subdomain is not None:
            resolved_subdomain = validate_subdomain(data.subdomain)
            await ensure_subdomain_unique(resolved_subdomain, db)
        else:
            resolved_subdomain = await slug_generator(data.company_name, db)
    except SubdomainValidationError as exc:
        raise _subdomain_error_to_http(exc)

    otp = _gen_otp()
    otp_hash = get_password_hash(otp)
    pending_doc = {
        "email": email_norm,
        "name": data.name,
        "company_name": data.company_name,
        "password_hash": get_password_hash(data.password),
        "subdomain": resolved_subdomain,
        "vehicle_count": data.vehicle_count,
        "role": data.role or UserRole.SUPER_ADMIN,
        "timezone": data.timezone or DEFAULT_TIMEZONE,
        "country": data.country,
        "preferred_currency": data.preferred_currency,
        "units_system": data.units_system,
        "locale": data.locale,
        "origin_url": data.origin_url,
        "otp_hash": otp_hash,
        "attempts": 0,
        "expires_at": utcnow() + timedelta(minutes=15),
        "created_at": utcnow(),
    }
    # Overwrite any earlier pending row for this email — they may have
    # just requested a fresh OTP.
    await db.pending_registrations.update_one(
        {"email": email_norm}, {"$set": pending_doc}, upsert=True,
    )

    # Send the OTP via the noreply mailbox using the branded template.
    subject = f"Your FleetShield365 verification code: {otp}"
    html = _email_template_branded(
        heading="Verify your email",
        body_html=(
            f"<p>Hi {(_safe_html(data.name) or 'there')},</p>"
            f"<p>Your FleetShield365 verification code is:</p>"
            f"<p style=\"font-size: 32px; font-weight: 700; letter-spacing: 6px; "
            f"text-align: center; color: #0891B2; margin: 24px 0;\">{otp}</p>"
            f"<p>This code expires in 15 minutes. If you didn't request it, ignore this email.</p>"
        ),
    )
    try:
        await send_system_email(email_norm, subject, html)
    except Exception as exc:
        logger.warning("register-request OTP send failed for %s: %s", email_norm, exc)

    return {"status": "otp_sent", "email": email_norm}


@api_router.post("/auth/register-verify")
@limiter.limit("10/minute")
async def register_verify(data: RegisterVerifyPayload, request: Request):
    """Step 2 of OTP registration. Verifies OTP, creates company + user."""
    email_norm = data.email.lower()
    pending = await db.pending_registrations.find_one({"email": email_norm})
    if not pending:
        raise HTTPException(status_code=404, detail="No pending registration for this email. Start over.")
    if pending.get("expires_at") and pending["expires_at"] < utcnow():
        await db.pending_registrations.delete_one({"_id": pending["_id"]})
        raise HTTPException(status_code=400, detail="Verification code expired. Start over.")
    attempts = int(pending.get("attempts", 0))
    if attempts >= 5:
        await db.pending_registrations.delete_one({"_id": pending["_id"]})
        raise HTTPException(status_code=429, detail="Too many wrong codes. Start over.")
    if not verify_password(data.otp.strip(), pending["otp_hash"]):
        await db.pending_registrations.update_one(
            {"_id": pending["_id"]}, {"$inc": {"attempts": 1}},
        )
        raise HTTPException(status_code=400, detail="Incorrect verification code.")

    # OTP correct — create company + admin user.
    pricing_now = await get_pricing()
    trial_on = bool(pricing_now.get("trial_enabled", True))
    norm_country = (pending.get("country") or "").upper()[:2] or None
    units = pending.get("units_system") or ("imperial" if norm_country in {"US", "LR", "MM"} else "metric")
    currency = (pending.get("preferred_currency") or "").upper()[:3] or None

    company_doc = {
        "name": pending["company_name"],
        "subdomain": pending["subdomain"],
        "vehicle_count": pending["vehicle_count"],
        "subscription_status": "trialing" if trial_on else "past_due",
        "subscription_plan": "pro",
        "trial_end": (
            (utcnow() + timedelta(days=pricing_now["trial_days"])).isoformat()
            if trial_on else None
        ),
        "max_vehicles": pricing_now.get("trial_max_vehicles") if trial_on else 0,
        "stripe_customer_id": None,
        "stripe_subscription_id": None,
        "timezone": pending.get("timezone") or DEFAULT_TIMEZONE,
        "country": norm_country,
        "preferred_currency": currency,
        "units_system": units,
        "locale": pending.get("locale"),
        "created_at": utcnow().isoformat(),
    }
    company_result = await db.companies.insert_one(company_doc)
    company_id = str(company_result.inserted_id)
    user_role = pending.get("role") if pending.get("role") in [UserRole.SUPER_ADMIN, UserRole.ADMIN] else UserRole.SUPER_ADMIN
    user_doc = {
        "email": email_norm,
        "password_hash": pending["password_hash"],
        "name": pending["name"],
        "role": user_role,
        "company_id": company_id,
        "email_verified": True,
        "email_verified_at": utcnow(),
        "created_at": utcnow().isoformat(),
    }
    user_result = await db.users.insert_one(user_doc)
    user_doc["_id"] = user_result.inserted_id

    # Clean up pending row.
    await db.pending_registrations.delete_one({"_id": pending["_id"]})

    # Welcome email (best-effort).
    try:
        welcome_html = _email_template_branded(
            heading=f"Welcome to FleetShield365, {_safe_html(pending['name']) or 'there'}!",
            body_html=(
                f"<p>Your fleet account <strong>{_safe_html(pending['company_name'])}</strong> is live.</p>"
                f"<p>Sign in at "
                f"<a href=\"https://{pending['subdomain']}.fleetshield365.com\">"
                f"{pending['subdomain']}.fleetshield365.com</a>.</p>"
                f"<p>Tap the button below to jump straight to your dashboard.</p>"
            ),
            button_label="Open dashboard",
            button_url=f"https://{pending['subdomain']}.fleetshield365.com/dashboard",
        )
        await send_system_email(email_norm, "Welcome to FleetShield365", welcome_html)
    except Exception as exc:
        logger.warning("register-verify welcome email failed for %s: %s", email_norm, exc)

    # Mint access token + return redirect_to so the web can hop to the
    # tenant subdomain (same shape as legacy register-company).
    token = await _mint_access_token(
        str(user_doc["_id"]), user_doc=user_doc,
        company_id=company_id, expires_delta=timedelta(days=365),
    )
    redirect_to = f"https://{pending['subdomain']}.fleetshield365.com/dashboard"

    return {
        "status": "registered",
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": str(user_doc["_id"]),
            "email": email_norm,
            "name": pending["name"],
            "role": user_role,
            "company_id": company_id,
            "email_verified": True,
        },
        "company": {
            "id": company_id,
            "name": pending["company_name"],
            "subdomain": pending["subdomain"],
            "timezone": company_doc["timezone"],
        },
        "redirect_to": redirect_to,
    }


# ============== Subscription (Future Ready) ==============

# Company Registration with Stripe (legacy single-step — kept for back-compat)
@api_router.post("/auth/register-company")
async def register_company(data: CompanyRegister):
    """Register a new company and admin user, optionally create Stripe checkout session"""
    # Phase 3 — uniform password policy at every set-password site.
    validate_password_policy(data.password)
    
    # Check if email already exists
    existing_user = await db.users.find_one({"email": data.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Resolve the tenant subdomain (Requirements 9.1, 9.3, 9.7). When the
    # caller supplied a subdomain we validate + uniqueness-check it;
    # otherwise we derive one from ``company_name`` via slug_generator.
    # Validation errors surface as 400/409 per _subdomain_error_to_http.
    try:
        if data.subdomain is not None:
            resolved_subdomain = validate_subdomain(data.subdomain)
            await ensure_subdomain_unique(resolved_subdomain, db)
        else:
            resolved_subdomain = await slug_generator(data.company_name, db)
    except SubdomainValidationError as exc:
        raise _subdomain_error_to_http(exc)

    # Create company. Phase 4.2 — owner can disable trials globally
    # (``trial_enabled=false`` in platform_config.pricing). When trials
    # are off, new tenants land on past_due and have to subscribe before
    # using the platform. When trials are on we honour the configured
    # trial_max_vehicles cap (None = unlimited during trial).
    pricing_now = await get_pricing()
    trial_on = bool(pricing_now.get("trial_enabled", True))
    # Phase 5 — derive region defaults so we always have something to
    # show even when the client didn't auto-detect. Country drives
    # everything else: currency, units, locale.
    norm_country = (data.country or "").upper()[:2] or None
    units = data.units_system
    if not units:
        units = "imperial" if norm_country in {"US", "LR", "MM"} else "metric"
    currency = (data.preferred_currency or "").upper()[:3] or None

    company_doc = {
        "name": data.company_name,
        "subdomain": resolved_subdomain,
        "vehicle_count": data.vehicle_count,
        "subscription_status": "trialing" if trial_on else "past_due",
        "subscription_plan": "pro",
        # trial_end stays absent when trials are disabled — the
        # require_active_subscription gate uses missing/expired as
        # "needs to pay before drivers can use the app".
        "trial_end": (
            (utcnow() + timedelta(days=pricing_now["trial_days"])).isoformat()
            if trial_on else None
        ),
        "max_vehicles": (
            pricing_now.get("trial_max_vehicles") if trial_on else 0
        ),
        "stripe_customer_id": None,
        "stripe_subscription_id": None,
        "timezone": data.timezone or DEFAULT_TIMEZONE,
        "country": norm_country,
        "preferred_currency": currency,
        "units_system": units,
        "locale": data.locale,
        "created_at": utcnow().isoformat(),
    }
    company_result = await db.companies.insert_one(company_doc)
    company_id = str(company_result.inserted_id)
    
    # Create admin user
    # Determine role: super_admin for Company Owner, admin for Admin (default to super_admin if not specified)
    user_role = data.role if data.role in [UserRole.SUPER_ADMIN, UserRole.ADMIN] else UserRole.SUPER_ADMIN
    
    user_doc = {
        "email": data.email,
        "password_hash": get_password_hash(data.password),
        "name": data.name,
        "role": user_role,
        "company_id": company_id,
        "email_verified": False,
        "created_at": utcnow().isoformat(),
    }
    user_result = await db.users.insert_one(user_doc)
    user_id = str(user_result.inserted_id)

    # Send email-verification link via the noreply mailbox. Failure here must
    # NOT block sign-up; the user can request a resend via /auth/resend-verification.
    try:
        verify_token = await _issue_email_token(user_id, "verify", ttl_hours=24)
        verify_origin = data.origin_url if (data.origin_url and _is_allowed_origin(data.origin_url)) else DEFAULT_ORIGIN_URL
        await send_verification_email(data.email, data.name, verify_token, verify_origin)
    except Exception as e:
        logger.error(f"register_company: failed to send verification email to {data.email}: {e}")
    
    # If Stripe is configured, create checkout session
    checkout_url = None
    if stripe.api_key and data.origin_url:
        try:
            # Create Stripe customer
            customer = stripe.Customer.create(
                email=data.email,
                name=data.name,
                metadata={
                    "company_id": company_id,
                    "company_name": data.company_name,
                }
            )
            
            # Update company with Stripe customer ID
            await db.companies.update_one(
                {"_id": company_result.inserted_id},
                {"$set": {"stripe_customer_id": customer.id}}
            )
            
            # Two-line-item subscription:
            #   1. Base price (quantity always 1)
            #   2. Per-vehicle price (quantity = number of vehicles)
            # We use the second line item's `quantity` to scale the bill
            # when the tenant adds/removes vehicles via
            # _sync_vehicle_quantity_to_stripe. A single lump-sum line
            # (the old shape) made Subscription.modify per-vehicle
            # awkward.
            base_price = pricing_now["base_price"]
            per_vehicle = pricing_now["per_vehicle"]
            currency = (pricing_now.get("currency") or "AUD").lower()

            line_items = [
                {
                    "price_data": {
                        "currency": currency,
                        "product_data": {"name": "FleetShield365 Pro — Base"},
                        "unit_amount": int(round(base_price * 100)),
                        "recurring": {"interval": "month"},
                    },
                    "quantity": 1,
                },
                {
                    "price_data": {
                        "currency": currency,
                        "product_data": {"name": "FleetShield365 Pro — Per vehicle"},
                        "unit_amount": int(round(per_vehicle * 100)),
                        "recurring": {"interval": "month"},
                    },
                    "quantity": max(1, int(data.vehicle_count)),
                },
            ]

            checkout_session = stripe.checkout.Session.create(
                customer=customer.id,
                payment_method_types=["card"],
                line_items=line_items,
                mode="subscription",
                success_url=f"{data.origin_url}/payment/success?session_id={{CHECKOUT_SESSION_ID}}",
                cancel_url=f"{data.origin_url}/pricing",
                subscription_data={
                    "trial_period_days": pricing_now["trial_days"],
                    "metadata": {
                        "company_id": company_id,
                        "vehicle_count": str(data.vehicle_count),
                    }
                },
            )
            checkout_url = checkout_session.url
        except Exception as e:
            logger.error(f"Stripe error: {e}")
            # Continue without Stripe - trial mode
    
    # Mint a full-claims JWT (Req 12.1-12.3).
    access_token = await _mint_access_token(
        user_id,
        company_id=company_id,
        subdomain=resolved_subdomain,
        role=data.role or UserRole.SUPER_ADMIN,
    )
    
    return {
        "access_token": access_token,
        "checkout_url": checkout_url,
        "company_id": company_id,
        "user_id": user_id,
        # Post-register bounce to the branded tenant host so the new
        # owner lands on <slug>.fleetshield365.com/dashboard instead of
        # the apex. Frontend AuthContext.register() reads this field.
        "redirect_to": f"https://{resolved_subdomain}.fleetshield365.com/dashboard",
        "subdomain": resolved_subdomain,
    }

# Get current user with company info (for website)
@api_router.get("/auth/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Get current user and company information"""
    company = None
    if current_user.get("company_id"):
        company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})
        if company:
            # Count vehicles
            vehicle_count = await db.vehicles.count_documents({"company_id": current_user["company_id"]})
            company = serialize_doc(company)
            company["vehicle_count"] = vehicle_count
            # Task 5.4: expose presigned logo URL alongside logo_object_key
            # (Requirements 21.12, 21.13).
            company["logo_url"] = _presign_if_key(
                "logos", company.get("logo_object_key")
            )
    
    user_data = {
        "id": current_user["id"],
        "email": current_user["email"],
        "name": current_user["name"],
        "role": current_user.get("role", "driver"),
        "company_name": company["name"] if company else None,
        "email_verified": bool(current_user.get("email_verified", False)),
    }
    
    return {
        "user": user_data,
        "company": company,
    }

# Stripe Webhook Handler — Phase 12 of TODO.md.
#
# Signature verification is MANDATORY unless explicitly disabled via
# STRIPE_WEBHOOK_ALLOW_UNVERIFIED=true (intended only for local dev).
# Production must always have STRIPE_WEBHOOK_SECRET configured — a
# missing secret in prod is now a 503, not a silent-accept that lets
# anyone POST forged events.
#
# Idempotency: every Stripe event has a unique `id`. We record it in
# the `stripe_events` collection on first receive and reject duplicates
# so a retry storm cannot apply the same upgrade twice.
@api_router.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events with mandatory signature verification."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    allow_unverified = (
        os.environ.get("STRIPE_WEBHOOK_ALLOW_UNVERIFIED", "false")
        .strip().lower() in ("true", "1", "yes")
    )

    if webhook_secret:
        if not sig_header:
            raise HTTPException(status_code=400, detail="Missing Stripe-Signature header")
        try:
            event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
        except Exception as e:
            logger.error(f"Webhook signature verification failed: {e}")
            raise HTTPException(status_code=400, detail="Invalid signature")
    elif allow_unverified:
        # Local dev escape hatch — never set this in production.
        logger.warning("Stripe webhook signature verification disabled (DEV ONLY)")
        event = json.loads(payload)
    else:
        # Prod without secret → fail closed. Don't silently accept.
        logger.error("Stripe webhook received but STRIPE_WEBHOOK_SECRET is not configured")
        raise HTTPException(
            status_code=503,
            detail="Webhook signing not configured on this server",
        )

    # Idempotency: drop duplicate event IDs. Stripe retries on 5xx,
    # so without this a network blip could promote a tenant twice.
    event_id = event.get("id")
    if event_id:
        existing = await db.stripe_events.find_one({"_id": event_id})
        if existing:
            logger.info(f"Stripe event {event_id} already processed; skipping")
            return {"status": "duplicate"}
        await db.stripe_events.insert_one({
            "_id": event_id,
            "type": event.get("type"),
            "received_at": datetime.now(timezone.utc),
        })
    
    event_type = event.get("type")
    data = event.get("data", {}).get("object", {})
    
    if event_type == "checkout.session.completed":
        # Payment successful, activate subscription
        company_id = data.get("metadata", {}).get("company_id")
        if company_id:
            await db.companies.update_one(
                {"_id": ObjectId(company_id)},
                {"$set": {
                    "subscription_status": "active",
                    "stripe_subscription_id": data.get("subscription"),
                }}
            )
    
    elif event_type == "customer.subscription.updated":
        # Subscription updated
        subscription_id = data.get("id")
        status = data.get("status")
        
        await db.companies.update_one(
            {"stripe_subscription_id": subscription_id},
            {"$set": {"subscription_status": status}}
        )
    
    elif event_type == "customer.subscription.deleted":
        # Subscription cancelled
        subscription_id = data.get("id")
        
        await db.companies.update_one(
            {"stripe_subscription_id": subscription_id},
            {"$set": {"subscription_status": "cancelled"}}
        )
    
    elif event_type == "invoice.payment_failed":
        # Payment failed
        subscription_id = data.get("subscription")
        
        await db.companies.update_one(
            {"stripe_subscription_id": subscription_id},
            {"$set": {"subscription_status": "past_due"}}
        )
    
    return {"received": True}

# ============== Push Notifications ==============

class PushTokenCreate(BaseModel):
    token: str
    platform: str = "ios"
    device_name: str = "Unknown Device"

class NotificationPreferencesUpdate(BaseModel):
    expiry_alerts: Optional[bool] = None
    # Per-window expiry lead times. When `expiry_alerts` is on, these gate
    # which days-until-expiry buckets actually emit emails. Keys align with
    # backend REMINDER_DAYS = [60, 30, 14, 7]. Missing → treat as "on" so
    # older notification_preferences docs keep emailing.
    expiry_alert_60d: Optional[bool] = None
    expiry_alert_30d: Optional[bool] = None
    expiry_alert_14d: Optional[bool] = None
    expiry_alert_7d: Optional[bool] = None
    expiry_alert_expired: Optional[bool] = None
    # Legacy keys (70/45/21) kept for backward compat with docs already
    # written before the UI/backend alignment on 2026-05-25. No UI surface.
    expiry_alert_70d: Optional[bool] = None
    expiry_alert_45d: Optional[bool] = None
    expiry_alert_21d: Optional[bool] = None
    issue_alerts: Optional[bool] = None
    missed_inspection_alerts: Optional[bool] = None
    daily_summary: Optional[bool] = None
    weekly_summary: Optional[bool] = None  # Phase 6 (2026-05-18) — per-user opt-in for Monday digest
    push_enabled: Optional[bool] = None
    email_enabled: Optional[bool] = None
    # Per-activity email toggles. These mirror the admin web settings
    # so the user can opt in/out of each event class without disabling
    # email entirely. Defaults match the historical "always send"
    # behaviour except for prestart/endshift/fuel which default off.
    prestart_email: Optional[bool] = None
    endshift_email: Optional[bool] = None
    fuel_email: Optional[bool] = None
    incident_email: Optional[bool] = None

@api_router.post("/push-tokens")
async def register_push_token(data: PushTokenCreate, current_user: dict = Depends(get_current_user)):
    """Register a push notification token for the current user"""
    # Check if token already exists
    existing = await db.push_tokens.find_one({"token": data.token})
    if existing:
        # Update existing token with new user
        await db.push_tokens.update_one(
            {"token": data.token},
            {"$set": {
                "user_id": current_user["id"],
                "company_id": current_user.get("company_id"),
                "platform": data.platform,
                "device_name": data.device_name,
                "updated_at": utcnow().isoformat(),
            }}
        )
    else:
        # Create new token
        await db.push_tokens.insert_one({
            "token": data.token,
            "user_id": current_user["id"],
            "company_id": current_user.get("company_id"),
            "platform": data.platform,
            "device_name": data.device_name,
            "created_at": utcnow().isoformat(),
        })
    
    return {"status": "registered"}

@api_router.delete("/push-tokens")
async def unregister_push_token(data: dict, current_user: dict = Depends(get_current_user)):
    """Unregister a push notification token"""
    token = data.get("token")
    if token:
        await db.push_tokens.delete_one({"token": token, "user_id": current_user["id"]})
    return {"status": "unregistered"}

@api_router.get("/notification-preferences")
async def get_notification_preferences(current_user: dict = Depends(get_current_user)):
    """Get notification preferences for the current user"""
    prefs = await db.notification_preferences.find_one({"user_id": current_user["id"]})
    
    if not prefs:
        # Return defaults. weekly_summary defaults True to preserve the
        # pre-toggle behaviour (every admin used to receive the Monday
        # digest unconditionally). Per-window expiry chips default True
        # so a fresh tenant gets the full reminder cadence.
        return {
            "expiry_alerts": True,
            "expiry_alert_60d": True,
            "expiry_alert_30d": True,
            "expiry_alert_14d": True,
            "expiry_alert_7d": True,
            "expiry_alert_expired": True,
            "issue_alerts": True,
            "missed_inspection_alerts": True,
            "daily_summary": False,
            "weekly_summary": True,
            "push_enabled": True,
            "email_enabled": True,
            "prestart_email": False,
            "endshift_email": False,
            "fuel_email": False,
            "incident_email": True,
        }

    return {
        "expiry_alerts": prefs.get("expiry_alerts", True),
        "expiry_alert_60d": prefs.get("expiry_alert_60d", True),
        "expiry_alert_30d": prefs.get("expiry_alert_30d", True),
        "expiry_alert_14d": prefs.get("expiry_alert_14d", True),
        "expiry_alert_7d": prefs.get("expiry_alert_7d", True),
        "expiry_alert_expired": prefs.get("expiry_alert_expired", True),
        "issue_alerts": prefs.get("issue_alerts", True),
        "missed_inspection_alerts": prefs.get("missed_inspection_alerts", True),
        "daily_summary": prefs.get("daily_summary", False),
        "weekly_summary": prefs.get("weekly_summary", True),
        "push_enabled": prefs.get("push_enabled", True),
        "email_enabled": prefs.get("email_enabled", True),
        "prestart_email": prefs.get("prestart_email", False),
        "endshift_email": prefs.get("endshift_email", False),
        "fuel_email": prefs.get("fuel_email", False),
        "incident_email": prefs.get("incident_email", True),
    }

@api_router.put("/notification-preferences")
async def update_notification_preferences(
    data: NotificationPreferencesUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update notification preferences for the current user"""
    update_data = {k: v for k, v in data.dict().items() if v is not None}
    update_data["updated_at"] = utcnow().isoformat()
    
    await db.notification_preferences.update_one(
        {"user_id": current_user["id"]},
        {"$set": update_data},
        upsert=True
    )
    
    return {"status": "updated"}

# Push notification sender helper
async def send_push_notification(user_ids: list, title: str, body: str, data: dict = None):
    """Send push notification to specific users via Expo Push Service"""
    import httpx
    
    # Get push tokens for these users
    tokens = await db.push_tokens.find({"user_id": {"$in": user_ids}}).to_list(100)
    
    if not tokens:
        logger.info(f"[Push] No tokens found for users: {user_ids}")
        return
    
    # Check user preferences
    messages = []
    for token_doc in tokens:
        # Check if user has push enabled
        prefs = await db.notification_preferences.find_one({"user_id": token_doc["user_id"]})
        if prefs and not prefs.get("push_enabled", True):
            continue
        
        messages.append({
            "to": token_doc["token"],
            "title": title,
            "body": body,
            "data": data or {},
            "sound": "default",
            "priority": "high",
        })
    
    if not messages:
        logger.info("[Push] No messages to send (all users have push disabled)")
        return
    
    # Send to Expo Push Service
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://exp.host/--/api/v2/push/send",
                json=messages,
                headers={"Content-Type": "application/json"},
            )
            logger.info(f"[Push] Sent {len(messages)} notifications: {response.status_code}")
    except Exception as e:
        logger.error(f"[Push] Failed to send notifications: {e}")

# Helper to send alert with both email and push
async def send_alert_notification(
    alert_type: str,
    title: str,
    message: str,
    user_ids: list,
    company_id: str,
    data: dict = None
):
    """Send alert via both email and push notification based on preferences"""
    
    for user_id in user_ids:
        prefs = await db.notification_preferences.find_one({"user_id": user_id})
        if not prefs:
            prefs = {"push_enabled": True, "email_enabled": True}
        
        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if not user:
            continue
        
        # Check alert type preferences
        type_enabled = True
        if alert_type == "expiry" and not prefs.get("expiry_alerts", True):
            type_enabled = False
        elif alert_type == "issue" and not prefs.get("issue_alerts", True):
            type_enabled = False
        elif alert_type == "missed" and not prefs.get("missed_inspection_alerts", True):
            type_enabled = False
        elif alert_type == "daily_summary" and not prefs.get("daily_summary", False):
            type_enabled = False
        
        if not type_enabled:
            continue
        
        # Send push notification
        if prefs.get("push_enabled", True):
            await send_push_notification([user_id], title, message, data)
        
        # Send email
        if prefs.get("email_enabled", True):
            await EmailService.send_email(
                to_email=user["email"],
                subject=f"FleetShield365 Alert: {title}",
                body=f"<h2>{title}</h2><p>{message}</p>",
                company_id=company_id,
                is_html=True
            )

# ============== Upgrade-to-paid checkout (existing tenant) ==============
#
# /auth/register-company creates checkout for brand-new tenants. This
# endpoint covers the other case: tenant already exists (still on
# trial or trial_expired) and wants to upgrade. We re-use the live
# pricing config, attach to the existing Stripe Customer if one
# exists from the original registration, and return the checkout URL
# for the web client to redirect to.

class UpgradeCheckoutRequest(BaseModel):
    origin_url: Optional[str] = None  # where to land after success/cancel


@api_router.post("/billing/upgrade-checkout")
async def create_upgrade_checkout(
    payload: UpgradeCheckoutRequest,
    current_user: dict = Depends(get_current_user),
):
    """Start a Stripe checkout session for an existing tenant.

    Auth: any role that can read the Billing tab (super_admin / admin).
    Drivers don't see Billing, so we gate to admins.
    """
    if current_user.get("role") not in (UserRole.SUPER_ADMIN, UserRole.ADMIN):
        raise HTTPException(status_code=403, detail="Only owners and admins can upgrade billing")

    if not stripe.api_key:
        raise HTTPException(
            status_code=503,
            detail="Stripe is not configured on this server. Set STRIPE_SECRET_KEY in .env.",
        )

    company_id = current_user.get("company_id")
    if not company_id:
        raise HTTPException(status_code=400, detail="User has no company")

    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Block double-charging — if tenant already has an active
    # subscription, send them to the Stripe billing portal instead
    # of creating a duplicate.
    if company.get("subscription_status") == "active" and company.get("stripe_subscription_id"):
        return {
            "already_active": True,
            "message": "You're already on the paid plan.",
            "checkout_url": None,
        }

    # Live vehicle count drives the quantity on the per-vehicle line.
    vehicle_count = await db.vehicles.count_documents({
        "company_id": company_id,
        "deleted_at": None,
    })
    vehicle_count = max(1, vehicle_count)

    pricing_now = await get_pricing()
    base_price = pricing_now["base_price"]
    per_vehicle = pricing_now["per_vehicle"]
    currency = (pricing_now.get("currency") or "AUD").lower()

    # Re-use existing Stripe customer if registration created one.
    customer_id = company.get("stripe_customer_id")
    try:
        if not customer_id:
            customer = stripe.Customer.create(
                email=current_user.get("email"),
                name=company.get("name") or current_user.get("name"),
                metadata={
                    "company_id": company_id,
                    "company_name": company.get("name") or "",
                },
            )
            customer_id = customer.id
            await db.companies.update_one(
                {"_id": ObjectId(company_id)},
                {"$set": {"stripe_customer_id": customer_id}},
            )

        # Default success/cancel URLs honour the caller-supplied origin
        # when it's on the platform domain (subdomain regex match), so
        # an Owner upgrading from `lalitcom.fleetshield365.com/settings`
        # lands back there rather than the apex. Falls back to the
        # platform apex otherwise.
        origin = payload.origin_url if (payload.origin_url and _is_allowed_origin(payload.origin_url)) else DEFAULT_ORIGIN_URL
        origin = origin.rstrip('/')

        line_items = [
            {
                "price_data": {
                    "currency": currency,
                    "product_data": {"name": "FleetShield365 Pro — Base"},
                    "unit_amount": int(round(base_price * 100)),
                    "recurring": {"interval": "month"},
                },
                "quantity": 1,
            },
            {
                "price_data": {
                    "currency": currency,
                    "product_data": {"name": "FleetShield365 Pro — Per vehicle"},
                    "unit_amount": int(round(per_vehicle * 100)),
                    "recurring": {"interval": "month"},
                },
                "quantity": vehicle_count,
            },
        ]

        # If the tenant is still inside their trial, honour the
        # remaining trial days on the new subscription so they don't
        # lose value by upgrading early. Otherwise no trial.
        sub_data: dict = {
            "metadata": {
                "company_id": company_id,
                "vehicle_count": str(vehicle_count),
                "upgrade": "true",
            },
        }
        try:
            ts = await get_trial_status(company_id)
            days_left = int(ts.get("days_left") or 0)
            if ts.get("status") == "trialing" and days_left > 0:
                sub_data["trial_period_days"] = days_left
        except Exception:
            pass

        checkout_session = stripe.checkout.Session.create(
            customer=customer_id,
            payment_method_types=["card"],
            line_items=line_items,
            mode="subscription",
            success_url=f"{origin}/settings?tab=billing&upgrade=success",
            cancel_url=f"{origin}/settings?tab=billing&upgrade=cancelled",
            subscription_data=sub_data,
        )
        return {
            "already_active": False,
            "checkout_url": checkout_session.url,
            "session_id": checkout_session.id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"create_upgrade_checkout failed for {company_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not start checkout: {e}")


@api_router.get("/subscription")
async def get_subscription(current_user: dict = Depends(get_current_user)):
    """Subscription snapshot for the admin billing page. Phase 6
    (2026-05-18 owner review): now returns the live pricing so the
    page can render real numbers in the tenant's preferred currency
    (was previously hard-coded "$0 Founding Member" on the frontend)."""
    company = await db.companies.find_one({"_id": ObjectId(current_user["company_id"])})

    # Get trial status
    trial_status = await get_trial_status(current_user["company_id"])

    plans = {
        "basic": {"max_vehicles": 5, "price": 0},
        "standard": {"max_vehicles": 20, "price": 49},
        # ``max_vehicles: null`` represents "unlimited" for the pro plan.
        # ``float("inf")`` is not JSON-serializable and crashes the
        # response encoder — use None and let the client render "∞".
        "pro": {"max_vehicles": None, "price": 99}
    }

    current_plan = company.get("subscription_plan", "basic")
    plan_details = plans.get(current_plan, plans["basic"])

    # Live pricing from platform_config — single source of truth shared
    # with /api/pricing and the marketing pages.
    pricing = await get_pricing()
    active_vehicles = await db.vehicles.count_documents({
        **_soft_delete_filter(),
        "company_id": current_user["company_id"],
    })

    return {
        "current_plan": current_plan,
        "plan_details": plan_details,
        "active_vehicles": active_vehicles,
        "billing_history": company.get("billing_history", []),
        "trial_status": trial_status.get("status"),
        "trial_days_left": trial_status.get("days_left"),
        "trial_end": trial_status.get("trial_end"),
        "trial_enabled": pricing.get("trial_enabled", True),
        "is_active": trial_status.get("is_active", False),
        "subscription_message": trial_status.get("message"),
        "subscription_status": company.get("subscription_status"),
        # Live pricing (always in the platform currency from owner panel).
        "pricing": {
            "base_price": pricing["base_price"],
            "per_vehicle": pricing["per_vehicle"],
            "trial_days": pricing["trial_days"],
            "currency": pricing.get("currency", "AUD"),
            "cadence": pricing.get("cadence", "monthly"),
        },
        # Tenant's preferred display currency. The frontend converts
        # the platform-currency amounts above into this for display.
        # Stripe still charges in the platform currency.
        "preferred_currency": company.get("preferred_currency"),
        "country": company.get("country"),
    }


# ============== Support Requests ==============

@api_router.post("/support")
async def create_support_request(
    request_data: SupportRequestCreate,
    current_user: dict = Depends(get_current_user)
):
    """Create a new support request"""
    support_request = {
        "_id": ObjectId(),
        "company_id": current_user["company_id"],
        "user_id": str(current_user["_id"]),
        "user_name": current_user.get("name", "Unknown"),
        "user_email": current_user.get("email", ""),
        "user_role": current_user.get("role", "driver"),
        "subject": request_data.subject,
        "message": request_data.message,
        "category": request_data.category,
        "status": SupportRequestStatus.OPEN,
        "admin_response": None,
        "created_at": utcnow(),
        "updated_at": utcnow(),
        "resolved_at": None,
    }
    
    await db.support_requests.insert_one(support_request)
    
    return {
        "id": str(support_request["_id"]),
        "message": "Support request submitted successfully. We'll get back to you soon!",
        "ticket_number": f"SR-{str(support_request['_id'])[-6:].upper()}"
    }

@api_router.get("/support")
async def get_support_requests(
    current_user: dict = Depends(get_current_user),
    status: Optional[str] = None,
    limit: int = 50
):
    """Get support requests - admins see all for company, users see their own"""
    query = {"company_id": current_user["company_id"]}
    
    # Non-admins only see their own requests
    if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        query["user_id"] = str(current_user["_id"])
    
    if status:
        query["status"] = status
    
    requests = await db.support_requests.find(query).sort("created_at", -1).limit(limit).to_list(limit)
    
    return [{
        "id": str(r["_id"]),
        "ticket_number": f"SR-{str(r['_id'])[-6:].upper()}",
        "user_name": r.get("user_name"),
        "user_email": r.get("user_email"),
        "user_role": r.get("user_role"),
        "subject": r.get("subject"),
        "message": r.get("message"),
        "category": r.get("category"),
        "status": r.get("status"),
        "admin_response": r.get("admin_response"),
        "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
        "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None,
        "resolved_at": r.get("resolved_at").isoformat() if r.get("resolved_at") else None,
    } for r in requests]

@api_router.get("/support/stats")
async def get_support_stats(
    current_user: dict = Depends(get_current_user)
):
    """Get support request stats for admins"""
    if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    company_id = current_user["company_id"]
    
    total = await db.support_requests.count_documents({"company_id": company_id})
    open_count = await db.support_requests.count_documents({"company_id": company_id, "status": "open"})
    in_progress = await db.support_requests.count_documents({"company_id": company_id, "status": "in_progress"})
    resolved = await db.support_requests.count_documents({"company_id": company_id, "status": "resolved"})
    
    return {
        "total": total,
        "open": open_count,
        "in_progress": in_progress,
        "resolved": resolved,
    }

@api_router.get("/support/{request_id}")
async def get_support_request(
    request_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get a single support request. Phase 6 (2026-05-18): platform
    owner sees every ticket; company admins see their tenant's
    tickets; drivers see only their own."""
    request = await db.support_requests.find_one({"_id": ObjectId(request_id)})

    if not request:
        raise HTTPException(status_code=404, detail="Support request not found")

    is_platform_owner = bool(current_user.get("is_platform_owner")) or current_user.get("role") == "platform_owner"
    if not is_platform_owner:
        # Tenant scope check.
        if request["company_id"] != current_user["company_id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        if current_user.get("role") not in [UserRole.SUPER_ADMIN, UserRole.ADMIN]:
            if request["user_id"] != str(current_user["_id"]):
                raise HTTPException(status_code=403, detail="Access denied")
    
    return {
        "id": str(request["_id"]),
        "ticket_number": f"SR-{str(request['_id'])[-6:].upper()}",
        "user_name": request.get("user_name"),
        "user_email": request.get("user_email"),
        "user_role": request.get("user_role"),
        "subject": request.get("subject"),
        "message": request.get("message"),
        "category": request.get("category"),
        "status": request.get("status"),
        "admin_response": request.get("admin_response"),
        "created_at": request.get("created_at").isoformat() if request.get("created_at") else None,
        "updated_at": request.get("updated_at").isoformat() if request.get("updated_at") else None,
        "resolved_at": request.get("resolved_at").isoformat() if request.get("resolved_at") else None,
    }

@api_router.put("/support/{request_id}")
async def update_support_request(
    request_id: str,
    update_data: SupportRequestUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Update support request — company admins (own tenant only) AND
    platform_owner (any tenant). Phase 6 (2026-05-18) extends this so
    the owner can reply / resolve / close any ticket platform-wide.
    """
    is_platform_owner = bool(current_user.get("is_platform_owner")) or current_user.get("role") == "platform_owner"
    is_admin = current_user.get("role") in [UserRole.SUPER_ADMIN, UserRole.ADMIN]
    if not (is_admin or is_platform_owner):
        raise HTTPException(status_code=403, detail="Admin access required")

    request = await db.support_requests.find_one({"_id": ObjectId(request_id)})
    if not request:
        raise HTTPException(status_code=404, detail="Support request not found")

    # Company admins can only touch their own tenant's tickets. Owner
    # bypasses this check.
    if not is_platform_owner and request["company_id"] != current_user["company_id"]:
        raise HTTPException(status_code=403, detail="Access denied")

    update_fields: dict = {"updated_at": utcnow()}

    if update_data.status:
        update_fields["status"] = update_data.status
        if update_data.status in [SupportRequestStatus.RESOLVED, SupportRequestStatus.CLOSED]:
            update_fields["resolved_at"] = utcnow()

    if update_data.admin_response:
        update_fields["admin_response"] = update_data.admin_response
        update_fields["responded_by"] = (
            "platform_owner" if is_platform_owner else "admin"
        )
        update_fields["responded_at"] = utcnow()

    await db.support_requests.update_one(
        {"_id": ObjectId(request_id)},
        {"$set": update_fields}
    )

    return {"message": "Support request updated successfully"}


# ============== Developer: email-template preview ==============
#
# One-shot helper for the platform owner to preview every outgoing
# email template at a single inbox. Calls each generator with sample
# data and sends to the supplied address. Owner asked for this on
# 2026-05-27 to QA branding + formatting across the 21 templates.

@api_router.post("/developer/preview-all-templates")
async def preview_all_email_templates(
    to: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Fire every email template once to a single inbox for QA."""
    sent: list = []

    async def fire(label: str, subject: str, html: str, sender: str = "alerts"):
        ok = await _send_via_smtp(to, f"[PREVIEW {label}] {subject}", html, sender=sender)
        sent.append({"template": label, "ok": ok})

    # --- BRANDED TEMPLATES (noreply@) ---
    await fire(
        "01 Forgot Password",
        "Reset your FleetShield365 password",
        _email_template_branded(
            heading="Reset your password",
            body_html="<p>You asked to reset your FleetShield365 password. Click the button to choose a new one. The link expires in 24 hours.</p>",
            button_label="Reset password",
            button_url="https://fleetshield365.com/reset-password?token=PREVIEW",
        ),
        sender="noreply",
    )

    await fire(
        "02 Email Verification",
        "Verify your FleetShield365 email",
        _email_template_branded(
            heading="Verify your email address",
            body_html="<p>Thanks for signing up. Click below to confirm this is the right email address.</p>",
            button_label="Verify email",
            button_url="https://fleetshield365.com/verify-email?token=PREVIEW",
        ),
        sender="noreply",
    )

    await fire(
        "03 Invite Admin or Driver",
        "You've been invited to FleetShield365",
        _email_template_branded(
            heading="You've been invited to QA Test Fleet",
            body_html="<p>QA Test Fleet has invited you to join their fleet management workspace as an <strong>Admin</strong>. Click the button below to set your password and get started.</p>",
            button_label="Accept invitation",
            button_url="https://fleetshield365.com/set-password?token=PREVIEW",
        ),
        sender="noreply",
    )

    await fire(
        "04 Verify-email Reminder",
        "Please verify your FleetShield365 email",
        _email_template_branded(
            heading="One step to go",
            body_html="<p>We sent a verification link earlier; in case it landed in spam, here it is again.</p>",
            button_label="Verify email",
            button_url="https://fleetshield365.com/verify-email?token=PREVIEW",
        ),
        sender="noreply",
    )

    await fire(
        "05 Invite Resend",
        "Your FleetShield365 invitation",
        _email_template_branded(
            heading="Your invitation",
            body_html="<p>QA Test Fleet is waiting for you to set your password. The link below opens the same sign-up screen.</p>",
            button_label="Set password",
            button_url="https://fleetshield365.com/set-password?token=PREVIEW",
        ),
        sender="noreply",
    )

    await fire(
        "06 OTP Register",
        "Your FleetShield365 verification code",
        _email_template_branded(
            heading="Your verification code",
            body_html="<p style='font-size:14px'>Enter this code on the sign-up page within 15 minutes.</p>"
                      "<div style='text-align:center;margin:24px 0;font-size:32px;letter-spacing:8px;font-weight:600;color:#0d9488'>483217</div>"
                      "<p style='font-size:13px;color:#64748b'>If you didn't request this, you can ignore this email.</p>",
        ),
        sender="noreply",
    )

    await fire(
        "07 Welcome (post-register)",
        "Welcome to FleetShield365",
        _email_template_branded(
            heading="Welcome aboard",
            body_html="<p>Your FleetShield365 workspace <strong>QA Test Fleet</strong> is ready. Open the dashboard to add your first vehicle and operator.</p>",
            button_label="Open dashboard",
            button_url="https://qatest.fleetshield365.com/dashboard",
        ),
        sender="noreply",
    )

    await fire(
        "08 Contact Form Auto-reply",
        "We received your message",
        _email_template_branded(
            heading="Thanks for getting in touch",
            body_html="<p>We've received your message and will reply within one business day. If your request is urgent, reply to this email or call our office number.</p>",
        ),
        sender="noreply",
    )

    # --- ALERTS TEMPLATES (alerts@) — inline HTML, fire helpers directly with sample data ---

    sample_alerts = [
        {"vehicle": "Truck SAM-202", "label": "Rego", "expiry": "2026-06-15", "days": 14},
        {"vehicle": "Trailer ABC-543", "label": "Insurance", "expiry": "2026-07-01", "days": 30},
    ]
    expiry_html = "<html><body style='font-family:Arial,sans-serif;padding:20px'><h2 style='color:#F97316'>FleetShield365 Expiry Alerts</h2><p>Hi QA Test Fleet Admin,</p><p>The following items require your attention:</p><ul style='margin:20px 0'>"
    for a in sample_alerts:
        expiry_html += f"<li>{a['label']} for {a['vehicle']} expires in {a['days']} days ({a['expiry']})</li>"
    expiry_html += "</ul></body></html>"
    await fire("09 Expiry Alert (vehicle)", "2 Expiry Alerts Require Attention", expiry_html)

    defect_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#EF4444'>Defect Alert - Mercedes Actros (SAM-202)</h2>"
        "<p>Driver <strong>Sarah Wilson</strong> reported the following on the pre-start inspection:</p>"
        "<p style='background:#FEF2F2;border-left:4px solid #EF4444;padding:12px'>Light leaking from headlight, dashboard warning light on.</p>"
        "<p>Open the inspection in the FleetShield365 admin panel to review the photos.</p></body></html>"
    )
    await fire("10 Defect Alert", "[DEFECT ALERT] Mercedes Actros - light leaking from headlight", defect_html)

    missed_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#F97316'>FleetShield365 Missed Inspection Alert</h2>"
        "<p>Hi QA Test Fleet Admin,</p>"
        "<p>The following vehicles did not complete their prestart inspection today:</p>"
        "<ul style='margin:20px 0'><li>Sweep Truck 1 (SWP-001)</li><li>Asset Probe 780931 (ASP-780931)</li></ul>"
        "<p>Please follow up with the assigned drivers.</p></body></html>"
    )
    await fire("11 Missed Inspection", "2 Vehicle(s) Missed Inspection Today", missed_html)

    repeated_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#F97316'>Repeated Issues - Sweep Truck 1 (SWP-001)</h2>"
        "<p>This vehicle has been reported with issues 4 times in the last 7 days:</p>"
        "<ul style='margin:20px 0'>"
        "<li>2026-05-22 - Brake warning light</li>"
        "<li>2026-05-24 - Same warning light</li>"
        "<li>2026-05-26 - Light still on</li>"
        "<li>2026-05-27 - Still unresolved</li></ul>"
        "<p>Consider taking this vehicle out of service until inspected.</p></body></html>"
    )
    await fire("12 Repeated Issues", "[PATTERN ALERT] Sweep Truck 1 - 4 issues in 7 days", repeated_html)

    daily_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#0D9488'>Daily Summary - 27 May 2026</h2>"
        "<p>Hi QA Test Fleet Admin,</p>"
        "<div style='background:#F8FAFC;padding:20px;border-radius:8px;margin:20px 0'>"
        "<p><strong>Inspections Completed:</strong> 8</p>"
        "<p><strong>Inspections Missed:</strong> 2</p>"
        "<p><strong>Issues Reported:</strong> 1</p>"
        "<p><strong>Fuel Submissions:</strong> 3</p>"
        "<p><strong>Total Fuel:</strong> 240.5 L</p></div>"
        "<p>Log in for detailed reports.</p></body></html>"
    )
    await fire("13 Daily Summary", "Daily Summary - 27 May 2026", daily_html)

    weekly_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#0D9488'>Weekly Summary - Week ending 26 May 2026</h2>"
        "<p>Hi QA Test Fleet Admin,</p>"
        "<div style='background:#F8FAFC;padding:20px;border-radius:8px;margin:20px 0'>"
        "<table style='width:100%;border-collapse:collapse'>"
        "<tr><td>Total inspections</td><td style='text-align:right'><strong>47</strong></td></tr>"
        "<tr><td>Pre-start</td><td style='text-align:right'>32</td></tr>"
        "<tr><td>End-shift</td><td style='text-align:right'>15</td></tr>"
        "<tr><td>Incidents</td><td style='text-align:right'><strong>2</strong></td></tr>"
        "<tr><td>Fuel logs</td><td style='text-align:right'>18</td></tr>"
        "</table></div></body></html>"
    )
    await fire("14 Weekly Summary", "Weekly Summary - Week ending 26 May 2026", weekly_html)

    deletion_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#0F172A'>Account deletion confirmed</h2>"
        "<p>Hi Sarah,</p>"
        "<p>Your FleetShield365 account has been deleted. Your data will be retained for 30 days in case you change your mind; after that it will be permanently removed.</p>"
        "<p style='color:#64748B;font-size:12px'>If this wasn't you, reply immediately so we can restore the account.</p></body></html>"
    )
    await fire("15 Account Deletion", "Account Deletion Confirmation", deletion_html)

    creds_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px;background-color:#f8fafc'>"
        "<div style='max-width:500px;margin:0 auto;background:white;padding:30px;border-radius:12px'>"
        "<h2 style='color:#0f172a;margin-bottom:20px'>Welcome to FleetShield365!</h2>"
        "<p style='color:#475569'>Hi Sarah,</p>"
        "<p style='color:#475569'>You've been added as an operator for <strong>QA Test Fleet</strong>. You can now access the FleetShield365 mobile app.</p>"
        "<div style='background-color:#f1f5f9;padding:20px;border-radius:8px;margin:20px 0'>"
        "<h3 style='color:#0f172a;margin-top:0'>Your Login Details:</h3>"
        "<p style='color:#475569;margin:5px 0'><strong>Username:</strong> sarah.wilson</p>"
        "<p style='color:#475569;margin:5px 0'><strong>PIN:</strong> 4-digit code from your admin</p></div>"
        "<div style='text-align:center;margin:30px 0'>"
        "<a href='https://fleetshield365.com' style='background-color:#0d9488;color:white;padding:12px 30px;text-decoration:none;border-radius:8px;font-weight:bold'>Open FleetShield365</a></div></div></body></html>"
    )
    await fire("16 Send Driver Credentials", "Your Login Credentials for QA Test Fleet", creds_html)

    incident_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#EF4444'>URGENT: Incident Report</h2>"
        "<p>An incident has been reported by driver <strong>Sarah Wilson</strong>:</p>"
        "<div style='background:#FEF2F2;border-left:4px solid #EF4444;padding:16px;margin:16px 0'>"
        "<p><strong>Vehicle:</strong> Mercedes Actros (SAM-202)</p>"
        "<p><strong>Severity:</strong> MODERATE</p>"
        "<p><strong>Location:</strong> M1 Pacific Motorway, NSW</p>"
        "<p><strong>Description:</strong> Light scrape against bollard while reversing in depot.</p></div>"
        "<p>Open the FleetShield365 admin panel to review photos and update status.</p></body></html>"
    )
    await fire("17 Incident Notification", "URGENT Incident Report: Mercedes Actros - MODERATE", incident_html)

    workshop_html = (
        "<html><body style='font-family:Arial,sans-serif;padding:20px'>"
        "<h2 style='color:#F97316'>Defect Repair Request - SAM-202</h2>"
        "<p>Please attend to the following defects on Mercedes Actros (SAM-202):</p>"
        "<ul><li>Headlight light leak</li><li>Dashboard warning light</li></ul>"
        "<p><strong>Reported by:</strong> Sarah Wilson on 27 May 2026</p>"
        "<p><strong>Odometer:</strong> 124,580 km</p>"
        "<p>Reply to this email to confirm receipt and provide an ETA.</p></body></html>"
    )
    await fire("18 Workshop Repair Request", "Defect Repair Request - SAM-202", workshop_html)

    # --- create_alert / EmailService.send_alert_email path (5 alert_type variants) ---
    alert_subject_map = {
        "expiry_warning": "Reminder: Upcoming Vehicle Expiry",
        "expiry_critical": "CRITICAL: Document Has Expired",
        "driver_expiry_warning": "Reminder: Driver Document Expiring",
        "driver_expiry_critical": "CRITICAL: Driver Document Expired",
        "vehicle_offline": "Vehicle Status: Offline",
    }
    for i, (atype, subj) in enumerate(alert_subject_map.items(), start=19):
        color = "#EF4444" if "critical" in atype or "unsafe" in atype else "#F59E0B"
        html = (
            f"<html><body style='font-family:Arial,sans-serif;background-color:#F8FAFC;padding:20px'>"
            f"<div style='max-width:600px;margin:0 auto;background-color:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)'>"
            f"<div style='background-color:{color};color:white;padding:20px;text-align:center'>"
            f"<h1 style='margin:0;font-size:24px'>{subj}</h1></div>"
            f"<div style='padding:30px'><p style='font-size:16px;color:#334155;line-height:1.6'>"
            f"This is a sample {atype} alert sent for the FleetShield365 template QA preview. In production this email fires when the matching alert condition is met.</p>"
            f"<hr style='border:none;border-top:1px solid #E2E8F0;margin:20px 0'>"
            f"<p style='font-size:14px;color:#64748B'>This is an automated notification from FleetShield365. Please log in to your dashboard to take action.</p></div>"
            f"<div style='background-color:#F1F5F9;padding:15px;text-align:center'>"
            f"<p style='margin:0;font-size:12px;color:#94A3B8'>FleetShield365 - Vehicle Inspection Management</p></div></div></body></html>"
        )
        await fire(f"{i:02d} {atype}", subj, html)

    return {
        "to": to,
        "total": len(sent),
        "sent": [s for s in sent if s["ok"]],
        "failed": [s for s in sent if not s["ok"]],
    }


# ============== Developer: platform-wide support inbox ==============

@api_router.get("/developer/support")
async def developer_list_support_requests(
    current_user: dict = Depends(require_platform_owner),
    status: Optional[str] = None,
    limit: int = 200,
):
    """Platform owner inbox — every support ticket across every tenant.
    Annotated with company name so the owner can see who raised what.
    Phase 6 (2026-05-18)."""
    query: dict = {}
    if status:
        query["status"] = status

    requests = (
        await db.support_requests.find(query)
        .sort("created_at", -1)
        .limit(max(1, min(limit, 500)))
        .to_list(max(1, min(limit, 500)))
    )

    # Annotate with company name in one batch query.
    company_ids = {r.get("company_id") for r in requests if r.get("company_id")}
    companies = (
        await db.companies.find(
            {"_id": {"$in": [ObjectId(c) for c in company_ids if c]}}
        ).to_list(len(company_ids))
        if company_ids
        else []
    )
    company_map = {str(c["_id"]): c for c in companies}

    return [
        {
            "id": str(r["_id"]),
            "ticket_number": f"SR-{str(r['_id'])[-6:].upper()}",
            "company_id": r.get("company_id"),
            "company_name": (company_map.get(r.get("company_id", "")) or {}).get("name"),
            "company_subdomain": (company_map.get(r.get("company_id", "")) or {}).get("subdomain"),
            "user_name": r.get("user_name"),
            "user_email": r.get("user_email"),
            "user_role": r.get("user_role"),
            "subject": r.get("subject"),
            "message": r.get("message"),
            "category": r.get("category"),
            "status": r.get("status"),
            "admin_response": r.get("admin_response"),
            "responded_by": r.get("responded_by"),
            "responded_at": r.get("responded_at").isoformat() if isinstance(r.get("responded_at"), datetime) else r.get("responded_at"),
            "created_at": r.get("created_at").isoformat() if isinstance(r.get("created_at"), datetime) else r.get("created_at"),
            "updated_at": r.get("updated_at").isoformat() if isinstance(r.get("updated_at"), datetime) else r.get("updated_at"),
            "resolved_at": r.get("resolved_at").isoformat() if isinstance(r.get("resolved_at"), datetime) else r.get("resolved_at"),
        }
        for r in requests
    ]


@api_router.get("/developer/support/stats")
async def developer_support_stats(
    current_user: dict = Depends(require_platform_owner),
):
    """Platform-wide ticket counters for the owner inbox header."""
    return {
        "total": await db.support_requests.count_documents({}),
        "open": await db.support_requests.count_documents({"status": "open"}),
        "in_progress": await db.support_requests.count_documents({"status": "in_progress"}),
        "resolved": await db.support_requests.count_documents({"status": "resolved"}),
        "closed": await db.support_requests.count_documents({"status": "closed"}),
    }

# FAQ Data (static, no database needed)
FAQ_DATA = [
    {
        "category": "driver",
        "questions": [
            {
                "q": "How do I complete a pre-start inspection?",
                "a": "1. Open the app and select your vehicle from the dropdown\n2. Tap 'START PRESTART INSPECTION'\n3. Go through each checklist item and mark as OK or Not OK\n4. Add photos of any issues found\n5. Sign at the bottom and submit"
            },
            {
                "q": "What do I do if I find an issue during inspection?",
                "a": "Mark the item as 'Not OK', add a description of the issue, and take a photo. Your admin will be notified automatically. If the vehicle is unsafe to drive, do not operate it until the issue is resolved."
            },
            {
                "q": "How do I submit a fuel receipt?",
                "a": "1. Tap 'FUEL SUBMISSION' on the home screen\n2. Select the vehicle you fueled\n3. Enter the fuel amount, cost, and odometer reading\n4. Take a photo of the receipt\n5. Submit"
            },
            {
                "q": "Can I use the app without internet?",
                "a": "Yes! The app works offline. Your inspections, fuel submissions, and incident reports will be saved locally and automatically sync when you have internet again. You'll see a 'Pending Sync' indicator."
            },
            {
                "q": "How do I report an incident or accident?",
                "a": "1. Tap 'INCIDENT REPORT' (red button) on the home screen\n2. If someone is injured, tap the emergency banner to call 000\n3. Fill in the incident details, other party information, and take photos\n4. Submit the report - your admin will be notified"
            },
            {
                "q": "Where can I see my past inspections?",
                "a": "Your admin can view all inspection history in the Reports section of the admin website. As a driver, you can see your recent activity on the app's home screen."
            }
        ]
    },
    {
        "category": "admin",
        "questions": [
            {
                "q": "How do I add a new vehicle?",
                "a": "1. Go to Vehicles page\n2. Click '+ Add Vehicle'\n3. Fill in vehicle details (name, rego, type)\n4. Add expiry dates for registration, insurance, etc.\n5. Save"
            },
            {
                "q": "How do I add a new driver?",
                "a": "1. Go to Drivers page\n2. Click '+ Add Driver'\n3. Enter driver details and create login credentials\n4. Add license and certification expiry dates\n5. Click 'Send Login' to email their credentials"
            },
            {
                "q": "What are expiry alerts?",
                "a": "The system automatically monitors all expiry dates (vehicle rego, insurance, driver licenses, etc.) and sends alerts at 60, 30, 14, and 7 days before expiry. Critical items (7 days or less) appear in red."
            },
            {
                "q": "How do I view inspection reports?",
                "a": "Go to the Reports page. You can filter by date, vehicle, or inspection type. Click 'View Details' on any report to see the full inspection including photos and signatures."
            },
            {
                "q": "How do I assign drivers to vehicles?",
                "a": "Go to Vehicles page, find the vehicle, and click 'Assign'. Select one or more drivers who are authorized to operate that vehicle."
            },
            {
                "q": "How do I change my company logo?",
                "a": "Go to Settings > General tab. Click on the logo area to upload your company logo. This logo will appear on PDF reports and in the app."
            }
        ]
    },
    {
        "category": "general",
        "questions": [
            {
                "q": "Is my data secure?",
                "a": "Yes. All data is encrypted in transit (HTTPS) and at rest. We use industry-standard security practices and your data is never shared with third parties."
            },
            {
                "q": "How do I reset my password?",
                "a": "Contact your company admin to reset your password, or use the 'Forgot Password' link on the login screen."
            },
            {
                "q": "What devices does the app work on?",
                "a": "The driver app works on iOS and Android phones. The admin website works on any modern web browser (Chrome, Safari, Firefox, Edge)."
            }
        ]
    }
]

@api_router.get("/faq")
async def get_faq():
    """Get FAQ data - no auth required"""
    return FAQ_DATA


# ============== Developer Dashboard Stats ==============
#
# Task 9: The legacy ``DEVELOPER_KEY`` query-string gate has been removed
# (Req 15.1, 15.2). Every ``/api/developer/*`` route now depends on
# ``require_platform_owner`` which validates the bearer JWT carries
# ``role == "platform_owner"``. A valid bearer with any other role 403s;
# a missing/invalid bearer 401s upstream in ``get_current_user``.
# Platform-owner tenant scoping is exempt (Req 16.4) — the dependency
# deliberately does not filter DB queries by company_id.

@api_router.get("/developer/stats")
async def get_developer_stats(
    current_user: dict = Depends(require_platform_owner),
):
    """Get system-wide stats for developer/owner dashboard"""
    start_time = datetime.now(timezone.utc)
    
    try:
        # Get total counts
        total_companies = await db.companies.count_documents({})
        total_users = await db.users.count_documents({})
        total_drivers = await db.users.count_documents({"role": "driver"})
        total_admins = await db.users.count_documents({"role": {"$in": ["admin", "owner"]}})
        total_vehicles = await db.vehicles.count_documents({})
        total_inspections = await db.inspections.count_documents({})
        total_fuel_logs = await db.fuel_submissions.count_documents({})
        total_service_records = await db.service_records.count_documents({})
        total_incidents = await db.incidents.count_documents({})
        total_photos = await db.inspection_photos.count_documents({})
        
        # Estimate photo storage (avg 200KB per photo)
        estimated_photo_storage_mb = round(total_photos * 0.2, 1)
        
        # Today's stats
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_inspections = await db.inspections.count_documents({"timestamp": {"$gte": today_start}})
        today_fuel_logs = await db.fuel_submissions.count_documents({"timestamp": {"$gte": today_start}})
        today_new_users = await db.users.count_documents({"created_at": {"$gte": today_start}})
        today_new_companies = await db.companies.count_documents({"created_at": {"$gte": today_start}})
        
        # Active users today (users who logged in or submitted something)
        active_users_pipeline = [
            {"$match": {"timestamp": {"$gte": today_start}}},
            {"$group": {"_id": "$driver_id"}},
            {"$count": "count"}
        ]
        active_users_result = await db.inspections.aggregate(active_users_pipeline).to_list(1)
        today_active_users = active_users_result[0]["count"] if active_users_result else 0
        
        # Pre-start metrics
        week_start = today_start - timedelta(days=7)
        month_start = today_start - timedelta(days=30)
        prestart_today = await db.inspections.count_documents({
            "timestamp": {"$gte": today_start},
            "inspection_type": {"$in": ["pre_start", "pre-start", None]}
        })
        prestart_week = await db.inspections.count_documents({
            "timestamp": {"$gte": week_start},
            "inspection_type": {"$in": ["pre_start", "pre-start", None]}
        })
        prestart_month = await db.inspections.count_documents({
            "timestamp": {"$gte": month_start},
            "inspection_type": {"$in": ["pre_start", "pre-start", None]}
        })
        
        # Company breakdown
        companies = []
        async for company in db.companies.find({}, {"_id": 1, "name": 1, "created_at": 1, "trial_started_at": 1, "subscription_plan": 1}):
            company_id = str(company["_id"])
            user_count = await db.users.count_documents({"company_id": company_id})
            vehicle_count = await db.vehicles.count_documents({"company_id": company_id})
            inspection_count = await db.inspections.count_documents({"company_id": company_id})
            
            # Determine status
            if company.get("subscription_plan") and company["subscription_plan"] != "trial":
                status = "active"
            elif company.get("trial_started_at"):
                # Handle both datetime and string formats
                trial_started = company["trial_started_at"]
                if isinstance(trial_started, str):
                    try:
                        trial_started = datetime.fromisoformat(trial_started.replace('Z', '+00:00'))
                    except Exception:
                        trial_started = datetime.now(timezone.utc)
                trial_end = trial_started + timedelta(days=14)
                if datetime.now(timezone.utc) < trial_end:
                    status = "trialing"
                else:
                    status = "trial_expired"
            else:
                status = "unknown"
            
            # Handle created_at - could be datetime or string
            created_at_val = company.get("created_at")
            if created_at_val:
                if isinstance(created_at_val, datetime):
                    created_at_str = created_at_val.isoformat()
                else:
                    created_at_str = str(created_at_val)
            else:
                created_at_str = None
            
            companies.append({
                "id": company_id,
                "name": company.get("name", "Unknown"),
                "users": user_count,
                "vehicles": vehicle_count,
                "inspections": inspection_count,
                "status": status,
                "created_at": created_at_str
            })
        
        # Sort by inspections desc
        companies.sort(key=lambda x: x["inspections"], reverse=True)
        
        # Calculate response time
        response_time_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
        
        # Estimate database size (rough calculation)
        # Each document ~2KB avg, plus photos stored as base64
        estimated_db_size_mb = round(
            (total_users + total_vehicles + total_inspections + total_fuel_logs + 
             total_service_records + total_incidents + total_companies) * 0.002 + 
            estimated_photo_storage_mb, 1
        )
        
        return {
            "system": {
                "status": "online" if response_time_ms < 1000 else ("slow" if response_time_ms < 3000 else "error"),
                "status_message": "All systems operational",
                "response_time_ms": response_time_ms,
                "errors_24h": 0,  # TODO: Implement error tracking
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "totals": {
                "companies": total_companies,
                "users": total_users,
                "drivers": total_drivers,
                "admins": total_admins,
                "vehicles": total_vehicles,
                "inspections": total_inspections,
                "fuel_logs": total_fuel_logs,
                "service_records": total_service_records,
                "incidents": total_incidents,
                "photos": total_photos,
                "estimated_photo_storage_mb": estimated_photo_storage_mb
            },
            "prestart_metrics": {
                "today": prestart_today,
                "week": prestart_week,
                "month": prestart_month
            },
            "today": {
                "inspections": today_inspections,
                "fuel_logs": today_fuel_logs,
                "new_users": today_new_users,
                "new_companies": today_new_companies,
                "active_users": today_active_users
            },
            "companies": companies,
            "database": {
                "estimated_size_mb": estimated_db_size_mb,
                "max_size_mb": 512  # MongoDB Atlas free tier
            }
        }
    except Exception as e:
        logger.error(f"Developer stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/developer/company/{company_id}")
async def get_developer_company_details(
    company_id: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Get detailed company info for developer dashboard"""
    try:
        company = await db.companies.find_one({"_id": ObjectId(company_id)})
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Get all users in this company
        users = []
        async for user in db.users.find({"company_id": company_id}, {"password": 0}):
            users.append({
                "id": str(user["_id"]),
                "username": user.get("username", ""),
                "email": user.get("email", ""),
                "role": user.get("role", "driver"),
                "is_frozen": user.get("is_frozen", False),
                "created_at": user.get("created_at").isoformat() if isinstance(user.get("created_at"), datetime) else str(user.get("created_at", ""))
            })
        
        # Get vehicles
        vehicles = []
        async for vehicle in db.vehicles.find({"company_id": company_id}):
            vehicles.append({
                "id": str(vehicle["_id"]),
                "name": vehicle.get("name", ""),
                "registration_number": vehicle.get("registration_number", ""),
                "type": vehicle.get("type", "")
            })
        
        # Get recent activity
        recent_inspections = await db.inspections.count_documents({
            "company_id": company_id,
            "timestamp": {"$gte": datetime.now(timezone.utc) - timedelta(days=7)}
        })
        
        return {
            "id": str(company["_id"]),
            "name": company.get("name", "Unknown"),
            "email": company.get("email", ""),
            "created_at": company.get("created_at").isoformat() if isinstance(company.get("created_at"), datetime) else str(company.get("created_at", "")),
            "subscription_plan": company.get("subscription_plan", "trial"),
            "users": users,
            "vehicles": vehicles,
            "stats": {
                "total_users": len(users),
                "total_vehicles": len(vehicles),
                "inspections_this_week": recent_inspections
            }
        }
    except Exception as e:
        logger.error(f"Developer company details error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/developer/users")
async def get_developer_all_users(
    current_user: dict = Depends(require_platform_owner),
):
    """Get all users across all companies for developer dashboard"""
    try:
        users = []
        async for user in db.users.find({}, {"password": 0}):
            # Get company name
            company_name = "Unknown"
            if user.get("company_id"):
                company = await db.companies.find_one({"_id": ObjectId(user["company_id"])})
                if company:
                    company_name = company.get("name", "Unknown")
            
            users.append({
                "id": str(user["_id"]),
                "username": user.get("username", ""),
                "email": user.get("email", ""),
                "role": user.get("role", "driver"),
                "company_id": user.get("company_id", ""),
                "company_name": company_name,
                "is_frozen": user.get("is_frozen", False),
                "created_at": user.get("created_at").isoformat() if isinstance(user.get("created_at"), datetime) else str(user.get("created_at", ""))
            })
        
        return {"users": users}
    except Exception as e:
        logger.error(f"Developer users error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/developer/users/{user_id}/freeze")
async def toggle_user_freeze(
    user_id: str,
    freeze: bool = True,
    current_user: dict = Depends(require_platform_owner),
):
    """Freeze or unfreeze a user account"""
    try:
        result = await db.users.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"is_frozen": freeze, "updated_at": datetime.now(timezone.utc)}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {"message": f"User {'frozen' if freeze else 'unfrozen'} successfully"}
    except Exception as e:
        logger.error(f"Developer freeze user error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.put("/developer/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: str,
    new_password: str = "temp123",
    current_user: dict = Depends(require_platform_owner),
):
    """Reset a user's password (developer emergency access)"""
    try:
        validate_password_policy(new_password)
        target = await db.users.find_one({"_id": ObjectId(user_id)}, {"password_hash": 1})
        if not target:
            raise HTTPException(status_code=404, detail="User not found")
        reject_same_password(new_password, target.get("password_hash"))

        hashed_password = get_password_hash(new_password)
        result = await db.users.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {
                "password_hash": hashed_password,
                "is_frozen": False,
                "updated_at": datetime.now(timezone.utc)
            }}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="User not found")

        return {"message": "Password reset successfully", "temp_password": new_password}
    except Exception as e:
        logger.error(f"Developer reset password error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.delete("/developer/company/{company_id}")
async def delete_company(
    company_id: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Delete a company and all associated data (Developer only)"""
    try:
        deleted = {
            "users": (await db.users.delete_many({"company_id": company_id})).deleted_count,
            "vehicles": (await db.vehicles.delete_many({"company_id": company_id})).deleted_count,
            "inspections": (await db.inspections.delete_many({"company_id": company_id})).deleted_count,
            "inspection_photos": (await db.inspection_photos.delete_many({"company_id": company_id})).deleted_count,
            "fuel_submissions": (await db.fuel_submissions.delete_many({"company_id": company_id})).deleted_count,
            "incidents": (await db.incidents.delete_many({"company_id": company_id})).deleted_count,
            "alerts": (await db.alerts.delete_many({"company_id": company_id})).deleted_count,
            "service_records": (await db.service_records.delete_many({"company_id": company_id})).deleted_count,
        }
        
        result = await db.companies.delete_one({"_id": ObjectId(company_id)})
        deleted["company"] = result.deleted_count
        
        return {"message": "Company deleted", "deleted": deleted}
    except Exception as e:
        logger.error(f"Developer delete company error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/developer/platform-stats")
async def developer_platform_stats(
    current_user: dict = Depends(require_platform_owner),
):
    """Platform-wide stats for the owner dashboard.

    One endpoint that returns:
      * tenant + vehicle + inspection counts
      * subscription expiry distribution by criticality bucket
      * MinIO storage breakdown by bucket (raw bytes + object count)
      * compression-savings estimate vs raw (per STORAGE-PLAN ratio)

    All cheap — the heavy bit is the MinIO list, capped per bucket so
    a tenant with millions of objects doesn't stall this call.
    """
    now = utcnow()

    tenant_count = await db.companies.count_documents({
        **_soft_delete_filter(),
    })
    vehicle_count = await db.vehicles.count_documents({
        **_soft_delete_filter(),
    })
    inspection_count = await db.inspections.count_documents({})
    driver_count = await db.users.count_documents({
        **_soft_delete_filter(),
        "role": UserRole.DRIVER,
    })

    # Subscription criticality buckets. We look at companies'
    # ``trial_ends_at`` / ``subscription_ends_at`` whichever is later
    # (the company is "active" until the later of the two passes).
    companies = await db.companies.find(
        {**_soft_delete_filter()},
        {"_id": 1, "name": 1, "subdomain": 1, "subscription_status": 1,
         "trial_ends_at": 1, "subscription_ends_at": 1, "suspended": 1},
    ).to_list(2000)
    buckets = {"expired": [], "lt7d": [], "7_30d": [], "gt30d": [], "no_expiry": []}
    for co in companies:
        trial_end = co.get("trial_ends_at") if isinstance(co.get("trial_ends_at"), datetime) else None
        sub_end = co.get("subscription_ends_at") if isinstance(co.get("subscription_ends_at"), datetime) else None
        # Pick the later of the two as the "effective end".
        candidates = [d for d in (trial_end, sub_end) if d is not None]
        eff_end = max(candidates) if candidates else None
        item = {
            "company_id": str(co["_id"]),
            "name": co.get("name", ""),
            "subdomain": co.get("subdomain"),
            "suspended": bool(co.get("suspended")),
            "effective_end": eff_end.isoformat() if eff_end else None,
            "status": co.get("subscription_status"),
        }
        if eff_end is None:
            buckets["no_expiry"].append(item)
        else:
            days_left = (eff_end - now).days
            if days_left < 0:
                buckets["expired"].append({**item, "days_left": days_left})
            elif days_left < 7:
                buckets["lt7d"].append({**item, "days_left": days_left})
            elif days_left < 30:
                buckets["7_30d"].append({**item, "days_left": days_left})
            else:
                buckets["gt30d"].append({**item, "days_left": days_left})

    # Phase 4.4 (2026-05-18 plan):
    #  * Cap lifted from 5000 → 50000 objects per bucket so most tenants
    #    are no longer truncated. We still flag `capped: true` if we hit
    #    the new cap so the UI can render an "at least" qualifier.
    #  * Dropped the hard-coded 89% compression ratio — we don't measure
    #    raw input sizes anywhere, so the savings claim was fiction. The
    #    response now reports bytes_stored only.
    #  * Added a `categories` breakdown sourced from cheap Mongo counts
    #    × a per-category size constant so the owner can see "X photos"
    #    rather than only "bucket Y has Z bytes".
    BUCKETS = (
        "logos", "compliance", "inspection-photos", "fuel-receipts",
        "incident-photos", "incident-attachments", "service-records",
        "maintenance", "signatures", "photos",
    )
    SCAN_CAP = 50000
    storage: dict = {
        "buckets": {}, "total_bytes": 0, "total_objects": 0,
        "bytes_stored_actual": 0, "capped_buckets": [],
    }
    for name in BUCKETS:
        try:
            paginator = object_store._s3_client.get_paginator("list_objects_v2")
            total_b = 0
            total_n = 0
            scanned = 0
            capped_this_bucket = False
            for page in paginator.paginate(Bucket=name, PaginationConfig={"MaxItems": SCAN_CAP}):
                for obj in page.get("Contents") or []:
                    total_b += obj.get("Size", 0) or 0
                    total_n += 1
                    scanned += 1
                    if scanned >= SCAN_CAP:
                        capped_this_bucket = True
                        break
                if scanned >= SCAN_CAP:
                    break
            storage["buckets"][name] = {"bytes": total_b, "objects": total_n}
            storage["total_bytes"] += total_b
            storage["total_objects"] += total_n
            if capped_this_bucket:
                storage["capped_buckets"].append(name)
        except Exception as exc:
            storage["buckets"][name] = {"error": f"{type(exc).__name__}: {exc}"}
    storage["bytes_stored_actual"] = storage["total_bytes"]
    storage["capped"] = bool(storage["capped_buckets"])

    # Phase 4.4 — content-category breakdown. Counts are exact Mongo
    # aggregations; the bytes column is `count × per-category KB`, so
    # call it an estimate to distinguish from the authoritative per-
    # bucket totals.
    storage["categories"] = await _compute_storage_categories(company_filter=None)

    return {
        "tenants": tenant_count,
        "vehicles": vehicle_count,
        "drivers": driver_count,
        "inspections": inspection_count,
        "subscriptions": {
            "buckets": {
                "expired": len(buckets["expired"]),
                "critical_lt_7d": len(buckets["lt7d"]),
                "warning_7_30d": len(buckets["7_30d"]),
                "ok_gt_30d": len(buckets["gt30d"]),
                "no_expiry": len(buckets["no_expiry"]),
            },
            # Surface the actual list of expiring tenants so the
            # dashboard can show them inline (capped at 50 to keep the
            # response bounded).
            "expiring_soon": (buckets["expired"] + buckets["lt7d"] + buckets["7_30d"])[:50],
        },
        "storage": storage,
        "generated_at": now.isoformat(),
    }


@api_router.get("/developer/storage-breakdown/{company_id}")
async def developer_storage_breakdown(
    company_id: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Per-tenant storage breakdown (drives the Owner Storage info popover).

    Owner review 2026-05-20 — previously this returned estimate-based
    bytes (count × per-category KB constant) which never matched the
    real bytes the Organisations table read straight from MinIO. Fix:
    pull real per-bucket bytes from MinIO (same logic as /orgs/{id}/storage)
    and split each category's share proportionally by Mongo count within
    that bucket. Result: modal totals reconcile with the table.
    """
    if not ObjectId.is_valid(company_id):
        raise HTTPException(status_code=400, detail="Invalid company_id")
    company = await db.companies.find_one(
        {"_id": ObjectId(company_id)},
        {"name": 1, "subdomain": 1},
    )
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    categories = await _compute_storage_categories(
        company_filter={"company_id": company_id},
    )

    # Real per-bucket bytes — same scan as /developer/orgs/{id}/storage.
    BUCKET_PREFIXES: dict = {
        "logos":                [f"{company_id}/"],
        "compliance":           [f"driver-docs/{company_id}/", f"vehicle-docs/{company_id}/"],
        "inspection-photos":    [f"{company_id}/"],
        "fuel-receipts":        [f"{company_id}/"],
        "incident-photos":      [f"{company_id}/"],
        "incident-attachments": [f"{company_id}/"],
        "service-records":      [f"{company_id}/"],
        "maintenance":          [f"{company_id}/"],
        "signatures":           [f"{company_id}/"],
        "photos":               [f"{company_id}/"],
    }
    bucket_bytes: dict = {}
    bucket_objects: dict = {}
    for name, prefixes in BUCKET_PREFIXES.items():
        b_bytes = 0
        b_objs = 0
        try:
            paginator = object_store._s3_client.get_paginator("list_objects_v2")
            for prefix in prefixes:
                for page in paginator.paginate(
                    Bucket=name, Prefix=prefix,
                    PaginationConfig={"MaxItems": 50000},
                ):
                    for obj in page.get("Contents") or []:
                        b_bytes += obj.get("Size", 0) or 0
                        b_objs += 1
        except Exception:
            pass
        bucket_bytes[name] = b_bytes
        bucket_objects[name] = b_objs

    # Map each category to the MinIO bucket it lives in.
    CATEGORY_TO_BUCKET = {
        "prestart_photos":         "inspection-photos",
        "endshift_photos":         "inspection-photos",
        "inspection_signatures":   "signatures",
        "incident_damage_photos":  "incident-photos",
        "incident_scene_photos":   "incident-photos",
        "incident_other_photos":   "incident-photos",
        "fuel_receipts":           "fuel-receipts",
        "service_attachments":     "service-records",
        "maintenance_invoices":    "maintenance",
        "driver_documents":        "compliance",
        "company_logos":           "logos",
        "vehicle_images":          "photos",
    }
    # Sum of category counts within each bucket — for proportional split.
    counts_in_bucket: dict = {}
    for cat in categories:
        bucket = CATEGORY_TO_BUCKET.get(cat["key"])
        if bucket:
            counts_in_bucket[bucket] = counts_in_bucket.get(bucket, 0) + (cat.get("count") or 0)

    # Allocate real bytes to each category proportionally to its count
    # within its bucket. Falls back to the legacy estimate when the
    # bucket scan returns 0 objects but Mongo says the category has rows
    # (rare — usually means the object_key field is stale).
    for cat in categories:
        bucket = CATEGORY_TO_BUCKET.get(cat["key"])
        if not bucket:
            cat["bytes_actual"] = 0
            continue
        bb = bucket_bytes.get(bucket, 0)
        denom = counts_in_bucket.get(bucket, 0)
        if denom > 0 and bb > 0:
            cat["bytes_actual"] = int(bb * (cat.get("count", 0) / denom))
        else:
            cat["bytes_actual"] = 0

    category_object_count = sum(c.get("count", 0) for c in categories)
    total_bytes_actual = sum(c.get("bytes_actual", 0) for c in categories)
    total_bytes_real_buckets = sum(bucket_bytes.values())
    total_objects_real_buckets = sum(bucket_objects.values())

    return {
        "company_id": company_id,
        "company_name": company.get("name"),
        "subdomain": company.get("subdomain"),
        "categories": categories,
        # Match what the Organisations table shows — real MinIO scan,
        # all objects under the tenant's prefix (includes thumbnails +
        # orphans). The per-category counts only see Mongo-indexed files.
        "total_objects": total_objects_real_buckets,
        "total_bytes": total_bytes_real_buckets,
        # Category sum (lower; ignores thumbnails + orphan objects).
        "categorised_objects": category_object_count,
        "total_bytes_categorised": total_bytes_actual,
        # Legacy fields kept for backward-compat with older clients.
        "total_objects_estimate": category_object_count,
        "total_bytes_estimate": sum(c.get("bytes_estimate", 0) for c in categories),
        "bucket_totals": bucket_bytes,
        "bucket_objects": bucket_objects,
        "generated_at": utcnow().isoformat(),
    }


@api_router.get("/developer/recent-activity")
async def developer_recent_activity(
    limit: int = 100,
    current_user: dict = Depends(require_platform_owner),
):
    """Platform-wide recent activity for the owner dashboard.

    Pulls from audit_trail across all tenants (platform owner sees
    everything). Capped at 200; default 100. Each row carries the
    tenant context so the UI can render "company X · user Y · action Z".
    """
    actual_limit = max(1, min(limit, 200))
    rows = await db.audit_trail.find({}).sort("timestamp", -1).limit(actual_limit).to_list(actual_limit)

    # Enrich with user + company names (small joins; capped roster).
    user_ids = list({str(r.get("user_id")) for r in rows if r.get("user_id")})
    users = await db.users.find(
        {"_id": {"$in": [ObjectId(u) for u in user_ids if ObjectId.is_valid(u)]}},
        {"name": 1, "email": 1, "company_id": 1},
    ).to_list(500)
    user_map = {str(u["_id"]): u for u in users}
    co_ids = list({u.get("company_id") for u in users if u.get("company_id")})
    companies = await db.companies.find(
        {"_id": {"$in": [ObjectId(c) for c in co_ids if ObjectId.is_valid(c)]}},
        {"name": 1, "subdomain": 1},
    ).to_list(500)
    co_map = {str(c["_id"]): c for c in companies}

    enriched = []
    for r in rows:
        u = user_map.get(str(r.get("user_id"))) or {}
        co = co_map.get(str(u.get("company_id"))) or {}
        enriched.append({
            **serialize_doc(r),
            "user_name": u.get("name") or u.get("email"),
            "company_name": co.get("name"),
            "company_subdomain": co.get("subdomain"),
        })
    return {"items": enriched, "count": len(enriched)}


# ============== Public pricing ==============
#
# Anonymous endpoint so the landing + /pricing pages can render the
# live numbers without authenticating. Cached via the existing
# `get_pricing()` helper which reads platform_config.

@api_router.get("/pricing")
async def public_pricing():
    """Public pricing — used by landing page + /pricing."""
    p = await get_pricing()
    return {
        "base_price": p["base_price"],
        "vehicle_price": p["per_vehicle"],
        "trial_days": p["trial_days"],
        "currency": p.get("currency", "AUD"),
        "cadence": p.get("cadence", "monthly"),
    }


# ============== Developer: pricing config ==============

@api_router.get("/developer/pricing")
async def developer_get_pricing(
    current_user: dict = Depends(require_platform_owner),
):
    """Read the platform-wide pricing config + Stripe sync state.

    Phase 4.1 (2026-05-18 plan) — raw Stripe identifiers (price_xxx,
    webhook secrets, API keys) are never returned. The UI only ever
    sees boolean "configured / not configured" booleans so a screenshot
    of the owner panel can't leak anything operationally sensitive.
    """
    p = await get_pricing()
    stripe_cfg = p.get("stripe") or {}
    return {
        "base_price": p["base_price"],
        "per_vehicle": p["per_vehicle"],
        "trial_days": p["trial_days"],
        "trial_enabled": p.get("trial_enabled", True),
        "trial_max_vehicles": p.get("trial_max_vehicles"),
        "currency": p.get("currency", "AUD"),
        "cadence": p.get("cadence", "monthly"),
        "stripe_status": {
            "configured": bool(stripe.api_key),
            "webhook_configured": bool(os.environ.get("STRIPE_WEBHOOK_SECRET")),
            "base_price_set": bool(stripe_cfg.get("base_price_id")),
            "vehicle_price_set": bool(stripe_cfg.get("vehicle_price_id")),
            "last_synced_at": stripe_cfg.get("synced_at"),
        },
        "updated_at": p.get("updated_at"),
    }


class PricingUpdate(BaseModel):
    base_price: Optional[float] = None
    per_vehicle: Optional[float] = None
    trial_days: Optional[int] = None
    trial_enabled: Optional[bool] = None
    trial_max_vehicles: Optional[int] = None  # None == unlimited
    currency: Optional[str] = None
    # `cadence` was removed — only monthly billing is supported. Kept
    # on the model as Optional[str] to gracefully ignore old clients
    # still sending it, but the value is never persisted.
    cadence: Optional[str] = None


@api_router.put("/developer/pricing")
async def developer_set_pricing(
    payload: PricingUpdate,
    current_user: dict = Depends(require_platform_owner),
):
    """Update platform pricing. Persists to platform_config and, when
    Stripe is configured, also pushes new Price objects to Stripe.

    Stripe push failures DO NOT fail the request — the local save is
    authoritative; the response carries ``stripe_synced=false`` so the
    UI can show a banner.
    """
    update: dict = {}
    for k in (
        "base_price", "per_vehicle", "trial_days", "currency",
        "trial_enabled", "trial_max_vehicles",
    ):
        v = getattr(payload, k)
        if v is None:
            continue
        if k in {"base_price", "per_vehicle"} and (v < 0 or v > 10000):
            raise HTTPException(status_code=400, detail=f"{k} out of range")
        if k == "trial_days" and (v < 0 or v > 365):
            raise HTTPException(status_code=400, detail="trial_days must be 0-365")
        if k == "trial_max_vehicles" and v is not None and (v < 0 or v > 10000):
            raise HTTPException(status_code=400, detail="trial_max_vehicles must be 0-10000 or null")
        update[k] = v
    if not update:
        raise HTTPException(status_code=400, detail="No pricing fields provided")

    update["updated_at"] = utcnow()
    update["updated_by"] = str(current_user.get("_id"))

    await db.platform_config.update_one(
        {"_id": "pricing"},
        {"$set": update},
        upsert=True,
    )

    stripe_synced = False
    stripe_error: Optional[str] = None
    stripe_state: Optional[dict] = None
    if stripe.api_key:
        try:
            current = await get_pricing()
            currency = (current.get("currency") or "AUD").lower()
            interval = "month"  # Annual plan was removed — monthly only.
            # Create new Price for the base; vehicle add-on stays as a
            # separate price object so we can vary per_vehicle without
            # touching base. Stripe Prices are immutable, so we make
            # new ones and archive the previous IDs (best-effort).
            base_price_obj = stripe.Price.create(
                currency=currency,
                unit_amount=int(round(current["base_price"] * 100)),
                recurring={"interval": interval},
                product_data={"name": "FleetShield365 Pro — Base"},
                metadata={"role": "base", "updated_at": update["updated_at"].isoformat()},
            )
            vehicle_price_obj = stripe.Price.create(
                currency=currency,
                unit_amount=int(round(current["per_vehicle"] * 100)),
                recurring={"interval": interval},
                product_data={"name": "FleetShield365 Pro — Per Vehicle"},
                metadata={"role": "per_vehicle", "updated_at": update["updated_at"].isoformat()},
            )

            # Archive previous prices (non-fatal)
            try:
                prev = await db.platform_config.find_one({"_id": "pricing"})
                old = (prev or {}).get("stripe") or {}
                for old_id in [old.get("base_price_id"), old.get("vehicle_price_id")]:
                    if old_id:
                        try:
                            stripe.Price.modify(old_id, active=False)
                        except Exception:
                            pass
            except Exception:
                pass

            stripe_state = {
                "base_price_id": base_price_obj.id,
                "vehicle_price_id": vehicle_price_obj.id,
                "synced_at": utcnow().isoformat(),
            }
            await db.platform_config.update_one(
                {"_id": "pricing"},
                {"$set": {"stripe": stripe_state}},
            )
            stripe_synced = True
        except Exception as e:
            logger.error(f"developer_set_pricing: Stripe sync failed: {e}")
            stripe_error = str(e)

    # Audit
    try:
        await log_audit_trail(
            str(current_user.get("_id")),
            "update", "pricing_config", "pricing",
            "platform-owner-panel",
            {"changes": {k: update.get(k) for k in update if k != "updated_at"}, "stripe_synced": stripe_synced},
        )
    except Exception:
        pass

    return {
        "ok": True,
        # Phase 4.1 — never echo raw Stripe IDs back to the owner UI.
        "stripe_status": {
            "configured": bool(stripe.api_key),
            "webhook_configured": bool(os.environ.get("STRIPE_WEBHOOK_SECRET")),
            "base_price_set": bool((stripe_state or {}).get("base_price_id")),
            "vehicle_price_set": bool((stripe_state or {}).get("vehicle_price_id")),
            "last_synced_at": (stripe_state or {}).get("synced_at"),
        },
        "stripe_synced": stripe_synced,
        "stripe_error": stripe_error,
    }


# ============== Developer: server disk usage ==============

@api_router.get("/developer/orgs/{company_id}/storage")
async def developer_org_storage(
    company_id: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Per-org MinIO storage usage. Owner panel calls this on demand
    from the Organisations table (not auto-loaded — could touch tens
    of thousands of objects across all tenants if we did)."""
    # Phase 4.4 — cap raised from 5000 → 50000 to match the platform
    # endpoint. Adds a content-category breakdown sourced from the
    # tenant's own collections.
    # Owner review 2026-05-19: most buckets store objects under
    # "<company_id>/..." but `compliance` (driver licence / medical /
    # cert photos) lives under "driver-docs/<company_id>/...". The
    # original loop only scanned "<company_id>/" so the entire driver-
    # docs slice was being reported as 0 bytes. Each bucket now lists
    # its own prefix(es) so the totals match what's actually on disk.
    BUCKET_PREFIXES: dict = {
        "logos":                [f"{company_id}/"],
        "compliance":           [f"driver-docs/{company_id}/", f"vehicle-docs/{company_id}/"],
        "inspection-photos":    [f"{company_id}/"],
        "fuel-receipts":        [f"{company_id}/"],
        "incident-photos":      [f"{company_id}/"],
        "incident-attachments": [f"{company_id}/"],
        "service-records":      [f"{company_id}/"],
        "maintenance":          [f"{company_id}/"],
        "signatures":           [f"{company_id}/"],
        "photos":               [f"{company_id}/"],
    }
    SCAN_CAP = 50000
    total_bytes = 0
    total_objects = 0
    capped_buckets: list = []
    by_bucket: dict = {}
    for name, prefixes in BUCKET_PREFIXES.items():
        try:
            paginator = object_store._s3_client.get_paginator("list_objects_v2")
            b_bytes = 0
            b_objects = 0
            scanned = 0
            capped_this = False
            for prefix in prefixes:
                for page in paginator.paginate(
                    Bucket=name,
                    Prefix=prefix,
                    PaginationConfig={"MaxItems": SCAN_CAP},
                ):
                    for obj in page.get("Contents") or []:
                        b_bytes += obj.get("Size", 0) or 0
                        b_objects += 1
                        scanned += 1
                        if scanned >= SCAN_CAP:
                            capped_this = True
                            break
                    if scanned >= SCAN_CAP:
                        break
                if scanned >= SCAN_CAP:
                    break
            by_bucket[name] = {"bytes": b_bytes, "objects": b_objects}
            total_bytes += b_bytes
            total_objects += b_objects
            if capped_this:
                capped_buckets.append(name)
        except Exception as exc:
            by_bucket[name] = {"error": f"{type(exc).__name__}: {exc}"}

    categories = await _compute_storage_categories(
        company_filter={"company_id": company_id}
    )

    return {
        "company_id": company_id,
        "total_bytes": total_bytes,
        "bytes_stored_actual": total_bytes,
        "total_objects": total_objects,
        "by_bucket": by_bucket,
        "buckets": by_bucket,  # alias for parity with platform endpoint
        "capped": bool(capped_buckets),
        "capped_buckets": capped_buckets,
        "categories": categories,
        "generated_at": utcnow().isoformat(),
    }


@api_router.get("/developer/server-disk")
async def developer_server_disk(
    current_user: dict = Depends(require_platform_owner),
):
    """Server-side disk usage for the owner Storage page.

    Uses ``shutil.disk_usage`` on the EC2 root partition + a handful
    of well-known mount points (Mongo data dir, MinIO data dir). All
    sizes in bytes. The frontend formats to GB.
    """
    import shutil as _shutil
    import os as _os

    def _safe(p: str) -> Optional[dict]:
        try:
            usage = _shutil.disk_usage(p)
            return {
                "path": p,
                "total": usage.total,
                "used": usage.used,
                "free": usage.free,
                "percent": round((usage.used / usage.total) * 100, 1) if usage.total else 0.0,
            }
        except Exception:
            return None

    def _dir_size(p: str) -> Optional[int]:
        try:
            total = 0
            for dirpath, _dirnames, filenames in _os.walk(p):
                for f in filenames:
                    fp = _os.path.join(dirpath, f)
                    try:
                        total += _os.path.getsize(fp)
                    except OSError:
                        pass
            return total
        except Exception:
            return None

    root = _safe("/")
    mongo = _safe("/var/lib/fleetshield365/mongo")
    minio = _safe("/var/lib/fleetshield365/minio")

    # 2026-05-22 — owner reported the Storage page showed 7.4 MB for
    # Mongo while real disk usage was 433 MB. Root cause: the
    # fleetshield service user can't read mongo's diagnostic.data /
    # journal subdirs so os.walk silently skipped them. Use Mongo's
    # own dbStats command to get the authoritative storage size
    # instead of walking the filesystem. The MinIO path also had a
    # capitalisation bug ("MinIO" vs the actual "minio") which is
    # fixed above.
    mongo_size_bytes: Optional[int] = None
    try:
        stats = await db.command("dbStats")
        # storageSize = on-disk allocated space (including indexes +
        # padding) — matches what `du -s` would report.
        mongo_size_bytes = int(stats.get("storageSize") or 0) + int(stats.get("indexSize") or 0)
    except Exception:
        mongo_size_bytes = _dir_size("/var/lib/fleetshield365/mongo")

    return {
        "root": root,
        "mongo": mongo,
        "minio": minio,
        "mongo_size_bytes": mongo_size_bytes,
        "minio_size_bytes": _dir_size("/var/lib/fleetshield365/minio"),
        "generated_at": utcnow().isoformat(),
    }


# ============== Developer: audit log viewer ==============

@api_router.get("/developer/audit")
async def developer_audit_log(
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    company_id: Optional[str] = None,
    start: Optional[str] = None,  # ISO date
    end: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    current_user: dict = Depends(require_platform_owner),
):
    """Filterable audit-trail viewer for the owner panel."""
    page = max(1, page)
    page_size = max(1, min(page_size, 200))

    query: dict = {}
    if user_id:
        query["user_id"] = user_id
    if action:
        query["action"] = {"$regex": action, "$options": "i"}
    if company_id:
        # audit_trail rows don't always store company_id — fall back to
        # looking up which users belong to the company and filtering by
        # those user_ids.
        co_users = await db.users.find(
            {"company_id": company_id},
            {"_id": 1},
        ).to_list(2000)
        co_user_ids = [str(u["_id"]) for u in co_users]
        if co_user_ids:
            existing = query.get("user_id")
            if existing:
                # combine — both filters must match. user_id is single,
                # so override with $in only if compatible.
                query["user_id"] = {"$in": [existing] if isinstance(existing, str) else co_user_ids}
            else:
                query["user_id"] = {"$in": co_user_ids}
        else:
            return {"items": [], "total": 0, "page": page, "page_size": page_size}
    if start:
        try:
            query.setdefault("timestamp", {})["$gte"] = datetime.fromisoformat(start.replace("Z", "+00:00"))
        except Exception:
            pass
    if end:
        try:
            query.setdefault("timestamp", {})["$lte"] = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except Exception:
            pass

    total = await db.audit_trail.count_documents(query)
    rows = (
        await db.audit_trail
        .find(query)
        .sort("timestamp", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
        .to_list(page_size)
    )

    # Enrich
    user_ids = list({str(r.get("user_id")) for r in rows if r.get("user_id")})
    users = await db.users.find(
        {"_id": {"$in": [ObjectId(u) for u in user_ids if ObjectId.is_valid(u)]}},
        {"name": 1, "email": 1, "company_id": 1},
    ).to_list(500)
    user_map = {str(u["_id"]): u for u in users}
    co_ids = list({u.get("company_id") for u in users if u.get("company_id")})
    companies = await db.companies.find(
        {"_id": {"$in": [ObjectId(c) for c in co_ids if ObjectId.is_valid(c)]}},
        {"name": 1, "subdomain": 1},
    ).to_list(500)
    co_map = {str(c["_id"]): c for c in companies}

    items = []
    for r in rows:
        u = user_map.get(str(r.get("user_id"))) or {}
        co = co_map.get(str(u.get("company_id"))) or {}
        items.append({
            **serialize_doc(r),
            "user_name": u.get("name") or u.get("email"),
            "company_name": co.get("name"),
            "company_subdomain": co.get("subdomain"),
        })
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# ============== Developer: broadcast email ==============

_BROADCAST_LAST_RUN: dict = {"at": None}


class BroadcastRequest(BaseModel):
    subject: str
    body: str
    active_only: bool = True


@api_router.post("/developer/broadcast")
async def developer_broadcast(
    payload: BroadcastRequest,
    current_user: dict = Depends(require_platform_owner),
):
    """Send a broadcast email to every super_admin (per filter)."""
    subj = (payload.subject or "").strip()
    body = (payload.body or "").strip()
    if not subj or not body:
        raise HTTPException(status_code=400, detail="Subject and body are required")
    if len(subj) > 200:
        raise HTTPException(status_code=400, detail="Subject too long")
    if len(body) > 20000:
        raise HTTPException(status_code=400, detail="Body too long")

    # 60-second rate limit so an accidental double-click doesn't double-blast.
    now = utcnow()
    last = _BROADCAST_LAST_RUN.get("at")
    if last and (now - last).total_seconds() < 60:
        raise HTTPException(status_code=429, detail="Broadcasts are limited to one per minute")
    _BROADCAST_LAST_RUN["at"] = now

    # Resolve company filter
    company_query: dict = {}
    if payload.active_only:
        # Treat anything that's not explicitly "trial_expired" or
        # "suspended" as active.
        company_query["subscription_status"] = {"$nin": ["trial_expired", "suspended"]}
    co_docs = await db.companies.find(company_query, {"_id": 1}).to_list(2000)
    co_ids = [str(c["_id"]) for c in co_docs]
    if not co_ids:
        return {"sent": 0, "failed": 0, "recipients": 0}

    recipients = await db.users.find(
        {"company_id": {"$in": co_ids}, "role": UserRole.SUPER_ADMIN},
        {"email": 1, "name": 1},
    ).to_list(5000)

    html_body = (
        "<div style=\"font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;\">"
        + body.replace("\n", "<br/>")
        + "<hr style=\"margin-top:24px; border:none; border-top:1px solid #e5e7eb;\"/>"
        + "<p style=\"color:#94a3b8; font-size:12px;\">Sent from FleetShield365 platform team.</p>"
        + "</div>"
    )

    sent = 0
    failed = 0
    for r in recipients:
        addr = r.get("email")
        if not addr:
            continue
        try:
            await send_system_email(addr, subj, html_body) if "send_system_email" in globals() else await send_email_notification(addr, subj, html_body)
            sent += 1
        except Exception:
            failed += 1

    try:
        await log_audit_trail(
            str(current_user.get("_id")),
            "broadcast", "platform_email", "broadcast",
            "platform-owner-panel",
            {"subject": subj, "sent": sent, "failed": failed, "active_only": payload.active_only},
        )
    except Exception:
        pass

    return {"sent": sent, "failed": failed, "recipients": len(recipients)}


# ============== Owner-panel: org summary + charts ==============

@api_router.get("/developer/orgs/summary")
async def developer_orgs_summary(
    current_user: dict = Depends(require_platform_owner),
):
    """One row per tenant with all the fields the Organizations page
    needs. Computed off existing data — vehicle count, user count,
    subscription status, suspended flag, derived last_active_at and
    inactive_days. Cheap aggregations, fine to call on every page
    load (the owner panel is internal/low-volume)."""
    now = utcnow()
    companies = await db.companies.find({}, {
        "name": 1, "subdomain": 1, "subscription_status": 1, "subscription_plan": 1,
        "trial_end": 1, "vehicle_count": 1, "suspended": 1, "suspended_at": 1,
        "suspended_reason": 1, "created_at": 1, "deleted_at": 1,
    }).to_list(2000)
    companies = [c for c in companies if not c.get("deleted_at")]
    co_ids = [str(c["_id"]) for c in companies]

    # Owner review 2026-05-19: vehicle_count on the companies doc is the
    # value the owner typed at sign-up (defaults to 5). Replace it with
    # the live count of non-deleted vehicles in the vehicles collection
    # so the Organisations table reflects reality, not a stale stub.
    vehicle_rows = await db.vehicles.find(
        {"company_id": {"$in": co_ids}},
        {"company_id": 1, "deleted_at": 1},
    ).to_list(50000)
    vehicles_by_co: dict = {}
    for v in vehicle_rows:
        if v.get("deleted_at"):
            continue
        vehicles_by_co[v.get("company_id")] = vehicles_by_co.get(v.get("company_id"), 0) + 1

    # Fan-out user counts + last_active per company in one pass each.
    # user_count = admins + drivers (all non-deleted users on the
    # tenant). driver_count + admin_count are surfaced separately so
    # the UI can break it down if needed.
    user_rows = await db.users.find(
        {"company_id": {"$in": co_ids}},
        {"company_id": 1, "role": 1, "last_active_at": 1, "deleted_at": 1, "email": 1, "name": 1, "is_frozen": 1},
    ).to_list(20000)
    by_co: dict = {}
    for u in user_rows:
        if u.get("deleted_at"):
            continue
        cid = u.get("company_id")
        bucket = by_co.setdefault(cid, {
            "user_count": 0, "admin_count": 0, "driver_count": 0,
            "owner": None, "last_active": None,
        })
        bucket["user_count"] += 1
        role = u.get("role")
        if role == UserRole.DRIVER:
            bucket["driver_count"] += 1
        elif role in (UserRole.ADMIN, UserRole.SUPER_ADMIN):
            bucket["admin_count"] += 1
        if role == UserRole.SUPER_ADMIN and bucket["owner"] is None:
            bucket["owner"] = {
                "id": str(u["_id"]),
                "name": u.get("name"),
                "email": u.get("email"),
                "is_frozen": bool(u.get("is_frozen")),
            }
        la = u.get("last_active_at")
        if la and (bucket["last_active"] is None or la > bucket["last_active"]):
            bucket["last_active"] = la

    rows = []
    for c in companies:
        cid = str(c["_id"])
        bucket = by_co.get(cid, {
            "user_count": 0, "admin_count": 0, "driver_count": 0,
            "owner": None, "last_active": None,
        })
        last_active = bucket["last_active"]
        inactive_days: Optional[int] = None
        if last_active and isinstance(last_active, datetime):
            inactive_days = max(0, (now - last_active).days)

        trial_end = c.get("trial_end")
        trial_days_left: Optional[int] = None
        if trial_end:
            try:
                te = trial_end if isinstance(trial_end, datetime) else datetime.fromisoformat(str(trial_end).replace("Z", "+00:00"))
                if te.tzinfo:
                    te = te.astimezone(timezone.utc).replace(tzinfo=None)
                trial_days_left = (te - now).days
            except Exception:
                trial_days_left = None

        rows.append({
            "id": cid,
            "name": c.get("name"),
            "subdomain": c.get("subdomain"),
            "subscription_status": c.get("subscription_status"),
            "subscription_plan": c.get("subscription_plan"),
            "trial_end": trial_end.isoformat() if isinstance(trial_end, datetime) else trial_end,
            "trial_days_left": trial_days_left,
            # Owner review 2026-05-19: live counts, not the registration
            # placeholder. signup_vehicle_count preserved for context.
            "vehicle_count": vehicles_by_co.get(cid, 0),
            "signup_vehicle_count": c.get("vehicle_count") or 0,
            "user_count": bucket["user_count"],
            "admin_count": bucket["admin_count"],
            "driver_count": bucket["driver_count"],
            "suspended": bool(c.get("suspended")),
            "suspended_at": c.get("suspended_at").isoformat() if isinstance(c.get("suspended_at"), datetime) else c.get("suspended_at"),
            "suspended_reason": c.get("suspended_reason"),
            "created_at": c.get("created_at").isoformat() if isinstance(c.get("created_at"), datetime) else c.get("created_at"),
            "last_active_at": last_active.isoformat() if isinstance(last_active, datetime) else None,
            "inactive_days": inactive_days,
            "owner": bucket["owner"],
        })
    rows.sort(key=lambda r: (r.get("name") or "").lower())
    return {"items": rows, "count": len(rows)}


@api_router.get("/developer/charts/revenue")
async def developer_revenue_chart(
    days: int = 30,
    current_user: dict = Depends(require_platform_owner),
):
    """Daily PROJECTED revenue series. Computed from current pricing
    × active tenants. Real (Stripe-reported) revenue can replace this
    once a non-trivial number of subscriptions are live."""
    days = max(1, min(days, 365))
    pricing = await get_pricing()
    base = float(pricing.get("base_price") or 0)
    per_vehicle = float(pricing.get("per_vehicle") or 0)

    # For each day in the window, count tenants that were created on
    # or before that day, are active (not suspended, not trial_expired),
    # and were not deleted before that day. Single in-memory pass.
    companies = await db.companies.find({}, {
        "subscription_status": 1, "suspended": 1, "vehicle_count": 1,
        "created_at": 1, "deleted_at": 1,
    }).to_list(5000)

    def _as_dt(v) -> Optional[datetime]:
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo else v
        if isinstance(v, str):
            try:
                d = datetime.fromisoformat(v.replace("Z", "+00:00"))
                return d.astimezone(timezone.utc).replace(tzinfo=None) if d.tzinfo else d
            except Exception:
                return None
        return None

    today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    series = []
    for d in range(days - 1, -1, -1):
        day = today - timedelta(days=d)
        day_end = day + timedelta(days=1)
        revenue = 0.0
        for c in companies:
            created = _as_dt(c.get("created_at"))
            deleted = _as_dt(c.get("deleted_at"))
            if created is None or created >= day_end:
                continue
            if deleted is not None and deleted < day_end:
                continue
            if c.get("suspended"):
                continue
            if c.get("subscription_status") == "trial_expired":
                continue
            vc = int(c.get("vehicle_count") or 0)
            revenue += base + per_vehicle * vc
        series.append({"date": day.strftime("%Y-%m-%d"), "revenue": round(revenue, 2)})
    return {"series": series, "currency": pricing.get("currency", "AUD")}


@api_router.get("/developer/charts/activity")
async def developer_activity_chart(
    days: int = 30,
    current_user: dict = Depends(require_platform_owner),
):
    """Daily platform-wide counts of inspections / fuel logs / incidents."""
    days = max(1, min(days, 365))
    now = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days - 1)

    async def _bucket(collection_name: str, field: str = "timestamp"):
        cur = db[collection_name].aggregate([
            {"$match": {field: {"$gte": start}}},
            {"$project": {
                "day": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${field}"}}
            }},
            {"$group": {"_id": "$day", "n": {"$sum": 1}}},
        ])
        out: dict = {}
        async for r in cur:
            out[r["_id"]] = r["n"]
        return out

    insp = await _bucket("inspections", "timestamp")
    fuel = await _bucket("fuel_submissions", "timestamp")
    inc = await _bucket("incidents", "created_at")

    series = []
    for d in range(days - 1, -1, -1):
        key = (now - timedelta(days=d)).strftime("%Y-%m-%d")
        series.append({
            "date": key,
            "inspections": insp.get(key, 0),
            "fuel": fuel.get(key, 0),
            "incidents": inc.get(key, 0),
        })
    return {"series": series}


# ============== Owner-panel: one-off user + org email ==============

class OwnerEmailMessage(BaseModel):
    subject: str
    body: str


@api_router.get("/developer/users/{user_id}")
async def developer_get_user(
    user_id: str,
    current_user: dict = Depends(require_platform_owner),
):
    user = await db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return sanitize_user_doc(serialize_doc(user))


@api_router.post("/developer/users/{user_id}/send-email")
async def developer_email_user(
    user_id: str,
    payload: OwnerEmailMessage,
    current_user: dict = Depends(require_platform_owner),
):
    user = await db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    addr = user.get("email")
    if not addr:
        raise HTTPException(status_code=400, detail="User has no email on file")
    subj = (payload.subject or "").strip()
    body = (payload.body or "").strip()
    if not subj or not body:
        raise HTTPException(status_code=400, detail="Subject and body are required")
    html_body = (
        "<div style=\"font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;\">"
        + body.replace("\n", "<br/>")
        + "<hr style=\"margin-top:24px; border:none; border-top:1px solid #e5e7eb;\"/>"
        + "<p style=\"color:#94a3b8; font-size:12px;\">Sent from FleetShield365 platform team.</p>"
        + "</div>"
    )
    sender_helper = globals().get("send_system_email") or send_email_notification
    try:
        await sender_helper(addr, subj, html_body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Send failed: {e}")
    try:
        await log_audit_trail(
            str(current_user.get("_id")),
            "owner_email_user", "user", user_id,
            "platform-owner-panel",
            {"subject": subj, "to": addr},
        )
    except Exception:
        pass
    return {"ok": True, "sent_to": addr}


@api_router.post("/developer/companies/{company_id}/send-email-owner")
async def developer_email_org_owner(
    company_id: str,
    payload: OwnerEmailMessage,
    current_user: dict = Depends(require_platform_owner),
):
    """Send a one-off message to the super_admin of a tenant. If the
    tenant has multiple super_admins they all receive it."""
    owners = await db.users.find(
        {"company_id": company_id, "role": UserRole.SUPER_ADMIN},
        {"email": 1, "name": 1},
    ).to_list(20)
    targets = [u for u in owners if u.get("email")]
    if not targets:
        raise HTTPException(status_code=400, detail="No super_admin with email on file for this tenant")
    subj = (payload.subject or "").strip()
    body = (payload.body or "").strip()
    if not subj or not body:
        raise HTTPException(status_code=400, detail="Subject and body are required")
    html_body = (
        "<div style=\"font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;\">"
        + body.replace("\n", "<br/>")
        + "<hr style=\"margin-top:24px; border:none; border-top:1px solid #e5e7eb;\"/>"
        + "<p style=\"color:#94a3b8; font-size:12px;\">Sent from FleetShield365 platform team.</p>"
        + "</div>"
    )
    sender_helper = globals().get("send_system_email") or send_email_notification
    sent = 0
    failed = 0
    for o in targets:
        try:
            await sender_helper(o["email"], subj, html_body)
            sent += 1
        except Exception:
            failed += 1
    try:
        await log_audit_trail(
            str(current_user.get("_id")),
            "owner_email_org", "company", company_id,
            "platform-owner-panel",
            {"subject": subj, "sent": sent, "failed": failed},
        )
    except Exception:
        pass
    return {"ok": True, "sent": sent, "failed": failed, "recipients": [o["email"] for o in targets]}


# ============== Owner-panel: landing-media config ==============

@api_router.get("/landing/media")
async def public_landing_media():
    """Public endpoint — landing page hits this to render the
    'See it in the field' photo strip. Owner panel writes via the
    /developer/landing-media/upload endpoint.

    For items that carry an ``object_key`` (owner-uploaded), we always
    regenerate the public URL from the current OBJECT_STORE_PUBLIC_ENDPOINT
    so the URL stays correct across env changes.
    """
    doc = await db.platform_config.find_one({"_id": "landing_media"})
    if doc and isinstance(doc.get("items"), list) and doc["items"]:
        items = doc["items"]
        base = OBJECT_STORE_PUBLIC_ENDPOINT.rstrip("/")
        for it in items:
            ok = it.get("object_key")
            if ok:
                it["url"] = f"{base}/logos/{ok}"
        return {"items": items}
    return {"items": _DEFAULT_LANDING_MEDIA}


class LandingMediaItem(BaseModel):
    key: str
    title: str
    url: str
    alt: Optional[str] = None


class LandingMediaUpdate(BaseModel):
    items: List[LandingMediaItem]


@api_router.put("/developer/landing-media")
async def developer_set_landing_media(
    payload: LandingMediaUpdate,
    current_user: dict = Depends(require_platform_owner),
):
    items = [it.dict() for it in payload.items]
    await db.platform_config.update_one(
        {"_id": "landing_media"},
        {"$set": {"items": items, "updated_at": utcnow(), "updated_by": str(current_user.get("_id"))}},
        upsert=True,
    )
    return {"ok": True, "count": len(items)}


# Allowed slugs for the landing-media grid. Matches the six tiles on
# the marketing LandingPage.tsx. Keep this in sync with _DEFAULT_LANDING_MEDIA
# below + the keys hard-coded in the marketing page so an owner upload
# always lands on a slot the page actually renders.
_LANDING_MEDIA_SLUGS = {"trucks", "trailers", "excavators", "forklifts", "cranes", "utes"}


@api_router.post("/developer/landing-media/upload")
async def developer_upload_landing_media(
    key: str = Form(...),
    title: Optional[str] = Form(None),
    alt: Optional[str] = Form(None),
    file: UploadFile = File(...),
    current_user: dict = Depends(require_platform_owner),
):
    """Owner-panel hero-image upload for one landing tile.

    Validates the slug + file (image only, magic bytes, size cap), pushes
    bytes to the public ``logos`` bucket under ``landing/<key>.<ext>``,
    then upserts the matching entry in ``platform_config.landing_media.items``
    so the public ``GET /api/landing/media`` immediately serves the new URL.
    """
    if key not in _LANDING_MEDIA_SLUGS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid landing key. Allowed: {sorted(_LANDING_MEDIA_SLUGS)}",
        )

    contents = await file.read()
    # Reuse the "logo" upload group: image-only, 2 MB cap, magic-byte check.
    detected_format = _validate_upload_or_400(contents, "logo", "file")
    content_type = _FORMAT_TO_CONTENT_TYPE[detected_format]
    ext = {"jpeg": "jpg", "png": "png", "webp": "webp"}.get(detected_format, "jpg")

    # Platform-level asset — no tenant prefix. logos bucket is anonymous-readable.
    object_key = f"landing/{key}.{ext}"
    try:
        _upload_with_thumbnail("logos", object_key, contents, content_type, expected_company_id=None)
    except Exception as exc:
        logger.error(f"Landing media upload failed for key={key}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to upload image")

    # Compose the public URL. logos bucket has an anonymous-GET policy on
    # this stack so we serve a stable, cache-friendly URL rather than a
    # 1-hour presigned link. The marketing page caches per-pageview so a
    # long-lived URL keeps Cloudflare's edge cache happy.
    public_url = f"{OBJECT_STORE_PUBLIC_ENDPOINT.rstrip('/')}/logos/{object_key}"

    # Load existing items (or defaults), upsert the entry for this key.
    doc = await db.platform_config.find_one({"_id": "landing_media"})
    items: List[dict] = (doc or {}).get("items") or [dict(d) for d in _DEFAULT_LANDING_MEDIA]
    updated = False
    for it in items:
        if it.get("key") == key:
            if title is not None:
                it["title"] = title
            if alt is not None:
                it["alt"] = alt
            it["url"] = public_url
            it["object_key"] = object_key
            updated = True
            break
    if not updated:
        items.append({
            "key": key,
            "title": title or key.title(),
            "alt": alt or "",
            "url": public_url,
            "object_key": object_key,
        })

    await db.platform_config.update_one(
        {"_id": "landing_media"},
        {"$set": {"items": items, "updated_at": utcnow(), "updated_by": str(current_user.get("_id"))}},
        upsert=True,
    )

    return {"ok": True, "key": key, "url": public_url, "items": items}


_DEFAULT_LANDING_MEDIA: List[dict] = [
    {"key": "trucks",     "title": "Prime movers",          "alt": "Heavy truck on a highway at dusk",
     "url": "https://images.unsplash.com/photo-1601584115197-04ecc0da31d7?w=800&q=70&auto=format&fit=crop"},
    {"key": "trailers",   "title": "Trailers",              "alt": "Curtain-sided trailers at a depot",
     "url": "https://images.unsplash.com/photo-1591768793355-74d04bb6608f?w=800&q=70&auto=format&fit=crop"},
    {"key": "excavators", "title": "Excavators",            "alt": "Excavator on a construction site",
     "url": "https://images.unsplash.com/photo-1525908484335-9d2c2a01cd6e?w=800&q=70&auto=format&fit=crop"},
    {"key": "forklifts",  "title": "Forklifts",             "alt": "Forklift moving pallets in a warehouse",
     "url": "https://images.unsplash.com/photo-1601598851547-4302969d0614?w=800&q=70&auto=format&fit=crop"},
    {"key": "cranes",     "title": "Cranes",                "alt": "Mobile crane lifting steel beams",
     "url": "https://images.unsplash.com/photo-1605152276897-4f618f831968?w=800&q=70&auto=format&fit=crop"},
    {"key": "utes",       "title": "Light vehicles & utes", "alt": "Pickup truck on a country road",
     "url": "https://images.unsplash.com/photo-1568605117036-5fe5e7bab0b7?w=800&q=70&auto=format&fit=crop"},
]


@api_router.post("/developer/companies/{company_id}/suspend")
async def developer_suspend_company(
    company_id: str,
    reason: Optional[str] = None,
    current_user: dict = Depends(require_platform_owner),
):
    """Suspend a tenant (Phase 8 of TODO.md).

    Platform owner only. Suspended tenants' admins can still read their
    data but can't write anything (require_active_tenant returns 423).
    Login still works so they can pay / contact support.
    """
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    res = await db.companies.update_one(
        {"_id": oid},
        {
            "$set": {
                "suspended": True,
                "suspended_at": utcnow(),
                "suspended_by": str(current_user["_id"]),
                "suspended_reason": (reason or "").strip() or "Account suspended",
            }
        },
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Company not found")
    return {"status": "suspended", "company_id": company_id}


@api_router.post("/developer/companies/{company_id}/unsuspend")
async def developer_unsuspend_company(
    company_id: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Lift a suspension (Phase 8)."""
    try:
        oid = ObjectId(company_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid company_id")

    res = await db.companies.update_one(
        {"_id": oid},
        {
            "$set": {"suspended": False},
            "$unset": {"suspended_at": "", "suspended_by": "", "suspended_reason": ""},
        },
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Company not found")
    return {"status": "active", "company_id": company_id}


@api_router.delete("/developer/clear-all")
async def developer_clear_all(
    confirm: str,
    current_user: dict = Depends(require_platform_owner),
):
    """Drop every tenant document in every collection.

    Guarded by the ``require_platform_owner`` dependency (JWT role check)
    AND a mandatory ``confirm=DELETE_EVERYTHING`` query parameter
    (Requirement 15.9). Both gates must pass before a single
    ``delete_many({})`` runs; anything less returns HTTP 400.

    The platform-owner user document(s) are NOT deleted — the scratch
    space must stay reachable immediately after a reset so the operator
    does not have to re-run ``bootstrap_platform_owner.py`` mid-migration.
    """

    if confirm != "DELETE_EVERYTHING":
        raise HTTPException(
            status_code=400,
            detail="Pass confirm=DELETE_EVERYTHING to proceed.",
        )

    collections = [
        "companies", "vehicles", "inspections", "inspection_photos",
        "fuel_submissions", "incidents", "alerts", "service_records",
        "maintenance_logs", "support_requests", "audit_trail",
        "password_resets", "email_logs", "notification_preferences",
        "push_tokens", "temp_photos", "photos",
    ]
    deleted: dict[str, int] = {}
    for coll in collections:
        try:
            result = await db[coll].delete_many({})
            deleted[coll] = result.deleted_count
        except Exception as exc:
            logger.warning(
                "developer_clear_all: failed to clear %s: %s", coll, exc
            )
            deleted[coll] = 0

    # Users: wipe everyone except platform owners so the owner dashboard
    # is still reachable after the reset without re-running the bootstrap
    # script.
    deleted["users"] = (
        await db.users.delete_many({"is_platform_owner": {"$ne": True}})
    ).deleted_count

    return {"message": "All tenant data deleted", "deleted": deleted}


# ============== Tenant Resolution (Req 11) ==============


class TenantResolveRequest(BaseModel):
    """Request body for POST /api/tenant/resolve.

    The subdomain is provided lowercased by the web client (the router
    derives it from ``window.location.hostname``), but we still normalize
    + validate server-side so a direct cURL with mixed case or trailing
    whitespace still works consistently.
    """

    subdomain: str


class TenantResolveResponse(BaseModel):
    """Response for POST /api/tenant/resolve (Req 11.3).

    ``logo_url`` is a presigned GET URL for the company logo stored in
    the ``logos`` bucket; null when the tenant has not uploaded a logo.
    ``logo_object_key`` is exposed for debugging and for clients that
    want to cache the URL locally.
    """

    company_id: str
    name: str
    logo_object_key: Optional[str] = None
    logo_url: Optional[str] = None


@api_router.post("/tenant/resolve", response_model=TenantResolveResponse)
@limiter.limit("10/minute")
async def resolve_tenant(payload: TenantResolveRequest, request: Request = None):
    """Look up a tenant by subdomain slug and return branding info.

    No auth required (Req 11.2). Reserved slugs never resolve and
    always 404 (Req 11.5) — ``www``, ``api``, ``admin``, ``owner`` etc.
    are not tenants. Unknown slugs 404 (Req 11.4). Successful resolution
    returns the company id, display name, and a presigned logo URL so
    the web client can render the branded login form before auth.
    """

    raw = (payload.subdomain or "").strip().lower()
    if not raw or raw in RESERVED_SUBDOMAINS:
        raise HTTPException(status_code=404, detail="Tenant not found")

    company = await db.companies.find_one(
        {"subdomain": raw},
        {"name": 1, "logo_object_key": 1},
    )
    if not company:
        raise HTTPException(status_code=404, detail="Tenant not found")

    logo_key = company.get("logo_object_key")
    return TenantResolveResponse(
        company_id=str(company["_id"]),
        name=company.get("name", ""),
        logo_object_key=logo_key,
        logo_url=_presign_if_key("logos", logo_key) if logo_key else None,
    )


# ============== Company Subdomain Rename (Req 17) ==============


# Cooldown between successive subdomain renames. Also used as the grace
# window during which a retired slug remains reserved before it becomes
# claimable again (Req 17.5).
SUBDOMAIN_RENAME_COOLDOWN: timedelta = timedelta(days=30)


class SubdomainRenameRequest(BaseModel):
    """Body for PUT /api/company/subdomain.

    ``new_subdomain`` is validated against ``SUBDOMAIN_REGEX`` and the
    reserved list; malformed / reserved values 400; collisions with an
    active tenant 409; requests inside the 30-day cooldown 429 with a
    ``next_permitted_at`` timestamp in the response body.
    """

    new_subdomain: str


def _subdomain_recently_retired(
    history: List[dict],
    candidate: str,
    now: datetime,
) -> bool:
    """Return True if ``candidate`` appears in ``history`` within the
    cooldown window (Req 17.5). Entries older than the cooldown free up
    the slug for re-use.
    """

    cutoff = now - SUBDOMAIN_RENAME_COOLDOWN
    for entry in history or []:
        if entry.get("old_subdomain") != candidate:
            continue
        changed_at = entry.get("changed_at")
        if isinstance(changed_at, datetime) and changed_at >= cutoff:
            return True
    return False


@api_router.get("/company/subdomain-suggest")
async def suggest_subdomain_from_name(
    name: str,
    current_user: dict = Depends(get_current_user),
):
    """Owner review 2026-05-18: when an admin renames the company, we
    want to suggest a matching tenant subdomain rather than make them
    type one. The slug_generator helper already handles uniqueness by
    suffixing a, b, c, … against active tenants + the recently-retired
    history. This endpoint just exposes that for the Settings UI.
    """
    role = current_user.get("jwt_role") or current_user.get("role")
    if role not in {UserRole.SUPER_ADMIN, UserRole.PLATFORM_OWNER}:
        raise HTTPException(
            status_code=403,
            detail="Only super_admin or platform_owner may rename the subdomain",
        )
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="name is required")

    company_id = current_user.get("company_id")
    company = await db.companies.find_one(
        {"_id": ObjectId(company_id)},
        {"subdomain": 1, "subdomain_history": 1, "subdomain_last_renamed_at": 1},
    )
    current_subdomain = (company or {}).get("subdomain")

    suggested = await slug_generator(name, db)
    # If the slug_generator picked the same slug we already have, there
    # is nothing to rename — surface that to the UI so it can hide the
    # prompt rather than offering a confusing "rename to <current>" CTA.
    is_same = (suggested == current_subdomain)

    # Cooldown — reuse the same window the actual rename endpoint enforces.
    next_permitted_at = None
    last = (company or {}).get("subdomain_last_renamed_at")
    if isinstance(last, datetime):
        next_permitted_at = (last + SUBDOMAIN_RENAME_COOLDOWN).isoformat()

    return {
        "current_subdomain": current_subdomain,
        "suggested_subdomain": suggested,
        "is_same": is_same,
        "next_permitted_at": next_permitted_at,
        "in_cooldown": bool(
            isinstance(last, datetime)
            and (utcnow() - last) < SUBDOMAIN_RENAME_COOLDOWN
        ),
    }


@api_router.put("/company/subdomain")
async def rename_company_subdomain(
    payload: SubdomainRenameRequest,
    current_user: dict = Depends(get_current_user),
):
    """Rename the authenticated tenant's subdomain.

    Authz: only ``super_admin`` (company owner) or ``platform_owner``
    may rename a subdomain (Req 17.6); any other role 403s. Rate limit:
    the most recent entry in ``companies.subdomain_history`` must be
    older than ``SUBDOMAIN_RENAME_COOLDOWN`` (Req 17.3), otherwise 429
    with ``next_permitted_at``. Uniqueness check excludes the current
    tenant so a no-op rename does not 409 itself (Req 17.1).
    """

    role = current_user.get("jwt_role") or current_user.get("role")
    if role not in {UserRole.SUPER_ADMIN, UserRole.PLATFORM_OWNER}:
        raise HTTPException(
            status_code=403,
            detail="Only super_admin or platform_owner may rename the subdomain",
        )

    company_id = current_user.get("company_id")
    if not company_id:
        raise HTTPException(status_code=400, detail="User has no company context")

    # Validate + normalize the candidate (Req 17.2).
    try:
        new_slug = validate_subdomain(payload.new_subdomain)
    except SubdomainValidationError as exc:
        raise _subdomain_error_to_http(exc)

    company = await db.companies.find_one({"_id": ObjectId(company_id)})
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    current_slug = company.get("subdomain")
    if new_slug == current_slug:
        # No-op rename — treat as success without touching history or
        # starting a new cooldown.
        return {"subdomain": current_slug, "changed": False}

    # 30-day cooldown check (Req 17.3). Based on the most recent history
    # entry's changed_at. Missing / malformed history == no cooldown.
    history: List[dict] = company.get("subdomain_history") or []
    now = utcnow()
    if history:
        most_recent = history[-1]
        changed_at = most_recent.get("changed_at")
        if isinstance(changed_at, datetime):
            next_permitted = changed_at + SUBDOMAIN_RENAME_COOLDOWN
            if now < next_permitted:
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Subdomain rename is rate limited (30-day cooldown).",
                        "next_permitted_at": next_permitted.isoformat(),
                    },
                )

    # Uniqueness against active tenants (Req 17.1), excluding self.
    try:
        await ensure_subdomain_unique(
            new_slug, db, exclude_company_id=company_id
        )
    except SubdomainValidationError as exc:
        raise _subdomain_error_to_http(exc)

    # Freshly retired slugs are reserved for the cooldown window
    # (Req 17.5). Checking company history covers the "rename back to a
    # recently used value" case; a broader cross-tenant retired check is
    # out of scope because retired slugs are always scoped to the tenant
    # that held them.
    if _subdomain_recently_retired(history, new_slug, now):
        raise HTTPException(
            status_code=409,
            detail="Subdomain was recently retired; try again in 30 days.",
        )

    # Persist the rename + history entry atomically.
    history_entry = {
        "old_subdomain": current_slug,
        "changed_at": now,
    }
    await db.companies.update_one(
        {"_id": ObjectId(company_id)},
        {
            "$set": {"subdomain": new_slug, "updated_at": now},
            "$push": {"subdomain_history": history_entry},
        },
    )

    return {
        "subdomain": new_slug,
        "previous_subdomain": current_slug,
        "changed": True,
        "changed_at": now.isoformat(),
    }

# ============== Health Check ==============
#
# Req 2: these must be reachable without the /api prefix when the API is
# served standalone on EC2 (Nginx_Proxy forwards /health -> uvicorn /health
# and / -> uvicorn /). They are additionally mirrored under /api so the
# existing clients that call /api/health continue to work during the
# cutover.

@app.get("/")
async def app_root():
    """Root endpoint reachable without /api prefix (Req 2.2)."""
    return {"message": "FleetShield365 API", "version": "1.0.0"}


@app.get("/health")
async def app_health():
    """Health probe reachable without /api prefix (Req 2.1, 2.3).

    Response body contains ``status: "ok"`` per Req 2.1.
    """
    return {"status": "ok", "timestamp": utcnow().isoformat()}


@api_router.get("/")
async def root():
    return {"message": "FleetShield365 API", "version": "1.0.0"}

@api_router.get("/health")
async def health_check():
    """Liveness + dependency probe (Phase 9 of TODO.md).

    Pings Mongo (ismaster) and MinIO (head_bucket on logos) with
    tight timeouts. Returns HTTP 200 only when all three layers
    answer; HTTP 503 otherwise with a per-dependency breakdown so
    the external uptime monitor can render a useful incident page.

    Kept fast — the goal is <100 ms when healthy. The MinIO ping
    uses head_bucket against the public logos bucket, which is
    cheap and validates both the access key and network path.
    """
    started = utcnow()
    deps: dict = {"mongo": "unknown", "minio": "unknown"}
    ok = True

    # Mongo: a short-timeout server_info call.
    try:
        await asyncio.wait_for(client.server_info(), timeout=2.0)
        deps["mongo"] = "ok"
    except Exception as exc:
        deps["mongo"] = f"down: {type(exc).__name__}"
        ok = False

    # MinIO: head_bucket on the always-present logos bucket. Run
    # the blocking boto3 call on a thread so it doesn't block the
    # event loop on a network stall.
    try:
        await asyncio.wait_for(
            asyncio.to_thread(
                object_store._s3_client.head_bucket, Bucket="logos"
            ),
            timeout=2.0,
        )
        deps["minio"] = "ok"
    except Exception as exc:
        deps["minio"] = f"down: {type(exc).__name__}"
        ok = False

    body = {
        "status": "ok" if ok else "degraded",
        "timestamp": started.isoformat(),
        "dependencies": deps,
    }
    if not ok:
        return _JSONResponse(status_code=503, content=body)
    return body


# Phase 6 of TODO.md — minimum-version gate for the mobile app.
# Defaults are tolerant (min=0) so existing builds never get force-
# upgraded by accident. Set the env vars when you actually need to
# cut off old clients (e.g. after a breaking API change).
MIN_IOS_BUILD_NUMBER = _phase3_env_int("MOBILE_MIN_IOS_BUILD", 0)
MIN_ANDROID_VERSION_CODE = _phase3_env_int("MOBILE_MIN_ANDROID_VERSION_CODE", 0)
RECOMMENDED_VERSION = os.environ.get("MOBILE_RECOMMENDED_VERSION", "1.0.0").strip() or "1.0.0"


@api_router.get("/version-min")
async def get_version_min():
    """Return the minimum mobile build that's still allowed to call the API.

    Mobile clients call this on cold start. When the device's installed
    build is below the floor, the app shows a "Please update" screen
    that links to the relevant store and refuses to submit anything.

    Defaults are 0/0 so this is a no-op until you actually need to
    drop old clients. The store URLs are stable platform-level
    constants; they don't change per env.
    """
    return {
        "min_ios_build": MIN_IOS_BUILD_NUMBER,
        "min_android_version_code": MIN_ANDROID_VERSION_CODE,
        "recommended_version": RECOMMENDED_VERSION,
        "store_url_ios": "https://apps.apple.com/app/fleetshield365/id6760111342",
        "store_url_android": "https://play.google.com/store/apps/details?id=com.fleetshield365&pcampaignid=web_share",
    }

# Include router
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_origin_regex=r"^https://[a-z0-9-]+\.fleetshield365\.com$",
    allow_credentials=True,
    # Phase 3 — explicit method + header allowlist. Earlier "*" allowed
    # every possible method/header through preflight; the platform only
    # uses the verbs below and only needs the headers below.
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
)

async def ensure_indexes() -> None:
    """Idempotently create every index the application relies on.

    Called from the FastAPI startup event and also available for the
    deploy-time migration script to invoke standalone (Req 12.3, 24.5).
    Every ``create_index`` call is idempotent on MongoDB — if the index
    already exists with the same definition it is a no-op; if it exists
    with a conflicting definition Mongo raises and the caller sees a
    clear error.
    """

    from pymongo.collation import Collation

    # --- users ---------------------------------------------------------
    # Legacy indexes on users.email and users.company_id_username may
    # exist from earlier releases with different options; drop then
    # recreate sparse-unique so the new shape is authoritative.
    try:
        await db.users.drop_index("email_1")
    except Exception:
        pass
    try:
        await db.users.drop_index("company_id_1_username_1")
    except Exception:
        pass

    await db.users.create_index("email", unique=True, sparse=True)
    await db.users.create_index("username", unique=True, sparse=True)
    await db.users.create_index([("company_id", 1), ("role", 1)])
    await db.users.create_index("company_id")
    # Phase 4 — phone uniqueness. Sparse so users without a phone
    # don't conflict (multiple nulls allowed). Helps the duplicate-
    # driver-by-phone detection on create/invite.
    try:
        await db.users.create_index("phone", unique=True, sparse=True)
    except Exception as exc:
        # Pre-existing duplicate phones in production would block index
        # creation. Log and skip rather than fail every backend start
        # until the duplicates are cleaned up manually.
        logger.warning("Could not create unique sparse index on users.phone: %s", exc)
    # Phase 4 — common query: list soft-deleted rows for the Trash view.
    # Compound index supports the {company_id, deleted_at} sort path
    # used by /api/admin/recently-deleted.
    for coll_name in ("vehicles", "users", "service_records", "maintenance_logs", "incidents"):
        try:
            await db[coll_name].create_index([("company_id", 1), ("deleted_at", -1)])
        except Exception:
            pass

    # --- companies -----------------------------------------------------
    # Case-insensitive unique sparse index on companies.subdomain
    # (Req 9.2, 9.6). The unique constraint is enforced by Mongo at the
    # server level so concurrent registrations cannot both claim the
    # same slug even if the app-level check races.
    await db.companies.create_index(
        "subdomain",
        unique=True,
        sparse=True,
        collation=Collation(locale="en", strength=2),
    )

    # --- vehicles ------------------------------------------------------
    await db.vehicles.create_index("company_id")
    await db.vehicles.create_index([("company_id", 1), ("registration_number", 1)])
    await db.vehicles.create_index([("company_id", 1), ("status", 1)])
    await db.vehicles.create_index([("company_id", 1), ("rego_expiry", 1)])
    await db.vehicles.create_index([("company_id", 1), ("insurance_expiry", 1)])
    await db.vehicles.create_index([("company_id", 1), ("safety_certificate_expiry", 1)])
    await db.vehicles.create_index([("company_id", 1), ("coi_expiry", 1)])

    # --- inspections / photos -----------------------------------------
    await db.inspections.create_index("company_id")
    await db.inspections.create_index([("company_id", 1), ("timestamp", -1)])
    await db.inspections.create_index([("company_id", 1), ("vehicle_id", 1), ("timestamp", -1)])
    await db.inspections.create_index([("driver_id", 1), ("timestamp", -1)])
    await db.inspection_photos.create_index([("vehicle_id", 1), ("created_at", -1)])
    await db.inspection_photos.create_index("inspection_id")

    # --- alerts, fuel, incidents, service, maintenance, support, photos
    await db.alerts.create_index("company_id")
    await db.alerts.create_index([("company_id", 1), ("is_read", 1)])
    await db.alerts.create_index([("company_id", 1), ("created_at", -1)])
    await db.fuel_submissions.create_index("company_id")
    await db.fuel_submissions.create_index([("company_id", 1), ("timestamp", -1)])
    await db.incidents.create_index("company_id")
    await db.incidents.create_index([("company_id", 1), ("created_at", -1)])
    await db.incidents.create_index([("company_id", 1), ("status", 1)])

    # 2026-05-19 — idempotency lookups (per-tenant). Sparse so old rows
    # without an idempotency_key don't bloat the index. Used by the
    # POST /inspections/* + /fuel + /incidents handlers to dedupe
    # queue-retry submissions from the mobile app.
    for coll_name in ("inspections", "fuel_submissions", "incidents"):
        try:
            await db[coll_name].create_index(
                [("company_id", 1), ("idempotency_key", 1)],
                sparse=True,
            )
        except Exception as exc:
            logger.warning("idempotency index on %s failed: %s", coll_name, exc)
    await db.service_records.create_index("company_id")
    await db.service_records.create_index([("company_id", 1), ("service_date", -1)])
    await db.service_records.create_index([("company_id", 1), ("vehicle_id", 1)])
    await db.maintenance_logs.create_index("company_id")
    await db.maintenance_logs.create_index([("company_id", 1), ("service_date", -1)])
    await db.support_requests.create_index("company_id")
    await db.photos.create_index("company_id")

    # --- audit_trail ---------------------------------------------------
    await db.audit_trail.create_index("company_id")
    await db.audit_trail.create_index([("timestamp", -1)])
    await db.audit_trail.create_index("user_id")

    # --- password_resets + email_logs ---------------------------------
    # TTL index on password_resets.expires_at auto-removes spent tokens.
    #
    # The mobile PIN-reset flow stores `otp` + `user_id` on the same
    # collection but never sets `token`. The legacy index was
    # ``unique=True`` only — MongoDB treats a missing field as null, so
    # any second mobile reset hit a DuplicateKey on token=null and the
    # whole request 500'd. Fix: rebuild the index as ``sparse=True`` so
    # docs without a token are simply skipped from the index.
    try:
        existing = await db.password_resets.index_information()
        for name, spec in existing.items():
            if name == "token_1" and not spec.get("sparse"):
                await db.password_resets.drop_index(name)
                logger.info("Dropped legacy non-sparse token index on password_resets")
                break
    except Exception as exc:
        logger.warning(f"Could not inspect password_resets indexes: {exc}")
    await db.password_resets.create_index("token", unique=True, sparse=True)
    await db.password_resets.create_index(
        "expires_at", expireAfterSeconds=0
    )
    await db.email_logs.create_index([("sent_at", -1)])

    # --- notifications (Phase 7 — in-app feed) ------------------------
    # Per-user feed: list-unread + sort-by-created-at-desc are the hot
    # paths. Compound indexes cover both.
    await db.notifications.create_index([("user_id", 1), ("created_at", -1)])
    await db.notifications.create_index([("user_id", 1), ("read", 1)])

    # --- revoked_tokens (Phase 3 — JWT revocation list) ---------------
    # TTL index expires entries automatically when the underlying
    # token would have expired naturally — so the collection never
    # grows past the active token horizon.
    await db.revoked_tokens.create_index(
        "expires_at", expireAfterSeconds=0
    )
    await db.revoked_tokens.create_index("user_id")

    # --- email_tokens (verify + invite) -------------------------------
    # Added 2026-05-12 (Phase 1 of STORAGE-PLAN.txt). Stored documents:
    #   {token, user_id, type:"verify"|"invite", expires_at, created_at}
    # Token lookups are by ``token`` (unique). TTL on ``expires_at``
    # auto-cleans expired rows. Compound on (user_id, type) supports
    # the "is there already a verify token for this user?" check
    # used by the resend-verification handler.
    await db.email_tokens.create_index("token", unique=True)
    await db.email_tokens.create_index(
        "expires_at", expireAfterSeconds=0
    )
    await db.email_tokens.create_index([("user_id", 1), ("type", 1)])

    # --- temp_photos TTL (Phase 1 cleanup of upload staging area) ----
    # Documents carry an `expires_at` field; the TTL index makes Mongo
    # delete them automatically 24h after upload if they never got
    # linked to an inspection.
    try:
        await db.temp_photos.create_index(
            "expires_at", expireAfterSeconds=0
        )
    except Exception:
        # Index may already exist with a different name; not fatal.
        pass

    # --- pending_registrations TTL (OTP-first signup, 2026-05-25) ----
    # Rows self-delete 1s after their expires_at so we don't accumulate
    # stale OTP requests. Also unique on email so a fresh request
    # overwrites instead of stacking.
    try:
        await db.pending_registrations.create_index(
            "expires_at", expireAfterSeconds=0,
        )
        await db.pending_registrations.create_index("email", unique=True)
    except Exception:
        pass

    logger.info("ensure_indexes(): DB indexes verified")


@app.on_event("startup")
async def startup_event_indexes():
    """Run the ensure_indexes bootstrap on app startup (Req 24.5, 12.3)."""
    try:
        await ensure_indexes()
    except Exception as exc:
        logger.error("ensure_indexes() failed at startup: %s", exc)
    
    # One-time migration: backfill is_safe for end_shift inspections missing it
    end_shift_missing = await db.inspections.count_documents({
        "type": "end_shift",
        "is_safe": {"$exists": False}
    })
    if end_shift_missing > 0:
        # Safe if no new_damage AND no incident_today
        await db.inspections.update_many(
            {"type": "end_shift", "is_safe": {"$exists": False}, "new_damage": {"$ne": True}, "incident_today": {"$ne": True}},
            {"$set": {"is_safe": True}}
        )
        await db.inspections.update_many(
            {"type": "end_shift", "is_safe": {"$exists": False}},
            {"$set": {"is_safe": False}}
        )
        logger.info(f"Migration: Backfilled is_safe for {end_shift_missing} end-shift inspections")
    
    # Start weekly summary scheduler
    asyncio.create_task(weekly_summary_scheduler())
    logger.info("Weekly summary scheduler started")

    # Daily summary (8 PM Sydney, opt-in per admin)
    asyncio.create_task(daily_summary_scheduler())
    logger.info("Daily summary scheduler started")

    # Missed inspection check (11:30 PM Sydney, default-on per admin)
    asyncio.create_task(missed_inspection_scheduler())
    logger.info("Missed inspection scheduler started")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
