"""S3-compatible object store client wrapper over MinIO.

This module provides a thin wrapper around the ``boto3`` S3 client configured
to talk to a MinIO instance. It centralizes:

* Reading the ObjectStore configuration from environment variables (the same
  env vars and defaults that :mod:`server` reads), so callers do not have to
  import anything from :mod:`server` and we avoid circular imports.
* Construction of a single module-level ``boto3`` S3 client using SigV4 so
  presigned URLs validate against MinIO.
* Helpers for the two common write paths (raw bytes, base64 string), a
  presigned GET URL helper that rewrites the URL to the public
  ``OBJECT_STORE_PUBLIC_ENDPOINT`` host so traffic flows through Nginx_Proxy
  at ``https://api.fleetshield365.com/files/...`` (Requirement 21.13), and a
  delete helper.

All helpers let ``botocore.exceptions.ClientError`` propagate to the caller
so the FastAPI handler can decide on the response semantics.

Requirements covered:

* 21.12 — presigned GET URLs with a bounded TTL.
* 21.13 — presigned URL host rewritten to the public Nginx_Proxy endpoint.
"""

from __future__ import annotations

import base64
import binascii
import os
import re
from urllib.parse import urlsplit, urlunsplit

import boto3
from botocore.config import Config

__all__ = [
    "TenantPrefixViolation",
    "upload_bytes",
    "upload_base64",
    "presign_get",
    "get_bytes",
    "delete",
]


class TenantPrefixViolation(PermissionError):
    """Raised when an Object_Key is not prefixed with the caller's company_id.

    Task 5.5 / Requirement 21.14 require that every non-public object key
    begin with the ``<company_id>`` of the authenticated uploader. This
    class surfaces that authorization violation as a distinct exception so
    the FastAPI handler can translate it into HTTP 403 (not 400 — the
    payload is well-formed; the request is forbidden by policy).

    ``PermissionError`` is chosen as the base class because the violation
    is fundamentally an access-control failure: the caller tried to write
    into another tenant's namespace. Any callsite that wants to catch
    tenant-prefix issues specifically can catch ``TenantPrefixViolation``;
    broader ``PermissionError`` catches still work.
    """


# ---------------------------------------------------------------------------
# Configuration (read from env, matching the defaults used by server.py).
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    """Return the non-empty value of env var ``name`` or raise ``RuntimeError``.

    Mirrors the fail-fast validation used by :mod:`server` for the object
    store credential pair so importing this module with missing creds fails
    loudly rather than later at the first S3 call.
    """

    value: str = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Required env var {name!r} is missing or empty"
        )
    return value


# Required credentials. No defaults — fail-fast on import if absent.
_OBJECT_STORE_ACCESS_KEY: str = _require_env("OBJECT_STORE_ACCESS_KEY")
_OBJECT_STORE_SECRET_KEY: str = _require_env("OBJECT_STORE_SECRET_KEY")

# Internal endpoint the API uses to reach MinIO (Req 3.1).
_OBJECT_STORE_ENDPOINT: str = (
    os.environ.get("OBJECT_STORE_ENDPOINT", "").strip()
    or "http://127.0.0.1:9000"
)

# Public endpoint presigned URLs are rewritten to (Req 21.13).
_OBJECT_STORE_PUBLIC_ENDPOINT: str = (
    os.environ.get("OBJECT_STORE_PUBLIC_ENDPOINT", "").strip()
    or "https://api.fleetshield365.com/files"
)

# S3 region string. MinIO accepts any value; us-east-1 is the conventional
# default used across S3 SDKs.
_OBJECT_STORE_REGION: str = (
    os.environ.get("OBJECT_STORE_REGION", "").strip()
    or "us-east-1"
)

# Presign TTL cap (Requirements 21.12, 21.15). Parsed defensively: missing,
# empty, non-integer, or non-positive values fall back to the documented
# default rather than failing startup.
_DEFAULT_PRESIGN_TTL_SECONDS: int = 3600
_raw_presign_ttl: str = os.environ.get(
    "OBJECT_STORE_PRESIGN_TTL_SECONDS", ""
).strip()
try:
    _parsed_presign_ttl: int = (
        int(_raw_presign_ttl)
        if _raw_presign_ttl
        else _DEFAULT_PRESIGN_TTL_SECONDS
    )
except ValueError:
    _parsed_presign_ttl = _DEFAULT_PRESIGN_TTL_SECONDS
_OBJECT_STORE_PRESIGN_TTL_SECONDS: int = (
    _parsed_presign_ttl
    if _parsed_presign_ttl > 0
    else _DEFAULT_PRESIGN_TTL_SECONDS
)


# ---------------------------------------------------------------------------
# Module-level boto3 S3 client.
# ---------------------------------------------------------------------------


# ``signature_version='s3v4'`` is required so presigned URLs validate against
# MinIO; ``s3={'addressing_style': 'path'}`` ensures path-style URLs of the
# form ``<endpoint>/<bucket>/<key>`` rather than virtual-hosted-style, which
# is what MinIO expects and what the Nginx_Proxy ``/files/`` rewrite assumes.
_s3_client = boto3.client(
    "s3",
    endpoint_url=_OBJECT_STORE_ENDPOINT,
    aws_access_key_id=_OBJECT_STORE_ACCESS_KEY,
    aws_secret_access_key=_OBJECT_STORE_SECRET_KEY,
    region_name=_OBJECT_STORE_REGION,
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


# Map of lowercase file extensions to the Content-Type we stamp on the
# uploaded object. Anything not in this map falls back to
# ``application/octet-stream`` per RFC 2046.
_CONTENT_TYPE_BY_EXT: dict[str, str] = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "pdf": "application/pdf",
}


# Matches a ``data:...;base64,`` prefix on a data URL so callers can pass
# either a raw base64 string or a full data URL from a browser/mobile client.
_DATA_URL_PREFIX_RE: re.Pattern[str] = re.compile(
    r"^data:[^;,]*(?:;[^,]*)*;base64,", re.IGNORECASE
)


def _validate_tenant_prefix(
    bucket: str,
    key: str,
    expected_company_id: str,
) -> None:
    """Raise ``TenantPrefixViolation`` if ``key`` is not tenant-scoped to the caller.

    Per Requirement 21.14 (and design Section 4.18), every non-public
    object key begins with a ``<company_id>`` path segment tying the
    object to the authenticated uploader's tenant. The position of that
    segment depends on the bucket:

    * ``compliance`` keys are of the form
      ``driver-docs/<company_id>/<user_id>/<doc>.<ext>`` — the
      ``<company_id>`` lives at the *second* path segment.
    * Every other bucket (``logos``, ``photos``, ``inspection-photos``,
      ``signatures``, ``fuel-receipts``, ``maintenance``,
      ``service-records``, ``incident-photos``, ``incident-attachments``)
      uses ``<company_id>/<resource>/<uuid>.<ext>`` — ``<company_id>`` is
      the *first* path segment.

    The ``logos`` bucket is public but still enforced: a company owner
    must not be able to overwrite another tenant's logo.

    Leading slashes on the key are stripped before comparison so the
    caller may pass either ``"<company_id>/..."`` or
    ``"/<company_id>/..."`` interchangeably.

    Requirements: 21.14.
    """

    if not expected_company_id:
        # Validation is opt-in per call. Callers that have no JWT context
        # (unit tests, internal tools) can still use the API.
        return

    # Strip a single leading slash for robustness; callers today do not
    # include one but we tolerate it to avoid a confusing false positive.
    normalized_key: str = key[1:] if key.startswith("/") else key
    segments: list[str] = normalized_key.split("/")

    # Compliance bucket uses a "driver-docs/<company_id>/..." layout; the
    # company_id lives at index 1.
    if bucket == "compliance":
        tenant_segment_index: int = 1
    else:
        tenant_segment_index = 0

    actual_segment: str = (
        segments[tenant_segment_index]
        if len(segments) > tenant_segment_index
        else ""
    )

    if actual_segment != expected_company_id:
        raise TenantPrefixViolation(
            f"Object_Key tenant prefix mismatch on bucket {bucket!r}: "
            f"key {key!r} has tenant segment {actual_segment!r}, "
            f"expected {expected_company_id!r}"
        )


def upload_bytes(
    bucket: str,
    key: str,
    data: bytes,
    content_type: str,
    expected_company_id: str | None = None,
) -> None:
    """Upload ``data`` to ``<bucket>/<key>`` with the given ``content_type``.

    Thin wrapper around ``s3.put_object``. When ``expected_company_id`` is
    provided, the key is first validated to ensure its ``<company_id>``
    path segment matches, per Requirement 21.14 — a mismatch raises
    :class:`TenantPrefixViolation` (a ``PermissionError`` subclass)
    *before* any bytes are sent to MinIO, so cross-tenant writes can
    never leak into another tenant's namespace.

    ``botocore.exceptions.ClientError`` propagates to the caller so the
    FastAPI handler can decide on the response semantics.

    Requirements: 21.10, 21.14.
    """

    if expected_company_id:
        _validate_tenant_prefix(bucket, key, expected_company_id)

    _s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def upload_base64(
    bucket: str,
    key: str,
    b64_string: str,
    default_ext: str = "jpg",
    expected_company_id: str | None = None,
) -> None:
    """Decode ``b64_string`` and upload the bytes to ``<bucket>/<key>``.

    Accepts either a raw base64 payload or a full ``data:<mime>;base64,<...>``
    data URL; the ``data:`` prefix, if present, is stripped before decoding.
    The Content-Type is inferred from ``default_ext`` using the mapping
    ``jpg/jpeg -> image/jpeg``, ``png -> image/png``, ``pdf ->
    application/pdf``; any other extension falls back to
    ``application/octet-stream``.

    When ``expected_company_id`` is provided, the tenant prefix of
    ``key`` is validated before any bytes are decoded or uploaded, per
    Requirement 21.14. A mismatch raises :class:`TenantPrefixViolation`.

    Raises ``ValueError`` with a clear message if the payload is not valid
    base64. ``botocore.exceptions.ClientError`` from the underlying upload
    propagates to the caller.

    Requirements: 21.10, 21.11 (stored document must not contain base64),
    21.14 (tenant prefix).
    """

    if expected_company_id:
        # Fail-fast on tenant-prefix mismatch before spending cycles on
        # base64 decoding — a forbidden key should never touch the
        # decoder path.
        _validate_tenant_prefix(bucket, key, expected_company_id)

    if not isinstance(b64_string, str):
        raise ValueError("upload_base64 requires a str payload")

    payload: str = _DATA_URL_PREFIX_RE.sub("", b64_string).strip()
    if not payload:
        raise ValueError("upload_base64 received an empty base64 payload")

    try:
        data: bytes = base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(
            f"upload_base64 received invalid base64 data: {exc}"
        ) from exc

    ext: str = (default_ext or "").lower().lstrip(".")
    content_type: str = _CONTENT_TYPE_BY_EXT.get(
        ext, "application/octet-stream"
    )

    # expected_company_id has already been validated above; pass ``None``
    # into the inner call so we do not duplicate the work.
    upload_bytes(bucket, key, data, content_type)


def presign_get(
    bucket: str,
    key: str,
    ttl_seconds: int | None = None,
) -> str:
    """Return a presigned GET URL for ``<bucket>/<key>``.

    The URL host is rewritten from the internal ``OBJECT_STORE_ENDPOINT`` to
    the public ``OBJECT_STORE_PUBLIC_ENDPOINT`` (Requirement 21.13), so the
    returned URL looks like
    ``https://api.fleetshield365.com/files/<bucket>/<key>?X-Amz-...``. The
    SigV4 query-string parameters are preserved verbatim; only the scheme,
    host, and path prefix are rewritten.

    ``ttl_seconds`` is capped at ``OBJECT_STORE_PRESIGN_TTL_SECONDS`` to
    satisfy Requirement 21.15. Values that are ``None``, non-positive, or
    above the cap are coerced to the cap.

    Requirements: 21.12, 21.13, 21.15 (cap).
    """

    cap: int = _OBJECT_STORE_PRESIGN_TTL_SECONDS
    if ttl_seconds is None or ttl_seconds <= 0 or ttl_seconds > cap:
        expires_in: int = cap
    else:
        expires_in = ttl_seconds

    internal_url: str = _s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )

    return _rewrite_to_public_host(internal_url)


def delete(bucket: str, key: str) -> None:
    """Delete the object at ``<bucket>/<key>``.

    Thin wrapper around ``s3.delete_object``. S3 and MinIO treat deletion of
    a missing key as a no-op (no error), so callers can call this
    idempotently. ``botocore.exceptions.ClientError`` propagates to the
    caller.
    """

    _s3_client.delete_object(Bucket=bucket, Key=key)


def get_bytes(bucket: str, key: str) -> bytes:
    """Return the full object body at ``<bucket>/<key>`` as ``bytes``.

    Thin wrapper around ``s3.get_object`` used by server-side code paths
    that need the raw bytes rather than a presigned URL — for example PDF
    regeneration and the driver-document ZIP export, which assemble the
    output in-process and cannot follow an HTTP redirect to MinIO.

    The response body is fully consumed and the underlying stream is
    closed before returning. ``botocore.exceptions.ClientError`` propagates
    to the caller so the caller can decide whether to fall back to a
    legacy base64 field or surface the error.
    """

    response = _s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        return body.read()
    finally:
        # ``StreamingBody`` exposes ``.close()``; call it defensively so the
        # underlying connection is released back to the pool even if a
        # caller forgets to handle a partial read.
        close = getattr(body, "close", None)
        if callable(close):
            close()


# ---------------------------------------------------------------------------
# Internal helpers.
# ---------------------------------------------------------------------------


def _rewrite_to_public_host(internal_url: str) -> str:
    """Rewrite an internal presigned URL to the public Nginx_Proxy host.

    Given a URL produced by ``boto3.generate_presigned_url`` pointing at
    ``_OBJECT_STORE_ENDPOINT`` (for example
    ``http://127.0.0.1:9000/<bucket>/<key>?X-Amz-Signature=...``), return
    the equivalent URL pointing at ``_OBJECT_STORE_PUBLIC_ENDPOINT`` (for
    example ``https://api.fleetshield365.com/files/<bucket>/<key>?X-Amz-...``).

    The full query string (signature, credential, date, expires, signed
    headers, etc.) is preserved verbatim so MinIO's SigV4 validation
    succeeds. Only ``scheme``, ``netloc``, and the leading path prefix are
    replaced.

    Requirement: 21.13.
    """

    internal_parts = urlsplit(internal_url)
    public_parts = urlsplit(_OBJECT_STORE_PUBLIC_ENDPOINT)

    # Join the public endpoint path prefix (e.g. "/files") with the object
    # path (e.g. "/bucket/key"). We normalize both to avoid producing
    # "//" between them or losing the leading slash.
    public_prefix: str = public_parts.path.rstrip("/")
    object_path: str = internal_parts.path
    if not object_path.startswith("/"):
        object_path = "/" + object_path
    combined_path: str = f"{public_prefix}{object_path}"

    return urlunsplit(
        (
            public_parts.scheme,
            public_parts.netloc,
            combined_path,
            internal_parts.query,
            internal_parts.fragment,
        )
    )
