"""Unit tests for the per-upload validation pipeline added in Phase 1+2.

Covers:
  * _detect_format: magic-byte sniffing for JPEG/PNG/WebP/PDF/unknown
  * _validate_upload_or_400: size cap (HTTP 413) + format allowlist (HTTP 415)
  * _generate_thumbnail: produces JPEG bytes from a 4x4 PNG
  * _thumbnail_key_for: derives <stem>_thumb.jpg

These tests don't need MongoDB, MinIO, or SMTP — only the helpers
themselves and Pillow. They run with: pytest tests/test_upload_validation.py
"""
from __future__ import annotations

import os
import struct
import sys
import zlib
from io import BytesIO

import pytest


# Ensure required env vars are set so server.py module load succeeds.
os.environ.setdefault("JWT_SECRET", "test-secret-not-real")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017/test")
os.environ.setdefault("OBJECT_STORE_ACCESS_KEY", "test-access")
os.environ.setdefault("OBJECT_STORE_SECRET_KEY", "test-secret")

# Backend lives one directory up.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import HTTPException  # noqa: E402

import server  # noqa: E402


# ---------------------------------------------------------------------------
# _detect_format
# ---------------------------------------------------------------------------


def test_detect_format_jpeg():
    assert server._detect_format(b"\xFF\xD8\xFF\xE0\x00\x10JFIF" + b"\x00" * 10) == "jpeg"


def test_detect_format_png():
    assert server._detect_format(b"\x89PNG\r\n\x1a\n" + b"\x00" * 10) == "png"


def test_detect_format_pdf():
    assert server._detect_format(b"%PDF-1.4\n" + b"\x00" * 10) == "pdf"


def test_detect_format_webp():
    # RIFF<size>WEBP  — size bytes are ignored by the detector.
    data = b"RIFF" + b"\x00\x00\x00\x00" + b"WEBP" + b"\x00" * 8
    assert server._detect_format(data) == "webp"


def test_detect_format_short_input():
    assert server._detect_format(b"\xFF") is None
    assert server._detect_format(b"") is None


def test_detect_format_unknown():
    # GIF89a header — not in our allowlist.
    assert server._detect_format(b"GIF89a" + b"\x00" * 10) is None


# ---------------------------------------------------------------------------
# _validate_upload_or_400
# ---------------------------------------------------------------------------


def _jpeg_bytes(n: int) -> bytes:
    """Return n bytes starting with a valid JPEG SOI marker."""
    body = b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00"
    return body + b"\x00" * max(0, n - len(body))


def _png_bytes(n: int) -> bytes:
    head = b"\x89PNG\r\n\x1a\n"
    return head + b"\x00" * max(0, n - len(head))


def _pdf_bytes(n: int) -> bytes:
    head = b"%PDF-1.4\n"
    return head + b"\x00" * max(0, n - len(head))


def test_validate_image_ok():
    fmt = server._validate_upload_or_400(_jpeg_bytes(1024), "inspection", "field")
    assert fmt == "jpeg"


def test_validate_oversize_rejected_413():
    huge = _jpeg_bytes(server.UPLOAD_MAX_BYTES["inspection"] + 1)
    with pytest.raises(HTTPException) as exc:
        server._validate_upload_or_400(huge, "inspection", "photo")
    assert exc.value.status_code == 413
    assert "max allowed" in exc.value.detail.lower()


def test_validate_pdf_when_image_expected_415():
    # /fuel type allows only image; sending a PDF should 415.
    with pytest.raises(HTTPException) as exc:
        server._validate_upload_or_400(_pdf_bytes(2048), "fuel", "receipt")
    assert exc.value.status_code == 415


def test_validate_image_when_pdf_expected_415():
    # incident_pdf allows only pdf — JPEG should 415.
    with pytest.raises(HTTPException) as exc:
        server._validate_upload_or_400(_jpeg_bytes(2048), "incident_pdf", "attachment")
    assert exc.value.status_code == 415


def test_validate_signature_must_be_png():
    # signatures bucket is png-only. JPEG should 415.
    with pytest.raises(HTTPException) as exc:
        server._validate_upload_or_400(_jpeg_bytes(1024), "signature", "sig")
    assert exc.value.status_code == 415

    # PNG goes through.
    fmt = server._validate_upload_or_400(_png_bytes(1024), "signature", "sig")
    assert fmt == "png"


def test_validate_image_or_pdf_accepts_both():
    # service group accepts JPEG, PNG, WebP, PDF.
    assert server._validate_upload_or_400(_jpeg_bytes(512), "service", "f") == "jpeg"
    assert server._validate_upload_or_400(_pdf_bytes(512), "service", "f") == "pdf"


def test_validate_unknown_format_rejected():
    with pytest.raises(HTTPException) as exc:
        server._validate_upload_or_400(b"GIF89a" + b"\x00" * 16, "inspection", "p")
    assert exc.value.status_code == 415


# ---------------------------------------------------------------------------
# _generate_thumbnail
# ---------------------------------------------------------------------------


def _build_small_png() -> bytes:
    """Build a 4x4 RGB PNG in pure Python so the test has no fixture file."""
    width = height = 4
    # PNG signature
    data = b"\x89PNG\r\n\x1a\n"
    # IHDR chunk
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    data += struct.pack(">I", len(ihdr)) + b"IHDR" + ihdr + \
        struct.pack(">I", zlib.crc32(b"IHDR" + ihdr))
    # IDAT chunk: 4 rows of 4 RGB pixels + per-row filter byte
    raw = b""
    for _ in range(height):
        raw += b"\x00" + (b"\xFF\x00\x00" * width)  # red pixels
    compressed = zlib.compress(raw)
    data += struct.pack(">I", len(compressed)) + b"IDAT" + compressed + \
        struct.pack(">I", zlib.crc32(b"IDAT" + compressed))
    # IEND chunk
    data += struct.pack(">I", 0) + b"IEND" + struct.pack(">I", zlib.crc32(b"IEND"))
    return data


def test_generate_thumbnail_returns_jpeg_bytes():
    png = _build_small_png()
    thumb = server._generate_thumbnail(png, max_side=200)
    assert thumb is not None
    assert thumb[:3] == b"\xFF\xD8\xFF"  # JPEG SOI


def test_generate_thumbnail_handles_garbage_gracefully():
    # Pillow should fail on random bytes; helper must return None, not raise.
    assert server._generate_thumbnail(b"\x00" * 100) is None


# ---------------------------------------------------------------------------
# _thumbnail_key_for
# ---------------------------------------------------------------------------


def test_thumbnail_key_for_with_extension():
    assert server._thumbnail_key_for("a/b/c.jpg") == "a/b/c_thumb.jpg"
    assert server._thumbnail_key_for("driver-docs/co/dr/license-front.jpg") \
        == "driver-docs/co/dr/license-front_thumb.jpg"


def test_thumbnail_key_for_without_extension():
    assert server._thumbnail_key_for("a/b/c") == "a/b/c_thumb.jpg"


# ---------------------------------------------------------------------------
# Format group sanity
# ---------------------------------------------------------------------------


def test_every_upload_type_has_a_format_group():
    for type_key in server.UPLOAD_MAX_BYTES:
        if type_key == "default":
            continue
        assert type_key in server.UPLOAD_FORMAT_GROUP, \
            f"Missing UPLOAD_FORMAT_GROUP[{type_key!r}]"


def test_size_caps_match_storage_plan():
    # Sanity: defaults from STORAGE-PLAN.txt come through when env unset.
    assert server.UPLOAD_MAX_BYTES["logo"] == 2 * 1024 * 1024
    assert server.UPLOAD_MAX_BYTES["inspection"] == 3 * 1024 * 1024
    assert server.UPLOAD_MAX_BYTES["incident_pdf"] == 5 * 1024 * 1024
    assert server.UPLOAD_MAX_BYTES["signature"] == 512 * 1024
