"""One-shot migration: move inspections.pdf_base64 → MinIO.

Run on the EC2 box from the backend dir:
    cd /opt/fleetshield365/backend
    sudo -u fleetshield bash -lc "source ../venv/bin/activate && \
        python migrations/migrate_inspection_pdf_to_minio.py"

Behaviour:
  * Walks every inspection document carrying a non-empty pdf_base64.
  * Decodes the bytes and uploads to
    inspection-photos/<company_id>/<inspection_id>/report.pdf.
  * On successful upload, sets pdf_object_key and unsets pdf_base64
    in one Mongo update.
  * Skips documents that already have pdf_object_key set.
  * Resumable — if interrupted, re-run picks up where it left off.
  * Idempotent — running twice is a no-op after the first pass.

Output: per-batch summary lines + a final tally. Failures are logged
to stdout and the script continues; rerun to retry just the failures.
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys
from datetime import datetime

# Reach into the parent directory so `import server` works.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from motor.motor_asyncio import AsyncIOMotorClient  # noqa: E402

import object_store  # noqa: E402


BATCH_SIZE = 50


async def main() -> int:
    mongo_url = os.environ["MONGO_URL"]
    db_name = os.environ.get("DB_NAME", "fleetguard_db")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    query = {
        "pdf_base64": {"$exists": True, "$ne": None, "$ne": ""},
        "pdf_object_key": {"$in": [None, "", False]},
    }

    total = await db.inspections.count_documents(query)
    print(f"[{datetime.utcnow().isoformat()}] Found {total} inspection(s) to migrate")
    if total == 0:
        return 0

    migrated = 0
    skipped = 0
    failed = 0

    cursor = db.inspections.find(
        query, {"_id": 1, "company_id": 1, "pdf_base64": 1},
    )
    async for doc in cursor:
        inspection_id = str(doc["_id"])
        company_id = doc.get("company_id")
        b64 = doc.get("pdf_base64") or ""

        if not company_id:
            print(f"  SKIP {inspection_id}: missing company_id")
            skipped += 1
            continue

        try:
            payload = b64.split(",", 1)[1] if b64.startswith("data:") else b64
            pdf_bytes = base64.b64decode(payload, validate=True)
        except Exception as exc:
            print(f"  FAIL {inspection_id}: invalid base64 ({exc})")
            failed += 1
            continue

        key = f"{company_id}/{inspection_id}/report.pdf"
        try:
            object_store.upload_bytes(
                "inspection-photos",
                key,
                pdf_bytes,
                "application/pdf",
                expected_company_id=company_id,
            )
        except Exception as exc:
            print(f"  FAIL {inspection_id}: MinIO upload failed ({exc})")
            failed += 1
            continue

        try:
            await db.inspections.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {"pdf_object_key": key},
                    "$unset": {"pdf_base64": ""},
                },
            )
        except Exception as exc:
            print(f"  FAIL {inspection_id}: Mongo update failed ({exc})")
            failed += 1
            continue

        migrated += 1
        if migrated % BATCH_SIZE == 0:
            print(f"  ...{migrated}/{total} migrated, {failed} failed, {skipped} skipped")

    print(
        f"[{datetime.utcnow().isoformat()}] Migration complete: "
        f"{migrated} migrated, {skipped} skipped, {failed} failed (of {total})"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
