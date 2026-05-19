"""One-shot migration — set every existing driver's PIN to ``0000``.

Owner request 2026-05-19: existing drivers should be migrated to the new
4-digit PIN sign-in. This script resets every user with ``role == "driver"``
(or admin accounts flagged as ``is_also_operator``) to PIN ``0000`` and
flips ``auth_mode`` to ``pin``.

Safety:
  * Dry-run by default. Pass ``--apply`` to actually write.
  * Only resets drivers/operators — admins/super_admins/platform_owner
    are skipped so dashboard logins keep working.
  * Skips any user whose ``auth_mode`` is already ``"pin"`` AND whose
    ``pin_reset_at`` timestamp exists — assume the admin set a custom PIN
    after the migration and we shouldn't clobber it.

Usage on the EC2 box:

    sudo -u fleetshield /opt/fleetshield365/backend/.venv/bin/python \\
        /opt/fleetshield365/backend/migrations/reset_driver_pins_to_0000.py \\
        --apply
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient

DEFAULT_PIN = "0000"


def _hash(pin: str) -> str:
    return bcrypt.hashpw(pin.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


async def main(apply: bool) -> None:
    mongo_url = os.getenv("MONGO_URL") or os.getenv("MONGODB_URL")
    db_name = os.getenv("DB_NAME") or "fleetguard_db"
    if not mongo_url:
        print("ERROR: MONGO_URL env var is required", file=sys.stderr)
        sys.exit(2)

    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    hashed = _hash(DEFAULT_PIN)
    now = datetime.now(timezone.utc)

    # Match driver accounts + admin-as-operator accounts. Skip those
    # already marked as PIN-reset (admin chose a custom PIN already).
    filt = {
        "$and": [
            {
                "$or": [
                    {"role": "driver"},
                    {"is_also_operator": True},
                ]
            },
            {
                "$or": [
                    {"auth_mode": {"$ne": "pin"}},
                    {"pin_reset_at": {"$exists": False}},
                ]
            },
        ]
    }

    total = await db.users.count_documents(filt)
    print(f"Found {total} driver/operator account(s) eligible for PIN reset.")
    if total == 0:
        return

    if not apply:
        print("DRY RUN — no changes written. Pass --apply to commit.")
        # Show a small sample so the operator can sanity-check.
        sample = await db.users.find(filt, {"name": 1, "username": 1, "email": 1, "role": 1}).limit(10).to_list(10)
        for s in sample:
            print(
                f"  · {s.get('name','?'):30}  @{s.get('username','?'):16}  "
                f"{s.get('email','-'):30}  role={s.get('role','?')}"
            )
        return

    result = await db.users.update_many(
        filt,
        {
            "$set": {
                "password_hash": hashed,
                "auth_mode": "pin",
                "pin_reset_at": now,
                "pin_reset_reason": "bulk_migration_2026_05_19",
            }
        },
    )
    print(f"Updated {result.modified_count} document(s). Default PIN: {DEFAULT_PIN}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write changes (default is dry run).",
    )
    args = parser.parse_args()
    asyncio.run(main(apply=args.apply))
