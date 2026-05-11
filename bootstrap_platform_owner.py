"""Idempotent one-shot bootstrap script that seeds the first platform owner.

Run this once against a freshly provisioned MongoDB_Instance (Req 24.3,
24.4) to create the single user document carrying
``role = "platform_owner"`` and ``is_platform_owner = true``. On every
subsequent run the script is a no-op — if a platform-owner user already
exists, it exits with status 0 and logs the reason.

Initial password sourcing, in order of precedence:

1. ``PLATFORM_OWNER_INITIAL_PASSWORD`` environment variable — used by
   the deploy automation so the operator does not type the password
   into a shell.
2. ``--password`` CLI argument — convenient for ad-hoc runs, but leaves
   the password in shell history, so avoid in production.
3. An interactive prompt via ``getpass`` on stdin — the safest manual
   path, with password echo disabled.

The email address is taken from the ``PLATFORM_OWNER_EMAIL`` env var or
the ``--email`` argument; there is no interactive prompt for email to
keep the happy-path automatable.

Usage::

    python bootstrap_platform_owner.py --email owner@fleetshield365.com
    PLATFORM_OWNER_INITIAL_PASSWORD=... python bootstrap_platform_owner.py
    python bootstrap_platform_owner.py --email owner@... --password '...'

The script relies on the same ``MONGO_URL``, ``DB_NAME``, ``JWT_SECRET``,
``OBJECT_STORE_ACCESS_KEY``, and ``OBJECT_STORE_SECRET_KEY`` env vars as
``server.py`` (Req 3.4-3.6) — it imports ``server`` indirectly via the
``db`` handle so startup validation runs before we touch anything.

Exit codes:

* 0 — a platform-owner user now exists (newly created OR already
       present; both cases are success for the idempotent contract).
* 2 — bad CLI usage (missing email, empty password, etc.).
* 3 — unexpected runtime error (DB unreachable, etc.).
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import logging
import os
import sys
from datetime import datetime

from bson import ObjectId


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("bootstrap_platform_owner")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Idempotently seed the first platform_owner user on a fresh "
            "FleetShield365 MongoDB."
        )
    )
    parser.add_argument(
        "--email",
        default=os.environ.get("PLATFORM_OWNER_EMAIL"),
        help=(
            "Email address for the platform owner user. Defaults to the "
            "PLATFORM_OWNER_EMAIL env var."
        ),
    )
    parser.add_argument(
        "--password",
        default=None,
        help=(
            "Initial password (NOT recommended via CLI; prefer "
            "PLATFORM_OWNER_INITIAL_PASSWORD env var or interactive "
            "prompt)."
        ),
    )
    parser.add_argument(
        "--name",
        default=os.environ.get(
            "PLATFORM_OWNER_NAME", "FleetShield365 Platform Owner"
        ),
        help="Display name for the seeded user.",
    )
    return parser.parse_args(argv)


def _resolve_password(cli_password: str | None) -> str:
    """Return the initial password, preferring env > CLI > interactive."""

    env_password = os.environ.get("PLATFORM_OWNER_INITIAL_PASSWORD")
    if env_password:
        return env_password
    if cli_password:
        return cli_password
    # Interactive path — no echo.
    password = getpass.getpass("Platform owner initial password: ")
    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        raise SystemExit("Passwords do not match; aborting.")
    return password


async def _bootstrap(email: str, password: str, name: str) -> int:
    # Import lazily so ``server``'s startup env-var validation only runs
    # when the script actually executes (Req 3.4-3.6). Importing at
    # module top-level would also validate env vars when only parsing
    # ``--help`` which is annoying.
    import server  # noqa: E402

    db = server.db
    users = db.users

    # Idempotency: a platform_owner user can be identified by either the
    # role or the dedicated flag. Presence of either counts as "already
    # bootstrapped" so we don't create a second seed user when the first
    # one's role has been manually flipped away from platform_owner.
    existing = await users.find_one(
        {
            "$or": [
                {"role": server.UserRole.PLATFORM_OWNER},
                {"is_platform_owner": True},
            ]
        }
    )
    if existing:
        logger.info(
            "Platform owner already exists (id=%s, email=%s). Nothing to do.",
            existing.get("_id"),
            existing.get("email"),
        )
        return 0

    now = datetime.utcnow()
    doc = {
        "_id": ObjectId(),
        "email": email.strip().lower(),
        "username": email.strip().lower(),
        "password_hash": server.get_password_hash(password),
        "name": name,
        "role": server.UserRole.PLATFORM_OWNER,
        "is_platform_owner": True,
        "company_id": None,
        "assigned_vehicles": [],
        "created_at": now,
        "updated_at": now,
    }
    await users.insert_one(doc)
    logger.info(
        "Created platform owner user id=%s email=%s. "
        "Remember to rotate the password after first login.",
        doc["_id"],
        doc["email"],
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    if not args.email:
        print(
            "error: --email or PLATFORM_OWNER_EMAIL is required",
            file=sys.stderr,
        )
        return 2
    try:
        password = _resolve_password(args.password)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if not password or not password.strip():
        print("error: password must not be empty", file=sys.stderr)
        return 2

    try:
        return asyncio.run(_bootstrap(args.email, password, args.name))
    except Exception as exc:  # pragma: no cover - operational error path
        logger.exception("bootstrap failed: %s", exc)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
