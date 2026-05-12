# FleetShield365 — Backend

FastAPI service backing the FleetShield365 fleet-management platform
(web admin + mobile inspections). Deployed on a single AWS EC2 box
(`ap-southeast-2`) fronted by Cloudflare. Data lives in self-hosted
MongoDB 7 and MinIO on the same box.

## Repo layout

```
fleetshield365-backend/
├── server.py                     ← single-module FastAPI app (~10k lines)
├── object_store.py               ← MinIO S3 wrapper + tenant-prefix validator
├── bootstrap_platform_owner.py   ← idempotent platform-owner seeder
├── requirements.txt              ← pinned Python deps
├── deploy.sh                     ← EC2 deploy: git pull + venv + .env + systemctl
├── fleetshield365-api.service    ← systemd unit
├── .env.example                  ← every env var the app reads, documented
├── migrations/
│   └── migrate_inspection_pdf_to_minio.py
└── tests/
    ├── test_upload_validation.py
    ├── test_security_phase3.py
    └── test_softdelete_phase4.py
```

## Run locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then fill in real values
uvicorn server:app --reload --port 8000
```

Required env vars (fail-fast at import time):
- `JWT_SECRET` — at least 32 random bytes
- `MONGO_URL` — full connection string including auth
- `OBJECT_STORE_ACCESS_KEY` / `OBJECT_STORE_SECRET_KEY` — MinIO creds

Optional (sane defaults in `.env.example`):
- SMTP (`SMTP_USER` / `SMTP_PASSWORD` and the noreply pair)
- Upload limits (`UPLOAD_MAX_BYTES_*`)
- Lockout config (`ACCOUNT_LOCKOUT_*`)
- Stripe (`STRIPE_SECRET_KEY` / `STRIPE_WEBHOOK_SECRET`)
- Logging (`LOG_JSON=true` for production JSON output, `false` for human-readable dev)
- Mobile version gate (`MOBILE_MIN_IOS_BUILD`, `MOBILE_MIN_ANDROID_VERSION_CODE`)

## Tests

```bash
pytest tests/test_upload_validation.py tests/test_security_phase3.py tests/test_softdelete_phase4.py -v
```

These are pure unit tests on helpers — no Mongo or MinIO required.
The other test files under `tests/` are HTTP integration suites
that hit a live preview backend (see each file's `BASE_URL` env
var) and may fail without that environment set up.

## Deploy

`deploy.sh` is run on the EC2 box (root or via sudo). It:

1. `git pull` the latest `main` into `/opt/fleetshield365/repo`
2. Refreshes the venv
3. Materializes `/opt/fleetshield365/backend/.env` from AWS Systems
   Manager Parameter Store (`/fleetshield365/prod/*`)
4. Installs `ghostscript` for async PDF compression (Phase 2 of
   `STORAGE-PLAN.txt`) — idempotent
5. Runs `ensure_indexes()` once
6. `systemctl restart fleetshield365-api.service`

Rollback: `git reset --hard HEAD~1` in the same dir, then restart.

## Architecture highlights

- **Multi-tenant by subdomain.** Every JWT carries a `subdomain`
  claim; `get_current_user` rejects stale tokens after a rename.
  Cross-tenant reads return 404, not 403, to avoid info leak.
- **MinIO for every photo + PDF.** Mongo never stores bytes — only
  tenant-scoped object keys. Read paths emit presigned URLs through
  Nginx so the browser fetches directly. See `object_store.py` and
  the `_upload_with_thumbnail` helper.
- **Soft delete.** vehicles / users / companies / service_records /
  maintenance_logs / incidents carry a `deleted_at` field. Read
  queries default to excluding them; admins restore from the Trash
  view or purge older-than-30-days via a manual button.
- **Phase 3 security baseline.** `sanitize_user_doc` strips the
  password hash from every response. JWTs carry `jti`, revoked on
  logout/refresh via a TTL-indexed `revoked_tokens` collection.
  Rate limits on `/auth/login` (5/min), `/forgot-password` (3/min),
  `/resend-verification` (2/min), `/contact` (3/min),
  `/tenant/resolve` (10/min). Account lockout after 5 failed logins
  in 15 minutes.

## Companion docs

- `../CLAUDE.md` — full project handbook (architecture, infra,
  credentials, runbook)
- `../TODO.md` — phase-ordered active issue list
- `../STORAGE-PLAN.txt` — storage + cost math (per-photo, per-fleet)
- `../STORAGE-IMPLEMENTATION-NOTES.txt` — what shipped in each phase

## Contributing

Edit `server.py` locally → run pytest → commit + push → SSH to EC2
and run `deploy.sh` (or wait for the next scheduled pull if you set
that up). Branch protection on `main` is recommended but not yet
configured.
