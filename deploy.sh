#!/usr/bin/env bash
#
# FleetShield365 backend deploy script (Req 3.2).
#
# Runs on the EC2_Instance (Amazon Linux 2023, under the service account
# that owns /opt/fleetshield365/). Pulls the latest main branch,
# refreshes the venv, materializes /opt/fleetshield365/backend/.env from
# AWS Systems Manager Parameter Store, triggers the idempotent
# ensure_indexes() bootstrap, then hot-restarts the systemd unit.
#
# This script is idempotent — safe to re-run at any time.
#
# IMPORTANT: never commit real secret values. Every secret read below
# comes from /fleetshield365/prod/<name> in SSM Parameter Store
# (Requirement 3.3).

set -euo pipefail
IFS=$'\n\t'

# --- Paths ------------------------------------------------------------
APP_ROOT="/opt/fleetshield365"
REPO_DIR="${APP_ROOT}/repo"
BACKEND_DIR="${REPO_DIR}/fleetshield365-backend"
VENV_DIR="${APP_ROOT}/venv"
ENV_FILE="${APP_ROOT}/backend/.env"

# SSM path prefix for every secret that backs the runtime .env.
SSM_PREFIX="/fleetshield365/prod"

# Ordered list of env vars to materialize (see Requirement 3.1). Each
# one is fetched individually from SSM as a SecureString.
ENV_VAR_NAMES=(
  MONGO_URL
  DB_NAME
  JWT_SECRET
  STRIPE_SECRET_KEY
  STRIPE_WEBHOOK_SECRET
  SENDGRID_API_KEY
  SENDER_EMAIL
  CONTACT_RECIPIENT_EMAIL
  PLATFORM_OWNER_USER_IDS
  DEFAULT_ORIGIN_URL
  CORS_ALLOWED_ORIGINS
  OBJECT_STORE_ENDPOINT
  OBJECT_STORE_PUBLIC_ENDPOINT
  OBJECT_STORE_ACCESS_KEY
  OBJECT_STORE_SECRET_KEY
  OBJECT_STORE_REGION
  OBJECT_STORE_PRESIGN_TTL_SECONDS
)

log() {
  echo "[deploy $(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"
}

# --- Step 1: git pull -------------------------------------------------
log "Pulling latest main in ${REPO_DIR}"
git -C "${REPO_DIR}" fetch --quiet origin main
git -C "${REPO_DIR}" checkout --quiet main
git -C "${REPO_DIR}" reset --hard origin/main

# --- Step 2: venv + deps ---------------------------------------------
if [[ ! -d "${VENV_DIR}" ]]; then
  log "Creating venv at ${VENV_DIR}"
  python3 -m venv "${VENV_DIR}"
fi

log "Installing Python dependencies"
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
"${VENV_DIR}/bin/pip" install --quiet --upgrade -r "${BACKEND_DIR}/requirements.txt"

# --- Step 3: materialize .env from SSM -------------------------------
# Using --with-decryption so SecureString values come back in plaintext.
# tmp file is created with mode 0600 *before* any secret is written, and
# the final mv is atomic so the running process never reads a truncated
# .env mid-update.
log "Materializing ${ENV_FILE} from SSM ${SSM_PREFIX}/*"
mkdir -p "$(dirname "${ENV_FILE}")"
TMP_ENV="$(mktemp "${ENV_FILE}.XXXXXX")"
chmod 600 "${TMP_ENV}"

for name in "${ENV_VAR_NAMES[@]}"; do
  value="$(
    aws ssm get-parameter \
      --name "${SSM_PREFIX}/${name}" \
      --with-decryption \
      --query 'Parameter.Value' \
      --output text
  )"
  # Note: SSM values MAY contain "=" (connection strings). Only the
  # first "=" between name and value matters for the .env format, so
  # do not further escape.
  printf '%s=%s\n' "${name}" "${value}" >> "${TMP_ENV}"
done

chmod 600 "${TMP_ENV}"
chown root:root "${TMP_ENV}" 2>/dev/null || true
mv -f "${TMP_ENV}" "${ENV_FILE}"

# --- Step 4: ensure_indexes bootstrap --------------------------------
# Run the idempotent index bootstrap as a one-shot BEFORE restarting
# the long-running service so a failing index op surfaces as a failed
# deploy instead of a cryptic 500 later (Req 12.3, 24.5).
log "Running ensure_indexes() one-shot"
(
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
  cd "${BACKEND_DIR}"
  "${VENV_DIR}/bin/python" -c "import asyncio, server; asyncio.run(server.ensure_indexes())"
)

# --- Step 5: restart systemd unit ------------------------------------
log "Restarting fleetshield365-api.service"
systemctl restart fleetshield365-api.service
systemctl is-active --quiet fleetshield365-api.service
log "Deploy complete"
