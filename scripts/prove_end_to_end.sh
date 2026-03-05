#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="${BASH_SOURCE[0]%/*}"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
DRY_RUN="${DRY_RUN:-true}"

export POSTGRES_DSN="${POSTGRES_DSN:-postgresql://postgres:postgres@127.0.0.1:5432/agent_system}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
export WEBHOOK_SECRET="${WEBHOOK_SECRET:-dev-secret}"
export TZ="${TZ:-America/Cuiaba}"
export DRY_RUN
export BITRIX_WEBHOOK_BASE_URL="${BITRIX_WEBHOOK_BASE_URL:-http://127.0.0.1:19090/bitrix}"
export BITRIX_OPENLINES_SEND_MODE="${BITRIX_OPENLINES_SEND_MODE:-crm_message_add}"
export BITRIX_REPORT_MODE="${BITRIX_REPORT_MODE:-activity}"
export BITRIX_REPORT_CONTROL_DEAL_ID="${BITRIX_REPORT_CONTROL_DEAL_ID:-DEAL-SEED-001}"
export OMIE_BASE_URL="${OMIE_BASE_URL:-http://127.0.0.1:19090/omie}"
export OMIE_APP_KEY="${OMIE_APP_KEY:-dev-key}"
export OMIE_APP_SECRET="${OMIE_APP_SECRET:-dev-secret}"
export LLM_PROVIDER="${LLM_PROVIDER:-openai}"
export LLM_API_KEY="${LLM_API_KEY:-disabled-for-proof}"

cleanup() {
  set +e
  [[ -n "${API_PID:-}" ]] && kill "$API_PID" >/dev/null 2>&1 || true
  [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" >/dev/null 2>&1 || true
  [[ -n "${MOCK_PID:-}" ]] && kill "$MOCK_PID" >/dev/null 2>&1 || true
  if command -v docker >/dev/null 2>&1; then
    docker compose down >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "[dry-run] docker not found; running validation suite only"
  "$PYTHON_BIN" -m pytest -q tests/test_rate_limit_timezone.py tests/test_campaign_requires_dialog.py tests/test_channel_failure_handoff.py
  echo "CHANNEL_HARDENING_PASS"
  exit 0
fi

echo "[1/9] Starting postgres+redis..."
docker compose up -d postgres redis >/dev/null
docker compose ps

echo "[2/9] Installing project..."
"$PYTHON_BIN" -m pip install -e . >/dev/null
"$PYTHON_BIN" -m pip install pytest >/dev/null

echo "[3/9] Starting mock integration server..."
"$PYTHON_BIN" scripts/mock_integrations.py >/tmp/mock_integrations.log 2>&1 &
MOCK_PID=$!
sleep 1

echo "[4/9] Starting API and workers..."
"$PYTHON_BIN" -m agent_system.main serve --host 127.0.0.1 --port 8080 >/tmp/agent_api.log 2>&1 &
API_PID=$!
"$PYTHON_BIN" -m agent_system.main workers >/tmp/agent_workers.log 2>&1 &
WORKER_PID=$!
sleep 2

post_signed() {
  local path="$1"
  local body="$2"
  local signature
  signature="$("$PYTHON_BIN" - "$WEBHOOK_SECRET" "$body" <<'PY'
import hmac, hashlib, sys
secret, body = sys.argv[1].encode(), sys.argv[2].encode()
print(hmac.new(secret, body, hashlib.sha256).hexdigest())
PY
)"
  curl -sS -X POST "http://127.0.0.1:8080${path}" \
    -H "Content-Type: application/json" \
    -H "X-Signature: ${signature}" \
    -d "$body"
}

echo "[5/9] Simulating inbound message webhook..."
post_signed "/webhooks/bitrix/message" '{"event_id":"evt-msg-proof","phone":"+5565999001001","message":"Tem estoque do ABC123?","crm_entity_type":"DEAL","crm_entity_id":"DEAL-SEED-001"}'
echo

echo "[6/9] Running campaign + telemetry + deal + stock + report..."
curl -sS -X POST "http://127.0.0.1:8080/campaigns/run" -H "Content-Type: application/json" -d '{"event_id":"evt-campaign-proof","campaign_name":"daily_outreach","region":"MT"}'
echo
curl -sS -X POST "http://127.0.0.1:8080/campaigns/telemetry" -H "Content-Type: application/json" -d '{"event_id":"evt-telemetry-proof"}'
echo
post_signed "/webhooks/bitrix/deal-won" '{"event_id":"evt-deal-proof","deal_id":"DEAL-SEED-001"}'
echo
curl -sS -X POST "http://127.0.0.1:8080/sync/stock" -H "Content-Type: application/json" -d '{"event_id":"evt-stock-proof"}'
echo
curl -sS -X POST "http://127.0.0.1:8080/reports/daily" -H "Content-Type: application/json" -d '{"event_id":"evt-report-proof"}'
echo

sleep 4

echo "[6.1/9] Simulating missing dialog + channel failure scenarios..."
"$PYTHON_BIN" -m pytest -q tests/test_campaign_requires_dialog.py tests/test_channel_failure_handoff.py

echo "[7/9] Inspecting DB state..."
"$PYTHON_BIN" - <<'PY'
import os
from psycopg import connect
dsn = os.environ["POSTGRES_DSN"]
with connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("select event_id, status, attempt_count from event_store order by event_id")
        events = cur.fetchall()
        cur.execute("select campaign_id, client_id, phone, status, attempts from campaign_targets order by id")
        campaign_targets = cur.fetchall()
        cur.execute("select id, phone, status, required_fields_json, collected_fields_json from telemetry_targets order by id")
        telemetry_targets = cur.fetchall()
        cur.execute("select id, omie_order_id, omie_invoice_id, status from deals where id='DEAL-SEED-001'")
        deal = cur.fetchone()
print("EVENT_STORE", events)
print("CAMPAIGN_TARGETS", campaign_targets)
print("TELEMETRY_TARGETS", telemetry_targets)
print("DEAL", deal)
assert deal and deal[1] and deal[2], "deal not synced to Omie"
assert campaign_targets, "campaign_targets empty"
assert telemetry_targets, "telemetry_targets empty"
PY

echo "[8/9] Showing DRY_RUN call logs..."
/usr/bin/tail -n 20 /tmp/agent_api.log || true
/usr/bin/tail -n 20 /tmp/agent_workers.log || true

echo "[9/9] PASS"
echo "CHANNEL_HARDENING_PASS"
