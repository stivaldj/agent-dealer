# Bitrix + Omie + LLM Agent System

Production-oriented backend for Bitrix24 OpenLines (WhatsApp), Omie ERP, Redis queues, and PostgreSQL event store.

## Setup

1. Start infrastructure:
   ```bash
   docker compose up -d postgres redis
   ```
2. Install:
   ```bash
   python -m pip install -e .
   ```
3. Configure env:
   ```bash
   cp .env.example .env
   # then export vars from .env in your shell
   ```
4. Run API + workers:
   ```bash
   python -m agent_system.main serve --host 0.0.0.0 --port 8080
   python -m agent_system.main workers
   ```

## Environment Variables

See `.env.example` for the full list. Main groups:

- Bitrix:
  - `BITRIX_WEBHOOK_BASE_URL` (preferred webhook auth)
  - `BITRIX_REST_BASE_URL` + `BITRIX_OAUTH_TOKEN` (OAuth mode)
  - `BITRIX_OPENLINES_SEND_MODE=crm_message_add|bot_message_add`
  - `BITRIX_BOT_ID` (required for bot mode)
- Omie:
  - `OMIE_BASE_URL`
  - `OMIE_APP_KEY`
  - `OMIE_APP_SECRET`
- LLM:
  - `LLM_PROVIDER=openai`
  - `LLM_API_KEY`
  - `LLM_BASE_URL`
  - `MODEL_CONVERSATION`, `MODEL_CAMPAIGN`, `MODEL_SUMMARY`
- Platform:
- `POSTGRES_DSN`
- `REDIS_URL`
- `TZ=America/Cuiaba`
- `DRY_RUN=true|false` (when `true`, external Bitrix/Omie calls are logged instead of executed)

## Endpoints

- `POST /webhooks/bitrix/message`
- `POST /webhooks/bitrix/deal-won`
- `GET /stock?sku=...`
- `POST /sync/stock`
- `POST /campaigns/run`
- `POST /campaigns/telemetry`
- `POST /reports/daily`

## OpenLines + Report Modes

- Inbound message webhook can include `crm_entity_type`, `crm_entity_id`, `dialog_id`.
- Replies are posted back through:
  - `imopenlines.crm.message.add` when `BITRIX_OPENLINES_SEND_MODE=crm_message_add`
  - `imbot.message.add` when `BITRIX_OPENLINES_SEND_MODE=bot_message_add`
- Daily report target controlled by:
  - `BITRIX_REPORT_MODE=activity|openline|task`

## Proof Script

Run full local proof:

```bash
./scripts/prove_end_to_end.sh
```

What it does:

- starts `postgres` + `redis`
- starts local mock Bitrix/Omie server
- starts API + workers
- sends signed webhooks and stock/report jobs
- validates DB rows (`event_store`, `deals`, `stock_snapshot`, `daily_reports`)
- prints `PASS` on success

## Seed / Import

Seed SQL:

```bash
psql "$POSTGRES_DSN" -f scripts/seed_demo_data.sql
```

CSV import:

```bash
python scripts/import_clients_machines_csv.py path/to/clients_machines.csv
```

CSV columns expected:
- `client_id,client_name,phone,city,state,machine_id,brand,model,serial,year,telemetry_active`

## Tests

```bash
python -m unittest -v tests/test_agent_system.py
```

Includes:

- Bitrix request building
- Omie request building
- Deal idempotency (duplicate prevention)
- Retry to DLQ transition
- Outbound caps + opt-out checks
- Scheduler timezone/weekday logic
