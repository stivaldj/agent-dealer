from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("POSTGRES_DSN", os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/agent_system"))
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "change-me")
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"
    tz: str = os.getenv("TZ", "America/Cuiaba")
    max_request_body_bytes: int = int(os.getenv("MAX_REQUEST_BODY_BYTES", str(1024 * 128)))
    campaign_contact_cooldown_hours: int = int(os.getenv("CAMPAIGN_CONTACT_COOLDOWN_HOURS", "24"))
    max_retry_attempts: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "5"))
    stock_sync_interval_minutes: int = int(os.getenv("STOCK_SYNC_INTERVAL_MINUTES", "15"))
    weekday_outreach_hour: int = int(os.getenv("WEEKDAY_OUTREACH_HOUR", "9"))
    weekday_followup_hour: int = int(os.getenv("WEEKDAY_FOLLOWUP_HOUR", "14"))
    telemetry_campaign_hour: int = int(os.getenv("TELEMETRY_CAMPAIGN_HOUR", "10"))
    daily_report_hour: int = int(os.getenv("DAILY_REPORT_HOUR", "18"))
    scheduler_skip_weekends: bool = os.getenv("SCHEDULER_SKIP_WEEKENDS", "1") == "1"
    scheduler_skip_holidays: bool = os.getenv("SCHEDULER_SKIP_HOLIDAYS", "0") == "1"
    scheduler_holidays_csv: str = os.getenv("SCHEDULER_HOLIDAYS_CSV", "")
    rate_limit_per_minute: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "180"))
    bitrix_webhook_base_url: str = os.getenv("BITRIX_WEBHOOK_BASE_URL", "")
    bitrix_rest_base_url: str = os.getenv("BITRIX_REST_BASE_URL", "")
    bitrix_oauth_token: str = os.getenv("BITRIX_OAUTH_TOKEN", "")
    bitrix_openlines_send_mode: str = os.getenv("BITRIX_OPENLINES_SEND_MODE", "crm_message_add")
    bitrix_bot_id: str = os.getenv("BITRIX_BOT_ID", "")
    bitrix_default_assignee_id: str = os.getenv("BITRIX_DEFAULT_ASSIGNEE_ID", "")
    bitrix_report_mode: str = os.getenv("BITRIX_REPORT_MODE", "activity")
    bitrix_report_control_deal_id: str = os.getenv("BITRIX_REPORT_CONTROL_DEAL_ID", "")
    bitrix_report_openline_entity_type: str = os.getenv("BITRIX_REPORT_OPENLINE_ENTITY_TYPE", "DEAL")
    bitrix_report_openline_entity_id: str = os.getenv("BITRIX_REPORT_OPENLINE_ENTITY_ID", "")
    bitrix_report_task_user_id: str = os.getenv("BITRIX_REPORT_TASK_USER_ID", "")
    omie_base_url: str = os.getenv("OMIE_BASE_URL", "https://app.omie.com.br/api/v1")
    omie_app_key: str = os.getenv("OMIE_APP_KEY", "")
    omie_app_secret: str = os.getenv("OMIE_APP_SECRET", "")
    omie_products_service: str = os.getenv("OMIE_PRODUCTS_SERVICE", "geral/produtos")
    omie_products_list_call: str = os.getenv("OMIE_PRODUCTS_LIST_CALL", "ListarProdutos")
    omie_clients_service: str = os.getenv("OMIE_CLIENTS_SERVICE", "geral/clientes")
    omie_client_list_call: str = os.getenv("OMIE_CLIENT_LIST_CALL", "ListarClientes")
    omie_client_upsert_call: str = os.getenv("OMIE_CLIENT_UPSERT_CALL", "UpsertCliente")
    omie_order_service: str = os.getenv("OMIE_ORDER_SERVICE", "produtos/pedido")
    omie_order_create_call: str = os.getenv("OMIE_ORDER_CREATE_CALL", "IncluirPedido")
    omie_invoice_service: str = os.getenv("OMIE_INVOICE_SERVICE", "produtos/faturamento")
    omie_invoice_call: str = os.getenv("OMIE_INVOICE_CALL", "FaturarPedido")
    omie_stock_service: str = os.getenv("OMIE_STOCK_SERVICE", "estoque/consulta")
    omie_stock_list_call: str = os.getenv("OMIE_STOCK_LIST_CALL", "ListarPosicoesEstoque")
    llm_provider: str = os.getenv("LLM_PROVIDER", "openai")
    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_base_url: str = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
    llm_timeout_seconds: float = float(os.getenv("LLM_TIMEOUT_SECONDS", "20"))
    llm_model_conversation: str = os.getenv("MODEL_CONVERSATION", os.getenv("LLM_MODEL_CONVERSATION", "gpt-4.1-mini"))
    llm_model_campaign: str = os.getenv("MODEL_CAMPAIGN", os.getenv("LLM_MODEL_CAMPAIGN", "gpt-4.1-mini"))
    llm_model_summary: str = os.getenv("MODEL_SUMMARY", "gpt-4.1-mini")
    outbound_daily_cap: int = int(os.getenv("OUTBOUND_DAILY_CAP", "1"))
    outbound_campaign_weekly_cap: int = int(os.getenv("OUTBOUND_CAMPAIGN_WEEKLY_CAP", "1"))
    outbound_include_opt_out_text: bool = os.getenv("OUTBOUND_INCLUDE_OPT_OUT_TEXT", "1") == "1"
    telemetry_required_fields: str = os.getenv("TELEMETRY_REQUIRED_FIELDS", "installer_name,install_date,machine_hours")


SETTINGS = Settings()
