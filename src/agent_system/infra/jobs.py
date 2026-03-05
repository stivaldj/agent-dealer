from __future__ import annotations

from typing import Any

from ..app import AgentApp


def run_job(app: AgentApp, queue: str, job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if queue == "conversation_queue" and job_type == "incoming_message":
        return app.conversation.handle_incoming(
            phone=payload["phone"],
            message=payload["message"],
            crm_entity_type=payload.get("crm_entity_type"),
            crm_entity_id=payload.get("crm_entity_id"),
            dialog_id=payload.get("dialog_id"),
            event_id=payload.get("event_id"),
        )

    if queue == "campaign_queue" and job_type == "outreach_campaign":
        return app.campaign.run_outreach(
            campaign_name=payload.get("campaign_name", "daily_outreach"),
            machine_type=payload.get("machine_type"),
            region=payload.get("region"),
            product_category=payload.get("product_category"),
            wave=payload.get("wave", "initial"),
        )

    if queue == "campaign_queue" and job_type == "follow_up":
        return app.campaign.run_followup()

    if queue == "campaign_queue" and job_type == "telemetry_activation":
        return app.telemetry.run_activation_campaign()

    if queue == "erp_queue" and job_type == "deal_won":
        return app.deal_closing.close_won_deal(event_id=payload["event_id"], deal_id=payload["deal_id"])

    if queue == "erp_queue" and job_type == "stock_sync":
        return {"synced_rows": app.stock.full_sync()}

    if queue == "report_queue" and job_type == "daily_report":
        return app.reporting.generate_daily()

    raise ValueError(f"unsupported_job {queue}:{job_type}")
