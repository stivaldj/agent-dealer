from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .channels.openlines_channel import OpenLinesChannel
from .db import Database
from .infra.llm_router import LLMRouter
from .infra.observability import Metrics
from .infra.queue import QueueBroker
from .integrations.bitrix import BitrixClient
from .integrations.omie import OmieClient
from .services.campaign import CampaignService
from .services.conversation import ConversationService
from .services.deal_closing import DealClosingService
from .services.handoff import HandoffService
from .services.reporting import ReportingService
from .services.stock import StockService
from .services.telemetry import TelemetryService
from .integrations.omie import OmieIntegrationError


@dataclass
class AgentApp:
    db: Database
    bitrix: BitrixClient
    omie: OmieClient
    llm_router: LLMRouter
    queue: QueueBroker
    metrics: Metrics
    stock: StockService
    campaign: CampaignService
    handoff: HandoffService
    conversation: ConversationService
    deal_closing: DealClosingService
    telemetry: TelemetryService
    reporting: ReportingService
    channel: OpenLinesChannel

    def bootstrap_demo_data(self) -> None:
        if self.db.customers():
            return
        self.db.add_customer(
            {"id": "CUST-001", "name": "Agro Norte", "phone": "+5565999001001", "region": "MT", "store": "Cuiaba"}
        )
        self.db.add_client(
            {
                "id": "CUST-001",
                "name": "Agro Norte",
                "phone": "+5565999001001",
                "city": "Cuiaba",
                "state": "MT",
            }
        )
        self.db.add_customer(
            {"id": "CUST-002", "name": "Fazenda Sol", "phone": "+5565999001002", "region": "MT", "store": "Rondonopolis"}
        )
        self.db.add_client(
            {
                "id": "CUST-002",
                "name": "Fazenda Sol",
                "phone": "+5565999001002",
                "city": "Rondonopolis",
                "state": "MT",
            }
        )
        self.db.add_machine(
            {
                "id": "MACH-001",
                "client_id": "CUST-001",
                "brand": "CASE",
                "model": "CASE-580N",
                "serial": "SN580A",
                "year": 2023,
                "telemetry_status": "inactive",
            }
        )
        self.db.add_machine_ownership(client_id="CUST-001", machine_id="MACH-001")
        self.db.add_machine(
            {
                "id": "MACH-002",
                "client_id": "CUST-002",
                "brand": "CASE",
                "model": "CASE-770EX",
                "serial": "SN770B",
                "year": 2022,
                "telemetry_status": "active",
                "telemetry_active": True,
            }
        )
        self.db.add_machine_ownership(client_id="CUST-002", machine_id="MACH-002")
        self.db.upsert_offer_rule(
            {
                "id": "seed-parts-offer",
                "rule_type": "machine",
                "predicate": {"state": "MT"},
                "offer_template": "Temos pecas e kits para sua maquina {machine_model}.",
                "sku_list": ["ABC123", "XYZ777"],
                "priority": 10,
                "enabled": True,
            }
        )
        try:
            self.stock.full_sync()
        except OmieIntegrationError:
            pass
        self.db.create_deal(
            {
                "id": "DEAL-SEED-001",
                "customer_id": "CUST-001",
                "products": [{"sku": "ABC123", "qty": 1}],
                "status": "NEW",
            }
        )

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


def create_app(database_url: str | None = None, db_path: str | None = None) -> AgentApp:
    # db_path is kept for compatibility with previous call sites.
    db = Database(database_url=database_url or db_path)
    bitrix = BitrixClient()
    omie = OmieClient()
    llm_router = LLMRouter()
    queue = QueueBroker()
    metrics = Metrics()
    channel = OpenLinesChannel(bitrix)

    stock = StockService(db, omie)
    handoff = HandoffService(db, bitrix)
    campaign = CampaignService(db, bitrix, llm_router, channel)
    conversation = ConversationService(db, bitrix, stock, handoff, llm_router, channel)
    deal_closing = DealClosingService(db, bitrix, omie)
    telemetry = TelemetryService(db, bitrix)
    reporting = ReportingService(db, bitrix)

    app = AgentApp(
        db=db,
        bitrix=bitrix,
        omie=omie,
        llm_router=llm_router,
        queue=queue,
        metrics=metrics,
        stock=stock,
        campaign=campaign,
        handoff=handoff,
        conversation=conversation,
        deal_closing=deal_closing,
        telemetry=telemetry,
        reporting=reporting,
        channel=channel,
    )
    app.bootstrap_demo_data()
    return app
