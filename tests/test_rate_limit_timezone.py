from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_system.infra.security import GlobalOutboundLimiter


class _FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, int] = {}
        self.expirations: dict[str, int] = {}

    def incr(self, key: str) -> int:
        self.data[key] = self.data.get(key, 0) + 1
        return self.data[key]

    def expire(self, key: str, ttl: int) -> None:
        self.expirations[key] = ttl


def test_rate_limit_timezone() -> None:
    limiter = GlobalOutboundLimiter()
    limiter._redis = _FakeRedis()  # type: ignore[assignment]

    tz = ZoneInfo("America/Cuiaba")
    before_midnight = datetime(2026, 3, 5, 23, 59, tzinfo=tz)
    after_midnight = datetime(2026, 3, 6, 0, 1, tzinfo=tz)

    ok1, _ = limiter.allow_send(
        phone="+556599900000",
        campaign_name="daily",
        daily_cap=1,
        weekly_campaign_cap=10,
        now_dt=before_midnight,
    )
    ok2, _ = limiter.allow_send(
        phone="+556599900000",
        campaign_name="daily",
        daily_cap=1,
        weekly_campaign_cap=10,
        now_dt=after_midnight,
    )

    assert ok1 is True
    assert ok2 is True
    assert "outbound:daily:+556599900000:20260305" in limiter._redis.data
    assert "outbound:daily:+556599900000:20260306" in limiter._redis.data
