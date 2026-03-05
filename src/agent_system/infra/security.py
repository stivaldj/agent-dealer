from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from redis import Redis

from ..config import SETTINGS


class RateLimiter:
    def __init__(self, limit_per_minute: int, redis_url: str | None = None) -> None:
        self.limit_per_minute = limit_per_minute
        self._redis: Redis | None = None
        try:
            self._redis = Redis.from_url(redis_url or SETTINGS.redis_url, decode_responses=True)
            self._redis.ping()
        except Exception:
            self._redis = None

    def allow(self, key: str) -> bool:
        if not self._redis:
            return True
        now = int(time.time())
        bucket = now // 60
        redis_key = f"rl:inbound:{key}:{bucket}"
        current = self._redis.incr(redis_key)
        if current == 1:
            self._redis.expire(redis_key, 120)
        return current <= self.limit_per_minute


class GlobalOutboundLimiter:
    def __init__(self, redis_url: str | None = None) -> None:
        self._redis: Redis | None = None
        try:
            self._redis = Redis.from_url(redis_url or SETTINGS.redis_url, decode_responses=True)
            self._redis.ping()
        except Exception:
            self._redis = None

    def allow_send(
        self,
        *,
        phone: str,
        campaign_name: str,
        daily_cap: int,
        weekly_campaign_cap: int,
        now_dt: datetime | None = None,
    ) -> tuple[bool, str]:
        if not self._redis:
            return True, "allowed_no_redis"
        tz = ZoneInfo(SETTINGS.tz)
        now = now_dt.astimezone(tz) if now_dt else datetime.now(tz)
        day_token = now.strftime("%Y%m%d")
        week_token = now.strftime("%Y%W")
        per_phone_day = f"outbound:daily:{phone}:{day_token}"
        per_campaign_week = f"outbound:campaign:{campaign_name}:{phone}:{week_token}"

        day_count = self._redis.incr(per_phone_day)
        if day_count == 1:
            self._redis.expire(per_phone_day, 60 * 60 * 24 * 2)
        if day_count > daily_cap:
            return False, "daily_cap"

        campaign_count = self._redis.incr(per_campaign_week)
        if campaign_count == 1:
            self._redis.expire(per_campaign_week, 60 * 60 * 24 * 10)
        if campaign_count > weekly_campaign_cap:
            return False, "campaign_weekly_cap"
        return True, "allowed"


def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def validate_json_fields(payload: dict[str, Any], required: list[str]) -> tuple[bool, str | None]:
    missing = [field for field in required if field not in payload]
    if missing:
        return False, f"missing_required_fields:{','.join(missing)}"
    return True, None


def validate_schema(payload: dict[str, Any], schema: dict[str, type]) -> tuple[bool, str | None]:
    for field, expected_type in schema.items():
        if field not in payload:
            return False, f"missing_required_fields:{field}"
        value = payload[field]
        if not isinstance(value, expected_type):
            return False, f"invalid_type:{field}"
    return True, None


def sanitize_message(text: str) -> str:
    for marker in ("OMIE_APP_SECRET", "BITRIX_OAUTH_TOKEN", "LLM_API_KEY"):
        if marker in text:
            text = text.replace(marker, "[REDACTED]")
    return text[:2000]


def stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
