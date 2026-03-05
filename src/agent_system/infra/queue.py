from __future__ import annotations

import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

from redis import Redis

from ..config import SETTINGS


CAMPAIGN_QUEUE = "campaign_queue"
CONVERSATION_QUEUE = "conversation_queue"
ERP_QUEUE = "erp_queue"
REPORT_QUEUE = "report_queue"


@dataclass
class QueueJob:
    queue: str
    job_type: str
    payload: dict[str, Any]

    def dumps(self) -> str:
        return json.dumps({"queue": self.queue, "job_type": self.job_type, "payload": self.payload})

    @staticmethod
    def loads(data: str) -> "QueueJob":
        raw = json.loads(data)
        return QueueJob(queue=raw["queue"], job_type=raw["job_type"], payload=raw["payload"])


class QueueBroker:
    def __init__(self, redis_url: str | None = None) -> None:
        self.redis_url = redis_url or SETTINGS.redis_url
        self._fallback = defaultdict(deque)
        self._redis: Redis | None = None
        try:
            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
            self._redis.ping()
        except Exception:
            self._redis = None

    def enqueue(self, queue: str, job_type: str, payload: dict[str, Any]) -> None:
        job = QueueJob(queue=queue, job_type=job_type, payload=payload).dumps()
        if self._redis:
            self._redis.rpush(queue, job)
            return
        self._fallback[queue].append(job)

    def dequeue(self, queue: str, timeout_seconds: int = 2) -> QueueJob | None:
        if self._redis:
            item = self._redis.blpop([queue], timeout=timeout_seconds)
            if not item:
                return None
            return QueueJob.loads(item[1])
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self._fallback[queue]:
                return QueueJob.loads(self._fallback[queue].popleft())
            time.sleep(0.1)
        return None
