from __future__ import annotations

import json
import logging
from collections import Counter
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
        }
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload)


def get_logger(name: str = "agent_system") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


class Metrics:
    def __init__(self) -> None:
        self._counter = Counter()

    def incr(self, key: str, value: int = 1) -> None:
        self._counter[key] += value

    def snapshot(self) -> dict[str, Any]:
        return dict(self._counter)
