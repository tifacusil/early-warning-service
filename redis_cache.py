import json
from typing import Any, Dict, List, Optional

import redis

import config


class RedisCache:
    def __init__(self) -> None:
        self._client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            password=config.REDIS_PASS or None,
            decode_responses=True,
        )

    @staticmethod
    def last_key(sensor_id: str) -> str:
        return f"alarm:last:{sensor_id}"

    @staticmethod
    def alerts_count_key() -> str:
        return "early_warning:alerts_generated"

    @staticmethod
    def recent_alerts_key() -> str:
        return "early_warning:alerts:recent"

    def get_last_reading(self, sensor_id: str) -> Optional[Dict[str, Any]]:
        raw = self._client.get(self.last_key(sensor_id))
        if not raw:
            return None
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(value, dict):
            return None
        return value

    def set_last_reading(self, sensor_id: str, reading: Dict[str, Any]) -> None:
        self._client.set(self.last_key(sensor_id), json.dumps(reading, separators=(",", ":")))

    def incr_alerts_generated(self) -> int:
        return int(self._client.incr(self.alerts_count_key()))

    def get_alerts_generated(self) -> int:
        value = self._client.get(self.alerts_count_key())
        if value is None:
            return 0
        try:
            return int(value)
        except ValueError:
            return 0

    def push_recent_alert(self, alert: Dict[str, Any], keep: int = 200) -> None:
        pipe = self._client.pipeline()
        pipe.lpush(self.recent_alerts_key(), json.dumps(alert, separators=(",", ":")))
        pipe.ltrim(self.recent_alerts_key(), 0, max(keep - 1, 0))
        pipe.execute()

    def get_recent_alerts(self, limit: int) -> List[Dict[str, Any]]:
        limit = max(0, min(int(limit), 200))
        if limit == 0:
            return []
        raw_items = self._client.lrange(self.recent_alerts_key(), 0, limit - 1)
        out: List[Dict[str, Any]] = []
        for raw in raw_items:
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                out.append(value)
        return out

