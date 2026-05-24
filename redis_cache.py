import json
from datetime import datetime, time, timezone
from typing import Any, Dict, List, Optional

import pymysql
import redis

import config


def _level_value(level: str) -> int:
    if level == "blue":
        return 1
    if level == "orange":
        return 2
    if level == "red":
        return 3
    return 0


def _in_quiet_hours(start: Optional[time], end: Optional[time], now: time) -> bool:
    if start is None or end is None:
        return False
    if start <= end:
        return start <= now <= end
    return now >= start or now <= end


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

    def record_alert_db(self, alert: Dict[str, Any]) -> None:
        conn = pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASS,
            database=config.MYSQL_DB,
            autocommit=True,
            connect_timeout=int(config.MYSQL_CONNECT_TIMEOUT_SECONDS),
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS alerts (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        created_at DATETIME NOT NULL,
                        message_id VARCHAR(64),
                        project_id VARCHAR(64) NOT NULL,
                        sensor_id VARCHAR(64) NOT NULL,
                        alert_level ENUM('blue','orange','red') NOT NULL,
                        deformation_speed_mm_day DOUBLE,
                        threshold_mm_day DOUBLE,
                        alert_timestamp DATETIME
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO alerts (
                        created_at,
                        message_id,
                        project_id,
                        sensor_id,
                        alert_level,
                        deformation_speed_mm_day,
                        threshold_mm_day,
                        alert_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        str(alert.get("message_id") or ""),
                        str(alert.get("project_id") or ""),
                        str(alert.get("sensor_id") or ""),
                        str(alert.get("alert_level") or ""),
                        float(alert["deformation_speed_mm_day"]) if alert.get("deformation_speed_mm_day") is not None else None,
                        float(alert["threshold_mm_day"]) if alert.get("threshold_mm_day") is not None else None,
                        _parse_mysql_datetime(alert.get("timestamp")),
                    ),
                )
        finally:
            conn.close()

    def get_push_device_tokens(self, project_id: str, alert_level: str) -> List[str]:
        if not project_id or not alert_level:
            return []
        if _level_value(alert_level) == 0:
            return []

        conn = pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASS,
            database=config.MYSQL_DB,
            autocommit=True,
            connect_timeout=int(config.MYSQL_CONNECT_TIMEOUT_SECONDS),
            cursorclass=pymysql.cursors.DictCursor,
        )

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        s.user_id,
                        s.min_alert_level,
                        s.push_enabled,
                        s.quiet_hours_start,
                        s.quiet_hours_end
                    FROM user_project_alert_subscription s
                    INNER JOIN users u ON u.id = s.user_id
                    WHERE
                        s.project_id = %s
                        AND u.status = 'active'
                        AND s.push_enabled = TRUE
                    """,
                    (project_id,),
                )
                subs = list(cur.fetchall())

                now = datetime.now(timezone.utc).time()
                allowed_user_ids: List[int] = []
                for row in subs:
                    min_level = str(row.get("min_alert_level") or "")
                    if _level_value(min_level) == 0:
                        continue
                    if _level_value(min_level) > _level_value(alert_level):
                        continue

                    start = row.get("quiet_hours_start")
                    end = row.get("quiet_hours_end")
                    if alert_level != "red" and _in_quiet_hours(start, end, now):
                        pass
                    allowed_user_ids.append(int(row["user_id"]))

                if not allowed_user_ids:
                    return []

                placeholders = ",".join(["%s"] * len(allowed_user_ids))
                cur.execute(
                    f"""
                    SELECT device_token
                    FROM user_device
                    WHERE user_id IN ({placeholders}) AND status = 'active'
                    """,
                    tuple(allowed_user_ids),
                )
                tokens = [str(r["device_token"]) for r in cur.fetchall() if r.get("device_token")]
                return list({t for t in tokens if t})
        finally:
            conn.close()


def _parse_mysql_datetime(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
