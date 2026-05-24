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
        conn = self._mysql_connect()
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

        conn = self._mysql_connect()

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
                        continue
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

    def get_push_device_tokens_for_project(self, project_id: str) -> List[str]:
        if not project_id:
            return []
        conn = self._mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT d.device_token
                    FROM user_project_alert_subscription s
                    INNER JOIN users u ON u.id = s.user_id
                    INNER JOIN user_device d ON d.user_id = u.id
                    WHERE
                        s.project_id = %s
                        AND u.status = 'active'
                        AND s.push_enabled = TRUE
                        AND d.status = 'active'
                    """,
                    (project_id,),
                )
                tokens = [str(r["device_token"]) for r in cur.fetchall() if r.get("device_token")]
                return list({t for t in tokens if t})
        finally:
            conn.close()

    def upsert_report_db(self, report: Dict[str, Any]) -> None:
        conn = self._mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS reports (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        report_id VARCHAR(64) NOT NULL,
                        project_id VARCHAR(64) NOT NULL,
                        period ENUM('daily','weekly') NOT NULL,
                        window_start DATETIME NOT NULL,
                        window_end DATETIME NOT NULL,
                        generated_at DATETIME NOT NULL,
                        content_json JSON NOT NULL,
                        UNIQUE KEY uk_report (project_id, period, window_start, window_end),
                        INDEX idx_reports_project_period (project_id, period, generated_at)
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO reports (
                        report_id,
                        project_id,
                        period,
                        window_start,
                        window_end,
                        generated_at,
                        content_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        report_id = VALUES(report_id),
                        generated_at = VALUES(generated_at),
                        content_json = VALUES(content_json)
                    """,
                    (
                        str(report.get("report_id") or ""),
                        str(report.get("project_id") or ""),
                        str(report.get("period") or ""),
                        _parse_mysql_datetime(report.get("window_start")) or "1970-01-01 00:00:00",
                        _parse_mysql_datetime(report.get("window_end")) or "1970-01-01 00:00:00",
                        _parse_mysql_datetime(report.get("generated_at")) or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        json.dumps(report, separators=(",", ":"), ensure_ascii=False),
                    ),
                )
        finally:
            conn.close()

    def get_latest_report_db(self, project_id: str, period: str) -> Optional[Dict[str, Any]]:
        if not project_id or period not in {"daily", "weekly"}:
            return None
        conn = self._mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT content_json
                    FROM reports
                    WHERE project_id = %s AND period = %s
                    ORDER BY generated_at DESC
                    LIMIT 1
                    """,
                    (project_id, period),
                )
                row = cur.fetchone()
                if not row or not row.get("content_json"):
                    return None
                try:
                    value = json.loads(row["content_json"])
                except json.JSONDecodeError:
                    return None
                return value if isinstance(value, dict) else None
        finally:
            conn.close()

    def report_exists_db(self, project_id: str, period: str, window_start: str, window_end: str) -> bool:
        if not project_id or period not in {"daily", "weekly"}:
            return False
        ws = _parse_mysql_datetime(window_start)
        we = _parse_mysql_datetime(window_end)
        if not ws or not we:
            return False
        conn = self._mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM reports
                    WHERE project_id = %s AND period = %s AND window_start = %s AND window_end = %s
                    LIMIT 1
                    """,
                    (project_id, period, ws, we),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()

    def summarize_alerts_db(self, project_id: str, window_start: str, window_end: str) -> Dict[str, Any]:
        ws = _parse_mysql_datetime(window_start)
        we = _parse_mysql_datetime(window_end)
        if not project_id or not ws or not we:
            return {"total": 0, "by_level": {"blue": 0, "orange": 0, "red": 0}, "max_speed_mm_day": None, "avg_speed_mm_day": None}

        conn = self._mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        alert_level,
                        COUNT(*) AS cnt,
                        MAX(deformation_speed_mm_day) AS max_speed,
                        AVG(deformation_speed_mm_day) AS avg_speed
                    FROM alerts
                    WHERE project_id = %s AND alert_timestamp >= %s AND alert_timestamp < %s
                    GROUP BY alert_level
                    """,
                    (project_id, ws, we),
                )
                rows = list(cur.fetchall())

                by = {"blue": 0, "orange": 0, "red": 0}
                max_speed = None
                sum_avg = 0.0
                sum_cnt = 0
                for r in rows:
                    lvl = str(r.get("alert_level") or "")
                    cnt = int(r.get("cnt") or 0)
                    if lvl in by:
                        by[lvl] = cnt
                    if r.get("max_speed") is not None:
                        m = float(r["max_speed"])
                        max_speed = m if max_speed is None else max(max_speed, m)
                    if r.get("avg_speed") is not None and cnt > 0:
                        sum_avg += float(r["avg_speed"]) * cnt
                        sum_cnt += cnt
                total = by["blue"] + by["orange"] + by["red"]
                avg_speed = (sum_avg / sum_cnt) if sum_cnt > 0 else None
                return {"total": total, "by_level": by, "max_speed_mm_day": max_speed, "avg_speed_mm_day": avg_speed}
        finally:
            conn.close()

    def set_last_message_processed_at(self, iso_ts: str) -> None:
        self._client.set("early_warning:last_message_processed_at", iso_ts)

    def get_last_message_processed_at(self) -> Optional[str]:
        v = self._client.get("early_warning:last_message_processed_at")
        return v if v else None

    def set_last_report_generated_at(self, project_id: str, period: str, iso_ts: str) -> None:
        self._client.set(f"early_warning:last_report_generated_at:{project_id}:{period}", iso_ts)

    def _mysql_connect(self):
        return pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASS,
            database=config.MYSQL_DB,
            autocommit=True,
            connect_timeout=int(config.MYSQL_CONNECT_TIMEOUT_SECONDS),
            cursorclass=pymysql.cursors.DictCursor,
        )


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
