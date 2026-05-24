import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import config


@dataclass(frozen=True)
class EvalResult:
    should_store_current: bool
    store_payload: Optional[Dict[str, Any]]
    alert: Optional[Dict[str, Any]]
    log_fields: Dict[str, Any]


def _parse_ts(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(value: Any) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x):
        return None
    return x


def _thresholds_for_project(project_id: str) -> Optional[Dict[str, float]]:
    t = config.THRESHOLDS.get(project_id)
    if not t:
        return None
    return {"red": float(t["red"]), "orange": float(t["orange"]), "blue": float(t["blue"])}


def _pick_level(speed_mm_day: float, thresholds: Dict[str, float]) -> Optional[Tuple[str, float]]:
    if speed_mm_day >= thresholds["red"]:
        return "red", thresholds["red"]
    if speed_mm_day >= thresholds["orange"]:
        return "orange", thresholds["orange"]
    if speed_mm_day >= thresholds["blue"]:
        return "blue", thresholds["blue"]
    return None


def evaluate_message(message: Dict[str, Any], previous: Optional[Dict[str, Any]]) -> EvalResult:
    project_id = message.get("project_id")
    sensor_id = message.get("sensor_id")
    sensor_type = message.get("sensor_type")
    payload = message.get("payload") if isinstance(message.get("payload"), dict) else None
    timestamp_raw = message.get("timestamp")

    log_fields: Dict[str, Any] = {
        "project_id": project_id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "timestamp": timestamp_raw,
    }

    if sensor_type != "gnss":
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "sensor_type_not_gnss"})

    if not isinstance(payload, dict):
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "missing_payload"})

    fix_quality = payload.get("fix_quality")
    if fix_quality != "rtk_fixed":
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "fix_quality_not_rtk_fixed"})

    if not isinstance(project_id, str) or not project_id:
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "missing_project_id"})

    if not isinstance(sensor_id, str) or not sensor_id:
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "missing_sensor_id"})

    thresholds = _thresholds_for_project(project_id)
    if thresholds is None:
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "unknown_project"})

    ts = _parse_ts(timestamp_raw)
    if ts is None:
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "bad_timestamp"})

    current_cum = _safe_float(payload.get("cumulative_displacement_mm"))
    if current_cum is None:
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "missing_cumulative_displacement"})

    store_current = {"timestamp": ts.isoformat().replace("+00:00", "Z"), "cumulative_displacement_mm": current_cum}

    if not previous:
        return EvalResult(True, store_current, None, {**log_fields, "previous": "none"})

    prev_ts = _parse_ts(previous.get("timestamp"))
    prev_cum = _safe_float(previous.get("cumulative_displacement_mm"))
    if prev_ts is None or prev_cum is None:
        return EvalResult(True, store_current, None, {**log_fields, "previous": "invalid_reset"})

    dt_seconds = (ts - prev_ts).total_seconds()
    log_fields = {
        **log_fields,
        "previous_timestamp": prev_ts.isoformat().replace("+00:00", "Z"),
        "previous_displacement_mm": prev_cum,
        "current_displacement_mm": current_cum,
        "dt_seconds": dt_seconds,
    }

    if dt_seconds <= 0:
        return EvalResult(False, None, None, {**log_fields, "skip_reason": "out_of_order_or_duplicate_timestamp"})

    if dt_seconds > config.MAX_DT_SECONDS:
        return EvalResult(True, store_current, None, {**log_fields, "skip_reason": "gap_too_large_reset"})

    speed_mm_day = (current_cum - prev_cum) / dt_seconds * 86400.0
    log_fields = {**log_fields, "deformation_speed_mm_day": speed_mm_day}

    level_pick = _pick_level(speed_mm_day, thresholds)
    if not level_pick:
        return EvalResult(True, store_current, None, {**log_fields, "alert": "none"})

    level, threshold = level_pick
    alert = {
        "schema_version": "1.0",
        "message_id": str(uuid.uuid4()),
        "project_id": project_id,
        "sensor_id": sensor_id,
        "alert_type": "deformation_speed",
        "alert_level": level,
        "deformation_speed_mm_day": round(speed_mm_day, 6),
        "current_displacement_mm": current_cum,
        "previous_displacement_mm": prev_cum,
        "threshold_mm_day": threshold,
        "timestamp": store_current["timestamp"],
    }

    return EvalResult(True, store_current, alert, {**log_fields, "alert": level, "threshold_mm_day": threshold})
