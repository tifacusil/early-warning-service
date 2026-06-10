import atexit
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from flask import Flask, Response, jsonify, request
from dotenv import load_dotenv
import requests

load_dotenv()

import config
from alert_publisher import AlertPublisher
from consumer import RabbitConsumer
from redis_cache import RedisCache


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


cache = RedisCache()
consumer = RabbitConsumer(cache)

app = Flask(__name__)


def _openapi_spec() -> dict:
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "early-warning",
            "version": "1.0.0",
            "description": "API servicio early-warning",
        },
        "paths": {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/HealthResponse"}}},
                        }
                    },
                }
            },
            "/alerts/recent": {
                "get": {
                    "summary": "Alertas recientes",
                    "parameters": [
                        {
                            "name": "limit",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "integer", "minimum": 0, "maximum": 200, "default": 10},
                            "description": "Cantidad maxima de alertas a devolver (0..200).",
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {"schema": {"$ref": "#/components/schemas/RecentAlertsResponse"}}
                            },
                        }
                    },
                }
            },
            "/reports/latest": {
                "get": {
                    "summary": "Reporte mas reciente",
                    "parameters": [
                        {"name": "project_id", "in": "query", "required": True, "schema": {"type": "string"}},
                        {
                            "name": "period",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string", "enum": ["daily", "weekly"], "default": "daily"},
                        },
                    ],
                    "responses": {
                        "200": {"description": "OK", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Report"}}}},
                        "404": {"description": "Not found"},
                    },
                }
            },
            "/reports/generate": {
                "post": {
                    "summary": "Genera reporte (ventana completa mas reciente)",
                    "parameters": [
                        {"name": "project_id", "in": "query", "required": True, "schema": {"type": "string"}},
                        {
                            "name": "period",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string", "enum": ["daily", "weekly"], "default": "daily"},
                        },
                        {
                            "name": "push",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "boolean", "default": False},
                        },
                    ],
                    "responses": {
                        "200": {"description": "OK", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Report"}}}}
                    },
                }
            },
        },
        "components": {
            "schemas": {
                "HealthResponse": {
                    "type": "object",
                    "required": ["status", "service", "alerts_generated"],
                    "properties": {
                        "status": {"type": "string"},
                        "service": {"type": "string"},
                        "alerts_generated": {"type": "integer"},
                    },
                },
                "Alert": {
                    "type": "object",
                    "required": [
                        "schema_version",
                        "message_id",
                        "project_id",
                        "sensor_id",
                        "alert_type",
                        "alert_level",
                        "deformation_speed_mm_day",
                        "current_displacement_mm",
                        "previous_displacement_mm",
                        "threshold_mm_day",
                        "timestamp",
                    ],
                    "properties": {
                        "schema_version": {"type": "string"},
                        "message_id": {"type": "string"},
                        "project_id": {"type": "string"},
                        "sensor_id": {"type": "string"},
                        "alert_type": {"type": "string"},
                        "alert_level": {"type": "string", "enum": ["blue", "orange", "red"]},
                        "deformation_speed_mm_day": {"type": "number"},
                        "current_displacement_mm": {"type": "number"},
                        "previous_displacement_mm": {"type": "number"},
                        "threshold_mm_day": {"type": "number"},
                        "timestamp": {"type": "string", "format": "date-time"},
                    },
                    "additionalProperties": True,
                },
                "RecentAlertsResponse": {
                    "type": "object",
                    "required": ["alerts"],
                    "properties": {"alerts": {"type": "array", "items": {"$ref": "#/components/schemas/Alert"}}},
                },
                "Report": {
                    "type": "object",
                    "required": ["schema_version", "report_id", "project_id", "period", "window_start", "window_end", "generated_at"],
                    "properties": {
                        "schema_version": {"type": "string"},
                        "report_id": {"type": "string"},
                        "project_id": {"type": "string"},
                        "period": {"type": "string", "enum": ["daily", "weekly"]},
                        "window_start": {"type": "string", "format": "date-time"},
                        "window_end": {"type": "string", "format": "date-time"},
                        "generated_at": {"type": "string", "format": "date-time"},
                        "summary": {"type": "object", "additionalProperties": True},
                        "system": {"type": "object", "additionalProperties": True},
                        "data_analysis": {"type": "object", "additionalProperties": True},
                    },
                    "additionalProperties": True,
                },
            }
        },
    }


@app.get("/openapi.json")
def openapi_json():
    return jsonify(_openapi_spec())


@app.get("/docs")
def docs():
    html = """<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>early-warning API docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
    <script>
      window.ui = SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui" });
    </script>
  </body>
</html>
"""
    return Response(html, mimetype="text/html")


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "healthy",
            "service": "early-warning",
            "alerts_generated": cache.get_alerts_generated(),
        }
    )


@app.get("/alerts/recent")
def alerts_recent():
    limit_raw = request.args.get("limit", "10")
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 10
    return jsonify({"alerts": cache.get_recent_alerts(limit)})


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _last_complete_daily_window(now_utc: datetime) -> tuple[datetime, datetime]:
    day_start = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
    return day_start - timedelta(days=1), day_start


def _last_complete_weekly_window(now_utc: datetime) -> tuple[datetime, datetime]:
    day_start = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
    week_start = day_start - timedelta(days=day_start.weekday())
    return week_start - timedelta(days=7), week_start


def _enrich_report(project_id: str, period: str, window_start: str, window_end: str) -> Optional[dict]:
    if not config.DATA_ANALYSIS_URL:
        return None
    try:
        resp = requests.post(
            config.DATA_ANALYSIS_URL,
            json={"project_id": project_id, "period": period, "window_start": window_start, "window_end": window_end},
            timeout=config.DATA_ANALYSIS_TIMEOUT_SECONDS,
        )
        if resp.status_code // 100 != 2:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except requests.RequestException:
        return None
    except ValueError:
        return None


def _generate_report(project_id: str, period: str, *, send_push: bool) -> dict:
    now = datetime.now(timezone.utc)
    if period == "weekly":
        ws_dt, we_dt = _last_complete_weekly_window(now)
    else:
        ws_dt, we_dt = _last_complete_daily_window(now)

    window_start = _iso_z(ws_dt)
    window_end = _iso_z(we_dt)
    generated_at = _iso_z(now)

    summary = cache.summarize_alerts_db(project_id, window_start, window_end)
    system = {
        "service": "early-warning",
        "alerts_generated_total": cache.get_alerts_generated(),
        "last_message_processed_at": cache.get_last_message_processed_at(),
    }

    report = {
        "schema_version": "1.0",
        "report_id": str(uuid.uuid4()),
        "project_id": project_id,
        "period": period,
        "window_start": window_start,
        "window_end": window_end,
        "generated_at": generated_at,
        "summary": summary,
        "system": system,
    }

    enriched = _enrich_report(project_id, period, window_start, window_end)
    if enriched is not None:
        report["data_analysis"] = enriched

    cache.upsert_report_db(report)
    cache.set_last_report_generated_at(project_id, period, generated_at)

    if send_push and config.REPORT_PUSH_ENABLED:
        tokens = cache.get_push_device_tokens_for_project(project_id)
        if tokens:
            publisher = AlertPublisher()
            title = f"Reporte {period} {project_id}"
            body = f"Alertas {summary.get('total', 0)} | R:{summary.get('by_level', {}).get('red', 0)} O:{summary.get('by_level', {}).get('orange', 0)} B:{summary.get('by_level', {}).get('blue', 0)}"
            publisher.publish_notification(
                title=title,
                body=body,
                data={
                    "type": "early_warning_report",
                    "report_id": report["report_id"],
                    "project_id": project_id,
                    "period": period,
                    "window_start": window_start,
                    "window_end": window_end,
                },
                tokens=tokens,
            )

    return report


class ReportScheduler(threading.Thread):
    def __init__(self) -> None:
        super().__init__(name="report-scheduler", daemon=True)
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger = logging.getLogger("early_warning.report_scheduler")
        while not self._stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                for project_id in list(config.THRESHOLDS.keys()):
                    ws, we = _last_complete_daily_window(now)
                    if now >= we + timedelta(minutes=5):
                        ws_iso = _iso_z(ws)
                        we_iso = _iso_z(we)
                        if not cache.report_exists_db(project_id, "daily", ws_iso, we_iso):
                            _generate_report(project_id, "daily", send_push=True)

                    ws_w, we_w = _last_complete_weekly_window(now)
                    if now >= we_w + timedelta(minutes=10):
                        ws_w_iso = _iso_z(ws_w)
                        we_w_iso = _iso_z(we_w)
                        if not cache.report_exists_db(project_id, "weekly", ws_w_iso, we_w_iso):
                            _generate_report(project_id, "weekly", send_push=True)
            except Exception as exc:
                logger.warning("scheduler_failed err=%s", exc)

            self._stop_event.wait(config.REPORT_CHECK_INTERVAL_SECONDS)


@app.get("/reports/latest")
def reports_latest():
    project_id = (request.args.get("project_id") or "").strip()
    period = (request.args.get("period") or "daily").strip().lower()
    if not project_id:
        return jsonify({"error": "project_id_required"}), 400
    report = cache.get_latest_report_db(project_id, period)
    if report is None:
        return jsonify({"error": "not_found"}), 404
    return jsonify(report)


@app.post("/reports/generate")
def reports_generate():
    project_id = (request.args.get("project_id") or "").strip()
    period = (request.args.get("period") or "daily").strip().lower()
    push_raw = (request.args.get("push") or "false").strip().lower()
    send_push = push_raw in {"1", "true", "yes", "y"}

    if not project_id:
        return jsonify({"error": "project_id_required"}), 400
    if period not in {"daily", "weekly"}:
        return jsonify({"error": "invalid_period"}), 400

    try:
        report = _generate_report(project_id, period, send_push=send_push)
        return jsonify(report)
    except Exception as exc:
        return jsonify({"error": "generate_failed", "detail": str(exc)}), 500


if __name__ == "__main__":
    consumer.start()
    if config.REPORT_SCHEDULER_ENABLED:
        ReportScheduler().start()

    if config.CONSUL_ENABLED:
        try:
            from consul_registrar import ConsulRegistrar

            registrar = ConsulRegistrar()
            registrar.register()
            atexit.register(registrar.deregister)
        except Exception as exc:
            logging.getLogger("early_warning.consul").warning("consul_setup_failed err=%s", exc)

    app.run(host="0.0.0.0", port=config.FLASK_PORT, debug=False, use_reloader=False)
