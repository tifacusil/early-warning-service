import logging

from flask import Flask, Response, jsonify, request
from dotenv import load_dotenv

load_dotenv()

import config
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


if __name__ == "__main__":
    consumer.start()
    app.run(host="0.0.0.0", port=config.FLASK_PORT, debug=False, use_reloader=False)
