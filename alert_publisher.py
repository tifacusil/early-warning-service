import json
import logging
from typing import Any, Dict, Optional

import pika
import requests

import config


logger = logging.getLogger("early_warning.alert_publisher")


class AlertPublisher:
    def __init__(self, channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
        self._channel = channel

    def publish_rabbitmq(self, alert: Dict[str, Any]) -> None:
        project_id = alert.get("project_id")
        level = alert.get("alert_level")
        routing_key = f"alerts.{project_id}.{level}"
        body = json.dumps(alert, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        self._channel.basic_publish(
            exchange=config.ALERTS_EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )

    def publish_http(self, alert: Dict[str, Any], url: Optional[str] = None) -> None:
        target = url if url is not None else config.ALERT_HTTP_URL
        if not target:
            return
        try:
            resp = requests.post(target, json=alert, timeout=config.HTTP_TIMEOUT_SECONDS)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("http_publish_failed url=%s err=%s", target, exc)

