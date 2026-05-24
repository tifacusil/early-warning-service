import json
import logging
import threading
from typing import Any, Dict, Optional

import pika
import requests

import config


logger = logging.getLogger("early_warning.alert_publisher")
_firebase_lock = threading.Lock()
_firebase_ready = False
_firebase_import_ok = True

try:
    import firebase_admin
    from firebase_admin import credentials, messaging
except ImportError:
    firebase_admin = None
    credentials = None
    messaging = None
    _firebase_import_ok = False


class AlertPublisher:
    def __init__(self, channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None) -> None:
        self._channel = channel

    def publish_rabbitmq(self, alert: Dict[str, Any]) -> None:
        if self._channel is None:
            return
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

    def publish_notification(
        self,
        *,
        title: str,
        body: str,
        data: Dict[str, Any],
        tokens: Optional[list[str]] = None,
        topic: Optional[str] = None,
    ) -> None:
        if not _firebase_import_ok:
            return
        if not _ensure_firebase():
            return

        resolved_topic = (topic if topic is not None else config.FIREBASE_TOPIC).strip()
        resolved_tokens = [t for t in tokens if t] if tokens is not None else []

        notification = messaging.Notification(title=title, body=body)
        payload = {k: str(v) for k, v in data.items() if v is not None}

        try:
            if resolved_topic:
                messaging.send(messaging.Message(topic=resolved_topic, notification=notification, data=payload))
                return
            if resolved_tokens:
                messaging.send_multicast(messaging.MulticastMessage(tokens=resolved_tokens, notification=notification, data=payload))
        except Exception as exc:
            logger.warning("push_publish_failed err=%s", exc)

    def publish_push(self, alert: Dict[str, Any], *, tokens: Optional[list[str]] = None, topic: Optional[str] = None) -> None:
        level = str(alert.get("alert_level") or "")
        sensor_id = str(alert.get("sensor_id") or "")
        speed = alert.get("deformation_speed_mm_day")
        speed_str = str(speed) if speed is not None else ""
        if tokens is None:
            tokens_raw = (config.FIREBASE_DEVICE_TOKENS or "").strip()
            resolved_tokens = [t.strip() for t in tokens_raw.split(",") if t.strip()] if tokens_raw else []
        else:
            resolved_tokens = tokens

        data = dict(alert)
        data["type"] = "early_warning_alert"
        self.publish_notification(
            title=f"Alerta {level.upper()}",
            body=f"Sensor {sensor_id} velocidad {speed_str} mm/día",
            data=data,
            tokens=resolved_tokens,
            topic=topic,
        )


def _ensure_firebase() -> bool:
    global _firebase_ready
    if not _firebase_import_ok:
        return False
    if _firebase_ready:
        return True
    with _firebase_lock:
        if _firebase_ready:
            return True

        path = (config.FIREBASE_CREDENTIALS_PATH or "").strip()
        raw_json = (config.FIREBASE_CREDENTIALS_JSON or "").strip()
        info = None
        if not path and not raw_json:
            info = _service_account_from_env()
            if info is None:
                return False

        try:
            if path:
                cred = credentials.Certificate(path)
            else:
                if info is None:
                    info = json.loads(raw_json)
                cred = credentials.Certificate(info)
            firebase_admin.initialize_app(cred)
            _firebase_ready = True
            return True
        except Exception as exc:
            logger.warning("firebase_init_failed err=%s", exc)
            return False


def _service_account_from_env() -> Optional[Dict[str, Any]]:
    project_id = (config.FIREBASE_PROJECT_ID or "").strip()
    private_key_id = (config.FIREBASE_PRIVATE_KEY_ID or "").strip()
    private_key = (config.FIREBASE_PRIVATE_KEY or "").strip()
    client_email = (config.FIREBASE_CLIENT_EMAIL or "").strip()
    client_id = (config.FIREBASE_CLIENT_ID or "").strip()
    client_x509_cert_url = (config.FIREBASE_CLIENT_X509_CERT_URL or "").strip()

    if not project_id or not private_key_id or not private_key or not client_email or not client_id:
        return None

    private_key = private_key.replace("\\n", "\n")
    if "BEGIN PRIVATE KEY" not in private_key:
        return None

    if not client_x509_cert_url:
        client_x509_cert_url = f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email}"

    return {
        "type": "service_account",
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": client_x509_cert_url,
    }
