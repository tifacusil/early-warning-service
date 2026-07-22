import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pika

import config
from alert_publisher import AlertPublisher
from metrics import MESSAGES_PROCESSED, PERSIST_LATENCY
from redis_cache import RedisCache
from threshold_engine import evaluate_message


logger = logging.getLogger("early_warning.consumer")


def _connection_params() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
    return pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=60,
        connection_attempts=5,
        retry_delay=3,
    )


class RabbitConsumer(threading.Thread):
    def __init__(self, cache: RedisCache) -> None:
        super().__init__(name="rabbit-consumer", daemon=True)
        self._cache = cache
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._consume_once()
            except Exception as exc:
                logger.exception("consumer_crash err=%s", exc)
                time.sleep(2)

    def _consume_once(self) -> None:
        conn = pika.BlockingConnection(_connection_params())
        consume_channel = conn.channel()
        consume_channel.basic_qos(prefetch_count=20)

        publish_conn = pika.BlockingConnection(_connection_params())
        publish_channel = publish_conn.channel()
        publisher = AlertPublisher(publish_channel)

        def on_message(ch: Any, method: Any, properties: Any, body: bytes) -> None:
            delivery_tag = method.delivery_tag
            try:
                self._cache.set_last_message_processed_at(
                    datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                )
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict):
                    logger.warning("bad_message type=%s", type(payload))
                    ch.basic_ack(delivery_tag=delivery_tag)
                    return

                sensor_id = payload.get("sensor_id")
                previous: Optional[Dict[str, Any]] = None
                if isinstance(sensor_id, str) and sensor_id:
                    previous = self._cache.get_last_reading(sensor_id)

                result = evaluate_message(payload, previous)
                logger.info(
                    "eval sensor=%s project=%s ts=%s prev_ts=%s speed=%s dt=%s threshold=%s level=%s skip=%s note=%s",
                    result.log_fields.get("sensor_id"),
                    result.log_fields.get("project_id"),
                    result.log_fields.get("timestamp"),
                    result.log_fields.get("previous_timestamp"),
                    result.log_fields.get("deformation_speed_mm_day"),
                    result.log_fields.get("dt_seconds"),
                    result.log_fields.get("threshold_mm_day"),
                    result.log_fields.get("alert"),
                    result.log_fields.get("skip_reason"),
                    result.log_fields.get("note"),
                )

                if result.alert is not None:
                    try:
                        publisher.publish_rabbitmq(result.alert)
                    except Exception as exc:
                        logger.warning("rabbit_alert_publish_failed err=%s", exc)
                    publisher.publish_http(result.alert)
                    try:
                        tokens = self._cache.get_push_device_tokens(
                            str(result.alert.get("project_id") or ""),
                            str(result.alert.get("alert_level") or ""),
                        )
                        if not tokens:
                            logger.info(
                                "push_no_recipients project=%s level=%s",
                                str(result.alert.get("project_id") or ""),
                                str(result.alert.get("alert_level") or ""),
                            )
                        publisher.publish_push(result.alert, tokens=tokens)
                    except Exception as exc:
                        logger.warning("push_flow_failed err=%s", exc)
                    self._cache.incr_alerts_generated()
                    self._cache.push_recent_alert(result.alert)
                    try:
                        with PERSIST_LATENCY.time():
                            self._cache.record_alert_db(result.alert)
                    except Exception as exc:
                        logger.warning("db_record_failed err=%s", exc)

                if result.should_store_current and result.store_payload is not None:
                    sid = payload.get("sensor_id")
                    if isinstance(sid, str) and sid:
                        # Que no aterrice significa que la otra instancia ya
                        # guardo una lectura mas nueva de este sensor mientras
                        # evaluabamos; quedarse con la de ella es el punto (ver
                        # _BASELINE_CAS). Se loguea porque una tasa sostenida de
                        # esto es la senal de que el solapamiento entre
                        # instancias sobre un sensor se volvio habitual.
                        if not self._cache.set_last_reading(sid, result.store_payload):
                            logger.info("baseline_write_superseded sensor_id=%s", sid)

                ch.basic_ack(delivery_tag=delivery_tag)
                MESSAGES_PROCESSED.inc()
            except json.JSONDecodeError:
                logger.warning("json_decode_error body_len=%s", len(body))
                ch.basic_ack(delivery_tag=delivery_tag)
            except Exception as exc:
                logger.exception("process_failed err=%s", exc)
                ch.basic_nack(delivery_tag=delivery_tag, requeue=True)

        consume_channel.basic_consume(queue=config.QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
        logger.info("consumer_started queue=%s host=%s:%s", config.QUEUE_NAME, config.RABBITMQ_HOST, config.RABBITMQ_PORT)
        try:
            consume_channel.start_consuming()
        finally:
            try:
                if consume_channel.is_open:
                    consume_channel.close()
            finally:
                if conn.is_open:
                    conn.close()
            try:
                if publish_channel.is_open:
                    publish_channel.close()
            finally:
                if publish_conn.is_open:
                    publish_conn.close()
