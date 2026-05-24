import os


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "204.168.203.32")
RABBITMQ_PORT = _env_int("RABBITMQ_PORT", 5672)
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "subscriber")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "")
QUEUE_NAME = os.getenv("QUEUE_NAME", "early_warning.evaluate")
ALERTS_EXCHANGE = os.getenv("ALERTS_EXCHANGE", "sensors.raw")

REDIS_HOST = os.getenv("REDIS_HOST", "204.168.203.32")
REDIS_PORT = _env_int("REDIS_PORT", 6379)
REDIS_PASS = os.getenv("REDIS_PASS", "")

FLASK_PORT = _env_int("FLASK_PORT", 8083)

MAX_DT_SECONDS = _env_int("MAX_DT_SECONDS", 7200)

ALERT_HTTP_URL = os.getenv("ALERT_HTTP_URL", "")
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "5"))

THRESHOLDS = {
    "shangtan": {"red": 20, "orange": 15, "blue": 10},
    "lingzhan": {"red": 15, "orange": 10, "blue": 5},
}

