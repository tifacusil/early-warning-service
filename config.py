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

FIREBASE_CREDENTIALS_PATH = os.getenv("FIREBASE_CREDENTIALS_PATH", "")
FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_CREDENTIALS_JSON", "")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "")
FIREBASE_PRIVATE_KEY_ID = os.getenv("FIREBASE_PRIVATE_KEY_ID", "")
FIREBASE_PRIVATE_KEY = os.getenv("FIREBASE_PRIVATE_KEY", "")
FIREBASE_CLIENT_EMAIL = os.getenv("FIREBASE_CLIENT_EMAIL", "")
FIREBASE_CLIENT_ID = os.getenv("FIREBASE_CLIENT_ID", "")
FIREBASE_CLIENT_X509_CERT_URL = os.getenv("FIREBASE_CLIENT_X509_CERT_URL", "")
FIREBASE_TOPIC = os.getenv("FIREBASE_TOPIC", "")
FIREBASE_DEVICE_TOKENS = os.getenv("FIREBASE_DEVICE_TOKENS", "")

MYSQL_HOST = os.getenv("MYSQL_HOST", "204.168.203.32")
MYSQL_PORT = _env_int("MYSQL_PORT", 3306)
MYSQL_USER = os.getenv("MYSQL_USER", "landslide")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")
MYSQL_DB = os.getenv("MYSQL_DB", "landslide_monitoring")
MYSQL_CONNECT_TIMEOUT_SECONDS = float(os.getenv("MYSQL_CONNECT_TIMEOUT_SECONDS", "5"))

DATA_ANALYSIS_URL = os.getenv("DATA_ANALYSIS_URL", "")
DATA_ANALYSIS_TIMEOUT_SECONDS = float(os.getenv("DATA_ANALYSIS_TIMEOUT_SECONDS", "5"))

REPORT_SCHEDULER_ENABLED = os.getenv("REPORT_SCHEDULER_ENABLED", "true").lower() == "true"
REPORT_CHECK_INTERVAL_SECONDS = _env_int("REPORT_CHECK_INTERVAL_SECONDS", 60)
REPORT_PUSH_ENABLED = os.getenv("REPORT_PUSH_ENABLED", "true").lower() == "true"

THRESHOLDS = {
    "shangtan": {"red": 20, "orange": 15, "blue": 10},
    "lingzhan": {"red": 15, "orange": 10, "blue": 5},
}
