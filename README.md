# Early Warning Service

Microservice responsible for GNSS deformation-speed evaluation, alert generation, push notifications (FCM), and periodic report generation in the **Landslide Monitoring System** (Bai et al., 2020).

- **Port:** `8083`
- **Stack:** Python 3.10+ · Flask · pika (RabbitMQ) · redis · MySQL (PyMySQL) · Firebase Admin (FCM)

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Running the Service](#running-the-service)
- [API Reference](#api-reference)
- [Running Tests](#running-tests)
- [Build](#build)
- [Architecture Notes](#architecture-notes)

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10+ | Recommended: virtualenv/venv |
| RabbitMQ | 3.x | Queue `early_warning.evaluate` must already exist (service does not declare it) |
| Redis | 6.x+ | Used for last-reading cache, recent alerts, counters |
| MySQL | 8.x | Uses `landslide_monitoring`; `users/subscriptions/devices` tables must exist |
| Firebase | — | FCM via `firebase-admin` (service account required) |

---

## Getting Started

```bash
# Enter project
cd early-warning-demo

# Create virtual environment
python -m venv venv

# Activate (Linux/macOS)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Configuration

All configuration is driven by environment variables. The service reads a `.env` file from the working directory at startup (via `python-dotenv`), then falls back to real environment variables.

### RabbitMQ / Redis

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_HOST` | `204.168.203.32` | RabbitMQ host |
| `RABBITMQ_PORT` | `5672` | RabbitMQ port |
| `RABBITMQ_USER` | `subscriber` | RabbitMQ username |
| `RABBITMQ_PASS` | `""` | RabbitMQ password |
| `QUEUE_NAME` | `early_warning.evaluate` | Existing durable queue (service does not declare it) |
| `ALERTS_EXCHANGE` | `sensors.raw` | Exchange used to publish alerts |
| `REDIS_HOST` | `204.168.203.32` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASS` | `""` | Redis password |

### MySQL (Recipients + Storage)

| Variable | Default | Description |
|---|---|---|
| `MYSQL_HOST` | `204.168.203.32` | MySQL host |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_USER` | `landslide` | MySQL username |
| `MYSQL_PASS` | `""` | MySQL password |
| `MYSQL_DB` | `landslide_monitoring` | Database name |
| `MYSQL_CONNECT_TIMEOUT_SECONDS` | `5` | Connection timeout |

> **Recipients data model:** the service reads `users`, `user_project_alert_subscription`, and `user_device` following `modelo-destinatarios-3A2.md`.

### Firebase (Push)

Provide service account credentials using exactly one method:

| Method | Variables |
|---|---|
| File path | `FIREBASE_CREDENTIALS_PATH=/path/service-account.json` |
| JSON string | `FIREBASE_CREDENTIALS_JSON={...}` |
| Split env vars | `FIREBASE_PROJECT_ID`, `FIREBASE_PRIVATE_KEY_ID`, `FIREBASE_PRIVATE_KEY`, `FIREBASE_CLIENT_EMAIL`, `FIREBASE_CLIENT_ID` (optional `FIREBASE_CLIENT_X509_CERT_URL`) |

Optional routing:

| Variable | Default | Description |
|---|---|---|
| `FIREBASE_TOPIC` | `""` | If set, sends notifications to this topic |
| `FIREBASE_DEVICE_TOKENS` | `""` | Comma-separated tokens fallback (used only if no tokens are provided by DB) |

### HTTP / Service

| Variable | Default | Description |
|---|---|---|
| `FLASK_PORT` | `8083` | HTTP port |
| `MAX_DT_SECONDS` | `7200` | Max allowed Δt between GNSS readings before baseline reset |
| `ALERT_HTTP_URL` | `""` | Optional HTTP endpoint to POST alerts |
| `HTTP_TIMEOUT_SECONDS` | `5` | HTTP publish timeout |

### Reports

| Variable | Default | Description |
|---|---|---|
| `REPORT_SCHEDULER_ENABLED` | `true` | Enables background report scheduler |
| `REPORT_CHECK_INTERVAL_SECONDS` | `60` | Scheduler loop interval |
| `REPORT_PUSH_ENABLED` | `true` | Sends a push notification when a report is generated |
| `DATA_ANALYSIS_URL` | `""` | Optional Data Analysis endpoint to enrich reports |
| `DATA_ANALYSIS_TIMEOUT_SECONDS` | `5` | Data Analysis call timeout |

---

## Running the Service

```bash
# Reads .env automatically
python app.py
```

The service listens on `http://0.0.0.0:8083` (see `FLASK_PORT`).

---

## API Reference

All endpoints are public by default (no auth layer in this service).

---

### Health

#### `GET /health`

**Response `200`**
```json
{
  "status": "healthy",
  "service": "early-warning",
  "alerts_generated": 12
}
```

---

### Alerts

#### `GET /alerts/recent?limit=10`

Returns the last generated alerts (stored in Redis).

**Response `200`**
```json
{
  "alerts": [
    {
      "schema_version": "1.0",
      "message_id": "uuid",
      "project_id": "shangtan",
      "sensor_id": "GXZM-DB03",
      "alert_type": "deformation_speed",
      "alert_level": "orange",
      "deformation_speed_mm_day": 16.8,
      "current_displacement_mm": 1250.0,
      "previous_displacement_mm": 1247.5,
      "threshold_mm_day": 15,
      "timestamp": "2026-05-09T14:30:00Z"
    }
  ]
}
```

---

### Reports

#### `GET /reports/latest?project_id={id}&period={daily|weekly}`

Returns the most recent stored report for a project and period.

**Response `200`** (shape)
```json
{
  "schema_version": "1.0",
  "report_id": "uuid",
  "project_id": "shangtan",
  "period": "daily",
  "window_start": "2026-05-23T00:00:00Z",
  "window_end": "2026-05-24T00:00:00Z",
  "generated_at": "2026-05-24T00:05:12Z",
  "summary": {
    "total": 3,
    "by_level": { "blue": 1, "orange": 1, "red": 1 },
    "max_speed_mm_day": 48.0,
    "avg_speed_mm_day": 22.4
  },
  "system": {
    "service": "early-warning",
    "alerts_generated_total": 12,
    "last_message_processed_at": "2026-05-24T17:16:14Z"
  },
  "data_analysis": {}
}
```

**Response `404`** — when no report exists yet.

---

#### `POST /reports/generate?project_id={id}&period={daily|weekly}&push={true|false}`

Generates the latest complete window report (daily: previous UTC day, weekly: previous ISO week), stores it in MySQL, and optionally sends a push notification.

**Response `200`** — same shape as `/reports/latest`.

---

### OpenAPI

#### `GET /openapi.json`

Returns the OpenAPI specification.

---

#### `GET /docs`

Swagger UI rendering using `/openapi.json`.

---

## Running Tests

No automated test suite is included yet.

---

## Build

This service is deployed as source + virtualenv (Option A).

```bash
pip install -r requirements.txt
python app.py
```

---

## Architecture Notes

- **Queue is not declared** — `early_warning.evaluate` must exist already; the consumer uses manual acks.
- **Edge cases guarded** — rejects duplicate/out-of-order timestamps and resets baseline on large Δt (`MAX_DT_SECONDS`).
- **Recipients are external** — push recipients are resolved from MySQL tables (`users/subscriptions/devices`); this service does not seed them.
- **Storage**
  - MySQL: creates and writes `alerts` and `reports` tables if missing.
  - Redis: last reading cache, recent alerts, counters, and operational timestamps.
- **RabbitMQ publishing permissions** — the RabbitMQ user must have write access to exchange `sensors.raw` to publish alerts.

