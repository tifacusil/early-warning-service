# Modelo de destinatarios — Entregable concreto para 3A2

**Para:** Persona A (Early Warning, 3A2)
**De:** Persona B (User Management, 3B1)
**Propósito:** desbloquear el envío de SMS/push antes de que 3B1 esté terminado. Persona A puede arrancar contra un stub que respete este contrato; cuando 3B1 esté listo, solo cambia la URL.

---

## 1. Esquema SQL (V15)

Tres tablas nuevas + expansión del esqueleto de `users` (que existe desde Wave 1).

```sql
-- ============================================================
-- V15__create_user_management_tables.sql
-- ============================================================

-- 1.1 Expansión de users (esqueleto desde V1 de Wave 1)
ALTER TABLE users
  ADD COLUMN phone_e164    VARCHAR(20)  NULL       AFTER email,
  ADD COLUMN password_hash VARCHAR(255) NOT NULL   AFTER phone_e164,
  ADD COLUMN role          ENUM('admin','operator','viewer') NOT NULL DEFAULT 'viewer' AFTER password_hash,
  ADD COLUMN status        ENUM('active','inactive') NOT NULL DEFAULT 'active' AFTER role,
  ADD INDEX idx_users_status (status),
  ADD INDEX idx_users_role   (role);

-- 1.2 Suscripciones: qué usuario recibe alertas de qué proyecto y con qué umbral
CREATE TABLE user_project_alert_subscription (
  id                BIGINT       PRIMARY KEY AUTO_INCREMENT,
  user_id           BIGINT       NOT NULL,
  project_id        VARCHAR(64)  NOT NULL,
  min_alert_level   ENUM('blue','orange','red') NOT NULL DEFAULT 'blue',
  sms_enabled       BOOLEAN      NOT NULL DEFAULT TRUE,
  push_enabled      BOOLEAN      NOT NULL DEFAULT TRUE,
  email_enabled     BOOLEAN      NOT NULL DEFAULT FALSE,
  quiet_hours_start TIME         NULL,
  quiet_hours_end   TIME         NULL,
  created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

  UNIQUE KEY uk_user_project (user_id, project_id),
  FOREIGN KEY (user_id)    REFERENCES users    (id)         ON DELETE CASCADE,
  FOREIGN KEY (project_id) REFERENCES projects (project_id) ON DELETE CASCADE,

  INDEX idx_subs_project_level (project_id, min_alert_level)
);

-- 1.3 Dispositivos para push notification (FCM)
CREATE TABLE user_device (
  id              BIGINT       PRIMARY KEY AUTO_INCREMENT,
  user_id         BIGINT       NOT NULL,
  device_token    VARCHAR(512) NOT NULL,
  platform        ENUM('android','ios') NOT NULL,
  app_version     VARCHAR(32)  NULL,
  last_seen_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status          ENUM('active','revoked') NOT NULL DEFAULT 'active',
  created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

  UNIQUE KEY uk_device_token (device_token),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  INDEX idx_user_active (user_id, status)
);
```

### 1.1 Decisiones del esquema (referencia a Bai)

| Campo | Decisión | Fuente en Bai |
|---|---|---|
| `min_alert_level` enum 3 valores | blue/orange/red | Tablas 4 y 5: 3 niveles para Shangtan y Lingzhan |
| Suscripción **por proyecto** (no por rol) | tabla `user_project_alert_subscription` con UNIQUE(user_id, project_id) | §2.4: "operador solo ve proyectos asignados" |
| `phone_e164` en users | formato E.164 internacional | §2.4 menciona SMS como mecanismo explícito |
| `user_device` 1:N con `users` | FCM tokens separados | §2.5 + Figura 4: app Flutter iOS + Android |
| 3 roles (admin/operator/viewer) | enum cerrado | Figura 3: UI distingue gestión, operación, consulta |

---

## 2. Cómo se pobla la BD (seed)

Dos vías: **seed de arranque** (datos de prueba para desarrollo) y **flujo en producción** (registro vía API).

### 2.1 Seed de arranque para desarrollo y tests

`V16__seed_user_management.sql` (solo si `APP_SEED_ENABLED=true`, mismo patrón que Project Management):

```sql
-- ============================================================
-- V16__seed_user_management.sql
-- Solo se ejecuta si las tablas están vacías.
-- ============================================================

-- 2.1.1 Usuarios base
INSERT INTO users (username, email, phone_e164, password_hash, role, status) VALUES
  ('admin',              'admin@landslide.test',    '+51999111111',
   '$2a$10$placeholderHashReplaceInRuntime',         'admin',    'active'),

  ('operador_shangtan',  'op.shangtan@landslide.test','+51999222222',
   '$2a$10$placeholderHashReplaceInRuntime',         'operator', 'active'),

  ('operador_lingzhan',  'op.lingzhan@landslide.test','+51999333333',
   '$2a$10$placeholderHashReplaceInRuntime',         'operator', 'active'),

  ('supervisor',         'sup@landslide.test',       '+51999444444',
   '$2a$10$placeholderHashReplaceInRuntime',         'admin',    'active'),

  ('autoridad_regional', 'autoridad@gob.test',       '+51999555555',
   '$2a$10$placeholderHashReplaceInRuntime',         'viewer',   'active');

-- 2.1.2 Suscripciones (asume project_id 'shangtan' y 'lingzhan' ya sembrados por Project Management)

-- admin: recibe todo de ambos proyectos, sin quiet hours
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled, email_enabled)
SELECT id, 'shangtan', 'blue',   TRUE, TRUE, TRUE FROM users WHERE username='admin';
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled, email_enabled)
SELECT id, 'lingzhan', 'blue',   TRUE, TRUE, TRUE FROM users WHERE username='admin';

-- operador_shangtan: solo Shangtan, todos los niveles, con quiet hours nocturnos
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled,
   quiet_hours_start, quiet_hours_end)
SELECT id, 'shangtan', 'blue', TRUE, TRUE, '22:00', '06:00'
FROM users WHERE username='operador_shangtan';

-- operador_lingzhan: solo Lingzhan, todos los niveles, sin quiet hours
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled)
SELECT id, 'lingzhan', 'blue', TRUE, TRUE
FROM users WHERE username='operador_lingzhan';

-- supervisor: ambos proyectos pero solo Naranja+ (no quiere ruido de Azul)
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled)
SELECT id, 'shangtan', 'orange', TRUE, TRUE FROM users WHERE username='supervisor';
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled)
SELECT id, 'lingzhan', 'orange', TRUE, TRUE FROM users WHERE username='supervisor';

-- autoridad_regional: ambos proyectos, solo Roja (solo crisis), SMS sí pero push no
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled, email_enabled)
SELECT id, 'shangtan', 'red', TRUE, FALSE, TRUE FROM users WHERE username='autoridad_regional';
INSERT INTO user_project_alert_subscription
  (user_id, project_id, min_alert_level, sms_enabled, push_enabled, email_enabled)
SELECT id, 'lingzhan', 'red', TRUE, FALSE, TRUE FROM users WHERE username='autoridad_regional';

-- 2.1.3 Dispositivos (tokens FCM falsos para desarrollo)
INSERT INTO user_device (user_id, device_token, platform, app_version)
SELECT id, 'fake_fcm_token_admin_android',     'android', '1.0.0'
FROM users WHERE username='admin';

INSERT INTO user_device (user_id, device_token, platform, app_version)
SELECT id, 'fake_fcm_token_shangtan_android',  'android', '1.0.0'
FROM users WHERE username='operador_shangtan';

INSERT INTO user_device (user_id, device_token, platform, app_version)
SELECT id, 'fake_fcm_token_lingzhan_ios',      'ios',     '1.0.0'
FROM users WHERE username='operador_lingzhan';

INSERT INTO user_device (user_id, device_token, platform, app_version)
SELECT id, 'fake_fcm_token_supervisor_ios',    'ios',     '1.0.0'
FROM users WHERE username='supervisor';
```

**Por qué este seed sirve para 3A2:**

- Cubre los **dos proyectos del paper** (Shangtan y Lingzhan).
- Cubre los **tres niveles de alerta** del paper (blue/orange/red).
- Incluye un usuario por cada **canal** (admin con todo, viewer solo SMS+email sin push).
- Incluye **quiet hours** activas en un operador, para probar la regla "Rojo rompe quiet hours".
- Incluye **dispositivos en ambas plataformas** (android e ios) para probar el flujo FCM con cada uno.

### 2.2 Flujo de poblamiento en producción

```
┌──────────────┐    1) POST /auth/register     ┌──────────────────┐
│ Admin web UI │ ───────────────────────────▶ │ User Management  │
└──────────────┘    {username,email,phone,…}  │  → INSERT users  │
                                              └──────────────────┘

┌──────────────┐    2) POST /users/{id}/subscriptions
│ Admin web UI │ ───────────────────────────▶  → INSERT subscription
└──────────────┘    {project_id, min_alert_level, channels}

┌──────────────┐    3) POST /auth/login → JWT
│  App Flutter │ ◀──────────────────────────  → token + user_id
└──────────────┘

┌──────────────┐    4) POST /devices/register
│  App Flutter │ ───────────────────────────▶  → INSERT user_device
└──────────────┘    {fcm_token, platform}      (token obtenido de FCM
                                                 SDK al instalar la app)
```

**Operaciones que User Management expone (mínimas para 3A2):**

| Método | Path | Quién llama | Qué hace |
|---|---|---|---|
| POST | `/api/v1/auth/register` | Admin UI | Crea usuario |
| POST | `/api/v1/auth/login` | App + Web | Devuelve JWT |
| POST | `/api/v1/users/{user_id}/subscriptions` | Admin UI | Suscribe usuario a proyecto con nivel |
| POST | `/api/v1/devices/register` | App Flutter | Registra FCM token al login |
| **GET** | **`/api/v1/projects/{project_id}/alert-recipients?level=X`** | **Early Warning** | **Lista destinatarios — único endpoint que 3A2 usa** |
| POST | `/api/v1/devices/{token}/revoke` | Early Warning | Marca token muerto tras error de FCM |

---

## 3. El endpoint que 3A2 consume

Este es el único contrato que importa a Persona A. Todo lo demás puede cambiar.

### 3.1 Request

```http
GET /api/v1/projects/shangtan/alert-recipients?level=orange
Authorization: Bearer <service_jwt_for_early_warning>
```

### 3.2 Response (200 OK)

```json
{
  "project_id": "shangtan",
  "alert_level": "orange",
  "recipients": [
    {
      "user_id": 1,
      "username": "admin",
      "role": "admin",
      "channels": {
        "sms": {
          "enabled": true,
          "phone_e164": "+51999111111",
          "suppressed_by_quiet_hours": false
        },
        "push": {
          "enabled": true,
          "devices": [
            { "token": "fake_fcm_token_admin_android", "platform": "android" }
          ]
        },
        "email": {
          "enabled": true,
          "address": "admin@landslide.test"
        }
      }
    },
    {
      "user_id": 2,
      "username": "operador_shangtan",
      "role": "operator",
      "channels": {
        "sms": {
          "enabled": true,
          "phone_e164": "+51999222222",
          "suppressed_by_quiet_hours": true
        },
        "push": {
          "enabled": true,
          "devices": [
            { "token": "fake_fcm_token_shangtan_android", "platform": "android" }
          ]
        },
        "email": { "enabled": false, "address": null }
      }
    },
    {
      "user_id": 4,
      "username": "supervisor",
      "role": "admin",
      "channels": {
        "sms": { "enabled": true, "phone_e164": "+51999444444", "suppressed_by_quiet_hours": false },
        "push": {
          "enabled": true,
          "devices": [
            { "token": "fake_fcm_token_supervisor_ios", "platform": "ios" }
          ]
        },
        "email": { "enabled": false, "address": null }
      }
    }
  ],
  "generated_at": "2026-05-17T14:30:00Z"
}
```

Nótese: `autoridad_regional` no aparece porque su `min_alert_level='red'` y la consulta es `level=orange`.

### 3.3 Reglas que User Management aplica antes de responder

1. Filtra usuarios con `status='active'`.
2. Filtra suscripciones donde `min_alert_level <= level` (orden blue < orange < red).
3. Para SMS: si `quiet_hours` están activas en el momento de la consulta Y el `level != 'red'`, marca `suppressed_by_quiet_hours=true`. **Para `level='red'`, el campo siempre es `false`** — el paper trata las alertas Rojas (≥20 mm/día) como crisis inminente; no se suprimen nunca.
4. Para push: solo dispositivos con `status='active'`.
5. Si la lista de destinatarios queda vacía, retorna `recipients: []` con HTTP 200 (no es error).

### 3.4 Decisión que toma Early Warning, no User Management

User Management **informa** si SMS está en quiet hours pero no decide. Early Warning aplica:

```
si suppressed_by_quiet_hours → no enviar SMS, pero igual enviar push si está enabled
si push enabled              → enviar a todos los devices del usuario
si email enabled             → enviar email
log de cada intento en tabla `alerts` (definida por Persona A)
```

---

## 4. Stub para que Persona A arranque ya

Mientras Persona B implementa 3B1, Persona A puede correr este stub en local:

```python
# stub_user_management.py — corre en :8086 para que Early Warning lo consuma
from fastapi import FastAPI
from datetime import datetime, time
app = FastAPI()

FAKE_DATA = {
    "shangtan": [
        {"user_id": 1, "username": "admin", "role": "admin",
         "phone_e164": "+51999111111",
         "min_alert_level": "blue", "sms_enabled": True, "push_enabled": True,
         "email_enabled": True, "email": "admin@landslide.test",
         "quiet_hours": None,
         "devices": [{"token": "fake_fcm_token_admin_android", "platform": "android"}]},
        {"user_id": 2, "username": "operador_shangtan", "role": "operator",
         "phone_e164": "+51999222222",
         "min_alert_level": "blue", "sms_enabled": True, "push_enabled": True,
         "email_enabled": False, "email": None,
         "quiet_hours": (time(22,0), time(6,0)),
         "devices": [{"token": "fake_fcm_token_shangtan_android", "platform": "android"}]},
    ],
    "lingzhan": [
        {"user_id": 3, "username": "operador_lingzhan", "role": "operator",
         "phone_e164": "+51999333333",
         "min_alert_level": "blue", "sms_enabled": True, "push_enabled": True,
         "email_enabled": False, "email": None,
         "quiet_hours": None,
         "devices": [{"token": "fake_fcm_token_lingzhan_ios", "platform": "ios"}]},
    ],
}

LEVEL_ORDER = {"blue": 1, "orange": 2, "red": 3}

def in_quiet_hours(qh, now):
    if not qh: return False
    start, end = qh
    if start <= end:  return start <= now <= end
    return now >= start or now <= end  # rango cruza medianoche

@app.get("/api/v1/projects/{project_id}/alert-recipients")
def recipients(project_id: str, level: str):
    now = datetime.now().time()
    out = []
    for u in FAKE_DATA.get(project_id, []):
        if LEVEL_ORDER[u["min_alert_level"]] > LEVEL_ORDER[level]:
            continue
        suppressed = level != "red" and in_quiet_hours(u["quiet_hours"], now)
        out.append({
            "user_id": u["user_id"], "username": u["username"], "role": u["role"],
            "channels": {
                "sms":   {"enabled": u["sms_enabled"], "phone_e164": u["phone_e164"],
                          "suppressed_by_quiet_hours": suppressed},
                "push":  {"enabled": u["push_enabled"], "devices": u["devices"]},
                "email": {"enabled": u["email_enabled"], "address": u["email"]},
            }
        })
    return {"project_id": project_id, "alert_level": level,
            "recipients": out, "generated_at": datetime.utcnow().isoformat()+"Z"}
```

Persona A apunta Early Warning a `http://localhost:8086`. Cuando 3B1 esté listo, cambia la URL a la del servicio real. Cero retrabajo.

---

## 5. Resumen del entregable

| Archivo | Owner | Propósito |
|---|---|---|
| `db/migrations/V15__create_user_management_tables.sql` | Persona B | Schema productivo |
| `db/migrations/V16__seed_user_management.sql` | Persona B | Datos iniciales (admin + operadores de cada proyecto) |
| `stub_user_management.py` | Persona A (temporal) | Desbloquea 3A2 hasta que 3B1 esté listo |
| Contrato `/alert-recipients` documentado arriba | Ambos | Único punto de integración entre los dos servicios |
