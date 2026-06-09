-- ============================================================
-- Base de datos: early_warning_db
-- Servicio: early-warning-demo (Flask + PyMySQL)
-- Tablas: alerts, reports
-- ============================================================

CREATE DATABASE IF NOT EXISTS early_warning_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE early_warning_db;

-- Tabla de alertas tempranas
CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    created_at DATETIME NOT NULL,
    message_id VARCHAR(64),
    project_id VARCHAR(64) NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    alert_level ENUM('blue','orange','red') NOT NULL,
    deformation_speed_mm_day DOUBLE,
    threshold_mm_day DOUBLE,
    alert_timestamp DATETIME
);

-- Tabla de reportes periodicos
CREATE TABLE IF NOT EXISTS reports (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    report_id VARCHAR(64) NOT NULL,
    project_id VARCHAR(64) NOT NULL,
    period ENUM('daily','weekly') NOT NULL,
    window_start DATETIME NOT NULL,
    window_end DATETIME NOT NULL,
    generated_at DATETIME NOT NULL,
    content_json JSON NOT NULL,
    UNIQUE KEY uk_report (project_id, period, window_start, window_end),
    INDEX idx_reports_project_period (project_id, period, generated_at)
);
