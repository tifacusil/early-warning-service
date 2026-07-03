"""Instrumentacion Prometheus para la tesis (early-warning).

Metricas custom:
- ew_messages_processed_total: throughput (mensajes consumidos con exito).
- ew_persist_latency_seconds: latencia de persistencia MySQL (histogram -> histogram_quantile p50/p90/p95/p99).

CPU y memoria residente los expone prometheus_client automaticamente via el
ProcessCollector por defecto (process_cpu_seconds_total, process_resident_memory_bytes).
El endpoint /metrics lo monta prometheus_flask_exporter en app.py.
"""
from prometheus_client import Counter, Histogram

MESSAGES_PROCESSED = Counter(
    "ew_messages_processed",
    "Mensajes consumidos y procesados con exito",
)

PERSIST_LATENCY = Histogram(
    "ew_persist_latency_seconds",
    "Latencia de persistencia MySQL por mensaje",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
