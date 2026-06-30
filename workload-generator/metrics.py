"""Prometheus metrics, exposed for scraping over HTTP at /metrics."""
from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

OPERATIONS = Counter("workload_operations_total", "Operations executed", ["op"])
EVENTS = Counter("workload_events_total", "Change events produced")
ERRORS = Counter("workload_errors_total", "Failed operations")
OPERATION_SECONDS = Histogram("workload_operation_seconds", "Operation latency in seconds", ["op"])


def start_metrics_server(port: int) -> None:
    start_http_server(port)
