"""Runtime configuration for the workload generator, sourced from the environment."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    database_url: str
    workers: int
    target_rate: float          # aggregate operations per second across all workers
    duration_seconds: float     # 0 runs until interrupted
    insert_weight: float
    update_weight: float
    delete_weight: float
    id_offset: int              # generated rows use ids at or above this, clear of the seeded data
    seed: int
    metrics_port: int
    burst: bool                 # when true, bypass the rate limiter and push as fast as possible
    anomaly_rate: float         # fraction of line-items priced above list, as injected anomalies

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            database_url=os.getenv(
                "DATABASE_URL", "postgresql://cdc:cdc_pw@aqueduct-postgres:5432/yami"
            ),
            workers=int(os.getenv("WORKERS", "8")),
            target_rate=float(os.getenv("TARGET_RATE", "50")),
            duration_seconds=float(os.getenv("DURATION_SECONDS", "0")),
            insert_weight=float(os.getenv("INSERT_WEIGHT", "70")),
            update_weight=float(os.getenv("UPDATE_WEIGHT", "25")),
            delete_weight=float(os.getenv("DELETE_WEIGHT", "5")),
            id_offset=int(os.getenv("ID_OFFSET", "900000000")),
            seed=int(os.getenv("SEED", "42")),
            metrics_port=int(os.getenv("METRICS_PORT", "9100")),
            burst=os.getenv("BURST", "0").lower() in ("1", "true", "yes"),
            anomaly_rate=float(os.getenv("ANOMALY_RATE", "0.02")),
        )
