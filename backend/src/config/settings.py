import os
from pathlib import Path
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////tmp/stratum-hidden.sqlite")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
JWT_SECRET = os.getenv("JWT_SECRET", "stratum-hidden-development-secret-change-this")
SEED_DEMO = os.getenv("SEED_DEMO", "true").lower() == "true"
TASK_MODE = os.getenv("TASK_MODE", "redis")
ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", "/tmp/stratum-hidden-artifacts"))
# A queued/running run whose lease expired is treated as interrupted and can be
# resumed in place. The lease must comfortably exceed one decode's runtime.
RUN_LEASE_SECONDS = int(os.getenv("RUN_LEASE_SECONDS", "180"))
# How often the worker scans for expired leases to re-enqueue recoverable runs.
RECOVERY_INTERVAL_SECONDS = int(os.getenv("RECOVERY_INTERVAL_SECONDS", "15"))
