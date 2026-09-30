"""Central place to read configuration from environment variables.

Every component (simulators, streaming job, Airflow DAG, API) imports this
instead of calling os.environ directly, so there is exactly one definition
of each setting and its default.
"""
import os


def _env(name: str, default=None):
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# ---- Kafka ----
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC_TRIPS = _env("KAFKA_TOPIC_TRIPS", "trip-events")

# ---- Postgres (app database) ----
POSTGRES_HOST = _env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = _env_int("POSTGRES_PORT", 5432)
POSTGRES_DB = _env("POSTGRES_DB", "fleet")
POSTGRES_USER = _env("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = _env("POSTGRES_PASSWORD", "fleet_pw")

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


def postgres_dsn() -> str:
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )


def sqlalchemy_url() -> str:
    return (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


# ---- Simulated clock ----
SIM_BASE_DATE = _env("SIM_BASE_DATE", "2026-01-01")
SIM_DAY_REAL_SECONDS = _env_int("SIM_DAY_REAL_SECONDS", 300)
SIM_EMIT_INTERVAL_SECONDS = _env_float("SIM_EMIT_INTERVAL_SECONDS", 2)
SIM_VEHICLE_COUNT = _env_int("SIM_VEHICLE_COUNT", 40)
SIM_DRIVER_COUNT = _env_int("SIM_DRIVER_COUNT", 40)
SIM_MESSY_RATE = _env_float("SIM_MESSY_RATE", 0.06)

# ---- Business rules ----
IDLE_ALERT_SIM_HOURS = _env_float("IDLE_ALERT_SIM_HOURS", 2)
NO_DATA_ALERT_SECONDS = _env_int("NO_DATA_ALERT_SECONDS", 120)
INVALID_RATE_ALERT_THRESHOLD = _env_float("INVALID_RATE_ALERT_THRESHOLD", 0.05)

# ---- Data volume ----
DATA_DIR = _env("DATA_DIR", "./data")
PARQUET_DIR = f"{DATA_DIR}/parquet"
QUARANTINE_DIR = f"{DATA_DIR}/quarantine"
LANDING_DIR = f"{DATA_DIR}/landing"
REPORTS_DIR = f"{DATA_DIR}/reports"
CHECKPOINT_DIR = f"{DATA_DIR}/checkpoints"
