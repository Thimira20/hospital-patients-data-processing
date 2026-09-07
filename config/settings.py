"""
Single source of truth for every configuration value in the pipeline.

Every service (producers, Spark jobs, Airflow DAGs, the FastAPI app) imports
`get_settings()` from this module instead of reading `os.environ` directly or
hard-coding a number. This is what keeps "configuration management" honest:
change a value in `.env`, and it is picked up everywhere without touching code.

Usage:
    from config.settings import get_settings
    settings = get_settings()
    settings.vitals_interval_seconds
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- Simulated clock ----------------------------------------------
    sim_day_seconds: int = 300
    sim_start_date: str = "2026-01-01"

    # ---- Ingestion ------------------------------------------------------
    num_patients: int = 12
    vitals_interval_seconds: float = 2.0
    anomaly_probability: float = 0.05
    dirty_data_rate: float = 0.02
    malformed_rate: float = 0.005
    late_event_rate: float = 0.01
    late_event_max_delay_seconds: int = 40

    # ---- Kafka ------------------------------------------------------------
    kafka_bootstrap_internal: str = "kafka:9092"
    kafka_bootstrap_external: str = "localhost:29092"
    kafka_cluster_id: str = "e1HBZ7LpS_q5zP3Zpcr3TQ"

    topic_vitals_raw: str = "vitals.raw"
    topic_vitals_raw_partitions: int = 3
    topic_vitals_clean: str = "vitals.clean"
    topic_vitals_clean_partitions: int = 3
    topic_clinical_alerts: str = "clinical.alerts"
    topic_clinical_alerts_partitions: int = 1
    topic_vitals_dlq: str = "vitals.dlq"
    topic_vitals_dlq_partitions: int = 1

    # ---- Postgres -----------------------------------------------------
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db_vitals: str = "vitals"
    postgres_db_airflow: str = "airflow"

    # ---- Spark streaming ------------------------------------------------
    stream_trigger_seconds: int = 30
    stream_watermark_seconds: int = 45
    window_duration_minutes: int = 1
    window_slide_seconds: int = 30
    spark_shuffle_partitions: int = 4
    sustained_alert_windows: int = 2
    max_offsets_per_trigger: int = 2000

    # ---- Archiver ---------------------------------------------------------
    archiver_trigger_seconds: int = 30

    # ---- Serving API --------------------------------------------------
    api_port: int = 8000

    # ---- Observability --------------------------------------------------
    grafana_admin_user: str = "admin"
    grafana_admin_password: str = "admin"
    no_vitals_alert_minutes: int = 5
    stream_stalled_seconds: int = 120
    batch_late_minutes: int = 12

    # ---- Paths (container-internal) --------------------------------------
    data_root: str = "/data"
    landing_labs_dir: str = "/data/landing/labs"
    master_vitals_dir: str = "/data/master/vitals"
    checkpoint_stream_dir: str = "/data/checkpoints/stream"
    checkpoint_archiver_dir: str = "/data/checkpoints/archiver"
    reports_dir: str = "/data/reports"
    logs_dir: str = "/data/logs"
    quarantine_dir: str = "/data/quarantine"

    # ---- Derived helpers -------------------------------------------------
    @property
    def postgres_dsn_vitals(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db_vitals}"
        )

    @property
    def postgres_jdbc_url_vitals(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}" f"/{self.postgres_db_vitals}"

    @property
    def window_duration_str(self) -> str:
        return f"{self.window_duration_minutes} minute"

    @property
    def window_slide_str(self) -> str:
        return f"{self.window_slide_seconds} seconds"

    @property
    def watermark_str(self) -> str:
        return f"{self.stream_watermark_seconds} seconds"


@lru_cache
def get_settings() -> Settings:
    """Cached singleton — construct once per process."""
    return Settings()
