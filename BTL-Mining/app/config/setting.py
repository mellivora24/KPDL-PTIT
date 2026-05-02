from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    db_server: str = os.getenv("DM_DB_SERVER", "localhost")
    db_name: str = os.getenv("DM_DB_NAME", "DATA_MINING")
    db_trusted_connection: bool = os.getenv("DM_DB_TRUSTED", "true").lower() in {"1", "true", "yes", "y"}
    db_user: str = os.getenv("DM_DB_USER", "sa")
    db_password: str = os.getenv("DM_DB_PASSWORD", "123456")
    db_driver: str = os.getenv("DM_DB_DRIVER", "ODBC Driver 17 for SQL Server")
    model_path: Path = Path(os.getenv("DM_MODEL_PATH", BASE_DIR / "model" / "svm_pipeline.joblib"))
    metrics_path: Path = Path(os.getenv("DM_METRICS_PATH", BASE_DIR / "model" / "svm_metrics.json"))
    raw_data_path: Path = Path(os.getenv("DM_RAW_DATA_PATH", BASE_DIR / "data" / "raw" / "german.data"))
    random_state: int = int(os.getenv("DM_RANDOM_STATE", "42"))
    test_size: float = float(os.getenv("DM_TEST_SIZE", "0.2"))
    auto_retrain_enabled: bool = os.getenv("DM_AUTO_RETRAIN_ENABLED", "true").lower() in {"1", "true", "yes", "y"}
    auto_retrain_interval_minutes: int = int(os.getenv("DM_AUTO_RETRAIN_INTERVAL_MIN", "10"))
    auto_retrain_min_new_feedback: int = int(os.getenv("DM_AUTO_RETRAIN_MIN_NEW_FEEDBACK", "30"))
    auto_retrain_state_path: Path = Path(
        os.getenv("DM_AUTO_RETRAIN_STATE_PATH", BASE_DIR / "model" / "auto_retrain_state.json")
    )

    def _auth_part(self) -> str:
        if self.db_trusted_connection:
            return "Trusted_Connection=yes;"
        return f"UID={self.db_user};PWD={self.db_password};"

    @property
    def connection_string(self) -> str:
        return (
            f"DRIVER={{{self.db_driver}}};"
            f"SERVER={self.db_server};"
            f"DATABASE={self.db_name};"
            f"{self._auth_part()}"
            "TrustServerCertificate=yes;"
        )

    @property
    def master_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.db_driver}}};"
            f"SERVER={self.db_server};"
            "DATABASE=master;"
            f"{self._auth_part()}"
            "TrustServerCertificate=yes;"
        )


def get_settings() -> Settings:
    return Settings()
