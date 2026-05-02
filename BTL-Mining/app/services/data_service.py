from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.models.entities import ALL_FEATURE_COLUMNS

RAW_COLUMNS_WITH_TARGET = ALL_FEATURE_COLUMNS + ["target"]


class DataService:
    def __init__(self, raw_data_path: Path) -> None:
        self.raw_data_path = raw_data_path

    def load_raw_dataset(self) -> pd.DataFrame:
        df = pd.read_csv(self.raw_data_path, sep=r"\s+", header=None, names=RAW_COLUMNS_WITH_TARGET)
        df["target"] = df["target"].map({1: 0, 2: 1})
        return df

    @staticmethod
    def from_feedback_rows(rows: list[tuple]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=RAW_COLUMNS_WITH_TARGET)
        df = pd.DataFrame(rows, columns=RAW_COLUMNS_WITH_TARGET)
        df["target"] = df["target"].astype(int)
        return df

    @staticmethod
    def merge_for_retraining(raw_df: pd.DataFrame, feedback_df: pd.DataFrame) -> pd.DataFrame:
        if feedback_df.empty:
            return raw_df.copy()
        merged = pd.concat([raw_df, feedback_df], ignore_index=True)
        return merged
