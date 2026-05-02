from __future__ import annotations

from app.services.data_service import DataService
from app.services.db_service import DatabaseService
from app.services.ml_service import MLService


class RetrainController:
    def __init__(self, db_service: DatabaseService, data_service: DataService, ml_service: MLService) -> None:
        self.db_service = db_service
        self.data_service = data_service
        self.ml_service = ml_service

    def current_feedback_count(self) -> int:
        return self.db_service.count_feedback_rows()

    def retrain(self) -> dict:
        raw_df = self.data_service.load_raw_dataset()
        feedback_rows = self.db_service.fetch_feedback_training_rows()
        feedback_df = self.data_service.from_feedback_rows(feedback_rows)
        combined_df = self.data_service.merge_for_retraining(raw_df, feedback_df)
        metrics = self.ml_service.retrain_with_feedback(combined_df)
        metrics["feedback_rows"] = int(len(feedback_df))
        metrics["combined_rows"] = int(len(combined_df))
        return metrics
