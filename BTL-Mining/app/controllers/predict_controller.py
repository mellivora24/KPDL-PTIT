from __future__ import annotations

from app.models.entities import ApplicationInput, PredictionResult
from app.services.db_service import DatabaseService
from app.services.ml_service import MLService


class PredictController:
    def __init__(self, db_service: DatabaseService, ml_service: MLService) -> None:
        self.db_service = db_service
        self.ml_service = ml_service

    def predict_and_store(self, application: ApplicationInput, source: str = "SYSTEM") -> tuple[int, PredictionResult]:
        payload = application.to_dict()
        model_prediction, bad_probability, risk_score = self.ml_service.predict_single(payload)
        app_id = self.db_service.insert_application(
            payload=payload,
            data_source=source,
            model_prediction=model_prediction,
            model_probability=bad_probability,
        )
        result = PredictionResult(
            bad_probability=bad_probability,
            risk_score=risk_score,
            auto_decision=0 if bad_probability >= 0.5 else 1,
        )
        return app_id, result
