from __future__ import annotations

from app.models.entities import OutcomeInput
from app.services.db_service import DatabaseService


class OutcomeController:
    def __init__(self, db_service: DatabaseService) -> None:
        self.db_service = db_service

    def save_outcome(self, outcome: OutcomeInput) -> None:
        self.db_service.upsert_outcome(
            application_id=outcome.application_id,
            due_date=outcome.due_date,
            paid=outcome.paid,
            paid_date=outcome.paid_date,
            actual_outcome=outcome.actual_outcome,
        )
