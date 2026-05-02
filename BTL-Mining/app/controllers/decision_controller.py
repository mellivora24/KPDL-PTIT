from __future__ import annotations

from app.services.db_service import DatabaseService


class DecisionController:
    def __init__(self, db_service: DatabaseService) -> None:
        self.db_service = db_service

    def save_decision(self, application_id: int, decision: int) -> None:
        self.db_service.update_human_decision(application_id, decision)
