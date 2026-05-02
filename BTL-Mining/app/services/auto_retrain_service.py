from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import QObject, QTimer

from app.controllers.retrain_controller import RetrainController


class AutoRetrainService(QObject):
    def __init__(
        self,
        retrain_controller: RetrainController,
        state_path: Path,
        interval_minutes: int,
        min_new_feedback_rows: int,
        enabled: bool,
    ) -> None:
        super().__init__()
        self.retrain_controller = retrain_controller
        self.state_path = state_path
        self.interval_minutes = max(1, interval_minutes)
        self.min_new_feedback_rows = max(1, min_new_feedback_rows)
        self.enabled = enabled

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._check_and_retrain)
        self._state = self._load_state()

    def start(self) -> None:
        if not self.enabled:
            print("[AUTO_RETRAIN] Disabled")
            return
        self._timer.start(self.interval_minutes * 60 * 1000)
        print(
            "[AUTO_RETRAIN] Started with interval="
            f"{self.interval_minutes}m, threshold={self.min_new_feedback_rows}"
        )
        self._check_and_retrain()

    def stop(self) -> None:
        if self._timer.isActive():
            self._timer.stop()

    def _load_state(self) -> dict:
        if not self.state_path.exists():
            return {"last_feedback_rows": 0, "last_retrain_at": None}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {"last_feedback_rows": 0, "last_retrain_at": None}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def _check_and_retrain(self) -> None:
        try:
            current_feedback_rows = self.retrain_controller.current_feedback_count()
            last_feedback_rows = int(self._state.get("last_feedback_rows", 0))
            new_rows = current_feedback_rows - last_feedback_rows

            if new_rows < self.min_new_feedback_rows:
                print(
                    "[AUTO_RETRAIN] Skip: new feedback rows "
                    f"{new_rows}/{self.min_new_feedback_rows}"
                )
                return

            metrics = self.retrain_controller.retrain()
            self._state["last_feedback_rows"] = current_feedback_rows
            self._state["last_retrain_at"] = datetime.now().isoformat(timespec="seconds")
            self._state["last_metrics"] = metrics
            self._save_state()
            print(
                "[AUTO_RETRAIN] Retrain done: "
                f"feedback_rows={current_feedback_rows}, roc_auc={metrics.get('roc_auc')}"
            )
        except Exception as ex:
            print(f"[AUTO_RETRAIN] Error: {ex}")
