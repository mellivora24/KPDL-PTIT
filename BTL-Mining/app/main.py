from __future__ import annotations

import sys

from PyQt6.QtWidgets import QApplication, QMessageBox

from app.services.auto_retrain_service import AutoRetrainService

from app.config.setting import get_settings
from app.controllers.decision_controller import DecisionController
from app.controllers.outcome_controller import OutcomeController
from app.controllers.predict_controller import PredictController
from app.controllers.retrain_controller import RetrainController
from app.services.data_service import DataService
from app.services.db_service import DatabaseService
from app.services.ml_service import MLService
from app.views.main_window import MainWindow


def build_main_window() -> tuple[MainWindow, AutoRetrainService]:
	settings = get_settings()

	db_service = DatabaseService(settings.connection_string)
	data_service = DataService(settings.raw_data_path)
	ml_service = MLService(
		model_path=settings.model_path,
		metrics_path=settings.metrics_path,
		random_state=settings.random_state,
		test_size=settings.test_size,
	)

	if not settings.model_path.exists():
		raw_df = data_service.load_raw_dataset()
		ml_service.train(raw_df)

	predict_controller = PredictController(db_service=db_service, ml_service=ml_service)
	decision_controller = DecisionController(db_service=db_service)
	outcome_controller = OutcomeController(db_service=db_service)
	retrain_controller = RetrainController(
		db_service=db_service,
		data_service=data_service,
		ml_service=ml_service,
	)

	auto_retrain_service = AutoRetrainService(
		retrain_controller=retrain_controller,
		state_path=settings.auto_retrain_state_path,
		interval_minutes=settings.auto_retrain_interval_minutes,
		min_new_feedback_rows=settings.auto_retrain_min_new_feedback,
		enabled=settings.auto_retrain_enabled,
	)

	window = MainWindow(
		predict_controller=predict_controller,
		decision_controller=decision_controller,
		outcome_controller=outcome_controller,
		retrain_controller=retrain_controller,
	)
	return window, auto_retrain_service


def main() -> int:
	app = QApplication(sys.argv)
	try:
		window, auto_retrain_service = build_main_window()
		auto_retrain_service.start()
		app.aboutToQuit.connect(auto_retrain_service.stop)
		window.show()
		return app.exec()
	except Exception as ex:
		QMessageBox.critical(None, "Startup Error", str(ex))
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
