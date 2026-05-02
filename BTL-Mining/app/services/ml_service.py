from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import SVC

from app.models.entities import (
    SVM_CATEGORICAL_COLUMNS,
    SVM_FEATURE_COLUMNS,
    SVM_NUMERIC_COLUMNS,
)


class MLService:
    def __init__(self, model_path: Path, metrics_path: Path, random_state: int = 42, test_size: float = 0.2) -> None:
        self.model_path = model_path
        self.metrics_path = metrics_path
        self.random_state = random_state
        self.test_size = test_size
        self._pipeline: Pipeline | None = None

    def _build_pipeline(self) -> Pipeline:
        preprocessor = ColumnTransformer(
            transformers=[
                ("num", StandardScaler(), SVM_NUMERIC_COLUMNS),
                ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), SVM_CATEGORICAL_COLUMNS),
            ]
        )
        classifier = SVC(
            kernel="rbf",
            C=2.0,
            gamma="scale",
            probability=True,
            class_weight={0: 1, 1: 5},
            random_state=self.random_state,
        )
        return Pipeline([("preprocessor", preprocessor), ("classifier", classifier)])

    def train(self, dataset: pd.DataFrame) -> dict:
        X = dataset[SVM_FEATURE_COLUMNS]
        y = dataset["target"].astype(int)

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self.test_size,
            random_state=self.random_state,
            stratify=y,
        )

        pipeline = self._build_pipeline()
        pipeline.fit(X_train, y_train)

        y_pred = pipeline.predict(X_test)
        y_prob = pipeline.predict_proba(X_test)[:, 1]
        metrics = {
            "precision_bad": float(precision_score(y_test, y_pred, pos_label=1)),
            "recall_bad": float(recall_score(y_test, y_pred, pos_label=1)),
            "f1_bad": float(f1_score(y_test, y_pred, pos_label=1)),
            "roc_auc": float(roc_auc_score(y_test, y_prob)),
            "train_rows": int(len(X_train)),
            "test_rows": int(len(X_test)),
        }

        self._pipeline = pipeline
        self.save_model()
        self.save_metrics(metrics)
        return metrics

    def retrain_with_feedback(self, combined_dataset: pd.DataFrame) -> dict:
        return self.train(combined_dataset)

    def load_model(self) -> Pipeline:
        if self._pipeline is not None:
            return self._pipeline
        if not self.model_path.exists():
            raise FileNotFoundError(f"Model file not found: {self.model_path}")
        self._pipeline = joblib.load(self.model_path)
        return self._pipeline

    def predict_single(self, payload: dict) -> tuple[int, float, float]:
        pipeline = self.load_model()
        input_df = pd.DataFrame([payload])[SVM_FEATURE_COLUMNS]
        bad_probability = float(pipeline.predict_proba(input_df)[0, 1])
        prediction = 1 if bad_probability >= 0.5 else 0
        risk_score = round(bad_probability * 100.0, 2)
        return prediction, bad_probability, risk_score

    def save_model(self) -> None:
        if self._pipeline is None:
            raise ValueError("No trained pipeline available to save.")
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._pipeline, self.model_path)

    def save_metrics(self, metrics: dict) -> None:
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        self.metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
