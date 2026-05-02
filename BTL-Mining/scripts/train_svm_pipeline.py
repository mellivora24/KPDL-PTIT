from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import SVC


RAW_COLUMNS = [
    "status",
    "duration",
    "credit_history",
    "purpose",
    "credit_amount",
    "savings",
    "employment",
    "installment_rate",
    "personal_status",
    "other_debtors",
    "residence_since",
    "property",
    "age",
    "other_installment_plans",
    "housing",
    "existing_credits",
    "job",
    "people_liable",
    "telephone",
    "foreign_worker",
    "target",
]

# Feature subset selected by the team for SVM.
SVM_FEATURES = [
    "status",
    "credit_history",
    "credit_amount",
    "duration",
    "savings",
    "installment_rate",
    "age",
    "employment",
    "other_debtors",
]

NUMERIC_FEATURES = ["credit_amount", "duration", "installment_rate", "age"]
CATEGORICAL_FEATURES = [
    "status",
    "credit_history",
    "savings",
    "employment",
    "other_debtors",
]


def load_german_data(data_path: Path) -> pd.DataFrame:
    df = pd.read_csv(data_path, sep=r"\s+", header=None, names=RAW_COLUMNS)

    # Convert to binary label where 1 means "bad credit" and 0 means "good credit".
    df["target"] = df["target"].map({1: 0, 2: 1})
    if df["target"].isna().any():
        raise ValueError("Unexpected target values detected in dataset.")

    return df


def build_svm_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUMERIC_FEATURES),
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                CATEGORICAL_FEATURES,
            ),
        ]
    )

    # Cost-aware training: false negative (bad classified as good) is more expensive.
    model = SVC(
        kernel="rbf",
        C=2.0,
        gamma="scale",
        probability=True,
        class_weight={0: 1, 1: 5},
        random_state=42,
    )

    return Pipeline([
        ("preprocess", preprocessor),
        ("model", model),
    ])


def train_and_evaluate(df: pd.DataFrame, test_size: float, random_state: int) -> tuple[Pipeline, dict]:
    X = df[SVM_FEATURES]
    y = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )

    pipeline = build_svm_pipeline()
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    metrics = {
        "precision_bad": precision_score(y_test, y_pred, pos_label=1),
        "recall_bad": recall_score(y_test, y_pred, pos_label=1),
        "f1_bad": f1_score(y_test, y_pred, pos_label=1),
        "roc_auc": roc_auc_score(y_test, y_prob),
    }

    print("=" * 60)
    print("SVM evaluation (positive class = bad credit)")
    print("=" * 60)
    print(classification_report(y_test, y_pred, target_names=["good", "bad"]))
    print("Confusion matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("Metrics:")
    for key, value in metrics.items():
        print(f"- {key}: {value:.4f}")

    return pipeline, metrics


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    default_data = project_root / "data" / "raw" / "german.data"
    default_model_out = project_root / "model" / "svm_pipeline.joblib"
    default_metrics_out = project_root / "model" / "svm_metrics.json"

    parser = argparse.ArgumentParser(description="Train an SVM pipeline on German Credit data.")
    parser.add_argument("--data", type=Path, default=default_data, help="Path to german.data")
    parser.add_argument(
        "--model-out",
        type=Path,
        default=default_model_out,
        help="Output path for saved joblib model",
    )
    parser.add_argument(
        "--metrics-out",
        type=Path,
        default=default_metrics_out,
        help="Output path for metrics JSON",
    )
    parser.add_argument("--test-size", type=float, default=0.2, help="Test set ratio")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    df = load_german_data(args.data)
    pipeline, metrics = train_and_evaluate(df, args.test_size, args.random_state)

    args.model_out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, args.model_out)

    args.metrics_out.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_out.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"Saved model to: {args.model_out}")
    print(f"Saved metrics to: {args.metrics_out}")


if __name__ == "__main__":
    main()
