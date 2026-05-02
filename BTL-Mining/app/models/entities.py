from __future__ import annotations

from dataclasses import dataclass
from datetime import date


ALL_FEATURE_COLUMNS = [
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
]

SVM_FEATURE_COLUMNS = [
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

SVM_NUMERIC_COLUMNS = ["credit_amount", "duration", "installment_rate", "age"]
SVM_CATEGORICAL_COLUMNS = [
    "status",
    "credit_history",
    "savings",
    "employment",
    "other_debtors",
]


@dataclass(slots=True)
class ApplicationInput:
    status: str
    duration: int
    credit_history: str
    purpose: str
    credit_amount: int
    savings: str
    employment: str
    installment_rate: int
    personal_status: str
    other_debtors: str
    residence_since: int
    property: str
    age: int
    other_installment_plans: str
    housing: str
    existing_credits: int
    job: str
    people_liable: int
    telephone: str
    foreign_worker: str

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "duration": self.duration,
            "credit_history": self.credit_history,
            "purpose": self.purpose,
            "credit_amount": self.credit_amount,
            "savings": self.savings,
            "employment": self.employment,
            "installment_rate": self.installment_rate,
            "personal_status": self.personal_status,
            "other_debtors": self.other_debtors,
            "residence_since": self.residence_since,
            "property": self.property,
            "age": self.age,
            "other_installment_plans": self.other_installment_plans,
            "housing": self.housing,
            "existing_credits": self.existing_credits,
            "job": self.job,
            "people_liable": self.people_liable,
            "telephone": self.telephone,
            "foreign_worker": self.foreign_worker,
        }


@dataclass(slots=True)
class PredictionResult:
    bad_probability: float
    risk_score: float
    auto_decision: int


@dataclass(slots=True)
class OutcomeInput:
    application_id: int
    due_date: date | None
    paid: bool
    paid_date: date | None
    actual_outcome: int
