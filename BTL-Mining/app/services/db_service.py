from __future__ import annotations

from datetime import date

import pyodbc


class DatabaseService:
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def _connect(self) -> pyodbc.Connection:
        return pyodbc.connect(self.connection_string)

    def health_check(self) -> bool:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return cursor.fetchone()[0] == 1

    def insert_application(
        self,
        payload: dict,
        data_source: str,
        target: int | None = None,
        model_prediction: int | None = None,
        model_probability: float | None = None,
        human_decision: int | None = None,
    ) -> int:
        columns = [
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
            "data_source",
            "model_prediction",
            "model_probability",
            "human_decision",
        ]
        values = [
            payload["status"],
            payload["duration"],
            payload["credit_history"],
            payload["purpose"],
            payload["credit_amount"],
            payload["savings"],
            payload["employment"],
            payload["installment_rate"],
            payload["personal_status"],
            payload["other_debtors"],
            payload["residence_since"],
            payload["property"],
            payload["age"],
            payload["other_installment_plans"],
            payload["housing"],
            payload["existing_credits"],
            payload["job"],
            payload["people_liable"],
            payload["telephone"],
            payload["foreign_worker"],
            target,
            data_source,
            model_prediction,
            model_probability,
            human_decision,
        ]

        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO applications ({', '.join(columns)}) OUTPUT INSERTED.id VALUES ({placeholders})"

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, values)
            inserted_id = cursor.fetchone()[0]
            conn.commit()
            return int(inserted_id)

    def update_human_decision(self, application_id: int, decision: int) -> None:
        sql = "UPDATE applications SET human_decision = ? WHERE id = ?"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (decision, application_id))
            conn.commit()

    def upsert_outcome(
        self,
        application_id: int,
        due_date: date | None,
        paid: bool,
        paid_date: date | None,
        actual_outcome: int,
    ) -> None:
        sql_exists = "SELECT COUNT(1) FROM outcomes WHERE application_id = ?"
        sql_insert = (
            "INSERT INTO outcomes (application_id, due_date, paid, paid_date, actual_outcome) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        sql_update = (
            "UPDATE outcomes SET due_date = ?, paid = ?, paid_date = ?, actual_outcome = ?, updated_at = GETDATE() "
            "WHERE application_id = ?"
        )

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_exists, application_id)
            exists = cursor.fetchone()[0] > 0
            if exists:
                cursor.execute(sql_update, (due_date, int(paid), paid_date, actual_outcome, application_id))
            else:
                cursor.execute(sql_insert, (application_id, due_date, int(paid), paid_date, actual_outcome))
            conn.commit()

    def count_feedback_rows(self) -> int:
        sql = """
            SELECT COUNT(1)
            FROM outcomes o
            WHERE o.actual_outcome IS NOT NULL
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return int(cursor.fetchone()[0])

    def fetch_feedback_training_rows(self) -> list[tuple]:
        sql = """
            SELECT
                a.status,
                a.duration,
                a.credit_history,
                a.purpose,
                a.credit_amount,
                a.savings,
                a.employment,
                a.installment_rate,
                a.personal_status,
                a.other_debtors,
                a.residence_since,
                a.property,
                a.age,
                a.other_installment_plans,
                a.housing,
                a.existing_credits,
                a.job,
                a.people_liable,
                a.telephone,
                a.foreign_worker,
                o.actual_outcome
            FROM applications a
            INNER JOIN outcomes o ON o.application_id = a.id
            WHERE o.actual_outcome IS NOT NULL
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [tuple(row) for row in cursor.fetchall()]

    def fetch_pending_outcomes(self) -> list[tuple[int, str | None, int | float | None]]:
        """Lấy danh sách các hồ sơ chưa có kết quả trả nợ (actual_outcome IS NULL)"""
        sql = """
            SELECT a.id, a.purpose, a.credit_amount
            FROM applications a
            LEFT JOIN outcomes o ON o.application_id = a.id
            WHERE o.actual_outcome IS NULL OR o.id IS NULL
            ORDER BY a.id DESC
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
