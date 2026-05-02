import pyodbc
import pandas as pd
from datetime import datetime, timedelta

USE_NUMERIC = False
FILE_PATH = r"C:\Users\ADMIN\Documents\KPDL\BTL-MINING\data\raw\german.data"
FILE_PATH_NUMERIC = r"C:\Users\ADMIN\Documents\KPDL\BTL-MINING\data\raw\german.data-numeric"

CREATE_OUTCOMES = True  # actual_outcome = NULL

columns = [
    "status", "duration", "credit_history", "purpose",
    "credit_amount", "savings", "employment",
    "installment_rate", "personal_status",
    "other_debtors", "residence_since", "property",
    "age", "other_installment_plans", "housing",
    "existing_credits", "job", "people_liable",
    "telephone", "foreign_worker", "target"
]

def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=DATA_MINING;"
        "Trusted_Connection=yes;"
    )

def load_data():
    path = FILE_PATH_NUMERIC if USE_NUMERIC else FILE_PATH

    df = pd.read_csv(
        path,
        sep=" ",
        header=None,
        names=columns
    )

    # convert target: 1 good / 2 bad → 1/0
    df["target"] = df["target"].apply(lambda x: 1 if x == 1 else 0)

    return df

# ================= INSERT =================
def insert_data(df):
    conn = get_connection()
    cursor = conn.cursor()

    insert_app_query = """
        INSERT INTO applications (
            status, duration, credit_history, purpose,
            credit_amount, savings, employment,
            installment_rate, personal_status,
            other_debtors, residence_since, property,
            age, other_installment_plans, housing,
            existing_credits, job, people_liable,
            telephone, foreign_worker,
            target,
            data_source
        )
        OUTPUT INSERTED.id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    insert_outcome_query = """
        INSERT INTO outcomes (
            application_id,
            due_date,
            actual_outcome
        )
        VALUES (?, ?, NULL)
    """

    inserted = 0

    for _, row in df.iterrows():
        cursor.execute(insert_app_query, (
            str(row["status"]),
            int(row["duration"]),
            str(row["credit_history"]),
            str(row["purpose"]),
            int(row["credit_amount"]),
            str(row["savings"]),
            str(row["employment"]),
            int(row["installment_rate"]),
            str(row["personal_status"]),
            str(row["other_debtors"]),
            int(row["residence_since"]),
            str(row["property"]),
            int(row["age"]),
            str(row["other_installment_plans"]),
            str(row["housing"]),
            int(row["existing_credits"]),
            str(row["job"]),
            int(row["people_liable"]),
            str(row["telephone"]),
            str(row["foreign_worker"]),
            int(row["target"]),
            "UCI"
        ))

        app_id = cursor.fetchone()[0]

        if CREATE_OUTCOMES:
            due_date = datetime.now().date() + timedelta(days=180)

            cursor.execute(insert_outcome_query, (
                app_id,
                due_date
            ))

        inserted += 1

        if inserted % 100 == 0:
            conn.commit()
            print(f"Inserted {inserted} rows...")

    conn.commit()
    conn.close()

    print(f"\nDONE. Total inserted: {inserted}")

if __name__ == "__main__":
    df = load_data()
    insert_data(df)