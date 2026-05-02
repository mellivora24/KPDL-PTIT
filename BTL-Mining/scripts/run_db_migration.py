from __future__ import annotations

from pathlib import Path

import pyodbc

from app.config.setting import get_settings


def split_sql_batches(sql_text: str) -> list[str]:
    lines = sql_text.splitlines()
    batches: list[str] = []
    current: list[str] = []
    for line in lines:
        if line.strip().upper() == "GO":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
            continue
        current.append(line)
    final_batch = "\n".join(current).strip()
    if final_batch:
        batches.append(final_batch)
    return batches


def main() -> None:
    settings = get_settings()
    migration_path = Path(__file__).resolve().parent / "sql_migrations" / "00_create_database_up.sql"

    sql_text = migration_path.read_text(encoding="utf-8")
    batches = split_sql_batches(sql_text)

    conn_master = pyodbc.connect(settings.master_connection_string)
    try:
        cursor_master = conn_master.cursor()
        cursor_master.execute(
            f"IF DB_ID('{settings.db_name}') IS NULL CREATE DATABASE {settings.db_name}"
        )
        conn_master.commit()
    finally:
        conn_master.close()

    conn = pyodbc.connect(settings.connection_string)
    try:
        cursor = conn.cursor()
        for batch in batches:
            cleaned = "\n".join(
                line for line in batch.splitlines() if not line.strip().startswith("---")
            ).strip()
            if cleaned:
                cursor.execute(cleaned)
                conn.commit()
        print("Migration completed")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
