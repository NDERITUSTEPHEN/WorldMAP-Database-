from __future__ import annotations
import sqlite3
from typing import Any, Tuple

import pandas as pd


def _table_columns(cnx, table: str) -> set[str]:
    cur = cnx.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def _ensure_column(cnx, table: str, column: str, coltype: str) -> None:
    cols = _table_columns(cnx, table)
    if column not in cols:
        cnx.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")

def _ensure_table_batches(cnx) -> None:
    cnx.executescript("""
    CREATE TABLE IF NOT EXISTS batches (
      batch_id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      source_label TEXT,
      source_files TEXT,
      notes TEXT
    );
    CREATE INDEX IF NOT EXISTS ix_batches_created_at ON batches(created_at);
    """)

def connect(db_path: str) -> sqlite3.Connection:
    cnx = sqlite3.connect(db_path)
    cnx.execute("PRAGMA foreign_keys = ON;")
    return cnx

def init_db(db_path: str, schema_sql: str) -> None:
    cnx = connect(db_path)
    cnx.executescript(schema_sql)

    # Lightweight migrations for existing DBs
    _ensure_table_batches(cnx)
    _ensure_column(cnx, "applications", "batch_id", "TEXT")
    _ensure_column(cnx, "applications", "source_file", "TEXT")
    _ensure_column(cnx, "issuances", "batch_id", "TEXT")

    cnx.commit()
    cnx.close()

def insert_applications(db_path: str, df_apps: pd.DataFrame, source_file: str) -> int:
    cnx = connect(db_path)
    df = df_apps.copy()
    df["source_file"] = source_file

    cols = [
        "source_file",
        "full_name_original","phone_original","national_id_original","country","church_name","title",
        "congregation_size","requested_language","received_before","received_before_reason",
        "full_name_normalized","name_key","phone_normalized","national_id_normalized",
        "is_disqualified","disqualify_reason","needs_review","system_flags",
        "status","admin_notes","matched_person_id"
    ]

    for c in ["is_disqualified","disqualify_reason","needs_review","system_flags"]:
        if c not in df.columns:
            df[c] = 0 if c in {"is_disqualified","needs_review"} else ""
    df["status"] = df.get("status", "PENDING")
    df["admin_notes"] = df.get("admin_notes", "")
    df["matched_person_id"] = df.get("matched_person_id", None)

    df = df[cols]
    df.to_sql("applications", cnx, if_exists="append", index=False)
    n = len(df)
    cnx.commit()
    cnx.close()
    return n

def fetch_df(db_path: str, table: str, where: str = "", params: Tuple[Any,...] = ()) -> pd.DataFrame:
    cnx = connect(db_path)
    q = f"SELECT * FROM {table}"
    if where:
        q += " WHERE " + where
    q += " ORDER BY 1 ASC"
    df = pd.read_sql_query(q, cnx, params=params)
    cnx.close()
    return df

def fetch_applications(db_path: str, where: str = "", params: Tuple[Any,...] = ()) -> pd.DataFrame:
    return fetch_df(db_path, "applications", where, params)

def fetch_persons(db_path: str, where: str = "", params: Tuple[Any,...] = ()) -> pd.DataFrame:
    return fetch_df(db_path, "persons", where, params)

def fetch_issuances(db_path: str, where: str = "", params: Tuple[Any,...] = ()) -> pd.DataFrame:
    cnx = connect(db_path)
    q = "SELECT * FROM issuances"
    if where:
        q += " WHERE " + where
    q += " ORDER BY issued_at DESC"
    df = pd.read_sql_query(q, cnx, params=params)
    cnx.close()
    return df

def upsert_person_from_application(db_path: str, app_row: dict) -> int:
    cnx = connect(db_path)
    cur = cnx.cursor()

    phone = (app_row.get("phone_normalized") or "").strip() or None
    nid = (app_row.get("national_id_normalized") or "").strip() or None

    person_id = None
    if phone:
        cur.execute("SELECT person_id FROM persons WHERE phone_normalized = ? LIMIT 1", (phone,))
        r = cur.fetchone()
        if r:
            person_id = int(r[0])
    if person_id is None and nid:
        cur.execute("SELECT person_id FROM persons WHERE national_id_normalized = ? LIMIT 1", (nid,))
        r = cur.fetchone()
        if r:
            person_id = int(r[0])

    if person_id is None:
        cur.execute(
            "INSERT INTO persons(first_name,middle_name,last_name,full_name_original,full_name_normalized,name_key,phone_original,phone_normalized,national_id_original,national_id_normalized,country,church_name) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                app_row.get("first_name"),
                app_row.get("middle_name"),
                app_row.get("last_name"),
                app_row.get("full_name_original"),
                app_row.get("full_name_normalized"),
                app_row.get("name_key"),
                app_row.get("phone_original"),
                phone,
                app_row.get("national_id_original"),
                nid,
                app_row.get("country"),
                app_row.get("church_name"),
            ),
        )
        person_id = int(cur.lastrowid)
    else:
        cur.execute(
            "UPDATE persons SET church_name = COALESCE(NULLIF(?,''), church_name), country = COALESCE(NULLIF(?,''), country) WHERE person_id = ?",
            (app_row.get("church_name",""), app_row.get("country",""), person_id),
        )

    cnx.commit()
    cnx.close()
    return person_id

def set_application_status(db_path: str, application_id: int, status: str, admin_notes: str = "", matched_person_id = None) -> None:
    cnx = connect(db_path)
    cnx.execute(
        "UPDATE applications SET status = ?, admin_notes = ?, matched_person_id = ? WHERE application_id = ?",
        (status, admin_notes, matched_person_id, application_id),
    )
    cnx.commit()
    cnx.close()

def insert_issuance(db_path: str, person_id: int, issued_at: str, language: str, issued_by: str,
                   is_exception: int = 0, exception_type: str = "", exception_reason: str = "", notes: str = "", batch_id: str = "") -> None:
    cnx = connect(db_path)
    cnx.execute(
        "INSERT INTO issuances(person_id,issued_at,book_name,language,issued_by,notes,is_exception,exception_type,exception_reason,batch_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (person_id, issued_at, "Shepherd Staff", language, issued_by, notes, is_exception, exception_type, exception_reason, batch_id),
    )
    cnx.commit()
    cnx.close()


def update_applications_from_corrections(db_path: str, df_corr: pd.DataFrame) -> int:
    """Update applications by application_id with corrected values. Expects application_id column."""
    if "application_id" not in df_corr.columns:
        raise ValueError("Corrections file must contain 'application_id' column.")
    cnx = connect(db_path)
    cur = cnx.cursor()
    updated = 0
    cols = [
        "full_name_original","phone_original","national_id_original","country","church_name","title",
        "congregation_size","requested_language","received_before","received_before_reason",
        "full_name_normalized","name_key","phone_normalized","national_id_normalized",
        "is_disqualified","disqualify_reason","needs_review","system_flags","status"
    ]
    for _, r in df_corr.iterrows():
        aid = int(r["application_id"])
        vals = [r.get(c, None) for c in cols]
        cur.execute(
            """UPDATE applications SET
                full_name_original=?,
                phone_original=?,
                national_id_original=?,
                country=?,
                church_name=?,
                title=?,
                congregation_size=?,
                requested_language=?,
                received_before=?,
                received_before_reason=?,
                full_name_normalized=?,
                name_key=?,
                phone_normalized=?,
                national_id_normalized=?,
                is_disqualified=?,
                disqualify_reason=?,
                needs_review=?,
                system_flags=?,
                status=?
            WHERE application_id=?""",
            (*vals, aid),
        )
        if cur.rowcount:
            updated += 1
    cnx.commit()
    cnx.close()
    return updated

def bulk_set_status(db_path: str, application_ids: list[int], status: str, note: str = "") -> int:
    if not application_ids:
        return 0
    cnx = connect(db_path)
    cur = cnx.cursor()
    qmarks = ",".join(["?"] * len(application_ids))
    cur.execute(f"UPDATE applications SET status=?, admin_notes=COALESCE(NULLIF(admin_notes,''), ?) WHERE application_id IN ({qmarks})", (status, note, *application_ids))
    n = cur.rowcount
    cnx.commit()
    cnx.close()
    return n


def create_batch(db_path: str, batch_id: str, source_label: str = "", source_files: str = "", notes: str = "") -> None:
    cnx = connect(db_path)
    _ensure_table_batches(cnx)
    cnx.execute(
        "INSERT OR IGNORE INTO batches(batch_id, source_label, source_files, notes) VALUES (?,?,?,?)",
        (batch_id, source_label, source_files, notes),
    )
    cnx.commit()
    cnx.close()

def fetch_batches(db_path: str) -> pd.DataFrame:
    cnx = connect(db_path)
    _ensure_table_batches(cnx)
    df = pd.read_sql_query("SELECT * FROM batches ORDER BY created_at DESC", cnx)
    cnx.close()
    return df
