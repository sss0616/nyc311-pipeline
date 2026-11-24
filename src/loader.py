import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "output.db"
CLEANED_CSV_PATH = PROJECT_ROOT / "data" / "cleaned_data.csv"
QUERIES_PATH = PROJECT_ROOT / "sql" / "verification_queries.sql"
TABLE_NAME = "service_requests"

COLUMNS = {
    "unique_key": "TEXT PRIMARY KEY",
    "created_date": "TEXT NOT NULL",
    "closed_date": "TEXT",
    "complaint_type": "TEXT NOT NULL",
    "descriptor": "TEXT",
    "borough": "TEXT NOT NULL",
    "status": "TEXT NOT NULL",
    "agency": "TEXT",
    "latitude": "REAL CHECK (latitude IS NULL OR latitude BETWEEN 40 AND 41)",
    "longitude": "REAL CHECK (longitude IS NULL OR longitude BETWEEN -75 AND -73)",
    "resolution_hours": "REAL CHECK (resolution_hours IS NULL OR resolution_hours >= 0)",
    "created_date_only": "TEXT NOT NULL",
    "created_hour": "INTEGER CHECK (created_hour BETWEEN 0 AND 23)",
    "is_closed": "INTEGER NOT NULL CHECK (is_closed IN (0, 1))",
}
COLUMN_NAMES = list(COLUMNS)
NON_KEY_COLUMNS = [c for c in COLUMN_NAMES if c != "unique_key"]

CREATE_TABLE_SQL = (
    f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    "
    + ",\n    ".join(f"{name} {sql_type}" for name, sql_type in COLUMNS.items())
    + "\n)"
)

CREATE_INDEXES_SQL = [
    f"CREATE INDEX IF NOT EXISTS idx_borough ON {TABLE_NAME}(borough)",
    f"CREATE INDEX IF NOT EXISTS idx_complaint_type ON {TABLE_NAME}(complaint_type)",
]

CREATE_LOG_SQL = """
CREATE TABLE IF NOT EXISTS load_log (
    run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_time       TEXT    NOT NULL,
    input_records  INTEGER NOT NULL,
    inserted       INTEGER NOT NULL,
    updated        INTEGER NOT NULL,
    unchanged      INTEGER NOT NULL,
    rows_in_table  INTEGER NOT NULL
)
"""

UPSERT_SQL = (
    f"INSERT INTO {TABLE_NAME} ({', '.join(COLUMN_NAMES)})\n"
    f"VALUES ({', '.join('?' for _ in COLUMN_NAMES)})\n"
    "ON CONFLICT(unique_key) DO UPDATE SET\n    "
    + ",\n    ".join(f"{c} = excluded.{c}" for c in NON_KEY_COLUMNS)
    + "\nWHERE "
    + "\n   OR ".join(f"{TABLE_NAME}.{c} IS NOT excluded.{c}" for c in NON_KEY_COLUMNS)
)


def create_database(db_path=DB_PATH):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE_SQL)
        for statement in CREATE_INDEXES_SQL:
            conn.execute(statement)
        conn.execute(CREATE_LOG_SQL)
    print(f"[Loader] Database ready: {db_path}")


def _prepare_rows(cleaned_df):
    if cleaned_df is None or cleaned_df.empty:
        raise ValueError("Cleaned DataFrame is empty: nothing to load")

    missing = [c for c in COLUMN_NAMES if c not in cleaned_df.columns]
    if missing:
        raise ValueError(f"Cleaned data is missing columns: {missing}")

    if cleaned_df["unique_key"].duplicated().any():
        raise ValueError("Cleaned data still has duplicate unique_key values")

    frame = cleaned_df[COLUMN_NAMES].copy()
    frame["unique_key"] = frame["unique_key"].astype(str)
    frame = frame.astype(object).where(pd.notna(frame), None)

    rows = []
    for record in frame.itertuples(index=False, name=None):
        row = []
        for value in record:
            if hasattr(value, "item"):
                value = value.item()
            row.append(value)
        rows.append(tuple(row))
    return rows


def count_rows(db_path=DB_PATH, table_name=TABLE_NAME):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def table_checksum(db_path=DB_PATH):
    digest = hashlib.sha256()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            f"SELECT {', '.join(COLUMN_NAMES)} FROM {TABLE_NAME} ORDER BY unique_key"
        )
        for row in cursor:
            digest.update(repr(row).encode("utf-8"))
    return digest.hexdigest()


def load_to_sqlite(cleaned_df, db_path=DB_PATH):
    create_database(db_path)
    rows = _prepare_rows(cleaned_df)

    conn = sqlite3.connect(db_path)
    try:
        rows_before = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        changes_before = conn.total_changes

        with conn:
            conn.executemany(UPSERT_SQL, rows)

        rows_after = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        changed = conn.total_changes - changes_before
        inserted = rows_after - rows_before
        updated = changed - inserted
        unchanged = len(rows) - inserted - updated

        with conn:
            conn.execute(
                "INSERT INTO load_log (run_time, input_records, inserted, updated, "
                "unchanged, rows_in_table) VALUES (?, ?, ?, ?, ?, ?)",
                (datetime.now().isoformat(timespec="seconds"), len(rows),
                 inserted, updated, unchanged, rows_after),
            )
    except sqlite3.Error as error:
        print(f"[Loader] SQLite error, transaction was rolled back: {error}")
        raise
    finally:
        conn.close()

    stats = {
        "input_records": len(rows),
        "rows_before": rows_before,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "rows_after": rows_after,
    }
    print(
        f"[Loader] Input: {len(rows)} | inserted: {inserted} | updated: {updated} | "
        f"unchanged: {unchanged} | rows in table: {rows_after}"
    )
    return stats


def idempotency_test(cleaned_df, db_path=DB_PATH):
    print("\n[Idempotency] Load #1")
    first = load_to_sqlite(cleaned_df, db_path)
    checksum_first = table_checksum(db_path)

    print("[Idempotency] Load #2 (same data again)")
    second = load_to_sqlite(cleaned_df, db_path)
    checksum_second = table_checksum(db_path)

    passed = (
        first["rows_after"] == second["rows_after"]
        and second["inserted"] == 0
        and second["updated"] == 0
        and checksum_first == checksum_second
    )
    print(f"[Idempotency] Rows after load #1: {first['rows_after']}")
    print(f"[Idempotency] Rows after load #2: {second['rows_after']}")
    print(f"[Idempotency] Checksum #1: {checksum_first[:16]}...")
    print(f"[Idempotency] Checksum #2: {checksum_second[:16]}...")
    print(f"[Idempotency] Result: {'PASSED' if passed else 'FAILED'}")

    if not passed:
        raise RuntimeError("Idempotency test failed: second load changed the table")

    return {
        "first_load": first,
        "second_load": second,
        "checksum_first": checksum_first,
        "checksum_second": checksum_second,
        "passed": passed,
    }


def read_named_queries(path=QUERIES_PATH):
    queries = {}
    current_name = None
    lines = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            if current_name:
                queries[current_name] = "\n".join(lines).strip()
            current_name = stripped.split(":", 1)[1].strip()
            lines = []
        elif current_name and not stripped.startswith("--"):
            lines.append(line)
    if current_name:
        queries[current_name] = "\n".join(lines).strip()
    return queries


def run_verification_queries(db_path=DB_PATH, show=True):
    results = {}
    with sqlite3.connect(db_path) as conn:
        for name, sql in read_named_queries().items():
            results[name] = pd.read_sql_query(sql, conn)
            if show:
                print(f"\n--- SQL check: {name} ---")
                if results[name].empty:
                    print("(no rows)")
                else:
                    print(results[name].to_string(index=False))
    return results


def compare_counts(cleaned_df, db_path=DB_PATH):
    processed_keys = set(cleaned_df["unique_key"].astype(str))
    with sqlite3.connect(db_path) as conn:
        stored_keys = {row[0] for row in conn.execute(f"SELECT unique_key FROM {TABLE_NAME}")}

    result = {
        "processed_records": len(processed_keys),
        "stored_in_table": len(stored_keys),
        "processed_and_stored": len(processed_keys & stored_keys),
        "processed_but_missing": len(processed_keys - stored_keys),
    }
    print("\n[Loader] Processed vs stored:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    if result["processed_but_missing"]:
        raise RuntimeError("Some processed records were not stored in SQLite")
    return result


def read_cleaned_csv(path=CLEANED_CSV_PATH):
    if not Path(path).exists():
        raise FileNotFoundError(f"{path} not found. Run python main.py first.")
    return pd.read_csv(
        path,
        dtype={"unique_key": str, "created_date_only": str},
        float_precision="round_trip",
        keep_default_na=False,
        na_values=[""],
    )


if __name__ == "__main__":
    data = read_cleaned_csv()
    print(f"[Loader] Cleaned records read from CSV: {len(data)}")
    idempotency_test(data)
    compare_counts(data)
    run_verification_queries()
