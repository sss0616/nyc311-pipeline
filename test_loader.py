import sqlite3
import tempfile
from pathlib import Path

import pandas as pd

from src.cleaner import clean_nyc311_data
from src.loader import count_rows, idempotency_test, load_to_sqlite, table_checksum

records = pd.DataFrame([
    {"unique_key": "1001", "created_date": "2026-10-01T00:00:21.000",
     "closed_date": "2026-10-01T02:30:21.000", "complaint_type": "Illegal Parking",
     "descriptor": "Blocked Hydrant", "borough": "BROOKLYN", "status": "Closed",
     "agency": "NYPD", "latitude": "40.65", "longitude": "-73.95"},
    {"unique_key": "1002", "created_date": "2026-10-01T01:15:00.000",
     "closed_date": None, "complaint_type": "Noise - Residential",
     "descriptor": "N/A", "borough": "QUEENS", "status": "Open",
     "agency": "NYPD", "latitude": None, "longitude": None},
])
cleaned = clean_nyc311_data(records)

with tempfile.TemporaryDirectory() as tmp:
    db = Path(tmp) / "test.db"

    result = idempotency_test(cleaned, db)
    assert result["passed"] and count_rows(db) == 2

    with sqlite3.connect(db) as conn:
        try:
            conn.execute("INSERT INTO service_requests (unique_key, created_date, complaint_type, "
                         "borough, status, created_date_only, is_closed) "
                         "VALUES ('1001', 'x', 'x', 'x', 'x', 'x', 0)")
            raise AssertionError("duplicate key was accepted")
        except sqlite3.IntegrityError as error:
            print(f"Duplicate key rejected by SQLite: {error}")

    changed = records.copy()
    changed.loc[1, ["status", "closed_date"]] = ["Closed", "2026-10-01T03:15:00.000"]
    before = table_checksum(db)
    stats = load_to_sqlite(clean_nyc311_data(changed), db)
    assert stats["inserted"] == 0 and stats["updated"] == 1 and count_rows(db) == 2
    assert table_checksum(db) != before

    new_row = pd.concat([changed, records.iloc[[0]].assign(unique_key="1003")])
    stats = load_to_sqlite(clean_nyc311_data(new_row), db)
    assert stats["inserted"] == 1 and count_rows(db) == 3

print("Loader tests passed")
