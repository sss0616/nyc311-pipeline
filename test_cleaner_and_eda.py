import os
import sqlite3
import pandas as pd
from src.cleaner import clean_nyc311_data
from src.analysis import run_eda

sample_valid_records = pd.DataFrame([
    {
        "unique_key": "70593801",
        "created_date": "2026-10-01T00:00:21.000",
        "closed_date": "2026-10-01T02:30:21.000",
        "complaint_type": "Illegal Parking",
        "borough": "BROOKLYN",
        "status": "Closed",
        "latitude": 40.65,
        "longitude": -73.95,
    },
    {
        "unique_key": "70593801",
        "created_date": "2026-10-01T00:00:21.000",
        "closed_date": "2026-10-01T02:30:21.000",
        "complaint_type": "Illegal Parking",
        "borough": "BROOKLYN",
        "status": "Closed",
        "latitude": 40.65,
        "longitude": -73.95,
    },
    {
        "unique_key": "70593802",
        "created_date": "2026-10-01T01:15:00.000",
        "closed_date": "2026-10-01T02:00:00.000",
        "complaint_type": "Noise - Residential",
        "borough": "brooklyn",
        "status": "closed",
        "latitude": 40.68,
        "longitude": -73.94,
    },
    {
        "unique_key": "70593803",
        "created_date": "2026-10-02T10:00:00.000",
        "closed_date": "2026-10-04T10:00:00.000",
        "complaint_type": "HEAT/HOT WATER",
        "borough": "BRONX",
        "status": "Closed",
        "latitude": 40.84,
        "longitude": -73.86,
    },
    {
        "unique_key": "70593804",
        "created_date": "2026-10-03T14:20:00.000",
        "closed_date": None,
        "complaint_type": "Street Condition",
        "borough": "QUEENS",
        "status": "Open",
        "latitude": None,
        "longitude": None,
    },
])

cleaned = clean_nyc311_data(sample_valid_records)
assert len(cleaned) == 4
assert "resolution_hours" in cleaned.columns

os.makedirs("data", exist_ok=True)
test_db = "data/test_nyc311.db"
conn = sqlite3.connect(test_db)
cleaned.to_sql("service_requests", conn, if_exists="replace", index=False)
conn.close()

run_eda(db_path=test_db, table_name="service_requests", output_dir="figures/test_output")
print("Cleaner & EDA smoke tests passed!")