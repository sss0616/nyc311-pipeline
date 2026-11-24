# Data Dictionary - NYC 311 Service Requests

Source: NYC Open Data, 311 Service Requests from 2010 to Present
API: `https://data.cityofnewyork.us/resource/erm2-nwe9.json`
Sample: 500 requests, 100 per day for 2026-10-01, 10-08, 10-15, 10-22, 10-29.

## 1. Raw fields (data/raw_data.json and Kafka messages)

All values arrive from the API as JSON strings. A field is missing from a record when the API has no value.

| Field | Meaning | Raw type | Kind | Missing in sample | Pandera rule |
|---|---|---|---|---|---|
| unique_key | ID of the request | string | identifier | 0 | required, digits only |
| created_date | When the request was created | ISO datetime string | date/time | 0 | required, valid datetime |
| closed_date | When the request was closed | ISO datetime string | date/time | 30 | optional, valid datetime, not before created_date |
| complaint_type | Type of the problem | string | categorical | 0 | required, not blank |
| descriptor | Extra detail of the problem | string | categorical | 0 | - |
| borough | NYC borough | string | categorical | 0 | required, one of 5 boroughs |
| status | Status of the request | string | categorical | 0 | required, not blank |
| agency | Responsible city agency | string | categorical | 0 | - |
| latitude | Latitude of the location | numeric string | numerical | 5 | optional, 40-41 |
| longitude | Longitude of the location | numeric string | numerical | 5 | optional, -75 to -73 |

## 2. Table `service_requests` (data/output.db)

| Column | SQLite type | Constraint | Description / how it is made |
|---|---|---|---|
| unique_key | TEXT | PRIMARY KEY | request ID, stripped text |
| created_date | TEXT | NOT NULL | `YYYY-MM-DD HH:MM:SS` |
| closed_date | TEXT | - | `YYYY-MM-DD HH:MM:SS`, NULL if not closed |
| complaint_type | TEXT | NOT NULL | Title Case (e.g. `HEAT/HOT WATER` -> `Heat/Hot Water`) |
| descriptor | TEXT | - | stripped, `No Description` if missing |
| borough | TEXT | NOT NULL | UPPER CASE, `UNSPECIFIED` if missing |
| status | TEXT | NOT NULL | Title Case: Closed, Open, In Progress, Assigned |
| agency | TEXT | - | UPPER CASE agency code (NYPD, HPD, DOT, ...) |
| latitude | REAL | CHECK 40-41 or NULL | converted with `pd.to_numeric` |
| longitude | REAL | CHECK -75 to -73 or NULL | converted with `pd.to_numeric` |
| resolution_hours | REAL | CHECK ≥ 0 or NULL | (closed_date - created_date) in hours |
| created_date_only | TEXT | NOT NULL | day of creation `YYYY-MM-DD` |
| created_hour | INTEGER | CHECK 0-23 | hour of creation |
| is_closed | INTEGER | CHECK 0 or 1 | 1 if closed_date exists |

Indexes: `idx_borough`, `idx_complaint_type`.

## 3. Table `load_log` (data/output.db)

| Column | Type | Description |
|---|---|---|
| run_id | INTEGER PRIMARY KEY | number of the load |
| run_time | TEXT | time of the load |
| input_records | INTEGER | rows given to the loader |
| inserted | INTEGER | new rows |
| updated | INTEGER | existing rows where a value changed |
| unchanged | INTEGER | existing rows with the same values |
| rows_in_table | INTEGER | rows in service_requests after the load |

## 4. Other files

| File | Content |
|---|---|
| data/raw_data.json | original API response, not changed |
| data/valid_records.json | records that passed Pandera |
| data/invalid_records.csv | records that failed Pandera + `validation_error` |
| data/cleaned_data.csv | output of cleaner.py (input of loader.py) |
| data/pipeline_summary.json | record counts of the last run |
