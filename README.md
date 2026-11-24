# NYC 311 Data Streaming & Validation Pipeline (SIS 2)

SIS 2
Topic: NYC 311 Urban Service Requests: Data Streaming, Validation, and Analysis

## 1. Project overview

The pipeline collects real service requests from the NYC 311 Open Data API, sends them
through a real Apache Kafka broker, checks their quality with Pandera, cleans them with
pandas and NumPy, stores them in SQLite and analyses which problems are most common in
each borough and how fast they are solved.

```
API -> raw_data.json -> Kafka producer -> topic nyc311_requests -> Kafka consumer
    -> Pandera -> cleaner -> SQLite (output.db) -> analysis -> figures/
```

`main.py` runs all stages in this order and prints the record count of every stage.

## 2. Project structure

```
main.py                     run the whole pipeline
docker-compose.yml          Kafka broker
src/api_client.py           API collection
src/producer.py             Kafka producer
src/consumer.py             Kafka consumer
src/validator.py            Pandera validation
src/cleaner.py              cleaning
src/analysis.py             EDA and charts
src/loader.py               SQLite loading
sql/verification_queries.sql
test_validator.py, test_cleaner_and_eda.py, test_loader.py
data/                       raw_data.json, cleaned_data.csv, output.db ...
figures/                    charts
docs/data_dictionary.md
report/project_report.pdf
```

## 3. Requirements

- Python 3.10 or newer
- Docker Desktop (for the Kafka broker)
- Internet connection (only for API collection; `--skip-api` works offline)
- Python packages from `requirements.txt`: requests, kafka-python, pandas, pandera, numpy,
  matplotlib, seaborn

No API key is needed. The NYC Open Data API is public, so there are no secrets in this repository.

## 4. Installation

```bash
git clone https://github.com/Ersaiynn/SIS2_DataCollection.git
cd SIS2_DataCollection/nyc311-pipeline
python -m venv venv
# Windows: venv\Scripts\activate      macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
```

All commands below are run from the `nyc311-pipeline` folder.

## 5. Kafka broker setup

```bash
docker compose up -d          # starts container "nyc311-kafka" on localhost:9092
docker ps                     # check that nyc311-kafka is "Up"
```

Create the topic (main.py also creates it automatically if it is missing):

```bash
docker exec nyc311-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic nyc311_requests --partitions 1 --replication-factor 1
docker exec nyc311-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic nyc311_requests
```

| Setting | Value |
|---|---|
| Bootstrap server | `localhost:9092` |
| Topic | `nyc311_requests` |
| Partitions / replication | 1 / 1 |
| Message format | one JSON object per API record (UTF-8) |

## 6. API configuration

- Endpoint: `https://data.cityofnewyork.us/resource/erm2-nwe9.json` (Socrata SODA API)
- Parameters used in `src/api_client.py`:
  - `$select` = unique_key, created_date, closed_date, complaint_type, descriptor, borough, status, agency, latitude, longitude
  - `$where` = `created_date >= '<day>T00:00:00' AND created_date < '<next day>T00:00:00'`
  - `$order` = `created_date ASC, unique_key ASC`
  - `$limit` = 100 per day
- Days: 2026-10-01, 2026-10-08, 2026-10-15, 2026-10-22, 2026-10-29 -> 500 records.
- To change the sample, edit `SAMPLE_DAYS` or `$limit` in `src/api_client.py`.

## 7. Running the pipeline

### Option A - everything with one command (recommended)

```bash
docker compose up -d
python main.py               # API -> Kafka -> Pandera -> cleaning -> SQLite (x2) -> EDA
```

Useful flags:

```bash
python main.py --skip-api    # reuse data/raw_data.json (same data as in the report)
python main.py --reset-db    # delete data/output.db first, for a clean demo
python main.py --skip-eda    # do not rebuild charts
```

To keep the console output as evidence: `python main.py --skip-api --reset-db > run_log.txt`

### Option B - stage by stage (for the defense)

```bash
python src/api_client.py     # 1. collect 500 records -> data/raw_data.json
python src/producer.py       # 2. publish them to Kafka (prints published/failed)
python src/consumer.py       # 3. read the latest 500 messages (prints consumed count)
python src/validator.py      # 4. consume + validate + 3 invalid test records
python main.py --skip-api    # 5-7. full connected run, creates cleaned_data.csv and output.db
python src/loader.py         # 6. load cleaned_data.csv twice again, idempotency + SQL checks
python src/analysis.py       # 7. EDA from data/output.db -> figures/
```

Check the messages directly in Kafka:

```bash
docker exec nyc311-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nyc311_requests --from-beginning --max-messages 3
```

### Tests (no Kafka needed)

```bash
python test_validator.py
python test_cleaner_and_eda.py
python test_loader.py
```

## 8. Database and verification

Table `service_requests` in `data/output.db`:

- `unique_key TEXT PRIMARY KEY` - one row per 311 request, duplicates are impossible.
- NOT NULL on created_date, complaint_type, borough, status; CHECK rules on coordinates,
  `resolution_hours >= 0`, `created_hour` 0-23 and `is_closed` 0/1.
- Table `load_log` keeps one row per load (inserted / updated / unchanged).

Loading strategy (idempotent UPSERT):

```sql
INSERT INTO service_requests (...) VALUES (...)
ON CONFLICT(unique_key) DO UPDATE SET status = excluded.status, ...
WHERE service_requests.status IS NOT excluded.status OR ...;
```

A new key is inserted, a changed record is updated, an unchanged record is not touched.
`main.py` loads the same cleaned data twice and compares the row count and a SHA-256
checksum of the table. Expected: load #2 -> inserted 0, updated 0, unchanged 500, `PASSED`.

Run the verification queries by hand:

```bash
sqlite3 data/output.db < sql/verification_queries.sql
# or without the sqlite3 tool:
python -c "from src.loader import run_verification_queries; run_verification_queries()"
```

## 9. Expected output (with `--skip-api`, same data as the report)

```
API records collected              500
Kafka messages published           500
Kafka messages consumed            500
Valid records (Pandera)            500
Invalid records (Pandera)            0
Records after cleaning             500
Records stored in SQLite           500
Idempotency test                PASSED
```

Pandera negative test: 3 synthetic records are rejected (bad unique_key, unknown borough,
invalid created_date). They are never added to the real dataset.

Charts in `figures/`: `top_complaint_types.png`, `complaints_by_borough.png`,
`resolution_time_by_category.png`.

If you run without `--skip-api`, the API may return newer `status` / `closed_date` values
for the same requests. Then the first load shows some `updated` rows - this is expected,
and the second load is still `inserted 0, updated 0`.

## 10. How to check that the run was successful

1. The last line is `Pipeline finished successfully.`
2. Published = consumed = 500 and the consumer did not hang (it stops after the batch or after 30 s).
3. `Idempotency test PASSED`, SQL check `duplicate_keys` shows `(no rows)`.
4. `data/pipeline_summary.json` contains the counts and `"checksums_equal": true`.
5. Three PNG files are in `figures/`.

## 11. Common problems

| Problem | Fix |
|---|---|
| `Cannot connect to Kafka at localhost:9092` | Start Docker Desktop, then `docker compose up -d`; wait ~10 s |
| `Only N messages available ...` | Run the producer (or `python main.py`) before `src/consumer.py` |
| API error / timeout | Check the internet or use `python main.py --skip-api` |
| `ModuleNotFoundError: src` | Run commands from the `nyc311-pipeline` folder |

## 12. Team

Orynbayeva Symbat, Narbay Yersaiyn, Aman Yenglik, Zhaparov Bekzat
