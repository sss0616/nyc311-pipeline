import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from src.analysis import run_eda
from src.api_client import collect_data
from src.cleaner import clean_nyc311_data
from src.consumer import BOOTSTRAP_SERVERS, TOPIC_NAME, consume_messages, get_end_offset
from src.loader import (
    DB_PATH,
    TABLE_NAME,
    compare_counts,
    idempotency_test,
    run_verification_queries,
)
from src.producer import send_to_kafka
from src.validator import demo_invalid_records, validate_records

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
FIGURES_DIR = PROJECT_ROOT / "figures"
RAW_PATH = DATA_DIR / "raw_data.json"
VALID_PATH = DATA_DIR / "valid_records.json"
INVALID_PATH = DATA_DIR / "invalid_records.csv"
CLEANED_PATH = DATA_DIR / "cleaned_data.csv"
SUMMARY_PATH = DATA_DIR / "pipeline_summary.json"


def stage(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def ensure_topic():
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    except Exception as error:
        raise ConnectionError(
            f"Cannot connect to Kafka at {BOOTSTRAP_SERVERS}. "
            "Start it with: docker compose up -d"
        ) from error
    try:
        if TOPIC_NAME in admin.list_topics():
            print(f"[Kafka] Topic '{TOPIC_NAME}' already exists")
        else:
            admin.create_topics([NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)])
            print(f"[Kafka] Topic '{TOPIC_NAME}' created (1 partition, replication 1)")
    except TopicAlreadyExistsError:
        print(f"[Kafka] Topic '{TOPIC_NAME}' already exists")
    finally:
        admin.close()


def wait_for_end_offset(attempts=10):
    for attempt in range(1, attempts + 1):
        try:
            return get_end_offset()
        except RuntimeError:
            if attempt == attempts:
                raise
            time.sleep(1)


def explain_counts(counts):
    notes = []
    if counts["kafka_published"] != counts["api_records"]:
        notes.append(f"{counts['api_records'] - counts['kafka_published']} record(s) failed to publish to Kafka.")
    if counts["kafka_consumed"] != counts["kafka_published"]:
        notes.append("The consumer read a different number of messages than were published.")
    if counts["invalid_records"]:
        notes.append(
            f"{counts['invalid_records']} record(s) failed Pandera rules and were kept apart "
            f"in {INVALID_PATH.name} (not cleaned and not stored)."
        )
    if counts["after_cleaning"] != counts["valid_records"]:
        notes.append(
            f"Cleaning removed {counts['valid_records'] - counts['after_cleaning']} record(s) "
            "(duplicates by unique_key or missing required values)."
        )
    if counts["stored_in_sqlite"] != counts["after_cleaning"]:
        notes.append(
            "The table has more rows than this batch because earlier batches are also stored "
            "(the primary key still prevents duplicates)."
        )
    if not notes:
        notes.append(
            "All counts are equal: no lost messages, no invalid records, no duplicates."
        )
    notes.append(
        "The 3 invalid test records are not part of the dataset."
    )
    return notes


def main():
    parser = argparse.ArgumentParser(description="NYC 311 Kafka + Pandera + SQLite pipeline")
    parser.add_argument("--skip-api", action="store_true",
                        help="use the saved data/raw_data.json instead of calling the API")
    parser.add_argument("--reset-db", action="store_true",
                        help="delete data/output.db before loading")
    parser.add_argument("--skip-eda", action="store_true", help="do not build charts")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    counts = {}

    stage("STAGE 1. API DATA COLLECTION")
    if args.skip_api:
        if not RAW_PATH.exists():
            sys.exit(f"{RAW_PATH} not found. Run without --skip-api first.")
        with open(RAW_PATH, encoding="utf-8") as file:
            raw_records = json.load(file)
        print(f"[API] Skipped. Using saved raw data: {len(raw_records)} records")
    else:
        try:
            raw_records = collect_data()
        except Exception as error:
            sys.exit(f"[API] Collection failed: {error}\n"
                     "Check the internet connection or run: python main.py --skip-api")
    counts["api_records"] = len(raw_records)

    stage("STAGE 2. KAFKA PRODUCER")
    try:
        ensure_topic()
        start_offset = wait_for_end_offset()
        print(f"[Kafka] Start offset for this run: {start_offset}")
        producer_stats = send_to_kafka()
    except (ConnectionError, KafkaError, RuntimeError, ValueError) as error:
        sys.exit(f"[Kafka] Producer stage failed: {error}")
    counts["kafka_published"] = producer_stats["published"]
    counts["kafka_failed"] = producer_stats["failed"]

    stage("STAGE 3. KAFKA CONSUMER")
    try:
        consumed_records = consume_messages(
            expected_count=counts["kafka_published"], start_offset=start_offset
        )
    except (KafkaError, TimeoutError, RuntimeError, ValueError) as error:
        sys.exit(f"[Kafka] Consumer stage failed: {error}")
    counts["kafka_consumed"] = len(consumed_records)

    stage("STAGE 4. PANDERA VALIDATION")
    valid_df, invalid_df = validate_records(consumed_records)
    counts["valid_records"] = len(valid_df)
    counts["invalid_records"] = len(invalid_df)
    valid_df.to_json(VALID_PATH, orient="records", indent=2, force_ascii=False)
    invalid_df.to_csv(INVALID_PATH, index=False)
    if not invalid_df.empty:
        print("\nReal invalid records:")
        print(invalid_df[["unique_key", "validation_error"]].to_string(index=False))
    print("\nNegative test with 3 synthetic records (not part of the dataset):")
    demo_invalid_records()

    stage("STAGE 5. CLEANING AND TRANSFORMATION")
    cleaned_df = clean_nyc311_data(valid_df)
    cleaned_df.to_csv(CLEANED_PATH, index=False)
    print(f"[Cleaner] Saved: {CLEANED_PATH.relative_to(PROJECT_ROOT)}")
    counts["after_cleaning"] = len(cleaned_df)

    stage("STAGE 6. SQLITE LOADING + IDEMPOTENCY TEST")
    if args.reset_db and DB_PATH.exists():
        DB_PATH.unlink()
        print("[Loader] Old database deleted (--reset-db)")
    idempotency = idempotency_test(cleaned_df, DB_PATH)
    comparison = compare_counts(cleaned_df, DB_PATH)
    run_verification_queries(DB_PATH)
    counts["stored_in_sqlite"] = comparison["stored_in_table"]

    if not args.skip_eda:
        stage("STAGE 7. EXPLORATORY DATA ANALYSIS (reads from SQLite)")
        run_eda(db_path=str(DB_PATH), table_name=TABLE_NAME, output_dir=str(FIGURES_DIR))

    stage("PIPELINE SUMMARY")
    labels = [
        ("api_records", "API records collected"),
        ("kafka_published", "Kafka messages published"),
        ("kafka_consumed", "Kafka messages consumed"),
        ("valid_records", "Valid records (Pandera)"),
        ("invalid_records", "Invalid records (Pandera)"),
        ("after_cleaning", "Records after cleaning"),
        ("stored_in_sqlite", "Records stored in SQLite"),
    ]
    for key, label in labels:
        print(f"{label:<30}{counts[key]:>8}")
    print(f"{'Idempotency test':<30}{'PASSED' if idempotency['passed'] else 'FAILED':>8}")

    notes = explain_counts(counts)
    print("\nExplanation:")
    for note in notes:
        print(f"- {note}")

    summary = {
        "run_time": datetime.now().isoformat(timespec="seconds"),
        "counts": counts,
        "kafka_start_offset": start_offset,
        "idempotency": {
            "first_load": idempotency["first_load"],
            "second_load": idempotency["second_load"],
            "checksums_equal": idempotency["checksum_first"] == idempotency["checksum_second"],
        },
        "explanation": notes,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nSummary saved: {SUMMARY_PATH.relative_to(PROJECT_ROOT)}")
    print("Pipeline finished successfully.")


if __name__ == "__main__":
    main()
