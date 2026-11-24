import requests
import json
from pathlib import Path
from datetime import date, timedelta

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

SAMPLE_DAYS = [
    "2026-10-01",
    "2026-10-08",
    "2026-10-15",
    "2026-10-22",
    "2026-10-29"
]

FIELDS = [
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "descriptor",
    "borough",
    "status",
    "agency",
    "latitude",
    "longitude"
]

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "raw_data.json"


def collect_data():
    records = []

    for day in SAMPLE_DAYS:
        start_date = date.fromisoformat(day)
        end_date = start_date + timedelta(days=1)

        params = {
            "$select": ",".join(FIELDS),
            "$where": (
                f"created_date >= '{start_date}T00:00:00' "
                f"AND created_date < '{end_date}T00:00:00'"
            ),
            "$order": "created_date ASC, unique_key ASC",
            "$limit": 100
        }

        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=30
            )

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError("Unexpected API response format")

            records.extend(data)
            print(f"{day}: collected {len(data)} records")

        except (requests.RequestException, ValueError) as error:
            print(f"API error for {day}: {error}")
            raise

    if len(records) < 200:
        raise ValueError("Less than 200 records collected")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)

    print(f"\nTotal collected records: {len(records)}")
    print(f"Raw data saved to: {OUTPUT_PATH}")

    return records


if __name__ == "__main__":
    collect_data()
