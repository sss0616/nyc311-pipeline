import pandas as pd
import pandera.pandas as pa

ALLOWED_BOROUGHS = {
    "BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"
}
REQUIRED_COLUMNS = (
    "unique_key", "created_date", "complaint_type", "borough", "status"
)
OPTIONAL_COLUMNS = ("closed_date", "latitude", "longitude")


def _not_blank(series):
    return series.astype("string").str.strip().str.len().gt(0).fillna(False)


def _valid_datetime(series):
    return pd.to_datetime(series, errors="coerce").notna()


def _valid_optional_datetime(series):
    return series.isna() | _valid_datetime(series)


def _coordinate_in_range(series, minimum, maximum):
    converted = pd.to_numeric(series, errors="coerce")
    return series.isna() | converted.between(minimum, maximum)


def _closed_not_before_created(df):
    created = pd.to_datetime(df["created_date"], errors="coerce")
    closed = pd.to_datetime(df["closed_date"], errors="coerce")
    return closed.isna() | created.isna() | (closed >= created)


SCHEMA = pa.DataFrameSchema(
    {
        "unique_key": pa.Column(
            object,
            pa.Check(lambda s: s.astype("string").str.fullmatch(r"\d+").fillna(False),
                     error="unique_key must contain digits"),
            nullable=False,
        ),
        "created_date": pa.Column(
            object,
            pa.Check(_valid_datetime, error="created_date must be a valid datetime"),
            nullable=False,
        ),
        "complaint_type": pa.Column(
            object,
            pa.Check(_not_blank, error="complaint_type must not be blank"),
            nullable=False,
        ),
        "borough": pa.Column(
            object,
            pa.Check(
                lambda s: s.astype("string").str.strip().str.upper().isin(ALLOWED_BOROUGHS),
                error="borough must be an NYC borough",
            ),
            nullable=False,
        ),
        "status": pa.Column(
            object,
            pa.Check(_not_blank, error="status must not be blank"),
            nullable=False,
        ),
        "closed_date": pa.Column(
            object,
            pa.Check(_valid_optional_datetime, error="closed_date must be a valid datetime if present"),
            nullable=True,
        ),
        "latitude": pa.Column(
            object,
            pa.Check(lambda s: _coordinate_in_range(s, 40, 41),
                     error="latitude must be between 40 and 41"),
            nullable=True,
        ),
        "longitude": pa.Column(
            object,
            pa.Check(lambda s: _coordinate_in_range(s, -75, -73),
                     error="longitude must be between -75 and -73"),
            nullable=True,
        ),
    },
    checks=[
        pa.Check(_closed_not_before_created,
                 error="closed_date must not precede created_date")
    ],
    strict=False,
)


def validate_records(records):
    if not isinstance(records, list) or not records:
        raise ValueError("Expected a non-empty list of Kafka JSON records")
    if any(not isinstance(record, dict) for record in records):
        raise ValueError("Every Kafka message must be a JSON object")

    original = pd.DataFrame.from_records(records)
    missing = set(REQUIRED_COLUMNS) - set(original.columns)
    if missing:
        raise ValueError(f"Entire dataset is missing mandatory columns: {sorted(missing)}")

    prepared = original.copy()
    for column in OPTIONAL_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = None

    for column in SCHEMA.columns:
        prepared[column] = prepared[column].astype(object)

    failures = {}
    try:
        SCHEMA.validate(prepared, lazy=True)
    except pa.errors.SchemaErrors as exc:
        for _, issue in exc.failure_cases.iterrows():
            row_index = issue["index"]
            if pd.isna(row_index):
                raise RuntimeError(
                    f"Dataset-level validation issue: {issue['check']}"
                ) from exc
            row_index = int(row_index)
            label = f"{issue['column']}: {issue['check']}"
            failures.setdefault(row_index, []).append(label)

    rejected_indices = set(failures)
    valid_df = prepared.loc[
        [i for i in prepared.index if i not in rejected_indices]
    ].copy().reset_index(drop=True)
    invalid_df = prepared.loc[
        [i for i in prepared.index if i in rejected_indices]
    ].copy()
    invalid_df["validation_error"] = [
        "; ".join(dict.fromkeys(failures[i])) for i in invalid_df.index
    ]
    invalid_df = invalid_df.reset_index(drop=True)

    print(f"Records received: {len(original)}")
    print(f"Valid records: {len(valid_df)}")
    print(f"Invalid records: {len(invalid_df)}")
    return valid_df, invalid_df


def demo_invalid_records():
    base = {
        "unique_key": "70593801",
        "created_date": "2026-10-01T00:00:21.000",
        "closed_date": "2026-10-01T02:55:10.000",
        "complaint_type": "Illegal Parking",
        "borough": "BROOKLYN",
        "status": "Closed",
        "latitude": "40.68",
        "longitude": "-73.99",
    }
    wrong_id = {**base, "unique_key": "bad-id"}
    wrong_borough = {**base, "borough": "UNKNOWN"}
    wrong_date = {**base, "created_date": "not-a-date"}

    valid_df, invalid_df = validate_records(
        [wrong_id, wrong_borough, wrong_date]
    )
    assert len(valid_df) == 0 and len(invalid_df) == 3
    print("\nThree deliberately invalid test records:")
    print(invalid_df[["unique_key", "validation_error"]].to_string(index=False))


if __name__ == "__main__":
    from consumer import consume_messages

    kafka_records = consume_messages(expected_count=500)
    valid, invalid = validate_records(kafka_records)
    if not invalid.empty:
        print("\nReal invalid records:")
        print(invalid[["unique_key", "validation_error"]].to_string(index=False))

    print("\nSynthetic negative test")
    demo_invalid_records()
