from src.validator import validate_records, demo_invalid_records

base = {
    "unique_key": "70593801",
    "created_date": "2026-10-01T00:00:21.000",
    "closed_date": None,
    "complaint_type": "Illegal Parking",
    "borough": "BROOKLYN",
    "status": "Open",
    "latitude": None,
    "longitude": None,
}

valid, invalid = validate_records([base, {**base, "unique_key": "bad-id"}])
assert len(valid) == 1
assert len(invalid) == 1
assert "bad-id" in invalid["unique_key"].tolist()

demo_invalid_records()
print("Validation smoke tests passed")
