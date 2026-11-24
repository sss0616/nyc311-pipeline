import numpy as np
import pandas as pd


def clean_nyc311_data(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = df.copy()

    initial_count = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(subset=["unique_key"], keep="first")
    print(f"[Cleaner] Removed duplicates: {initial_count - len(cleaned_df)}")

    cleaned_df["unique_key"] = cleaned_df["unique_key"].astype(str).str.strip()
    cleaned_df["created_date"] = pd.to_datetime(cleaned_df["created_date"], errors="coerce")
    cleaned_df["closed_date"] = pd.to_datetime(cleaned_df["closed_date"], errors="coerce")

    cleaned_df["latitude"] = pd.to_numeric(cleaned_df["latitude"], errors="coerce")
    cleaned_df["longitude"] = pd.to_numeric(cleaned_df["longitude"], errors="coerce")

    cleaned_df = cleaned_df.dropna(subset=["unique_key", "created_date", "complaint_type"])

    cleaned_df["borough"] = cleaned_df["borough"].fillna("UNSPECIFIED")
    cleaned_df["status"] = cleaned_df["status"].fillna("Unknown")

    if "descriptor" in cleaned_df.columns:
        cleaned_df["descriptor"] = cleaned_df["descriptor"].fillna("No Description").astype(str).str.strip()
    if "agency" in cleaned_df.columns:
        cleaned_df["agency"] = cleaned_df["agency"].fillna("UNKNOWN").astype(str).str.strip().str.upper()

    cleaned_df["borough"] = (
        cleaned_df["borough"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"NAN": "UNSPECIFIED", "NONE": "UNSPECIFIED", "": "UNSPECIFIED"})
    )
    cleaned_df["status"] = cleaned_df["status"].astype(str).str.strip().str.title()
    cleaned_df["complaint_type"] = cleaned_df["complaint_type"].astype(str).str.strip().str.title()

    time_diff = cleaned_df["closed_date"] - cleaned_df["created_date"]
    cleaned_df["resolution_hours"] = time_diff.dt.total_seconds() / 3600.0
    cleaned_df.loc[cleaned_df["resolution_hours"] < 0, "resolution_hours"] = np.nan

    cleaned_df["created_date_only"] = cleaned_df["created_date"].dt.strftime("%Y-%m-%d")
    cleaned_df["created_hour"] = cleaned_df["created_date"].dt.hour
    cleaned_df["is_closed"] = cleaned_df["closed_date"].notna().astype(int)

    cleaned_df["created_date"] = cleaned_df["created_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    cleaned_df["closed_date"] = cleaned_df["closed_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    cleaned_df = cleaned_df.reset_index(drop=True)
    print(f"[Cleaner] Cleaned records ready for SQLite: {len(cleaned_df)}")
    return cleaned_df