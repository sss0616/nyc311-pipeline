import os
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def load_from_sqlite(db_path: str = "data/output.db", table_name: str = "service_requests") -> pd.DataFrame:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def run_eda(
    db_path: str = "data/output.db",
    table_name: str = "service_requests",
    output_dir: str = "figures",
    df_override: pd.DataFrame | None = None,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    df = df_override.copy() if df_override is not None else load_from_sqlite(db_path, table_name)

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce")

    print("=" * 60)
    print("1. DESCRIPTIVE STATISTICS")
    print("=" * 60)
    print(f"Total records: {len(df)}")
    print(f"Unique complaint types: {df['complaint_type'].nunique()}")
    print(f"Borough distribution: {df['borough'].value_counts().to_dict()}")
    print("\nNumeric summary:")
    print(df[["resolution_hours", "latitude", "longitude"]].describe().round(2))

    valid_res = df["resolution_hours"].dropna().to_numpy()
    print("\n" + "=" * 60)
    print("2. NUMPY RESOLUTION TIME METRICS (HOURS)")
    print("=" * 60)
    if len(valid_res) > 0:
        print(f"Valid closed requests: {len(valid_res)}")
        print(f"Mean:   {np.mean(valid_res):.2f} hrs")
        print(f"Median: {np.median(valid_res):.2f} hrs")
        print(f"Std:    {np.std(valid_res):.2f} hrs")
        print(f"Min:    {np.min(valid_res):.2f} hrs")
        print(f"Max:    {np.max(valid_res):.2f} hrs")
    else:
        print("No closed requests with valid resolution time.")

    brooklyn_closed_noise = df[
        (df["borough"] == "BROOKLYN")
        & (df["status"] == "Closed")
        & (df["complaint_type"].str.contains("Noise", case=False, na=False))
    ]
    print("\n" + "=" * 60)
    print("3. FILTERED SUBSET: CLOSED NOISE COMPLAINTS IN BROOKLYN")
    print("=" * 60)
    print(f"Matching records: {len(brooklyn_closed_noise)}")
    if not brooklyn_closed_noise.empty:
        print(f"Median resolution time: {brooklyn_closed_noise['resolution_hours'].median():.2f} hrs")

    print("\n" + "=" * 60)
    print("4. GROUPBY ANALYSIS BY BOROUGH")
    print("=" * 60)
    borough_summary = (
        df.groupby("borough")
        .agg(
            total_requests=("unique_key", "count"),
            closed_count=("is_closed", "sum"),
            mean_res_hours=("resolution_hours", "mean"),
            median_res_hours=("resolution_hours", "median"),
        )
        .sort_values(by="total_requests", ascending=False)
        .round(2)
    )
    print(borough_summary)

    print("\n" + "=" * 60)
    print("5. PIVOT TABLE: TOP 5 COMPLAINT TYPES BY BOROUGH")
    print("=" * 60)
    top5_types = df["complaint_type"].value_counts().nlargest(5).index
    pivot_res = pd.pivot_table(
        df[df["complaint_type"].isin(top5_types)],
        values="unique_key",
        index="complaint_type",
        columns="borough",
        aggfunc="count",
        fill_value=0,
    )
    print(pivot_res)

    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(10, 6))
    top_complaints = df["complaint_type"].value_counts().nlargest(10)
    sns.barplot(
        x=top_complaints.values,
        y=top_complaints.index,
        hue=top_complaints.index,
        palette="viridis",
        legend=False,
    )
    plt.title("Top 10 NYC 311 Complaint Types", fontsize=13, fontweight="bold")
    plt.xlabel("Number of Requests")
    plt.ylabel("Complaint Type")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "top_complaint_types.png"), dpi=300)
    plt.close()

    plt.figure(figsize=(9, 5))
    borough_counts = df[df["borough"] != "UNSPECIFIED"]["borough"].value_counts()
    sns.barplot(
        x=borough_counts.index,
        y=borough_counts.values,
        hue=borough_counts.index,
        palette="magma",
        legend=False,
    )
    plt.title("Number of NYC 311 Service Requests by Borough", fontsize=13, fontweight="bold")
    plt.xlabel("Borough")
    plt.ylabel("Number of Requests")
    plt.xticks(rotation=15)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "complaints_by_borough.png"), dpi=300)
    plt.close()

    plt.figure(figsize=(10, 6))
    closed_df = df.dropna(subset=["resolution_hours"])
    if not closed_df.empty:
        top_closed_cats = closed_df["complaint_type"].value_counts().nlargest(8).index
        res_by_cat = (
            closed_df[closed_df["complaint_type"].isin(top_closed_cats)]
            .groupby("complaint_type")["resolution_hours"]
            .median()
            .sort_values(ascending=False)
        )
        sns.barplot(
            x=res_by_cat.values,
            y=res_by_cat.index,
            hue=res_by_cat.index,
            palette="coolwarm",
            legend=False,
        )
        plt.title("Median Resolution Time (Hours) by Complaint Category", fontsize=13, fontweight="bold")
        plt.xlabel("Median Resolution Time (Hours)")
        plt.ylabel("Complaint Category")
    else:
        plt.text(0.5, 0.5, "No closed requests with resolution_hours available", ha="center", va="center")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "resolution_time_by_category.png"), dpi=300)
    plt.close()

    print(f"\n[Analysis] Charts saved to '{output_dir}/'.")


if __name__ == "__main__":
    run_eda()