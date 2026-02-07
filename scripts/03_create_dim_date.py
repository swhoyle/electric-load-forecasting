import pandas as pd
from datetime import date, timedelta

BRONZE_DATE_FILEPATH = "data/bronze/days.parquet"
YEAR_RANGE = (2025, 2026)
OUTPUT_FILEPATH = "data/silver/dim_date.parquet"

def month_to_season(month: int) -> int:
    """Convert month to season code: 1=Winter, 2=Spring, 3=Summer, 4=Fall"""
    if month in (12, 1, 2):
        return 1
    if month in (3, 4, 5):
        return 2
    if month in (6, 7, 8):
        return 3
    if month in (9, 10, 11):
        return 4

def create_dim_date() -> list[dict]:
    """
    Create a date dimension table
    """
    print("Loading bronze date data: {}...".format(BRONZE_DATE_FILEPATH))
    df = pd.read_parquet(BRONZE_DATE_FILEPATH)

    assert "date" in df.columns

    print("Creating date dimension...")
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["weekday"] = df["date"].dt.weekday
    df["season"] = df["month"].apply(month_to_season)
    df["is_weekend"] = df["weekday"].isin([5, 6])

    print("Saving date dimension to:", OUTPUT_FILEPATH)
    df.to_parquet(OUTPUT_FILEPATH, index=False)
    print("Shape: ", df.shape)
    print("Data Preview:")
    print(df.head())

if __name__ == "__main__":
    create_dim_date()