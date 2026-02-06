"""
Script to create a date dimension table.
The date dimension will have one row per date and include features like year, month, day, etc.

Input:
- Year range (e.g. 2025-2026)

Output:
- data/silver/dim_date.parquet
"""

import pandas as pd
from datetime import date, timedelta

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

def create_date_dim(start_year: int, end_year: int) -> list[dict]:
    """
    Create a date dimension table
    """
    date_dim = []
    cur_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    while cur_date <= end_date:
        year = cur_date.year
        month = cur_date.month
        day = cur_date.day
        weekday = cur_date.weekday()
        season = month_to_season(month)
        is_weekend = weekday in {5, 6}

        date_dim.append({
            "date": cur_date,
            "year": year,
            "quarter": (month - 1) // 3 + 1,
            "season": season,
            "month": month,
            "day": day,
            "weekday": weekday,
            "is_weekend": is_weekend,
        })

        cur_date += timedelta(days=1)

    df = pd.DataFrame(date_dim)
    df.to_parquet(OUTPUT_FILEPATH, index=False)

    return date_dim

if __name__ == "__main__":
    create_date_dim(YEAR_RANGE[0], YEAR_RANGE[1])