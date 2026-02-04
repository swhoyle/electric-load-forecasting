"""
Add features to silver power load datasets (silver -> gold).

Input:
- Reads:
    data/silver/power_load_1m.parquet
    data/silver/power_load_5m.parquet
    data/silver/power_load_10m.parquet
  Columns:
    timestamp | avg_load

Output:
- Saves:
    data/gold/power_load_1m_all_features.parquet
    data/gold/power_load_5m_all_features.parquet
    data/gold/power_load_10m_all_features.parquet

Adds:
- Temporal features (year, quarter, month, season, day, day_of_week, hour, time_of_day)
- Business feature (is_workday)
- Historic load lags (minute + day-based lags; in terms of the dataset interval)
"""

import numpy as np
import pandas as pd

SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

INPUTS = {
    "1m": f"{SILVER_DIR}/power_load_1m.parquet",
    "5m": f"{SILVER_DIR}/power_load_5m.parquet",
    "10m": f"{SILVER_DIR}/power_load_10m.parquet",
}

OUTPUTS = {
    "1m": f"{GOLD_DIR}/power_load_1m_all_features.parquet",
    "5m": f"{GOLD_DIR}/power_load_5m_all_features.parquet",
    "10m": f"{GOLD_DIR}/power_load_10m_all_features.parquet",
}


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["timestamp"])
    df["year"] = ts.dt.year
    df["quarter"] = ts.dt.quarter
    df["month"] = ts.dt.month
    df["day"] = ts.dt.day
    df["hour"] = ts.dt.hour

    # 0 = Sunday ... 6 = Saturday
    df["day_of_week"] = (ts.dt.dayofweek + 1) % 7

    # season: 1=Winter (Dec-Feb), 2=Spring (Mar-May), 3=Summer (Jun-Aug), 4=Fall (Sep-Nov)
    m = df["month"]
    df["season"] = np.select(
        [m.isin([12, 1, 2]), m.isin([3, 4, 5]), m.isin([6, 7, 8]), m.isin([9, 10, 11])],
        [1, 2, 3, 4],
        default=np.nan,
    ).astype("int8")

    # time_of_day: 0=morning(6-11), 1=afternoon(12-16), 2=evening(17-21), 3=night(22-5)
    h = df["hour"]
    df["time_of_day"] = np.select(
        [(h >= 6) & (h <= 11), (h >= 12) & (h <= 16), (h >= 17) & (h <= 21), (h >= 22) | (h <= 5)],
        [0, 1, 2, 3],
        default=np.nan,
    ).astype("int8")

    return df


def add_business_features(df: pd.DataFrame) -> pd.DataFrame:
    # workday: Mon-Fri
    # day_of_week: 0=Sun, 1=Mon, ..., 5=Fri, 6=Sat
    df["is_workday"] = df["day_of_week"].isin([1, 2, 3, 4, 5]).astype("int8")
    return df


def add_lag_features(df: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    s = df["avg_load"]

    minute_lags = [1, 2, 3, 4, 5, 10, 15, 20, 25, 30]
    for k in minute_lags:
        steps = k // interval_minutes
        if steps >= 1 and (k % interval_minutes == 0):
            df[f"power_{k}m"] = s.shift(steps)
        else:
            df[f"power_{k}m"] = np.nan

    day_lags = [1, 2, 3, 4, 5, 7, 14, 21]
    steps_per_day = int(24 * 60 / interval_minutes)
    for d in day_lags:
        df[f"power_{d}d"] = s.shift(d * steps_per_day)

    return df


def build_gold(interval_key: str, interval_minutes: int) -> pd.DataFrame:
    print(f"\nBuilding gold features for {interval_key}...")
    df = pd.read_parquet(INPUTS[interval_key])

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    df = add_time_features(df)
    df = add_business_features(df)
    df = add_lag_features(df, interval_minutes)

    return df


def validate(df: pd.DataFrame) -> None:
    assert "timestamp" in df.columns, "Missing timestamp"
    assert "avg_load" in df.columns, "Missing avg_load"
    assert df["timestamp"].is_monotonic_increasing, "Timestamps are not sorted"
    assert not df["avg_load"].isnull().any(), "avg_load contains nulls"


def main() -> None:
    configs = [("1m", 1), ("5m", 5), ("10m", 10)]

    for key, mins in configs:
        gold = build_gold(key, mins)
        validate(gold)

        out_path = OUTPUTS[key]
        print(f"Saving gold to {out_path}...")
        gold.to_parquet(out_path, index=False)

    print("\n✅ Gold datasets created successfully")


if __name__ == "__main__":
    main()
