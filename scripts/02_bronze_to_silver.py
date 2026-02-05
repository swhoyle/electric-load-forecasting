import numpy as np
import pandas as pd

BRONZE_PATH = "data/bronze/power_load_1s.parquet"   # expects: timestamp, load, day_class
SILVER_PATH = "data/silver/power_load_1m.parquet"

# Configure your history windows here (in minutes)
LAG_MINUTES = [1, 5, 15, 60, 1440]                 # 1m, 5m, 15m, 1h, 1d
ROLLING_MINUTES = [5, 15, 60, 240, 1440]           # 5m, 15m, 1h, 4h, 1d
SLOPE_MINUTES = [5, 15, 60]                        # slope over these lookback windows (minutes)

DAY_CLASS_TO_WORKDAY = {"none": 0, "half": 1, "full": 2}


def month_to_season(month: int) -> int:
    """1=Winter(Dec-Feb), 2=Spring(Mar-May), 3=Summer(Jun-Aug), 4=Fall(Sep-Nov)"""
    if month in (12, 1, 2):
        return 1
    if month in (3, 4, 5):
        return 2
    if month in (6, 7, 8):
        return 3
    return 4  # 9,10,11


def hour_to_time_of_day(hour: int) -> int:
    """0=morning(6-11), 1=afternoon(12-16), 2=evening(17-21), 3=night(22-5)"""
    if 6 <= hour <= 11:
        return 0
    if 12 <= hour <= 16:
        return 1
    if 17 <= hour <= 21:
        return 2
    return 3


def rolling_slope(y: np.ndarray) -> float:
    """
    Slope (per minute) by fitting a straight line to the window.
    y is an array of length window where x = [0, 1, ..., window-1] minutes.
    """
    if np.any(np.isnan(y)):
        return np.nan
    x = np.arange(len(y), dtype=float)
    # slope of best-fit line (degree 1)
    m, _b = np.polyfit(x, y, 1)
    return float(m)


def bronze_to_silver():
    print("Loading bronze data...")
    df = pd.read_parquet(BRONZE_PATH)

    # Basic validation
    required_cols = {"timestamp", "load", "day_class"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Bronze file missing columns: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").set_index("timestamp")

    # 1) Resample to 1-minute and keep workday classification
    # day_class should be constant within a day, so "first" is fine per minute.
    print("Resampling to 1-minute...")
    silver = (
        df.resample("1min")
          .agg(
              avg_load=("load", "mean"),
              day_class=("day_class", "first"),
          )
    )

    # (Optional) drop minutes where avg_load is NaN (happens if whole minute missing)
    # silver = silver.dropna(subset=["avg_load"])

    # 2) Business feature: workday code
    silver["workday"] = silver["day_class"].map(DAY_CLASS_TO_WORKDAY).astype("Int64")

    # 3) Temporal features from timestamp (index)
    ts = silver.index
    silver["year"] = ts.year.astype(int)
    silver["quarter"] = ts.quarter.astype(int)
    silver["month"] = ts.month.astype(int)
    silver["day"] = ts.day.astype(int)

    # day_of_week: user wants 0=Sunday ... 6=Saturday
    # pandas: Monday=0 ... Sunday=6 -> convert:
    silver["day_of_week"] = ((ts.dayofweek + 1) % 7).astype(int)

    silver["hour"] = ts.hour.astype(int)
    silver["season"] = ts.month.map(month_to_season).astype(int)
    silver["time_of_day"] = ts.hour.map(hour_to_time_of_day).astype(int)

    # 4) Load history features
    # Lags
    for m in LAG_MINUTES:
        silver[f"lag_{m}m"] = silver["avg_load"].shift(m)

    # Rolling stats
    for w in ROLLING_MINUTES:
        r = silver["avg_load"].rolling(window=w, min_periods=w)
        silver[f"rolling_mean_{w}m"] = r.mean()
        silver[f"rolling_std_{w}m"] = r.std()
        silver[f"rolling_max_{w}m"] = r.max()
        silver[f"rolling_min_{w}m"] = r.min()

    # Delta_X: difference between lag_X and lag_1m
    # (Your text had "la_1m" — assuming it means lag_1m.)
    if 1 not in LAG_MINUTES:
        raise ValueError("LAG_MINUTES must include 1 so delta_X can use lag_1m.")
    for m in LAG_MINUTES:
        if m == 1:
            continue
        silver[f"delta_{m}m"] = silver[f"lag_{m}m"] - silver["lag_1m"]

    # Slope_X: best-fit slope over the past X minutes, using values from t-X to t-1.
    # Implemented as slope of avg_load over a rolling window of size X, then shifted by 1
    # so it's strictly "past" (doesn't include current minute).
    for w in SLOPE_MINUTES:
        silver[f"slope_{w}m"] = (
            silver["avg_load"]
            .rolling(window=w, min_periods=w)
            .apply(rolling_slope, raw=True)
            .shift(1)
        )

    # Finalize
    silver = silver.reset_index()  # bring timestamp back as a column
    silver.to_parquet(SILVER_PATH, index=False)
    print("Silver dataset created successfully:", SILVER_PATH)
    print(silver.head())
    print("Rows:", silver.shape[0], "Cols:", silver.shape[1])


if __name__ == "__main__":
    bronze_to_silver()
