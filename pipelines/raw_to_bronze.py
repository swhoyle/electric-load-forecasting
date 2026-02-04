"""
Convert the raw MATLAB power load data to a two-column time series.

Input:
- Reads data from "/data/raw/P_data.mat":

Raw Data Structure:
- P_data: numpy array of shape (86400, 31) (power in W for each second in day, days)
- day_data: numpy array of shape (1, 31) with date strings for each day (e.g., "2020-01-01")

Output:
- Saves data to "raw/bronze/power_load_timeseries.parquet"
- pandas DataFrame with columns:
    timestamp | load
"""

import numpy as np
import pandas as pd
from scipy.io import loadmat

RAW_DATA_PATH = "data/raw/P_data.mat"
BRONZE_LOAD_DATA_PATH = "data/bronze/power_load.parquet"
BRONZE_DAY_CLASS_DATA_PATH = "data/bronze/days.parquet"

def raw_to_bronze():
    """
    Execute the raw to bronze data pipeline.
    """
    print("Executing raw to bronze pipeline...")

    # Load Raw Data
    print("Loading raw data from", RAW_DATA_PATH)
    raw_data = loadmat(RAW_DATA_PATH)

    # Extract relevant data
    p_data = raw_data.get("P_data")
    day_data = raw_data.get("day_data")
    day_class = raw_data.get("day_class")

    # Validate presence of required data
    print("Validating raw data...")
    if p_data is None:
        raise ValueError("P_data not found in raw data.")
    if day_data is None:
        raise ValueError("day_data not found in raw data.")
    if day_class is None:
        raise ValueError("day_class not found in raw data.")

    assert p_data.shape[0] == 86400, f"Expected 86400 seconds per day"
    assert p_data.shape[1] == day_data.shape[1], "Mismatch in number of days between P_data and day_data"
    assert p_data.shape[1] == day_class.shape[1], "Mismatch in number of days between P_data and day_class"
    assert all(v in ["full", "half", "none"] for v in day_class.squeeze()), "Invalid values in day_class"

    # Restructure Raw Data into Time Series DataFrame
    # One row per second.
    # Shape will be (d*86400, d) where d is number of days.
    #|---------------------|-------|
    #| timestamp           | load  |
    #|---------------------|-------|
    #| 2025-11-31 00:00:00 | 1000. |
    #| 2025-11-31 00:00:01 | 1820. |
    # ...
    #| 2025-12-31 11:59:59 | 927.  |
    #|---------------------|-------|
    print("Restructuring raw data into time series DataFrame...")
    df = (
        pd.DataFrame(p_data, columns=pd.to_datetime([d[0] for d in day_data.squeeze()]))
        .assign(second_of_day=lambda x: np.arange(len(x)))
        .melt("second_of_day", var_name="date", value_name="load")
        .assign(date=lambda x: pd.to_datetime(x["date"]))
        .assign(timestamp=lambda x: x["date"] + pd.to_timedelta(x["second_of_day"], unit="s"))
        .sort_values("timestamp")[["timestamp", "load"]]
        .reset_index(drop=True)
    )

    # Validate / Quality Check
    print("Validating new DataFrame...")
    assert df.columns.tolist() == ["timestamp", "load"], "DataFrame columns are incorrect"
    assert np.issubdtype(df.dtypes["load"], np.number), "Load column is not numeric"
    assert df.dtypes["timestamp"] == "datetime64[ns]", "Timestamp column is not datetime"
    expected_rows = p_data.shape[0] * p_data.shape[1]
    assert df.shape[0] == expected_rows, f"Expected {expected_rows} rows, got {df.shape[0]}"
    assert df["timestamp"].is_monotonic_increasing, "Timestamps are not sorted"
    midnight_loads = df[df["timestamp"].dt.time == pd.Timestamp("00:00:00").time()]["load"].to_numpy()
    noon_loads = df[df["timestamp"].dt.time == pd.Timestamp("12:00:00").time()]["load"].to_numpy()
    assert np.array_equal(noon_loads, p_data[43200, :]), "Noon loads do not match p_data[43200, :]."
    assert np.array_equal(p_data[0, :], midnight_loads), "Midnight loads do not match p_data[0, :]."

    # Create day class data
    print("Creating day class data...")
    day_class_df = pd.DataFrame({
        "date": pd.to_datetime([d[0] for d in day_data.squeeze()]),
        "day_class": [d[0] for d in day_class.squeeze()]
    })

    print("Validating day class DataFrame...")
    assert day_class_df.columns.tolist() == ["date", "day_class"], "Day Class DataFrame columns are incorrect"
    assert day_class_df.dtypes["date"] == "datetime64[ns]", "Date column is not datetime"
    assert day_class_df.dtypes["day_class"] == "object", "Day class column is not object"
    assert all(v in ["full", "half", "none"] for v in day_class_df["day_class"]), "Invalid values in day_class DataFrame"
    assert day_class_df.shape[0] == df["timestamp"].dt.date.nunique(), "Mismatch in number of days in day_class DataFrame"
    assert day_class_df["date"].is_monotonic_increasing, "Dates in day_class DataFrame are not sorted"
    assert day_class_df["date"].isin(df["timestamp"].dt.normalize()).all(), "Dates in day_class DataFrame do not match timestamps in main DataFrame"

    # Save to bronze layer
    print("Saving data to bronze layer...")
    df.to_parquet(BRONZE_LOAD_DATA_PATH, index=False)
    day_class_df.to_parquet(BRONZE_DAY_CLASS_DATA_PATH, index=False)
    print("Data saved successfully to", BRONZE_LOAD_DATA_PATH, "and", BRONZE_DAY_CLASS_DATA_PATH)

    # Data preview
    print("\nPreview of power load time series data:")
    print(df.head())
    print(df.shape)
    print("\nPreview of day class data:")
    print(day_class_df.head())
    print(day_class_df.shape)

if __name__ == "__main__":
    raw_to_bronze()