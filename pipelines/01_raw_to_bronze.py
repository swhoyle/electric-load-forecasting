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
BRONZE_DATA_PATH = "data/bronze/power_load.parquet"

# -------------------------------------------------
# Load MATLAB file
# -------------------------------------------------
print("Loading raw data...")
mat = loadmat(RAW_DATA_PATH)
P_data = mat["P_data"]          # shape: (seconds, days)
day_data = mat["day_data"]      # shape: (1, days)


# -------------------------------------------------
# Clean day_data into Python datetime objects
# -------------------------------------------------
print("Cleaning day data...")
dates = [d[0] for d in day_data.squeeze()]
dates = pd.to_datetime(dates)


# -------------------------------------------------
# Create seconds-of-day vector (0 → 86399)
# -------------------------------------------------
print("Creating seconds-of-day vector...")
seconds = np.arange(P_data.shape[0]) 


# -------------------------------------------------
# Build long (tidy) dataframe
# -------------------------------------------------
print("Building dataframe...")
df = (
    pd.DataFrame(P_data, columns=dates)
      .assign(second_of_day=seconds)
      .melt(
          id_vars="second_of_day",
          var_name="date",
          value_name="load"
      )
)


# -------------------------------------------------
# Create timestamp column
# -------------------------------------------------
print("Creating timestamp column...")
df["date"] = pd.to_datetime(df["date"], errors="raise")
df["timestamp"] = df["date"] + pd.to_timedelta(df["second_of_day"], unit="s")

# -------------------------------------------------
# Final two-column dataframe sorted by timestamp
# -------------------------------------------------
print("Finalizing dataframe...")
df = (
    df[["timestamp", "load"]]
    .sort_values("timestamp")
    .reset_index(drop=True)
)

# -------------------------------------------------
# Validate
# -------------------------------------------------
print("Validating dataframe...")
expected_rows = P_data.shape[0] * P_data.shape[1]
assert df.shape[0] == expected_rows, f"Expected {expected_rows} rows, got {df.shape[0]}"
assert df["timestamp"].is_monotonic_increasing, "Timestamps are not sorted!"
assert not df["load"].isnull().any(), "Load column contains null values!"
midnight_loads = df[df["timestamp"].dt.time == pd.Timestamp("00:00:00").time()]["load"].to_numpy()
noon_loads = df[df["timestamp"].dt.time == pd.Timestamp("12:00:00").time()]["load"].to_numpy()
assert np.array_equal(noon_loads, P_data[43200, :]), "Noon loads do not match P_data[43200, :]."
assert np.array_equal(P_data[0, :], midnight_loads), "Midnight loads do not match P_data[0, :]."
print("✅ Data validation passed!")

# -------------------------------------------------
# Save to bronze layer
# -------------------------------------------------
print("Saving to bronze layer...")
df.to_parquet(BRONZE_DATA_PATH, index=False)
print("✅ Data saved successfully to", BRONZE_DATA_PATH)

# -------------------------------------------------
# Preview
# -------------------------------------------------
print("\n🔍 Data Preview:")
print("\nShape:", df.shape)
print(df.dtypes)
print(df.head())
print(df.tail())


