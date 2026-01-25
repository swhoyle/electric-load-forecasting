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
raw = loadmat(RAW_DATA_PATH)
p_data = raw["P_data"]          # shape: (seconds, days)
day_data = raw["day_data"]      # shape: (1, days)


# -------------------------------------------------
# Restructure Raw Data
# -------------------------------------------------

# One row per second.
# Shape will be (days*86400, 2) = (31*86400, 2) = (2678400, 2)
    
#|---------------------|-------|
#| timestamp           | load  |
#|---------------------|-------|
#| 2025-11-31 00:00:00 | 1000. |
#| 2025-11-31 00:00:01 | 1820. |
# ...
#| 2025-12-31 11:59:59 | 927.  |
#|---------------------|-------|

df = (
    pd.DataFrame(p_data, columns=pd.to_datetime([d[0] for d in day_data.squeeze()]))
      .assign(second_of_day=lambda x: np.arange(len(x)))
      .melt("second_of_day", var_name="date", value_name="load")
      .assign(date=lambda x: pd.to_datetime(x["date"]))
      .assign(timestamp=lambda x: x["date"] + pd.to_timedelta(x["second_of_day"], unit="s"))
      .sort_values("timestamp")[["timestamp", "load"]]
      .reset_index(drop=True)
)

# -------------------------------------------------
# Validate / Quality Check
# -------------------------------------------------
print("Validating dataframe...")
expected_rows = p_data.shape[0] * p_data.shape[1]
assert df.shape[0] == expected_rows, f"Expected {expected_rows} rows, got {df.shape[0]}"
assert df["timestamp"].is_monotonic_increasing, "Timestamps are not sorted!"
assert not df["load"].isnull().any(), "Load column contains null values!"
midnight_loads = df[df["timestamp"].dt.time == pd.Timestamp("00:00:00").time()]["load"].to_numpy()
noon_loads = df[df["timestamp"].dt.time == pd.Timestamp("12:00:00").time()]["load"].to_numpy()
assert np.array_equal(noon_loads, p_data[43200, :]), "Noon loads do not match p_data[43200, :]."
assert np.array_equal(p_data[0, :], midnight_loads), "Midnight loads do not match p_data[0, :]."
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


