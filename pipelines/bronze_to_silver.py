"""
Collapse second-level power load data into fixed time intervals.

Input:
- Reads data from "data/bronze/power_load.parquet"
  Columns:
    timestamp | load

Output:
- Saves aggregated data to:
    silver/power_load_1m.parquet
    silver/power_load_5m.parquet
    silver/power_load_10m.parquet

Each output contains:
    timestamp | avg_load
"""

import pandas as pd

BRONZE_PATH = "data/bronze/power_load.parquet"
SILVER_DIR = "data/silver"

# -------------------------------------------------
# Load bronze data
# -------------------------------------------------
print("Loading bronze data...")
df = pd.read_parquet(BRONZE_PATH)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.set_index("timestamp")

# -------------------------------------------------
# Aggregate helper
# -------------------------------------------------
def collapse(interval: str, out_name: str) -> None:
    print(f"Collapsing to {interval} intervals...")
    (
        df.resample(interval)
          .mean()
          .rename(columns={"load": "avg_load"})
          .reset_index()
          .to_parquet(f"{SILVER_DIR}/{out_name}", index=False)
    )

# -------------------------------------------------
# Generate silver datasets
# -------------------------------------------------
collapse("1min", "power_load_1m.parquet")
collapse("5min", "power_load_5m.parquet")
collapse("10min", "power_load_10m.parquet")

print("✅ Silver datasets created successfully")
