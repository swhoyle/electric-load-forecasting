import numpy as np
import pandas as pd

BRONZE_LOAD_FILEPATH = "data/bronze/powerload_1s.parquet"
SILVER_PATH = "data/silver/"
DOWNSAMPLE_CONFIG = {
    "10S": "powerload_10s.parquet",
    "30S": "powerload_30s.parquet",
    "1min": "powerload_1m.parquet",
    "5min": "powerload_5m.parquet",
    "10min": "powerload_10m.parquet",
    "15min": "powerload_15m.parquet"
}

def downsample_and_save(df: pd.DataFrame, freq: str, path: str):
    """Downsample the load data and save to parquet."""
    print("Downsampling data to frequency:", freq)
    downsampled = (
        df.resample(freq)
          .agg(
              avg_load=("load", "mean")
          )
    )
    downsampled.to_parquet(path)
    print(f"\nSaved to {path}")
    print("Shape: ", downsampled.shape)
    print("Data Preview:")
    print(downsampled.head())

def downsample():
    print("Loading bronze data...")
    df = pd.read_parquet(BRONZE_LOAD_FILEPATH)

    # Basic validation
    assert list(df.columns) == ["timestamp", "load"]

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").set_index("timestamp")

    # Save original 1s data to silver as well
    df.to_parquet(SILVER_PATH + "powerload_1s.parquet") 

    # Downsample and save to silver
    for freq, filename in DOWNSAMPLE_CONFIG.items():
        downsample_and_save(df, freq, SILVER_PATH + filename)

if __name__ == "__main__":
    downsample()
