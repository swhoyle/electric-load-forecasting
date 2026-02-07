import numpy as np
import pandas as pd
from scipy.io import loadmat

RAW_FILEPATH = "data/raw/P_data.mat"
BRONZE_LOAD_FILEPATH = "data/bronze/powerload_1s.parquet"
BRONZE_DATE_FILEPATH = "data/bronze/days.parquet"

def process_raw():
    """
    Execute the raw to bronze data pipeline.
    """
    print("Executing raw to bronze pipeline...")

    # Load Raw Data
    print("Loading raw data from:", RAW_FILEPATH)
    raw_data = loadmat(RAW_FILEPATH)

    print("Cleaning and restructuring data...")
    p_data = raw_data.get("P_data")
    day_data = raw_data.get("day_data")
    day_class = raw_data.get("day_class")

    # Validate
    assert p_data is not None
    assert day_data is not None
    assert day_class is not None
    assert p_data.shape[0] == 86400
    assert p_data.shape[1] == day_data.shape[1] == day_class.shape[1]

    # Build base time series
    df = (
        pd.DataFrame(
            p_data,
            columns=pd.to_datetime([d[0] for d in day_data.squeeze()])
        )
        .assign(second_of_day=np.arange(86400))
        .melt(
            id_vars="second_of_day",
            var_name="date",
            value_name="load"
        )
        .assign(
            date=lambda x: pd.to_datetime(x["date"]),
            timestamp=lambda x: x["date"]
            + pd.to_timedelta(x["second_of_day"], unit="s")
        )
        .sort_values("timestamp")
        .reset_index(drop=True)
    )[["timestamp", "load"]]

    # Create Date Dimension
    date_df = pd.DataFrame({
        "date": pd.to_datetime([d[0] for d in day_data.squeeze()]),
        "day_class": [d[0] for d in day_class.squeeze()]
    }).rename(columns={"day_class": "workday"})

    # Validation
    assert date_df["workday"].isin(["full", "half", "none"]).all()
    assert len(date_df) == df["timestamp"].dt.date.nunique()
    assert date_df["date"].dt.date.min() == df["timestamp"].dt.date.min()
    assert date_df["date"].dt.date.max() == df["timestamp"].dt.date.max()

    # Save files
    print("Saving data...")
    df.to_parquet(BRONZE_LOAD_FILEPATH, index=False)
    print("Saved data to:", BRONZE_LOAD_FILEPATH)
    print("Data Shape:", df.shape)
    print("Sample Data:")
    print(df.head())

    date_df.to_parquet(BRONZE_DATE_FILEPATH, index=False)
    print("Saved data to:", BRONZE_DATE_FILEPATH)
    print("Data Shape:", date_df.shape)
    print("Sample Data:")
    print(date_df.head())

if __name__ == "__main__":
    process_raw()
