import numpy as np
import pandas as pd
from scipy.io import loadmat

RAW_DATA_PATH = "data/raw/P_data.mat"
BRONZE_LOAD_DATA_PATH = "data/bronze/power_load_1s.parquet"

def raw_to_bronze():
    """
    Execute the raw to bronze data pipeline.
    """
    print("Executing raw to bronze pipeline...")

    # Load Raw Data
    raw_data = loadmat(RAW_DATA_PATH)

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
    )

    # Create day-class mapping
    day_class_df = pd.DataFrame({
        "date": pd.to_datetime([d[0] for d in day_data.squeeze()]),
        "day_class": [d[0] for d in day_class.squeeze()]
    })

    # Merge day_class onto time series
    df = df.merge(day_class_df, on="date", how="left")
    df = df[["timestamp", "day_class", "load"]]

    # Final validation
    assert df["day_class"].isin(["full", "half", "none"]).all()
    assert df["timestamp"].is_monotonic_increasing
    assert df.shape[0] == p_data.size

    # Save single bronze file
    df.to_parquet(BRONZE_LOAD_DATA_PATH, index=False)

    print("Saved bronze data to:", BRONZE_LOAD_DATA_PATH)
    print(df.head())
    print(df.shape)

if __name__ == "__main__":
    raw_to_bronze()
