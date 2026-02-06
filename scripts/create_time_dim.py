"""
Script to create a time dimension table.
The time dimension will have one row per second and include features like hour, minute, second, etc.

Input:
- Time range (e.g. 00:00:00 to 23:59:59)

Output:
- data/silver/dim_time.parquet
"""

import pandas as pd

OUTPUT_FILEPATH = "data/silver/dim_time.parquet"

def hour_to_time_of_day(hour: int) -> int:
    """0=morning(6-11), 1=afternoon(12-16), 2=evening(17-21), 3=night(22-5)"""
    if 6 <= hour <= 11:
        return 0
    if 12 <= hour <= 16:
        return 1
    if 17 <= hour <= 21:
        return 2
    return 3

def create_time_dim() -> list[dict]:
    """
    Create a time dimension table
    """
    print("Creating time dimension...")
    time_dim = []
    for totalsecond in range(24 * 60 * 60):

        hour = totalsecond // 3600
        minute = (totalsecond % 3600) // 60
        second = totalsecond % 60
        time_dim.append({
            "time": f"{hour:02d}:{minute:02d}:{second:02d}",
            "hour": hour,
            "minute": minute,
            "second": second,
            "time_of_day": hour_to_time_of_day(hour),
        })

    df = pd.DataFrame(time_dim)
    df.to_parquet(OUTPUT_FILEPATH, index=False)
    print("Time dimension saved to", OUTPUT_FILEPATH)
    print("Data Shape:", df.shape)
    print("Data Preview:")
    print(df.head())

    return time_dim

if __name__ == "__main__":
    create_time_dim()