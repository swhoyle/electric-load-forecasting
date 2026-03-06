import pandas as pd

SILVER_LOAD_FILEPATH = "data/silver/powerload_1m.parquet"
SILVER_DATE_FILEPATH = "data/silver/dim_date.parquet"
SILVER_TIME_FILEPATH = "data/silver/dim_time.parquet"
GOLD_LOAD_FILEPATH = "data/gold/powerload_1m.parquet"

SEASON_MAPPING = {1: "Winter", 2: "Spring", 3: "Summer", 4: "Fall"}
MONTH_MAPPING = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                 7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
DAY_OF_WEEK_MAPPING = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}
WORKDAY_MAPPING = {0: "Non-Workday", 1: "Half-Workday", 2: "Full-Workday"}
TIME_OF_DAY_MAPPING = {0: "Night", 1: "Morning", 2: "Afternoon", 3: "Evening"}

def create_gold_file():
    print("Creating gold file...")
    print(f"Reading silver file from {SILVER_LOAD_FILEPATH}...")
    df = pd.read_parquet(SILVER_LOAD_FILEPATH)
    print(f"Reading date dimension from {SILVER_DATE_FILEPATH}...")
    date_df = pd.read_parquet(SILVER_DATE_FILEPATH)
    print(f"Reading time dimension from {SILVER_TIME_FILEPATH}...")
    time_df = pd.read_parquet(SILVER_TIME_FILEPATH)
    print()

    print("Processing data...")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df["time"] = df["timestamp"].dt.time
    date_df["date"] = pd.to_datetime(date_df["date"]).dt.date
    df = df.merge(date_df, on="date", how="left")
    time_df["time"] = pd.to_datetime(time_df["time"], format="%H:%M:%S").dt.time
    df = df.merge(time_df, on="time", how="left")

    #df["season_name"] = df["season"].map(SEASON_MAPPING)
    #df["month_name"] = df["month"].map(MONTH_MAPPING)
    #df["weekday_name"] = df["weekday"].map(DAY_OF_WEEK_MAPPING)
    #df["workday_name"] = df["workday"].map(WORKDAY_MAPPING)
    #df["time_of_day_name"] = df["time_of_day"].map(TIME_OF_DAY_MAPPING)

    print(f"Saving gold file to {GOLD_LOAD_FILEPATH}...")
    df.to_parquet(GOLD_LOAD_FILEPATH, index=False)
    print("Shape:", df.shape)
    print("Data Preview:")
    print(df.head())

if __name__ == "__main__":
    create_gold_file()