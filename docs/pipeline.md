# Data Pipeline

This document provides information about our data pipeline framework.

We split our data storage into several layers:

| Layer | Description |
| - | - |
| [**📄 Raw**](#-raw-layer) | Raw data provided by the customer |
| [**🥉 Bronze**](#-bronze-layer) | Raw data provided by the customer |
| [**🥈 Silver**](#-silver-layer) | Cleaned and transformed data |
| [**🥇 Gold**](#-gold-layer) | Business-ready data |
| [**⚙️ Model**](#️-model-layer) | Model prepared data


| Layer | Dataset | Description |
| - | - | - |
| **📄 Raw** | [`raw/P_data.mat`](#⚪-dataset-rawp_datamat)| Raw data file provided by the customer |
| **🥉 Bronze** | [`bronze/powerload_1s.parquet`](#🟠-dataset-bronzepowerload_1sparquet) | 1-second resolution power load|
| **🥉 Bronze** | [`bronze/dim_date.parquet`](#🟠-dataset-bronzedim_date.parquet) | Date dimension|
| **🥈 Silver** | [`silver/powerload_1s.parquet`](#⚪-dataset-bronzepowerload_1sparquet) | 1-second resolution power load|
| **🥈 Silver**| [`silver/powerload_10s.parquet`](#⚪-dataset-silverpowerload_1sparquet) | 10-second resolution power load|
| **🥈 Silver**| [`silver/powerload_30s.parquet`](#⚪-dataset-silverpowerload_30sparquet) | 30-second resolution power load|
| **🥈 Silver**| [`silver/powerload_1m.parquet`](#⚪-dataset-silverpowerload_1mparquet) | 1-minute resolution power load|
| **🥈 Silver**| [`silver/powerload_2m.parquet`](#⚪-dataset-silverpowerload_2mparquet) | 2-minute resolution power load|
| **🥈 Silver**| [`silver/powerload_5m.parquet`](#⚪-dataset-silverpowerload_5mparquet) | 5-minute resolution power load|
| **🥈 Silver**| [`silver/powerload_10m.parquet`](#⚪-dataset-silverpowerload_10mparquet) | 10-minute resolution power load|
| **🥈 Silver**| [`silver/powerload_15m.parquet`](#⚪-dataset-silverpowerload_15mparquet) | 15-minute resolution power load|
| **🥈 Silver**| [`silver/dim_date.parquet`](#⚪-dataset-silverdim_date.parquet) | Date dimension|

## 📄 Raw Layer

The raw layer contains raw, untouched data provided by the customer.

### ⚪ Dataset: `raw/P_data.mat`

Raw dataset provided by customer.

```
{
 'P_data': [
    [1925., ..., 1318.],
    ...,
    [1249., ..., 1463.]
 ],
 'day_data': [[
    ['2025/11/28'],
    ['2025/11/29'],
    ...
    ['2025/12/28']
 ]],
 'day_class': [[
    ['full'],
    ['half'],
    ...
    ['none']
]]
}

```

For this project we will assume the following assumptions about the raw data:

1. Data is provided as a `.mat` file
2. Data has two attributes: `P_data`, `day_data`, and `day_class`.
3. `P_data` contains an array of size (86400, d) where 86400 represents the number of seconds in a day and d represents the number of days of data provided.
4. The values of `P_data` are integers, representing the power load, in watts, recorded at a specific time of the day.
5. A power load value of `0` is assumed to be a valid power load of 0 watts.
6. If the power load recorded failed or is invalid, the value will be represented as `NaN`.
7. `day_data` and `day_class` contain an array of size (1, d) where d represents the number of days of data provided.
8. The values of `day_data` are dates, representing the corresponding date in `P_data` (based on the order / index)
9. The values of `day_class` are strings, representing the classification of the bussiness day, provided by the customer. The options are:
- `full`: Full working day
- `half`: Half working day
- `none`: Not a working day
10. We only are given one data file per customer with all power load data / days used to predict. We are not implementing any daily incremental load of data at the moment.

## 🥉 Bronze Layer

The bronze layer contains minimal cleaning and restructuring of raw data.


#### 🟠 Dataset: `bronze/powerload_1s.parquet`

1-second resolution power load data, in watts.

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 1023 |
| 2025-11-28 00:00:01   | nan  |
| ...                   |      |
| 2025-12-28 11:59:59   | 0  |


#### 🟠 Dataset: `bronze/dim_date.parquet`

Date dimension table with workday type field.

| date       | workday |
| ---------  | ---- |
| 2025-11-28 | full |
| 2025-11-29 | half |
| ...        |      |
| 2025-12-28 | none |

## 🥈 Silver Layer

Cleaned and transformed data

#### ⚪ Dataset: `silver/powerload_1s.parquet`
1-second resolution power load data

> Size = 86,400 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 1023 |
| 2025-11-28 00:00:01   | nan  |
| ...                   |      |
| 2025-12-28 11:59:59   | 0  |

#### ⚪ Dataset: `silver/powerload_10s.parquet`
10-second resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:00:09 AM

> Size = 8,640 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:00:10   | nan  |
| ...                   |      |
| 2025-12-28 11:59:50   | 0  |

#### ⚪ Dataset: `silver/powerload_30s.parquet`
30-second resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:00:29 AM

> Size = 2,880 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:00:30   | nan  |
| ...                   |      |
| 2025-12-28 11:59:30   | 0  |

#### ⚪ Dataset: `silver/powerload_1m.parquet`

30-second resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:00:59 AM

> Size = 1,440 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:01:00   | nan  |
| ...                   |      |
| 2025-12-28 11:59:00   | 0  |

#### ⚪ Dataset: `silver/powerload_2m.parquet`

2-minute resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:01:59 AM

> Size = 720 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:02:00   | nan  |
| ...                   |      |
| 2025-12-28 11:58:00   | 0  |

#### ⚪ Dataset: `silver/powerload_5m.parquet`

5-minute resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:04:59 AM

> Size = 288 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:05:00   | nan  |
| ...                   |      |
| 2025-12-28 11:55:00   | 0  |

#### ⚪ Dataset: `silver/powerload_10m.parquet`

10-minute resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:09:59 AM

> Size = 144 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:10:00   | nan  |
| ...                   |      |
| 2025-12-28 11:50:00   | 0  |


#### ⚪ Dataset: `silver/powerload_15m.parquet`

15-minute resolution power load data

> Timestamp 2025-11-28 00:00:00 represents average load between 12:00:00 AM - 12:014:59 AM

> Size = 96 * X days

| timestamp             | load |
| ---------             | ---- |
| 2025-11-28 00:00:00   | 730 |
| 2025-11-28 00:015:00   | nan  |
| ...                   |      |
| 2025-12-28 11:45:00   | 0  |

#### ⚪ Dataset: `silver/dim_date.parquet`

Date dimension table with one row per calendar date and derived date attributes.

| Column       | Type    | Description |
|-------------|---------|-------------|
| `date`        | date    | Calendar date |
| `workday`     | string  | Workday type: `full`, `half`, `none` |
| `year`        | int     | Calendar year |
| `quarter`     | int     | Quarter of year (1–4) |
| `season`      | int     | Season code (1=Winter, 2=Spring, 3=Summer, 4=Fall) |
| `month`       | int     | Month of year (1–12) |
| `day`         | int     | Day of month (1–31) |
| `weekday`     | int     | Day of week (0=Mon … 6=Sun) |
| `is_weekend`  | bool    | True if Saturday or Sunday |

**Example rows**

| date       | workday | year | quarter | season | month | day | weekday | is_weekend |
|-----------|---------|------|---------|--------|-------|-----|---------|------------|
| 2025-11-28 | full | 2025 | 4 | 4 | 11 | 28 | 4 | false |
| 2025-11-29 | half | 2025 | 4 | 4 | 11 | 29 | 5 | true |
| 2025-12-28 | none | 2025 | 4 | 1 | 12 | 28 | 6 | true |


#### ⚪ Dataset: `silver/dim_time.parquet`

Time dimension table with one row per second of day and basic time attributes.

| Column        | Type   | Description |
|--------------|--------|-------------|
| time         | string | Time of day in `HH:MM:SS` format |
| hour         | int    | Hour of day (0–23) |
| minute       | int    | Minute of hour (0–59) |
| second       | int    | Second of minute (0–59) |
| time_of_day  | int    | Time of day bucket: 0=morning (6–11), 1=afternoon (12–16), 2=evening (17–21), 3=night (22–5) |

**Example rows**

| time     | hour | minute | second | time_of_day |
|---------|------|--------|--------|-------------|
| 00:00:00 | 0 | 0 | 0 | 3 |
| 07:30:00 | 7 | 30 | 0 | 0 |
| 12:00:00 | 12 | 0 | 0 | 1 |
| 18:15:00 | 18 | 15 | 0 | 2 |
| 23:59:59 | 23 | 59 | 59 | 3 |

## 🥇 Gold Layer

## ⚙️ Model Layer