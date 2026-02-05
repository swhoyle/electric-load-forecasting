# Data Pipeline

This document provides information about our data pipeline framework.

We split our data storage into several layers:

1. **Raw**:
2. **Bronze**:
3. **Silver**:
4. **Gold**:

## Raw Layer

The raw layer contains raw, untouched data provided by the customer.

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

## Bronze Layer

The bronze layer contains minimal cleaning and restructuring of raw data.
