import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np

# -- Config ---
st.set_page_config(page_title="Electric Load Dashboard", layout="wide")

GOLD_FILEPATH = "data/gold/powerload_1m.parquet"
WORKDAY_LABELS = {
    0: "Non-Workday",
    1: "Half-Workday",
    2: "Full-Workday",
}

def get_workday_label(wd):
    return WORKDAY_LABELS.get(int(wd), str(wd))

# -- Data ---

@st.cache_data
def load_data():
    df = pd.read_parquet(GOLD_FILEPATH)
    if "timestamp" not in df.columns or "avg_load" not in df.columns or "workday" not in df.columns:
        st.error("Expected columns: timestamp, avg_load, workday")
        st.stop()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    df["date"] = df["timestamp"].dt.date
    return df

df = load_data()
dff = df.copy()

# -- Utils --

def resample_load(df_in: pd.DataFrame, freq_in: str) -> pd.DataFrame:
    return (
        df_in.set_index("timestamp")
            .resample(freq_in)
            .agg(avg_load=("avg_load", "mean"), workday=("workday", "first"))
            .reset_index()
    )

def compute_next_day_forecast(df_in: pd.DataFrame, freq: str = None) -> pd.DataFrame:
    """Compute a next-day forecast based on historical average by second-of-day.

    Returns a DataFrame with 'timestamp' and 'forecast' columns for the next calendar day
    at the inferred frequency from the input data.
    """
    d = df_in.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"])
    d = d.sort_values("timestamp")

    # compute second of day for historical points
    sod = d["timestamp"].dt.hour * 3600 + d["timestamp"].dt.minute * 60 + d["timestamp"].dt.second
    d = d.assign(second_of_day=sod)

    # average avg_load by second_of_day
    avg_by_sec = d.groupby("second_of_day", as_index=True)["avg_load"].mean()

    # infer frequency
    try:
        inferred = pd.infer_freq(d["timestamp"]) or None
    except Exception:
        inferred = None

    if freq is None:
        if inferred is not None:
            freq = inferred
        else:
            # fallback to median diff
            diffs = d["timestamp"].diff().dt.total_seconds().dropna()
            sec = int(diffs.mode().iat[0]) if not diffs.empty else 60
            freq = f"{sec}S" if sec < 60 else f"{int(sec/60)}T"

    # next calendar day's midnight
    last_day = d["timestamp"].dt.normalize().max()
    next_day_start = last_day + pd.Timedelta(days=1)
    next_day_end = next_day_start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    forecast_times = pd.date_range(start=next_day_start, end=next_day_end, freq=freq)
    sod_future = forecast_times.hour * 3600 + forecast_times.minute * 60 + forecast_times.second

    # map to avg; if missing, result will be NaN
    forecast_values = [avg_by_sec.get(int(s), np.nan) for s in sod_future]

    return pd.DataFrame({"timestamp": forecast_times, "forecast": forecast_values})

def plot_avg_profile_by_workday(df_in: pd.DataFrame, freq_in: str, chart_type: str = "line"):
    dff = resample_load(df_in, freq_in)
    dff["minute_of_day"] = dff["timestamp"].dt.hour * 60 + dff["timestamp"].dt.minute
    dff["hour_of_day"] = dff["minute_of_day"] / 60.0

    profiles = (
        dff.groupby(["workday", "hour_of_day"], as_index=False)
           .agg(avg_load=("avg_load", "mean"))
           .sort_values(["workday", "hour_of_day"])
    )

    # Color mapping for workdays (same as forecast)
    workday_colors = {
        0: "darkgrey",       # Non-Workday
        1: "orange",         # Half-Workday
        2: "blue",           # Full-Workday
    }
    
    # Lighter shade colors for fill
    fill_colors = {
        0: "rgba(169, 169, 169, 0.2)",  # Light darkgrey
        1: "rgba(255, 165, 0, 0.2)",    # Light orange
        2: "rgba(0, 0, 255, 0.2)",      # Light blue
    }

    fig = go.Figure()
    for wd in sorted(profiles["workday"].unique()):
        wd = int(wd)
        p = profiles[profiles["workday"] == wd]
        color = workday_colors.get(wd, "gray")
        if chart_type == "line":
            fig.add_trace(
                go.Scatter(
                    x=p["hour_of_day"],
                    y=p["avg_load"] / 1e3,
                    mode="lines",
                    name=WORKDAY_LABELS.get(wd, str(wd)),
                    line=dict(color=color, width=1.5),
                )
            )
        else:
            fig.add_trace(
                go.Bar(
                    x=p["hour_of_day"],
                    y=p["avg_load"] / 1e3,
                    name=WORKDAY_LABELS.get(wd, str(wd)),
                    marker=dict(color=color),
                )
            )

    # Compute next-day forecast at this resolution and plot it (shaded)
    try:
        forecast_df = compute_next_day_forecast(dff, freq=freq_in)
        if not forecast_df.empty:
            # convert to hour_of_day for the profile plot
            forecast_df = forecast_df.assign(
                hour_of_day=forecast_df["timestamp"].dt.hour + forecast_df["timestamp"].dt.minute / 60.0 + forecast_df["timestamp"].dt.second / 3600.0
            )
            fig.add_trace(
                go.Scatter(
                    x=forecast_df["hour_of_day"],
                    y=forecast_df["forecast"] / 1e3,
                    mode="lines",
                    name="24h Forecast (Avg)",
                    line=dict(color="green", width=2),
)
            )
    except Exception:
        pass

    fig.update_layout(
        title=f"Average Daily Power Load",
        xaxis=dict(title="Hour of Day", range=[0, 24]),
        yaxis_title="Average Power [kW]",
        template="plotly_white",
        hovermode="x unified",
        legend=dict(x=0, y=1.15, xanchor="left", yanchor="top", orientation="h"),
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_powerload_forecast(dff: pd.DataFrame, chart_type="line", selected_workday_codes=None):
    """
    Plot pre-filtered and pre-resampled data + 24-hour forecast,
    colored by workday type, scrollable horizontally. Lines break between different workday types.
    chart_type: "line" for line chart, "bar" for bar chart
    """
    dff = dff.copy()
    dff["timestamp"] = pd.to_datetime(dff["timestamp"])
    dff = dff.sort_values("timestamp")
    
    if dff.empty:
        st.warning("No data available.")
        return
    
    max_ts = dff["timestamp"].max()
    
    # Use data as-is (already resampled and filtered)
    all_data = dff.copy()
    all_data = all_data.dropna(subset=["avg_load"])
    
    # Create segments: break line when workday type changes
    all_data["workday_group"] = (all_data["workday"] != all_data["workday"].shift()).cumsum()
    
    # Calculate average load for forecast
    avg_load = dff["avg_load"].mean()
    
    # Infer resolution from data
    if len(all_data) > 1:
        time_diff = (all_data["timestamp"].iloc[1] - all_data["timestamp"].iloc[0]).total_seconds() / 60
        if time_diff == 1:
            resolution = "1T"
        elif time_diff == 10:
            resolution = "10T"
        elif time_diff == 30:
            resolution = "30T"
        elif time_diff == 60:
            resolution = "60T"
        else:
            resolution = "30T"
    else:
        resolution = "30T"
    
    # Create next-day forecast based on historical second-of-day averages
    forecast_df = compute_next_day_forecast(all_data, freq=resolution)
    
    # Color mapping for workdays
    workday_colors = {
        0: "darkgrey",       # Non-Workday
        1: "orange",         # Half-Workday
        2: "blue",           # Full-Workday
    }
    
    # Lighter shade colors for fill
    fill_colors = {
        0: "rgba(169, 169, 169, 0.2)",  # Light darkgrey
        1: "rgba(255, 165, 0, 0.2)",    # Light orange
        2: "rgba(0, 0, 255, 0.2)",      # Light blue
    }
    
    fig = go.Figure()
    
    if chart_type == "line":
        # Plot each segment separately so lines don't connect across workday boundaries
        for wd in sorted(all_data["workday"].unique()):
            wd_data = all_data[all_data["workday"] == wd]
            color = workday_colors.get(int(wd), "gray")
            
            # Group consecutive segments of this workday type
            for group_id in wd_data["workday_group"].unique():
                segment = wd_data[wd_data["workday_group"] == group_id].sort_values("timestamp")
                fig.add_trace(
                    go.Scatter(
                        x=segment["timestamp"],
                        y=segment["avg_load"] / 1e3,
                        mode="lines",
                        name=WORKDAY_LABELS.get(int(wd), wd),
                        line=dict(color=color, width=1.5),
                        fill="tozeroy",
                        fillcolor=fill_colors.get(int(wd), color),
                        showlegend=bool(group_id == wd_data["workday_group"].min()),  # Only show legend for first segment
                    )
                )
    else:  # bar chart
        # For bar chart, plot all data grouped by workday type
        for wd in sorted(all_data["workday"].unique()):
            wd_data = all_data[all_data["workday"] == wd]
            color = workday_colors.get(int(wd), "gray")
            fig.add_trace(
                go.Bar(
                    x=wd_data["timestamp"],
                    y=wd_data["avg_load"] / 1e3,
                    name=WORKDAY_LABELS.get(int(wd), wd),
                    marker=dict(color=color),
                    showlegend=True,
                )
            )
    
    # Plot single next-day forecast based on second-of-day historical averages
    fig.add_trace(
        go.Scatter(
            x=forecast_df["timestamp"],
            y=forecast_df["forecast"] / 1e3,
            mode="lines",
            name="24h Forecast (Avg)",
            line=dict(color="green", width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 128, 0, 0.15)",
            showlegend=True,
        )
    )

    fig.update_layout(
        title="⚡ Power Load Timeseries",
        xaxis_title="Timestamp",
        yaxis_title="Average Power [kW]",
        template="plotly_white",
        hovermode="x unified",
        xaxis=dict(
            rangeslider=dict(visible=True),
            type="date",
        ),
        legend=dict(x=0, y=1.15, xanchor="left", yanchor="top", orientation="h"),
    )
    
    st.plotly_chart(fig, use_container_width=True)

def show_settings(dff):
    all_dates = sorted(df["timestamp"].dt.date.unique())
    forecast_min_date = all_dates[0]
    forecast_max_date = all_dates[-1]
    default_start_date = all_dates[max(0, len(all_dates) - 7)]

    date_range = st.slider(
        "Date Range",
        min_value=forecast_min_date,
        max_value=forecast_max_date,
        value=(default_start_date, forecast_max_date),
    )

    resolution = st.radio(
        "Resolution",
        options=["1T", "10T", "30T", "60T"],
        format_func=lambda x: {"1T": "1 minute", "10T": "10 minutes", "30T": "30 minutes", "60T": "1 hour"}[x],
        index=2,
    )

    chart_type = st.radio(
        "Chart",
        options=["line", "bar"],
        format_func=lambda x: {"line": "Line", "bar": "Bar"}[x],
        index=0,
    )

    # Workday filter: allow user to select which workday types to include
    workday_options = [
        (2, WORKDAY_LABELS[2]),
        (1, WORKDAY_LABELS[1]),
        (0, WORKDAY_LABELS[0]),
    ]
    # display labels in the UI but keep numeric codes for filtering
    selected_workdays = st.multiselect(
        "Workday Types",
        options=[label for _, label in workday_options],
        default=[label for _, label in workday_options],
    )
    # map selected labels back to numeric codes
    selected_workday_codes = [code for code, label in workday_options if label in selected_workdays]

    start_date = date_range[0]
    end_date = date_range[1]

    if start_date is not None:
        dff = dff[dff["timestamp"].dt.date >= start_date]
    if end_date is not None:
        dff = dff[dff["timestamp"].dt.date <= end_date]

    # Apply workday filter if any selected (otherwise keep all)
    if selected_workday_codes:
        dff = dff[dff["workday"].isin(selected_workday_codes)]

    if resolution is not None:
        dff = resample_load(dff, resolution)

    return dff, chart_type, resolution, selected_workday_codes

# -- UI --
st.title("⚡ Electric Load Dashboard")

with st.popover("Settings"):
    dff, chart_type, resolution, selected_workday_codes = show_settings(dff)

c1,c2 = st.columns(2)
with c1:
    plot_avg_profile_by_workday(dff, resolution, chart_type)
with c2:
    plot_powerload_forecast(dff, chart_type, selected_workday_codes)

#with st.expander("Preview Data"):
#    st.dataframe(dff.head(500), use_container_width=True)