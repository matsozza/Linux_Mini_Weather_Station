import streamlit as st
import plotly.express as px
import pandas as pd
import os
import logging
import getpass
import sqlite3

from datetime import datetime, timedelta, timezone


# ==============================
# Configuration
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_FILE = os.path.join(
    BASE_DIR,
    f"weather_station_frontend_{getpass.getuser()}.log"
)

CACHE_DB = os.path.join(
    BASE_DIR,
    "weather_station_cache.db"
)

MAX_PLOT_POINTS = 300


# ==============================
# Setup Logging
# ==============================
with open(LOG_FILE, "w") as f:
    f.write("Log started\n")

logger = logging.getLogger("WeatherStationFrontend")
logger.setLevel(logging.DEBUG)
logger.propagate = False

if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    )
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)


# ==============================
# SQLite Data Access
# ==============================
def fetch_cached_data(timeframe, source):
    """Read weather data from the local SQLite cache."""

    if not os.path.exists(CACHE_DB):
        raise FileNotFoundError(
            f"Cache database not found: {CACHE_DB}"
        )

    timeframe_delta = {
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "15d": timedelta(days=15),
        "1 month": timedelta(days=30),
        "3 month": timedelta(days=90),
        "6 month": timedelta(days=180),
        "1 year": timedelta(days=365),
    }

    query = """
        SELECT
            timestamp,
            source,
            temperature AS data_temperature,
            pressure AS data_pressure,
            humidity AS data_humidity,
            loc_lat,
            loc_lon
        FROM weather_cache
        WHERE source = ?
    """

    params = [source]

    if timeframe != "all":
        start_time = (
            datetime.now(timezone.utc)
            - timeframe_delta[timeframe]
        )

        query += " AND timestamp >= ?"
        params.append(start_time.isoformat())

    query += " ORDER BY timestamp ASC"

    # Open database in read-only mode
    db_uri = f"file:{os.path.abspath(CACHE_DB)}?mode=ro"

    with sqlite3.connect(db_uri, uri=True) as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params=params
        )

    return df


# ==============================
# Plot Data Reduction
# ==============================
def reduce_plot_points(df, max_points=MAX_PLOT_POINTS):
    """
    Reduce a DataFrame to at most max_points by averaging measurements
    within time buckets.

    The original DataFrame is not modified. The returned DataFrame is
    intended for plotting only.
    """

    if max_points < 1:
        raise ValueError("max_points must be at least 1")

    if df.empty or len(df) <= max_points:
        return df.copy()

    work = df.copy()

    work["timestamp"] = pd.to_datetime(
        work["timestamp"],
        errors="coerce", format="ISO8601", utc=True
    )

    work = (
        work.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    if work.empty or len(work) <= max_points:
        return work

    # Assign each record to one of max_points time buckets.
    elapsed_seconds = (
        work["timestamp"] - work["timestamp"].iloc[0]
    ).dt.total_seconds()

    total_seconds = elapsed_seconds.iloc[-1]

    if total_seconds > 0:
        bucket_ids = (
            (elapsed_seconds / total_seconds * max_points)
            .astype(int)
            .clip(upper=max_points - 1)
        )
    else:
        # If all timestamps are identical, distribute rows by position.
        bucket_ids = (
            pd.Series(range(len(work))) * max_points // len(work)
        ).clip(upper=max_points - 1)

    work["_bucket"] = bucket_ids

    # Average numeric measurements in each bucket. Keep the first
    # timestamp in each bucket as its representative x-axis value.
    numeric_columns = work.select_dtypes(
        include="number"
    ).columns.tolist()

    aggregations = {
        "timestamp": "first",
        **{column: "mean" for column in numeric_columns}
    }

    result = (
        work.groupby("_bucket", sort=True)
        .agg(aggregations)
        .reset_index(drop=True)
    )

    logger.debug(
        "Plot downsampling: %s -> %s points (maximum: %s)",
        len(df),
        len(result),
        max_points
    )

    return result


# ==============================
# Dashboard UI Logic
# ==============================
def dashboard_ui():
    """This function contains all the Streamlit frontend code."""

    # st.set_page_config MUST be the first Streamlit command
    st.set_page_config(
        page_title="Weather Dashboard",
        layout="wide"
    )

    st.title("🌡️ Environmental Monitor")
    st.markdown("Real-time data from Weather Station")

    # ==============================
    # Sidebar Controls
    # ==============================
    st.sidebar.header("Settings")

    timeframe = st.sidebar.selectbox(
        "Select Timeframe",
        options=[
            "24h",
            "7d",
            "15d",
            "1 month",
            "3 month",
            "6 month",
            "1 year",
            "all"
        ],
        index=0
    )

    max_plot_points = st.sidebar.select_slider(
        "Maximum plot points (per dataset)",
        options=[100, 300, 500, 1000, 5000],
        value=MAX_PLOT_POINTS,
        format_func=lambda x: f"{x:,}"
    )

    # ==============================
    # Plot Creation
    # ==============================
    def create_plot(df, y_axis, title):
        logger.debug("Create plot: %s", title)

        fig = px.line(
            df,
            x="timestamp",
            y=y_axis,
            title=title,
            color="source",
            color_discrete_map={
                "Local": "#1f77b4",
                "Weather API": "#ff7f0e"
            }
        )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=40, b=20)
        )

        return fig

    # ==============================
    # Dashboard Update
    # ==============================
    @st.fragment(run_every="600s")
    def update_board():

        logger.debug("Update board")

        if not os.path.exists(CACHE_DB):
            st.warning(
                f"Cache database not found: {CACHE_DB}"
            )
            return

        # ------------------------------
        # Read local SQLite data
        # ------------------------------
        try:
            with st.spinner("Loading cached data..."):

                df_local = fetch_cached_data(
                    timeframe,
                    source="sensor"
                )

                df_api = fetch_cached_data(
                    timeframe,
                    source="api"
                )

        except Exception as e:
            logger.exception("Failed to read SQLite cache")
            st.error(f"Failed to read cached data: {e}")
            return

        logger.info(
            "Points loaded from SQLite — sensor: %s, API: %s",
            len(df_local),
            len(df_api)
        )

        # ------------------------------
        # Check available data
        # ------------------------------
        if df_local.empty and df_api.empty:
            st.warning(
                f"No cached data available for timeframe: {timeframe}."
            )
            return

        if df_local.empty:
            st.warning(
                f"No sensor data available for timeframe: {timeframe}."
            )

        if df_api.empty:
            st.warning(
                f"No weather API data available for timeframe: {timeframe}."
            )

        # ------------------------------
        # Convert timestamps to local timezone
        # ------------------------------
        target_tz = "America/Sao_Paulo"

        if not df_local.empty:
            df_local["timestamp"] = pd.to_datetime(
                df_local["timestamp"],
                format="ISO8601",
            )

        if not df_api.empty:
            df_api["timestamp"] = pd.to_datetime(
                df_api["timestamp"],
                format="ISO8601",
            )

        # ==============================
        # Overview Metrics
        # ==============================
        if not df_local.empty:

            logger.debug("Calculating overview metrics")

            # Latest available measurement
            latest_row = df_local.iloc[-1]

            # Initialize deltas
            temp_delta_24h = None
            pres_delta_24h = None
            humi_delta_24h = None

            # Find measurement closest to 24h ago
            target_time_24h = (
                df_local["timestamp"].max()
                - pd.Timedelta(hours=24)
            )

            df_indexed = (
                df_local
                .set_index("timestamp")
                .sort_index()
            )

            try:
                yesterday_row = (
                    df_indexed
                    .loc[:target_time_24h]
                    .iloc[-1]
                )

                temp_delta_24h = (
                    latest_row["data_temperature"]
                    - yesterday_row["data_temperature"]
                )

                pres_delta_24h = (
                    latest_row["data_pressure"]
                    - yesterday_row["data_pressure"]
                )

                humi_delta_24h = (
                    latest_row["data_humidity"]
                    - yesterday_row["data_humidity"]
                )

            except (IndexError, KeyError):
                logger.debug("No measurement available from 24h ago")

            # Display metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                value = latest_row["data_temperature"]

                st.metric(
                    label="Local Temperature",
                    value=f"{value:.1f} °C" if pd.notna(value) else "N/A",
                    delta=(
                        f"{temp_delta_24h:.1f} °C vs. 24h ago"
                        if temp_delta_24h is not None
                        and pd.notna(temp_delta_24h)
                        else None
                    )
                )

            with col2:
                value = latest_row["data_pressure"]

                st.metric(
                    label="Local Pressure",
                    value=f"{value:.1f} kPa" if pd.notna(value) else "N/A",
                    delta=(
                        f"{pres_delta_24h:.1f} kPa vs. 24h ago"
                        if pres_delta_24h is not None
                        and pd.notna(pres_delta_24h)
                        else None
                    )
                )

            with col3:
                value = latest_row["data_humidity"]

                st.metric(
                    label="Local Humidity",
                    value=f"{value:.1f} %" if pd.notna(value) else "N/A",
                    delta=(
                        f"{humi_delta_24h:.1f} % vs. 24h ago"
                        if humi_delta_24h is not None
                        and pd.notna(humi_delta_24h)
                        else None
                    )
                )

            st.divider()

        # ==============================
        # Dataset Selection
        # ==============================
        options = st.multiselect(
            "Select datasets to plot",
            ["Local", "Weather API"],
            default=["Local", "Weather API"]
        )

        # ==============================
        # Prepare Plot DataFrames
        # ==============================
        plot_frames = []

        if "Local" in options and not df_local.empty:

            # Downsample a copy. Keep df_local untouched for metrics/map.
            df_l = reduce_plot_points(
                df_local,
                max_points=max_plot_points
            )

            df_l["source"] = "Local"
            plot_frames.append(df_l)

        if "Weather API" in options and not df_api.empty:

            df_a = reduce_plot_points(
                df_api,
                max_points=max_plot_points
            )

            df_a["source"] = "Weather API"
            plot_frames.append(df_a)

        if not plot_frames:
            st.info(
                "Please select at least one available dataset to view the plots."
            )
            return

        # ------------------------------
        # Log final plotting volume
        # ------------------------------
        total_plot_points = sum(
            len(frame) for frame in plot_frames
        )

        logger.info(
            "Plotting %s total points across %s selected datasets "
            "(maximum %s per dataset)",
            total_plot_points,
            len(plot_frames),
            max_plot_points
        )

        # ==============================
        # Plotting Weather Data
        # ==============================
        df_plot = pd.concat(
            plot_frames,
            ignore_index=True
        )

        chart_col1, chart_col2, chart_col3 = st.columns(3)

        with chart_col1:
            st.plotly_chart(
                create_plot(
                    df_plot,
                    "data_temperature",
                    "Temperature"
                ),
                use_container_width=True
            )

        with chart_col2:
            st.plotly_chart(
                create_plot(
                    df_plot,
                    "data_pressure",
                    "Atmospheric Pressure"
                ),
                use_container_width=True
            )

        with chart_col3:
            st.plotly_chart(
                create_plot(
                    df_plot,
                    "data_humidity",
                    "Relative Humidity"
                ),
                use_container_width=True
            )

        st.divider()

        # ==============================
        # Station Location
        # ==============================
        if not df_local.empty:

            latest_location = df_local.iloc[-1]

            lat = latest_location["loc_lat"]
            lon = latest_location["loc_lon"]

            if pd.notna(lat) and pd.notna(lon):

                map_data = pd.DataFrame({
                    "lat": [lat],
                    "lon": [lon]
                })

                st.subheader("Station Location")
                st.map(map_data, zoom=12)


    # ==============================
    # Run Dashboard
    # ==============================
    update_board()


# ==============================
# Main
# ==============================
if __name__ == "__main__":

    if st.runtime.exists():
        dashboard_ui()
