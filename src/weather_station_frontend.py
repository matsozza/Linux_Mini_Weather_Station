import streamlit as st
import plotly.express as px
import pandas as pd
import os
import logging
import getpass
import sqlite3
import math

try:
    from timezonefinder import TimezoneFinder
except ImportError:
    TimezoneFinder = None

TIMEZONE_FINDER = TimezoneFinder() if TimezoneFinder is not None else None

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


def calculate_dew_point(df):
    """Add dew-point temperature (°C) using air temperature and RH.

    Uses the Magnus approximation. Rows with missing/invalid temperature
    or relative humidity outside (0, 100] produce NaN.
    """
    result = df.copy()

    temperature = pd.to_numeric(result["data_temperature"], errors="coerce")
    humidity = pd.to_numeric(result["data_humidity"], errors="coerce")

    # Magnus constants for liquid water, with temperature in Celsius.
    a = 17.625
    b = 243.04

    valid = (
        temperature.notna()
        & humidity.notna()
        & (humidity > 0)
        & (humidity <= 100)
        & (temperature > -b)
    )

    result["data_dew_point"] = float("nan")

    gamma = (
        (humidity[valid] / 100.0).map(math.log)
        + (a * temperature[valid]) / (b + temperature[valid])
    )

    result.loc[valid, "data_dew_point"] = (
        b * gamma / (a - gamma)
    )

    return result


# Running-comfort categories based on dew-point temperature (°C).
# These are practical guidance bands, not medical or heat-safety thresholds.
RUNNING_COMFORT_ZONES = [
    {"name": "Very comfortable", "upper": 10.0, "color": "#2E9D69", "range": "< 10 °C"},
    {"name": "Comfortable", "upper": 12.8, "color": "#83B96A", "range": "10–12.8 °C"},
    {"name": "Somewhat uncomfortable", "upper": 17.8, "color": "#D5B84E", "range": "12.8–17.8 °C"},
    {"name": "Uncomfortable", "upper": 20.6, "color": "#E99543", "range": "17.8–20.6 °C"},
    {"name": "Very uncomfortable", "upper": 23.9, "color": "#D96B4D", "range": "20.6–23.9 °C"},
    {"name": "Oppressive", "upper": float("inf"), "color": "#B94C55", "range": "> 23.9 °C"},
]


def get_running_comfort(dew_point):
    """Return the dew-point running-comfort category for a value in °C."""
    if pd.isna(dew_point):
        return None

    value = float(dew_point)

    for zone in RUNNING_COMFORT_ZONES:
        if value < zone["upper"]:
            return {
                "name": zone["name"],
                "color": zone["color"],
                "range": zone["range"],
            }

    # Defensive fallback; the final zone is open-ended.
    return {
        "name": RUNNING_COMFORT_ZONES[-1]["name"],
        "color": RUNNING_COMFORT_ZONES[-1]["color"],
        "range": RUNNING_COMFORT_ZONES[-1]["range"],
    }


def get_station_timezone(df_local, df_api):
    """Infer the station's IANA timezone from its latest available GPS fix.

    Prefer sensor coordinates; fall back to API coordinates if needed.
    Returns (timezone_name, latitude, longitude).
    """
    if TIMEZONE_FINDER is None:
        raise ImportError(
            "The 'timezonefinder' package is required. "
            "Install it with: pip install timezonefinder"
        )

    coordinate_frames = []
    for frame in (df_local, df_api):
        if not frame.empty and {"loc_lat", "loc_lon"}.issubset(frame.columns):
            coords = frame[["timestamp", "loc_lat", "loc_lon"]].copy()
            coords["loc_lat"] = pd.to_numeric(coords["loc_lat"], errors="coerce")
            coords["loc_lon"] = pd.to_numeric(coords["loc_lon"], errors="coerce")
            coords = coords.dropna(subset=["loc_lat", "loc_lon"])
            if not coords.empty:
                coordinate_frames.append(coords)

    if not coordinate_frames:
        raise ValueError("No valid GPS latitude/longitude found in the SQLite cache.")

    # Prefer sensor GPS data when available; otherwise use API GPS data.
    coordinates = coordinate_frames[0]
    if not coordinates.empty:
        coordinates = coordinates.sort_values("timestamp")
        latest = coordinates.iloc[-1]
        latitude = float(latest["loc_lat"])
        longitude = float(latest["loc_lon"])

    timezone_name = TIMEZONE_FINDER.timezone_at(
        lat=latitude,
        lng=longitude,
    )

    if timezone_name is None:
        raise ValueError(
            f"Could not determine timezone for GPS coordinates "
            f"({latitude}, {longitude})."
        )

    return timezone_name, latitude, longitude


def convert_timestamps_to_timezone(df, timezone_name):
    """Interpret cached timestamps as instants and display them in station time."""
    if df.empty:
        return df

    result = df.copy()
    result["timestamp"] = pd.to_datetime(
        result["timestamp"],
        format="ISO8601",
        errors="coerce",
        utc=True,
    ).dt.tz_convert(timezone_name)

    return (
        result.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


# ==============================
# Plot Data Reduction
# ==============================

def reduce_plot_points(df, max_points=MAX_PLOT_POINTS):
    """
    Reduce a DataFrame to at most max_points while ALWAYS preserving
    the newest measurement as an individual, unmodified point.

    Historical measurements are averaged into time buckets.
    The newest measurement is appended after historical reduction.
    """

    if max_points < 1:
        raise ValueError("max_points must be at least 1")

    if df.empty or len(df) <= max_points:
        return df.copy()

    work = (
        df.copy()
        .dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    if work.empty or len(work) <= max_points:
        return work

    # Preserve the newest measurement exactly as recorded.
    latest_point = work.iloc[[-1]].copy()

    # Reserve one plotting slot for the newest measurement.
    historical = work.iloc[:-1].copy()
    historical_points = max_points - 1

    if historical_points == 0:
        return latest_point.reset_index(drop=True)

    # Divide historical data into buckets.
    elapsed_seconds = (
        historical["timestamp"] - historical["timestamp"].iloc[0]
    ).dt.total_seconds()

    total_seconds = elapsed_seconds.iloc[-1]

    if total_seconds > 0:
        bucket_ids = (
            (elapsed_seconds / total_seconds * historical_points)
            .astype(int)
            .clip(upper=historical_points - 1)
        )
    else:
        bucket_ids = (
            pd.Series(range(len(historical))) * historical_points
            // len(historical)
        ).clip(upper=historical_points - 1)

    historical["_bucket"] = bucket_ids

    numeric_columns = historical.select_dtypes(
        include="number"
    ).columns.tolist()

    aggregations = {
        "timestamp": "first",
        **{column: "mean" for column in numeric_columns}
    }

    reduced = (
        historical.groupby("_bucket", sort=True)
        .agg(aggregations)
        .reset_index(drop=True)
    )

    # Append the exact newest measurement.
    result = pd.concat(
        [reduced, latest_point],
        ignore_index=True
    )

    logger.debug(
        "Plot downsampling: %s -> %s points "
        "(maximum: %s, newest point preserved)",
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

    def create_dew_point_plot(df):
        """Create dew-point comparison plot with running-comfort background bands."""
        logger.debug("Create plot: Dew Point & Running Comfort")

        fig = px.line(
            df,
            x="timestamp",
            y="data_dew_point",
            title="Dew Point & Running Comfort",
            color="source",
            color_discrete_map={
                "Local": "#1f77b4",
                "Weather API": "#ff7f0e",
            },
        )

        valid_values = pd.to_numeric(
            df["data_dew_point"], errors="coerce"
        ).dropna()

        if not valid_values.empty:
            y_min = min(float(valid_values.min()) - 1.0, 9.0)
            y_max = max(float(valid_values.max()) + 1.0, 25.0)
        else:
            y_min, y_max = 8.0, 25.0

        # Draw zones behind the data lines. The first and last zones extend
        # to the visible chart limits because they are open-ended categories.
        boundaries = [
            y_min,
            10.0,
            12.8,
            17.8,
            20.6,
            23.9,
            y_max,
        ]

        for index, zone in enumerate(RUNNING_COMFORT_ZONES):
            band_bottom = max(boundaries[index], y_min)
            band_top = min(boundaries[index + 1], y_max)
            if band_top <= band_bottom:
                continue

            fig.add_hrect(
                y0=band_bottom,
                y1=band_top,
                fillcolor=zone["color"],
                opacity=0.25,
                line_width=0,
                layer="below",
            )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            yaxis=dict(
                title="Dew point (°C)",
                range=[y_min, y_max],
            ),
        )

        return fig

    def display_running_comfort_legend():
        """Display a compact legend for the dew-point comfort bands."""
        legend_items = "".join(
            f'<span style="display:inline-flex;align-items:center;gap:6px;'
            f'margin:3px 14px 3px 0;white-space:nowrap;">'
            f'<span style="width:11px;height:11px;border-radius:2px;'
            f'background:{zone["color"]};display:inline-block;"></span>'
            f'<span>{zone["name"]} <span style="opacity:.7">({zone["range"]})</span></span>'
            f'</span>'
            for zone in RUNNING_COMFORT_ZONES
        )
        st.markdown(
            f'<div style="display:flex;flex-wrap:wrap;align-items:center;'
            f'font-size:0.82rem;line-height:1.5;margin-top:-4px;">'
            f'{legend_items}</div>',
            unsafe_allow_html=True,
        )

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
        # Determine station timezone from GPS and prepare timestamps
        # ------------------------------
        try:
            station_timezone, station_lat, station_lon = get_station_timezone(
                df_local,
                df_api,
            )

            df_local = convert_timestamps_to_timezone(
                df_local,
                station_timezone,
            )
            df_api = convert_timestamps_to_timezone(
                df_api,
                station_timezone,
            )

            logger.info(
                "Station timezone: %s (GPS: %.6f, %.6f)",
                station_timezone,
                station_lat,
                station_lon,
            )
            st.caption(
                f"Station timezone: {station_timezone} "
                f"(GPS: {station_lat:.5f}, {station_lon:.5f})"
            )

        except (ImportError, ValueError) as e:
            logger.exception("Could not determine station timezone")
            st.error(str(e))
            return

        # Calculate dew point independently for both data sources.
        if not df_local.empty:
            df_local = calculate_dew_point(df_local)

        if not df_api.empty:
            df_api = calculate_dew_point(df_api)

        # ==============================
        # Overview Metrics
        # ==============================
        def display_latest_metrics(df, source_label, accent, icon):
            """Display the latest readings inside a visually distinct panel."""
            with st.container(border=True):
                st.markdown(f"### {icon} :{accent}[{source_label}]")

                if df.empty:
                    st.info(f"No recent measurements available for {source_label}.")
                    return

                latest_row = df.iloc[-1]
                st.caption(
                    "Latest reading: "
                    + latest_row["timestamp"].strftime("%d/%m/%Y %H:%M:%S %Z")
                )

                # Preserve the existing 24-hour delta behavior for both sources.
                deltas = {
                    "data_temperature": None,
                    "data_pressure": None,
                    "data_humidity": None,
                    "data_dew_point": None,
                }

                target_time_24h = (
                    latest_row["timestamp"] - pd.Timedelta(hours=24)
                )
                previous_rows = df.loc[df["timestamp"] <= target_time_24h]

                if not previous_rows.empty:
                    previous_row = previous_rows.iloc[-1]
                    for field in deltas:
                        current_value = latest_row[field]
                        previous_value = previous_row[field]
                        if pd.notna(current_value) and pd.notna(previous_value):
                            deltas[field] = current_value - previous_value

                metrics = [
                    ("data_temperature", "Temperature", "°C"),
                    ("data_pressure", "Pressure", "kPa"),
                    ("data_humidity", "Relative Humidity", "%"),
                    ("data_dew_point", "Dew Point", "°C"),
                ]

                metric_columns = st.columns(2)
                for index, (field, label, unit) in enumerate(metrics):
                    value = latest_row[field]
                    delta = deltas[field]

                    with metric_columns[index % 2]:
                        st.metric(
                            label=label,
                            value=(
                                f"{value:.1f} {unit}"
                                if pd.notna(value)
                                else "N/A"
                            ),
                            delta=(
                                f"{delta:+.1f} {unit} vs. 24h ago"
                                if delta is not None and pd.notna(delta)
                                else None
                            ),
                        )

                comfort = get_running_comfort(latest_row["data_dew_point"])
                if comfort is not None:
                    st.markdown(
                        f'<div style="margin-top:8px;padding:9px 11px;'
                        f'border-radius:7px;background:{comfort["color"]}22;'
                        f'border-left:4px solid {comfort["color"]};">'
                        f'<strong>Running comfort:</strong> '
                        f'<span style="color:{comfort["color"]};font-weight:600;">'
                        f'{comfort["name"]}</span>'
                        f'<span style="opacity:.75;"> · {comfort["range"]}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("Running comfort: unavailable (missing dew-point data)")

        st.subheader("Latest Measurements")
        local_panel, api_panel = st.columns(2)

        with local_panel:
            display_latest_metrics(
                df_local,
                source_label="Local Station",
                accent="blue",
                icon="🏠",
            )

        with api_panel:
            display_latest_metrics(
                df_api,
                source_label="Weather API",
                accent="orange",
                icon="🌐",
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

        chart_col1, chart_col2 = st.columns(2)

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

        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            st.plotly_chart(
                create_plot(
                    df_plot,
                    "data_humidity",
                    "Relative Humidity"
                ),
                use_container_width=True
            )

        with chart_col4:
            st.plotly_chart(
                create_dew_point_plot(df_plot),
                use_container_width=True,
            )
            display_running_comfort_legend()

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
