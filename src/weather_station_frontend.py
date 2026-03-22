import streamlit as st
import plotly.express as px
import pandas as pd
import subprocess
import sys
import fcntl
import os
import logging
import getpass
import time
import threading 
import signal
from datetime import datetime
from weather_station_backend import WeatherStationBackend

# ==============================
# Configuration
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"weather_station_frontend_{getpass.getuser()}.log")

# ==============================
# Setup Logging
# ==============================
# Open / Create log file
with open(LOG_FILE, "w") as f:
    f.write("Log started\n")

logger = logging.getLogger("WeatherStationFrontend")
logger.setLevel(logging.DEBUG)
logger.propagate = False

# Individual handler
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

# ==============================
# Dashboard UI Logic
# ==============================
def dashboard_ui():
    """This function contains all the Streamlit frontend code."""
    backend = WeatherStationBackend()

    # st.set_page_config MUST be the first streamlit command called
    st.set_page_config(page_title="Weather Dashboard", layout="wide")

    st.title("🌡️ Environmental Monitor")
    st.markdown("Real-time data from Weather Station")

    # Sidebar for controls
    st.sidebar.header("Settings")
    timeframe = st.sidebar.selectbox(
        "Select Timeframe",
        options=["24h", "7d", "1 month", "1 year", "all"],
        index=0
    )

    def create_plot(df, y_axis, title):
        logger.debug("Create plot")
        fig = px.line(
            df, 
            x="timestamp", 
            y=y_axis, 
            title=title,
            color="source", # This separates the lines based on the dataset
            color_discrete_map={
                "Local": "#1f77b4",       # Blue for Local
                "Weather API": "#ff7f0e"  # Orange for API
            }
        )
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
        return fig

    @st.fragment(run_every="600s")
    def update_board():
        logger.debug("Update board")        
        with st.spinner("Fetching latest data..."):
            # Create dataframes based on DB data
            df_local = backend.fetch_aggregated_data_daily(timeframe, collection="sensor")
            df_api = backend.fetch_aggregated_data_daily(timeframe, collection="api")
        
            # Filter local data by avg. every 5 samples
            filtering_cols = ["data_temperature", "data_pressure", "data_humidity"]
            df_local[filtering_cols] = df_local[filtering_cols].rolling(window=10, min_periods=1).mean()
        
        if df_local is None or df_local.empty:
            st.warning(f"No sensor data available for timeframe: {timeframe}.")

        if df_api is None or df_api.empty:
            st.warning(f"No weather API data available for timeframe: {timeframe}.")

        TARGET_TZ = 'America/Sao_Paulo'
        df_local["timestamp"] = pd.to_datetime(df_local["timestamp"], utc=True).dt.tz_convert(TARGET_TZ)
        df_api["timestamp"] = pd.to_datetime(df_api["timestamp"], utc=True).dt.tz_convert(TARGET_TZ)
        latest = df_local.iloc[-1]

        # -------------------- Show overview of temperature --------------------
        
        # Get timestamp of 24h ago
        target_time_24h = df_local["timestamp"].max() - pd.Timedelta(hours=24)

        # Find the row closest to 24h ago
        df_indexed = df_local.set_index("timestamp").sort_index()
        try:
            yesterday_row = df_indexed.loc[:target_time_24h].iloc[-1]
            temp_delta_24h = df_local.iloc[-1]["data_temperature"] - yesterday_row["data_temperature"]
            pres_delta_24h = df_local.iloc[-1]["data_pressure"] - yesterday_row["data_pressure"]
            humi_delta_24h = df_local.iloc[-1]["data_humidity"] - yesterday_row["data_humidity"]
        except (IndexError, KeyError):
            temp_delta_24h = None

        # Display in Streamlit        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                label="Local Temperature", 
                value=f"{df_local.iloc[-1]['data_temperature']:.1f} °C", 
                delta=f"{temp_delta_24h:.1f} °C vs. 24h ago" if temp_delta_24h is not None else None
            )
        with col2:
            st.metric(
                label="Local Pressure", 
                value=f"{df_local.iloc[-1]['data_pressure']:.1f} kPa", 
                delta=f"{pres_delta_24h:.1f} kPa vs. 24h ago" if temp_delta_24h is not None else None
            )
        with col3:
            st.metric(
                label="Local Humidity", 
                value=f"{df_local.iloc[-1]['data_humidity']:.1f} %", 
                delta=f"{humi_delta_24h:.1f} % vs. 24h ago" if temp_delta_24h is not None else None
            )
        st.divider()

        # -------------------- Selecting weather data sources for plots --------------------
        options = st.multiselect("Select datasets to plot", ["Local", "Weather API"], default = ["Local", "Weather API"])
        plot_frames = []
        if "Local" in options:
            df_l = df_local.copy()
            df_l["source"] = "Local"
            plot_frames.append(df_l)

        if "Weather API" in options:
            df_a = df_api.copy()
            df_a["source"] = "Weather API"
            plot_frames.append(df_a)
        
        if not plot_frames:
            st.info("Please select at least one dataset to view the plots.")
            return
            
        # -------------------- Plotting weather data --------------------
        df_plot = pd.concat(plot_frames, ignore_index=True)
        chart_col1, chart_col2, chart_col3 = st.columns(3)
        with chart_col1:
            st.plotly_chart(create_plot(df_plot, "data_temperature", "Temperature"), use_container_width=True)
        with chart_col2:
            st.plotly_chart(create_plot(df_plot, "data_pressure", "Atmospheric Pressure"), use_container_width=True)
        with chart_col3:
            st.plotly_chart(create_plot(df_plot, "data_humidity", "Relative Humidity"), use_container_width=True)
        st.divider()

        # -------------------- Plot station location --------------------
        if not df_local.empty:
            map_data = pd.DataFrame({
                'lat': [df_local.iloc[-1]["loc_lat"]],
                'lon': [df_local.iloc[-1]["loc_lon"]]
            })
            st.subheader("Station Location")
            st.map(map_data, zoom=12)

    update_board()

if __name__ == "__main__":
    if st.runtime.exists():
        dashboard_ui()