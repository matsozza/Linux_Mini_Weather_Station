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
        options=["1h", "24h", "7d", "1 month", "1 year", "all"],
        index=1 
    )

    def create_plot(df, y_axis, title, color):
        logger.debug("Create plot")
        fig = px.line(df, x="timestamp", y=y_axis, title=title)
        fig.update_traces(line_color=color)
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
        return fig

    @st.fragment(run_every="600s")
    def update_board():
        logger.debug("Update board")        
        with st.spinner("Fetching latest data..."):
            # Ensure your backend has this specific method
            df = backend.fetch_aggregated_data_daily(timeframe)
        
        if df is None or df.empty:
            st.warning(f"No data available for timeframe: {timeframe}.")
            return

        TARGET_TZ = 'America/Sao_Paulo'
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(TARGET_TZ)
        latest = df.iloc[-1]

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current Temp", f"{latest['data_temperature']:.1f} °C")
        with col2:
            st.metric("Current Pressure", f"{latest['data_pressure']:.1f} hPa")
        with col3:
            st.metric("Current Humidity", f"{latest['data_humidity']:.1f} %")

        st.divider()

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(create_plot(df, "data_temperature", "data_temperature", "#EF553B"), use_container_width=True)
            st.plotly_chart(create_plot(df, "data_pressure", "Atmospheric Pressure", "#636EFA"), use_container_width=True)
        with chart_col2:
            st.plotly_chart(create_plot(df, "data_humidity", "Relative Humidity", "#00CC96"), use_container_width=True)

    update_board()

if __name__ == "__main__":
    if st.runtime.exists():
        dashboard_ui()
    