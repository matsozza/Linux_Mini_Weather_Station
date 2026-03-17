import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime
from weather_station_backend import WeatherStationBackend

# Initialize the Data API
backend = WeatherStationBackend()

st.set_page_config(page_title="Weather Dashboard", layout="wide")

st.title("🌡️ Environmental Monitor")
st.markdown("Real-time data from Weather Station")

# Sidebar for controls
st.sidebar.header("Settings")
timeframe = st.sidebar.selectbox(
    "Select Timeframe",
    options=["1h", "24h", "7d", "1 month", "1 year", "all"],
    index=2 # Default to 24h
)

# Plotting Logic
def create_plot(df, y_axis, title, color):
    fig = px.line(df, x="timestamp", y=y_axis, title=title)
    fig.update_traces(line_color=color)
    fig.update_layout(
        height=350,
        margin=dict(l=20, r=20, t=40, b=20) # Tighter margins
    )
    return fig

# Using fragment to refresh only the data board every 3 minutes
@st.fragment(run_every="600s")
def update_board():
    with st.spinner("Fetching latest data..."):
        df = backend.fetch_from_hourly_summary(timeframe)
    
    # Handle empty data gracefully to prevent app crashes
    if df is None or df.empty:
        st.warning(f"No data available for the selected timeframe: {timeframe}.")
        return

    if df.empty:
        st.warning("No valid sensor readings found in this timeframe.")
        return

    # Use a fixed timezone instead of server local time (Adjust as needed)
    TARGET_TZ = 'America/Sao_Paulo'
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(TARGET_TZ)

    # Get latest readings for metrics
    latest = df.iloc[-1]

    # Create Layout Columns
    col1, col2, col3 = st.columns(3)

    # Metric Summaries (Showing current value)
    with col1:
        st.metric("Current Temperature", f"{latest['temperature']:.1f} °C")
    with col2:
        st.metric("Current Pressure", f"{latest['pressure']:.1f} hPa")
    with col3:
        st.metric("Current Humidity", f"{latest['humidity']:.1f} %")

    st.divider()

    # Display Charts (Responsive width)
    # Using columns here to prevent massive vertical scrolling on wide screens
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.plotly_chart(create_plot(df, "temperature", "Temperature Over Time", "#EF553B"), use_container_width=True)
        st.plotly_chart(create_plot(df, "pressure", "Atmospheric Pressure", "#636EFA"), use_container_width=True)
        
    with chart_col2:
        st.plotly_chart(create_plot(df, "humidity", "Relative Humidity", "#00CC96"), use_container_width=True)

# Run the fragment
update_board()