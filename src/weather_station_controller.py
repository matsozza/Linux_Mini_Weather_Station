#!/usr/bin/env python3
"""
weather_service.py
Polling service for BMP280 + DHT22 with Firebase logging
"""

import time
import logging
import getpass
import signal
import requests
from datetime import datetime
import os
import sys
import fcntl
import threading

from bmp280.bmp280 import read_bmp280_pipe
from dht22_kernel.dht22 import read_dht22_data
from weather_station_backend import WeatherStationBackend

import firebase_admin
from firebase_admin import credentials, firestore

# ==============================
# Configuration
# ==============================
WEATHER_DATA_POLL_INTV_SEC = 60
LOCATION_DATA_POLL_CYCLES = 60*24 # Once a day

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"weather_station_controller_{getpass.getuser()}.log")

# ==============================
# Setup Logging
# ==============================
# Open / Create log file
with open(LOG_FILE, "w") as f:
    f.write("Log started\n")

logger = logging.getLogger("WeatherStationController")
logger.setLevel(logging.DEBUG)
logger.propagate = False

# Individual handler
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

# ==============================
# Graceful Shutdown
# ==============================
def handle_signal(sig, frame):
    logger.info(f"Received signal {sig}, shutting down...")

if threading.current_thread() is threading.main_thread():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

# ==============================
# Sensor Readout
# ==============================
def read_sensors():
    """
    Reads both BMP280 and DHT22 sensors.
    Returns a dict or None if failed.
    """
    try:
        bmp280 = read_bmp280_pipe()
        dht22 = read_dht22_data()
        logger.debug(f"DHT22: {dht22}")
        logger.debug(f"BMP280: {bmp280}")
        return {
            "data_temp_dht22": dht22.temperature / 10,
            "data_humi_dht22": dht22.humidity / 10,
            "data_valid_dht22": dht22.validity,
            "data_temp_bmp280": bmp280.temperature,
            "data_pres_bmp280": bmp280.pressure,
            "data_valid_bmp280": bmp280.validity,
        }
    except Exception as e:
        logger.error(f"Failed to read sensors: {e}")
        return None

# ==============================
# Location Data
# ==============================
def get_location():
    try:
        logger.debug(f"Getting location data")        
        response = requests.get("https://ipinfo.io/json")
        data = response.json()
        loc = data.get("loc", "").split(",")
        return {
            "loc_city": data.get("city"),
            "loc_region": data.get("region"),
            "loc_country": data.get("country"),
            "loc_lat": float(loc[0]),
            "loc_lon": float(loc[1])
        }
    except Exception as e:
        logger.error(f"Failed to get location: {e}")
        return {
            "loc_city": "Error - No Data",
            "loc_region": "Error - No Data",
            "loc_country": "Error - No Data",
            "loc_lat": "Error - No Data",
            "loc_lon": "Error - No Data"
        }

# ==============================
# Worker method
# ==============================
def weather_station_controller_worker(stop_event):
    # Allow only one instance of the process to run
    lock_file = open(f'/tmp/{os.path.splitext(os.path.basename(__file__))[0]}.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error("Service already running - Unable to run another instance.")
        sys.exit(1)
    
    # Start service - step by step
    logger.info("Weather Station Controller started.")
    
    # Get backend interface to the database
    backend = WeatherStationBackend()
    
    # Get current location for the first time
    location = get_location()

    # First read of sensors - discard data, hardware initializing, possibly imprecise
    read_sensors()     
    time.sleep(3)
    
    # Runtime - periodic polling
    loc_update = 0
    while not stop_event.is_set():
        data = read_sensors()
        backend.push_raw_data({**data, **location})
        time.sleep(WEATHER_DATA_POLL_INTV_SEC)
                
        # Update location periodically
        loc_update = (loc_update + 1) % LOCATION_DATA_POLL_CYCLES
        if loc_update == 0:
            location = get_location()                        

    logger.info("Weather Station Controller stopped / shutdown")

if __name__ == "__main__":
    weather_station_controller_worker()
