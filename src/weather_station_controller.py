#!/usr/bin/env python3
"""
weather_service.py
Polling service for BMP280 + DHT22 with Firebase logging
"""

import time
import logging
import signal
import requests
from datetime import datetime
import os
import sys
import fcntl

from bmp280.bmp280 import read_bmp280_pipe
from dht22_kernel.dht22 import read_dht22_data

import firebase_admin
from firebase_admin import credentials, firestore

# ==============================
# Configuration
# ==============================
POLL_INTERVAL_SEC = 60
LOCATION_POLL_CYCLES = 60*24 # Once a day

FIREBASE_KEY = "./database_key.json"
RAW_COLLECTION_NAME = "weather-data-raw"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "weather-station-controller.log")

# ==============================
# Setup Logging
# ==============================

# Check if we can write to it (if not, remove it)
if not os.access(LOG_FILE, os.W_OK) and os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

# Recreate it (it will be empty and owned by the current user)
with open(LOG_FILE, "w") as f:
    f.write("Log started\n")
os.chmod(LOG_FILE, 0o666) # Ensure suitable permissions for everyone

logger = logging.getLogger("WeatherStationController")
logger.setLevel(logging.INFO)
logger.propagate = False

# Individual handler
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

# ==============================
# Graceful Shutdown
# ==============================
def handle_signal(sig, frame):
    logger.info(f"Received signal {sig}, shutting down...")

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# ==============================
# Firebase Setup
# ==============================
def init_firebase():
    logger.info("Initializing Firebase...")
    cred = credentials.Certificate(FIREBASE_KEY)
    firebase_admin.initialize_app(cred)
    logger.info("Firebase initialized")
    return firestore.client()

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
# Firebase Push
# ==============================
def push_raw_data_to_firebase(database, raw_data):
    if not raw_data:
        return
    try:
        raw_data['timestamp'] = datetime.now().astimezone()
        timestamp_id = datetime.now().astimezone().strftime("%Y-%m-%d__%H-%M-%S__%z")
        doc_ref = database.collection(RAW_COLLECTION_NAME).document(timestamp_id)
        doc_ref.set(raw_data)
        logger.debug("Data pushed to Firebase")
    except Exception as e:
        logger.error(f"Failed to push to Firebase: {e}")

# ==============================
# Main Loop
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
    logger.info("Weather Service started.")
    database = init_firebase()
    location = get_location()

    # First read of sensors - discard data, hardware initializing, possibly imprecise
    read_sensors()     
    time.sleep(3)
    
    # Runtime - periodic polling
    loc_update = 0
    while not stop_event.is_set():
        data = read_sensors()
        push_raw_data_to_firebase(database, {**data, **location})
        time.sleep(POLL_INTERVAL_SEC)
                
        # Update location periodically
        loc_update = (loc_update + 1) % LOCATION_POLL_CYCLES
        if loc_update == 0:
            location = get_location()                        

    logger.info("Weather Service stopped / shutdown")

if __name__ == "__main__":
    weather_station_controller_worker()
