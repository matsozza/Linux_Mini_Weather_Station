#!/usr/bin/env python3
"""
weather_service.py
Polling service for BMP280 + DHT22 with Firebase logging
"""

import time
import logging
import signal
import requests
import os
from datetime import datetime

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
COLLECTION_NAME = "weather-data-raw"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "weather-station.log")

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

logger = logging.getLogger("WeatherStation")
logger.setLevel(logging.INFO)
logger.propagate = False

# ==============================
# Graceful Shutdown
# ==============================
running = True

def handle_signal(sig, frame):
    global running
    logger.info(f"Received signal {sig}, shutting down...")
    running = False

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
        logger.info(f"DHT22: {dht22}")
        logger.info(f"BMP280: {bmp280}")
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
        logger.info(f"Getting location data")        
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
def push_to_firebase(database, data):
    if not data:
        return
    try:
        doc_ref = database.collection(COLLECTION_NAME).document(f"{datetime.now()}")
        doc_ref.set(data)
        logger.info("Data pushed to Firebase")
    except Exception as e:
        logger.error(f"Failed to push to Firebase: {e}")

# ==============================
# Main Loop
# ==============================
def main():
    logger.info("Weather Service started.")
    database = init_firebase()
    location = get_location()
    
    # First read - discard data, hardware initializing
    read_sensors() 
    
    time.sleep(5)
    
    cycles = 0
    while running:
        data = read_sensors()
        push_to_firebase(database, {**data, **location})
        time.sleep(POLL_INTERVAL_SEC)
        cycles = cycles + 1
        
        # Update location periodically
        if cycles % LOCATION_POLL_CYCLES == 0:
            location = get_location()                        

    logger.info("Weather Service stopped.")

if __name__ == "__main__":
    main()
