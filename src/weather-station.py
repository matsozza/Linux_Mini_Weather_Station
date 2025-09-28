#!/usr/bin/env python3
"""
weather_service.py
Polling service for BMP280 + DHT22 with Firebase logging
"""

import time
import logging
import signal
from datetime import datetime

from bmp280.bmp280 import read_bmp280_pipe
from dht22_kernel.dht22 import read_dht22_data

import firebase_admin
from firebase_admin import credentials, firestore

# ==============================
# Configuration
# ==============================
POLL_INTERVAL_SEC = 60
LOG_FILE = "/home/matheus/weather.log"
FIREBASE_KEY = "./database_key.json"
COLLECTION_NAME = "weather-data-sjc"

# ==============================
# Setup Logging
# ==============================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger("WeatherService")

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
            "temperature_dht22": dht22.temperature / 10,
            "humidity_dht22": dht22.humidity / 10,
            "validity_dht22": dht22.validity,
            "temperature_bmp280": bmp280.temperature,
            "pressure_bmp280": bmp280.pressure,
            "validity_bmp280": bmp280.validity,
        }
    except Exception as e:
        logger.error(f"Failed to read sensors: {e}")
        return None

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

    while running:
        data = read_sensors()
        push_to_firebase(database, data)
        time.sleep(POLL_INTERVAL_SEC)

    logger.info("Weather Service stopped.")

if __name__ == "__main__":
    main()
