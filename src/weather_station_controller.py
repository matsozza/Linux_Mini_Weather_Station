#!/usr/bin/env python3
"""
weather_service.py
Polling service for BMP280 + DHT22 with Firebase logging
"""

import time
import logging
import getpass
import signal
import json
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
WEATHER_DATA_POLL_INTV_SEC = 30
LOCATION_DATA_POLL_CYCLES = 60*24 # Once a day

LOCAL_FILE = "./weather_station_sensor_buffer.jsonl"

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
# Sensor Readout & Treatment
# ==============================
def read_sensors():
    """
    Reads both BMP280 and DHT22 sensors.
    Returns a dict or None if failed.
    """
    try:
        bmp280 = read_bmp280_pipe()
        dht22 = read_dht22_data()
        logger.debug(f"    DHT22: {dht22}")
        logger.debug(f"    BMP280: {bmp280}")
        return {
            "data_temp_dht22": float(dht22.temperature / 10),
            "data_humi_dht22": float(dht22.humidity / 10),
            "data_valid_dht22": dht22.validity,
            "data_temp_bmp280": float(bmp280.temperature),
            "data_pres_bmp280": float(bmp280.pressure),
            "data_valid_bmp280": bmp280.validity,
        }
    except Exception as e:
        logger.error(f"Failed to read sensors: {e}")
        return None

def treat_sensors(sensors_dict):
    """
    Process sensor data based on their validity and
    populate high level environmental data
    """
    
    # Initialize treated data
    temperature = None
    humidity = None
    pressure = None
    
    # Populate treated data based on sensor availability
    if sensors_dict["data_valid_dht22"] and sensors_dict["data_valid_bmp280"]:
        temperature = sensors_dict["data_temp_dht22"]*0.5 + sensors_dict["data_temp_bmp280"]*0.5
        humidity = sensors_dict["data_humi_dht22"]
        pressure = sensors_dict["data_pres_bmp280"]               
    else:
        pass
    
    # Return treated data dictionary
    return {
        "data_temperature": (temperature),
        "data_humidity": (humidity),
        "data_pressure": (pressure)
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
        logger.error(f"Service {__file__} already running - Unable to run another instance.")
        sys.exit(1)
    
    # Start service - step by step
    logger.info("Weather Station Controller started.")
    
    # Get backend interface to the database
    backend = WeatherStationBackend()
    
    # Get current location for the first time
    location = backend.get_current_location()

    # First read of sensors - discard data, hardware initializing, possibly imprecise (observed in BMP280)
    read_sensors()     
    time.sleep(3)
    
    # Runtime - periodic polling
    loc_update = 0
    while not stop_event.is_set():
        # Read sensors
        logger.debug("\n")
        logger.debug("Reading sensors")
        sensors_dict = read_sensors()
        
        # Treat sensors data and push treated data to DB (or keep locally in case of failure)
        if sensors_dict is not None:
            logger.debug("Treating sensors")            
            sensors_treated_dict = treat_sensors(sensors_dict)       
            timestamp = datetime.now().astimezone()

            # Save sensor data locally (local file system)
            try:
                logger.debug(f"Writing sensor data to local file - Timestamp is {timestamp.isoformat()}")                
                os.makedirs(os.path.dirname(LOCAL_FILE), exist_ok=True)
                with open(LOCAL_FILE, "a") as f:
                    f.write(json.dumps({**sensors_treated_dict, "timestamp": timestamp.isoformat(), "location": location}) + "\n")
            except Exception as e:
                logger.debug(e)
           
            # Push sensor data to remote database + remove local copy if successful (limit pushes per cycle to don't block other sequential/cyclical tasks)
            try:                
                remaining = []
                with open(LOCAL_FILE, "r") as f:
                    for idx, line in enumerate(f):                        
                        try:                           
                            if idx>=5: # Early-break (no long block)
                                if idx==5:
                                    logger.debug(f"Total of {idx} sensor entries pushed (or attempted) to DB from local file in this cycle - Starting early break")      
                                remaining.append(line)      
                            else: # Regular DB push
                                file_sensor_full_entry = json.loads(line)
                                timestamp = datetime.fromisoformat(file_sensor_full_entry.pop("timestamp"))
                                location = file_sensor_full_entry.pop("location")
                                
                                logger.debug(f"Pushing sensor data to DB from local file - Timestamp is {timestamp.isoformat()}; File Entry is {idx}")                
                                backend.push_sensor_aggregated_data_daily(
                                    {**file_sensor_full_entry, **location},
                                    timestamp
                                ) # This method throws an error in case of failure (captured in the try-except)

                        except Exception as e:
                            logger.warning(f"Upload failed: {e}")
                            remaining.append(line)

                with open(LOCAL_FILE, "w") as f:
                    f.writelines(remaining)
                
            except Exception as e:
                logger.debug(e)                     
                               
        # Update location periodically (slower than sensor polling)
        loc_update = (loc_update + 1) % LOCATION_DATA_POLL_CYCLES
        if loc_update == 0:
            location = get_location()                        
            
        # Time to wait until polling sensors again
        time.sleep(WEATHER_DATA_POLL_INTV_SEC)

    logger.info("Weather Station Controller stopped / shutdown")

if __name__ == "__main__":
    weather_station_controller_worker()
