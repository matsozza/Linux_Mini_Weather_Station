import os
import sys
import fcntl
import pandas as pd
import firebase_admin
import logging
import getpass
import signal
import time
import threading
import numpy as np
from firebase_admin import credentials, firestore
from google.cloud import firestore as google_firestore
from google.cloud.firestore_v1 import FieldFilter
from datetime import datetime, timedelta

# ==============================
# Configuration
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"weather_station_backend_{getpass.getuser()}.log")

PROCESS_HOURLY_SUMMARY_INTERVAL_SEC = 60*5 # 5min

# ==============================
# Setup Logging
# ==============================
# Open / Create log file
with open(LOG_FILE, "w") as f:
    f.write("Log started\n")

logger = logging.getLogger("WeatherStationBackend")
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
# Backend for ext. & internal users
# ==============================

class WeatherStationBackend:
    _initialized = False
    _firebase_lock = threading.Lock()
    
    def __init__(self):
        # Thread safe initialization
        with WeatherStationBackend._firebase_lock:            
            # Use the same key path as your weather_service.py
            self.db_key = "./database_key.json"
            
            self.collection_name_raw = "weather-data-raw"
            self.collection_name_per_hour = "weather-data-per-hour"
            self.collection_name_per_day = "weather-data-per-day"
            
            self.collection_name_test = "weather-data-test"
            
            # Initialize Firebase only if not already initialized
            if (not firebase_admin._apps) and (not WeatherStationBackend._initialized):
                logger.info("Initializing Firebase...")
                cred = credentials.Certificate(self.db_key)
                firebase_admin.initialize_app(credential=cred)
                WeatherStationBackend._initialized = True
            logger.info("Firebase initialized")
        
            self.db = firestore.client()

# ------------------------------- Sensor & Aggregated Data (Real Time) -------------------------------

    def fetch_sensor_data(self, timeframe: str, limit: int = 60*24) -> pd.DataFrame:
        now = datetime.now().astimezone()

        lookback_map = {
            "5m": timedelta(minutes=5),
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "1 month": timedelta(days=30),
            "1 year": timedelta(days=365),
            "all": timedelta(days=36500),
        }

        start_time = now - lookback_map.get(timeframe, timedelta(hours=24))

        collection = self.db.collection(self.collection_name_raw)

        base_query = (
            collection
            .where(filter=FieldFilter("timestamp", ">=", start_time))
            .order_by("timestamp")
        )

        # Step 1: fetch only references
        refs = [doc.reference for doc in base_query.stream()]

        if not refs:
            return pd.DataFrame()

        # Step 2: evenly spaced indices
        limit = min(limit, len(refs)) if limit != -1 else len(refs)
        idx = np.linspace(0, len(refs) - 1, limit, dtype=int)

        # Step 3: fetch only selected docs
        docs = [refs[i].get().to_dict() for i in idx]

        return pd.DataFrame(docs).sort_values("timestamp")

    def fetch_aggregated_data_hourly(self, timeframe = "24h", limit = -1):
        now = datetime.now().astimezone()

        lookback_map = {
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "1 month": timedelta(days=30),
            "1 year": timedelta(days=365),
            "all": timedelta(days=36500),
        }

        start_time = now - lookback_map.get(timeframe, timedelta(hours=24))
        collection = self.db.collection(self.collection_name_per_hour)
        base_query = (
            collection
            .where(filter=FieldFilter("window_start", ">=", start_time))
            .order_by("window_start")
        )

        # Step 1: fetch only references
        refs = [doc.reference for doc in base_query.stream()]
        if not refs:
            return pd.DataFrame()

        # Step 2: evenly spaced indices
        limit = min(limit, len(refs)) if limit != -1 else len(refs)
        idx = np.linspace(0, len(refs) - 1, limit, dtype=int)

        # Step 3: fetch only selected docs
        docs = [refs[i].get().to_dict() for i in idx]
        
        # Get raw data from databse and sort based on timestamps
        df_raw = pd.DataFrame(docs).sort_values("window_start")
        
        # Create a dataframe with the main values sorted and processed
        df = pd.DataFrame()
        df["timestamp"] = df_raw["list_data_timestamps"].sum()
        df["data_temperature"] = (pd.DataFrame(df_raw["list_data_temp_dht22_values"].sum()) +  pd.DataFrame(df_raw["list_data_temp_bmp280_values"].sum()))/2
        df["data_pressure"] = df_raw["list_data_pres_bmp280_values"].sum()
        df["data_humidity"] = df_raw["list_data_humi_dht22_values"].sum()
        return df
    
    def push_sensor_data(self, sensor_dict):
        if not sensor_dict:
            return
        try:
            sensor_dict['timestamp'] = datetime.now().astimezone()
            timestamp_id = datetime.now().astimezone().strftime("%Y-%m-%d__%H-%M-%S__%z")
            doc_ref = self.db.collection(self.collection_name_raw).document(timestamp_id)
            doc_ref.set(sensor_dict)
            logger.debug("Sensor data pushed to Firebase")
        except Exception as e:
            logger.error(f"Failed to push to Firebase: {e}")
            
    def push_aggregated_data(self, data_dict, hourly=True, daily=True, force_timestamp = None):
        if not data_dict:
            return
        try:
            # Complete timestamp
            timestamp = None
            if force_timestamp is None:
                timestamp = datetime.now().astimezone()
            else:
                timestamp = force_timestamp
                
            # Hourly timestamp
            timestamp_hourly = timestamp.replace(minute=0, second=0, microsecond=0)
            doc_id_hourly = timestamp_hourly.strftime("%Y-%m-%d__%H-%M-%S__%z")               
            
            # Daily timestamp
            timestamp_daily = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            doc_id_daily = timestamp_daily.strftime("%Y-%m-%d_%z")
           
            # Prepare database document
            doc_data = {
                "count": firestore.Increment(1),
                "data": firestore.ArrayUnion([{
                            **data_dict,
                            "timestamp": timestamp
                        }])
                }
            
            # Write to hourly collection
            if hourly:
                doc_ref = self.db.collection(self.collection_name_per_hour).document(doc_id_hourly)
                doc_ref.set(doc_data, merge=True)
            
            # Write to daily collection
            if daily:
                doc_ref2 = self.db.collection(self.collection_name_per_day).document(doc_id_daily)                
                doc_ref2.set(doc_data, merge=True)
                                    
            logger.debug("Sensor data pushed to Firebase")
        except Exception as e:
            logger.error(f"Failed to push to Firebase: {e}")
    
# --------------------------------------- Backend operations -----------------------------------------
    
    def process_hourly_summary(self, prev_hours = 24):
            """
            Sweeps the last hour(s) of sensor data, calculates stats, 
            and saves to 'weather-data-hourly'.
            """
            
            # TODO: Remove
            return # Deactivated code for now
        
# ==============================
# Worker method
# ==============================
def weather_station_backend_worker(stop_event):    
    # Allow only one instance of the process to run
    lock_file = open(f'/tmp/{os.path.splitext(os.path.basename(__file__))[0]}.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error("Service already running - Unable to run another instance.")
        sys.exit(1)
    
    # Start service - step by step
    logger.info("Weather Station Backend started.")
    
    backend = WeatherStationBackend()
    logger.info("Firebase Connection Initialized.")
    
    while not stop_event.is_set():
        #backend.process_hourly_summary()
        time.sleep(PROCESS_HOURLY_SUMMARY_INTERVAL_SEC)

    logger.info("Weather Station Backend stopped / shutdown")

if __name__ == "__main__":
    # --- DEBUGGING / TESTING BLOCK ---
    logger.info("Starting Weather Station Backend Debug Mode...")
    
    try:
        # Initialize Service
        backend = WeatherStationBackend()
        logger.info("Firebase Connection Initialized.")
        
        # Start to move data to new DB format
        min_5_behind = 24*60
        while min_5_behind > 1:

            time_init = datetime.now().astimezone() - timedelta(minutes = min_5_behind)
            time_fin = time_init + timedelta(minutes=5)
            #time_fin = time_init + timedelta(minutes= 15)
            
            collection = backend.db.collection(backend.collection_name_raw)

            base_query = (
                collection
                .where(filter=FieldFilter("timestamp", ">=", time_init))
                .where(filter=FieldFilter("timestamp", "<=", time_fin))
                .order_by("timestamp")
            )

            # Get data from raw collection
            docs = [doc for doc in base_query.stream()]
            data_list = [doc.to_dict() for doc in docs]
            
            treated_data = []
            for data in data_list:
                # Initialize treated data
                temperature = None
                humidity = None
                pressure = None
                
                # Populate treated data based on sensor availability
                if data["data_valid_dht22"] and data["data_valid_bmp280"]:
                    temperature = data["data_temp_dht22"]*0.5 + data["data_temp_bmp280"]*0.5
                    humidity = data["data_humi_dht22"]
                    pressure = data["data_pres_bmp280"]               
                elif data["data_valid_dht22"]:
                    temperature = data["data_temp_dht22"]
                    humidity = data["data_humi_dht22"]
                elif data["data_valid_bmp280"]:
                    temperature = data["data_temp_bmp280"]
                    pressure = data["data_pres_bmp280"]  
                else:
                    pass
                
                # Return treated data dictionary
                treated_data.append( {
                    "loc_city": data["loc_city"],
                    "loc_country": data["loc_country"],
                    "loc_lat": data["loc_lat"],
                    "loc_lon": data["loc_lon"],
                    "loc_region": data["loc_region"],
                    
                    "data_temperature": (temperature),
                    "data_humidity": (humidity),
                    "data_pressure": (pressure),
                    
                    "timestamp": data["timestamp"].astimezone()
                })
                
            
            for idx, data in enumerate(treated_data):
                backend.push_aggregated_data(data, force_timestamp=data["timestamp"].astimezone())                
                    
            min_5_behind = min_5_behind - 5
        
    except Exception as e:
        logger.info(f"DEBUG FAILED: {e}")