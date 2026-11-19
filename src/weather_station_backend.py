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
import requests
import sqlite3
from pathlib import Path
from firebase_admin import credentials, firestore
from google.cloud import firestore as google_firestore
from google.cloud.firestore_v1 import FieldFilter
from datetime import datetime, timedelta, timezone

# ==============================
# Configuration
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"weather_station_backend_{getpass.getuser()}.log")

BACKEND_WORKER_INTERVAL_SEC = 60*5 # 5min

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
            
            self.collection_name_per_day = "weather-data-per-day"
            self.collection_name_api = "weather-data-api"
            
            self.connection_error_cntr = 0
            
            self.cache_db = "weather_station_cache.db"
            self._initialize_cache()
            
            self._cache_last_full_sync_date = None
            self._cache_last_sync_time = None
            
            # Initialize Firebase only if not already initialized
            if (not firebase_admin._apps) and (not WeatherStationBackend._initialized):
                logger.info("Initializing Firebase...")
                cred = credentials.Certificate(self.db_key)
                firebase_admin.initialize_app(credential=cred)
                WeatherStationBackend._initialized = True
            logger.info("Firebase initialized")
        
            self.db = firestore.client()
            
    def _initialize_cache(self):
        """Initialize local SQLite cache."""

        with sqlite3.connect(self.cache_db) as conn:

            conn.execute("""
                CREATE TABLE IF NOT EXISTS weather_cache (
                    timestamp TEXT NOT NULL,
                    source TEXT NOT NULL,

                    temperature REAL,
                    pressure REAL,
                    humidity REAL,

                    loc_lat REAL,
                    loc_lon REAL,

                    PRIMARY KEY (timestamp, source)
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_weather_cache_source_timestamp
                ON weather_cache(source, timestamp)
            """)

        logger.info("SQLite cache initialized")

# ----------------------------- Sensor Raw & Aggregated Data (Real Time) ----------------------------
            
    def push_sensor_aggregated_data_daily(self, data_dict, force_timestamp = None):
        if not data_dict:
            return
        try:
            # Complete timestamp
            timestamp = None
            if force_timestamp is None:
                timestamp = datetime.now().astimezone()
            else:
                timestamp = force_timestamp
                       
            # Daily timestamp
            timestamp_daily = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            doc_id_daily = timestamp_daily.strftime("%Y-%m-%d_%z")
           
            # Prepare database document
            doc_data = {
                "count": firestore.Increment(1),
                "timestamp_daily": timestamp_daily,
                "data": firestore.ArrayUnion([{
                            **data_dict,
                            "timestamp": timestamp
                        }])
                }

            # Write to daily collection
            self.db.collection(self.collection_name_per_day).document(doc_id_daily).set(doc_data, merge=True, timeout=5.0, retry=None)                                    
            logger.debug("Sensor data pushed to Firebase")
        except Exception as e:
            logger.error(f"Failed to push sensor data to Firebase: {e}")
            
            self.connection_error_cntr+=1            
            if self.connection_error_cntr > 10:
                logger.info("Error threshold exceeded - Restarting Backend client")
                sys.exit(1)
                self.connection_error_cntr=0                            
                
            raise # Re-raise exception to the caller
    
# ------------------------------ Weather API Data (from external service) ---------------------------
    def query_and_push_weather_api_aggregated_data_daily(self, force_ts = None):
        try:
            loc_data = self.get_current_location()

            # Open-Meteo API endpoint specifying exact current variables needed
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={loc_data['loc_lat']}&longitude={loc_data['loc_lon']}"
                f"&current=temperature_2m,relative_humidity_2m,surface_pressure"
            )
            
            # Send the GET request to the API
            response = requests.get(url)
            response.raise_for_status()  # Check for any HTTP errors
            
            # Parse the JSON response
            data = response.json()
            current = data.get("current", {})
            
            # Extract the specific data points
            api_data = {} 
            api_data["data_temperature"] = current.get("temperature_2m")
            api_data["data_humidity"] = current.get("relative_humidity_2m")
            api_data["data_pressure"] = current.get("surface_pressure")/10
            
            # Push to database
            timestamp=force_ts # Override timestamp
            if force_ts is None:
                timestamp = datetime.now().astimezone()
                                   
            # Daily timestamp
            timestamp_daily = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            doc_id_daily = timestamp_daily.strftime("%Y-%m-%d_%z")
           
            # Prepare database document
            doc_data = {
                "count": firestore.Increment(1),
                "timestamp_daily": timestamp_daily,
                "data": firestore.ArrayUnion([{
                            **api_data,
                            **loc_data,
                            "timestamp": timestamp
                        }])
                }

            # Write to daily collection
            self.db.collection(self.collection_name_api).document(doc_id_daily).set(doc_data, merge=True, timeout=5.0, retry=None)
                                    
            logger.debug("API data pushed to Firebase")    
        except Exception as e:
            logger.error(f"Failed to query & push API Data to Firebase - {e}")            

# ------------------------------ Location Data (from external service) ------------------------------

    def get_current_location(self):
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

# ------------------------------------- Data Fetchers - Common ---------------------------------------

    def fetch_aggregated_data_daily(self, timeframe = "24h", collection = "sensor", limit = -1):
        try:
            now = datetime.now().astimezone()
            lookback_map = {
                "5m": timedelta(minutes=5),
                "1h": timedelta(hours=1),
                "24h": timedelta(hours=24),
                "7d": timedelta(days=7),
                "1 month": timedelta(days=30),
                "3 months": timedelta(days=90),
                "6 months": timedelta(days=180),
                "1 year": timedelta(days=365),
                "all": timedelta(days=36500),
            }

            # Prepare used timestamps for database query and for filtering
            start_time = now - lookback_map.get(timeframe, timedelta(hours=24)) - timedelta(minutes=45) # + Extra 45min
            start_time_daily_query = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Prepare database query
            if collection == "sensor":
                collection = self.db.collection(self.collection_name_per_day)
            else:
                collection = self.db.collection(self.collection_name_api)
                
            base_query = (
                collection
                .where(filter=FieldFilter("timestamp_daily", ">=", start_time_daily_query))
            )

            # Read from Database
            docs = [doc.reference.get().to_dict() for doc in base_query.stream()]
            if not docs:
                return pd.DataFrame()
            
            # Parse data to a dataframe / concatenate database documents if needed
            df = pd.DataFrame()
            for doc in docs:
                df = pd.concat([df, pd.DataFrame(doc['data'])], ignore_index=True)
                
            # Sort by timestamp + Filter by 'start_time'
            df = df.sort_values(by="timestamp")
            df = df[(df["timestamp"] >= start_time)]
            return df
        except Exception as e:
            logger.error(f"Failed to fetch aggregated daily data from Firebase - {e}")
            return pd.DataFrame()

    def sync_local_cache_aggregated_data_daily(self):
        """Synchronize Firestore weather data with the local SQLite cache."""

        now = datetime.now(timezone.utc)
        today = now.date()

        # Decide between full and incremental sync
        full_sync = self._cache_last_full_sync_date != today

        if full_sync:
            start_time = now - timedelta(days=365)
            logger.info("Starting full cache sync (last 365 days)")
        else:
            start_time = self._cache_last_sync_time - timedelta(minutes=10)
            logger.info("Starting incremental cache sync")

        # Firestore query works at the daily-document level
        start_time_daily = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        collections = [
            (self.collection_name_per_day, "sensor"),
            (self.collection_name_api, "api"),
        ]

        rows = []

        try:
            for collection_name, source in collections:

                docs = (
                    self.db.collection(collection_name)
                    .where(
                        filter=FieldFilter(
                            "timestamp_daily", ">=", start_time_daily
                        )
                    )
                    .stream()
                )

                for doc in docs:
                    data = doc.to_dict().get("data", [])

                    for item in data:
                        timestamp = item.get("timestamp")

                        if timestamp is None:
                            continue

                        # Normalize timestamp to UTC
                        if isinstance(timestamp, str):
                            timestamp = datetime.fromisoformat(
                                timestamp.replace("Z", "+00:00")
                            )

                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        else:
                            timestamp = timestamp.astimezone(timezone.utc)

                        # Discard records outside the desired window
                        if timestamp < start_time:
                            continue

                        rows.append((
                            timestamp.isoformat(),
                            source,
                            item.get("data_temperature"),
                            item.get("data_pressure"),
                            item.get("data_humidity"),
                            item.get("loc_lat"),
                            item.get("loc_lon"),
                        ))

            # Insert or update SQLite records
            with sqlite3.connect(self.cache_db) as conn:
                conn.executemany("""
                    INSERT INTO weather_cache (
                        timestamp, source, temperature, pressure,
                        humidity, loc_lat, loc_lon
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)

                    ON CONFLICT(timestamp, source) DO UPDATE SET
                        temperature = COALESCE(
                            excluded.temperature, weather_cache.temperature
                        ),
                        pressure = COALESCE(
                            excluded.pressure, weather_cache.pressure
                        ),
                        humidity = COALESCE(
                            excluded.humidity, weather_cache.humidity
                        ),
                        loc_lat = COALESCE(
                            excluded.loc_lat, weather_cache.loc_lat
                        ),
                        loc_lon = COALESCE(
                            excluded.loc_lon, weather_cache.loc_lon
                        )
                """, rows)

            # Update sync state only after successful completion
            self._cache_last_sync_time = now

            if full_sync:
                self._cache_last_full_sync_date = today

            logger.info(
                f"Cache sync completed: {len(rows)} records processed"
            )

        except Exception:
            logger.exception("Cache synchronization failed")

# ==============================
# Worker method
# ==============================
def weather_station_backend_worker(stop_event):    
    # Allow only one instance of the process to run
    lock_file = open(f'/tmp/{os.path.splitext(os.path.basename(__file__))[0]}.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error(f"Service {__file__} already running - Unable to run another instance.")
        sys.exit(1)
    
    # Start service - step by step
    logger.info("Weather Station Backend started.")
    
    backend = WeatherStationBackend()
    logger.info("Firebase Connection Initialized.")
    
    while not stop_event.is_set():
        backend.query_and_push_weather_api_aggregated_data_daily()
        backend.sync_local_cache_aggregated_data_daily()
        time.sleep(BACKEND_WORKER_INTERVAL_SEC)

    logger.info("Weather Station Backend stopped / shutdown")

if __name__ == "__main__":
    # --- DEBUGGING / TESTING BLOCK ---
    logger.info("Starting Weather Station Backend Debug Mode...")
    
    try:
        # Initialize Service
        backend = WeatherStationBackend()
        logger.info("Firebase Connection Initialized.")
        
        if 1:
            data = backend.fetch_aggregated_data_daily()
        
        if 0:
            
            time_init = datetime(2026, 3, 10, 14, 30, 0).astimezone() 
            time_fin =  datetime(2026, 3, 21, 16, 13, 0).astimezone() 
            
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
                try:
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
                except Exception as e:
                    pass
            
            total = 0
            for idx, data in enumerate(treated_data):
                if idx%5 == 0:
                    #print(f"Date pushing - {data['timestamp'].astimezone()} - Total: {total}")
                    backend.push_sensor_aggregated_data_daily(data, force_timestamp=data["timestamp"].astimezone())                
                    total = total + 1

    except Exception as e:
        logger.info(f"DEBUG FAILED: {e}")