import os
import sys
import fcntl
import pandas as pd
import firebase_admin
import logging
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
LOG_FILE = os.path.join(BASE_DIR, "weather-station-backend.log")

PROCESS_HOURLY_SUMMARY_INTERVAL_SEC = 60*5 # 5min

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
            self.collection_name_hourly = "weather-data-hourly"
            
            # Initialize Firebase only if not already initialized
            if (not firebase_admin._apps) and (not WeatherStationBackend._initialized):
                logger.info("Initializing Firebase...")
                cred = credentials.Certificate(self.db_key)
                firebase_admin.initialize_app(credential=cred)
                WeatherStationBackend._initialized = True
            logger.info("Firebase initialized")
        
            self.db = firestore.client()

    def fetch_raw_data(self, timeframe: str, limit: int = 60*24) -> pd.DataFrame:
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
        import numpy as np
        limit = min(limit, len(refs)) if limit != -1 else len(refs)
        idx = np.linspace(0, len(refs) - 1, limit, dtype=int)

        # Step 3: fetch only selected docs
        docs = [refs[i].get().to_dict() for i in idx]

        return pd.DataFrame(docs).sort_values("timestamp")
    
    def push_raw_data(self, raw_data):
        if not raw_data:
            return
        try:
            raw_data['timestamp'] = datetime.now().astimezone()
            timestamp_id = datetime.now().astimezone().strftime("%Y-%m-%d__%H-%M-%S__%z")
            doc_ref = self.db.collection(self.collection_name_raw).document(timestamp_id)
            doc_ref.set(raw_data)
            logger.debug("Raw data pushed to Firebase")
        except Exception as e:
            logger.error(f"Failed to push to Firebase: {e}")
    
    def process_hourly_summary(self, prev_hours = 24):
            """
            Sweeps the last hour(s) of raw data, calculates stats, 
            and saves to 'weather-data-hourly'.
            """
            
            target_doc_ids = []
            now = datetime.now().astimezone()
            for prev_hour in range(1, prev_hours):
                target_time = (now - timedelta(hours = prev_hour)).replace(minute=0, second=0, microsecond=0)
                target_doc_ids.append(  target_time.strftime("%Y-%m-%d__%H-%M-%S__%z")  )
            
            found_doc_ids = [
                    self.db.collection(self.collection_name_hourly).document(doc_id).get()
                    for doc_id in target_doc_ids
            ]
            found_doc_ids = [d.id for d in found_doc_ids if d.exists]
            
            # Update target docs with only the ones that aren't present in the DB
            target_doc_ids = [doc for doc in target_doc_ids if doc not in found_doc_ids]

            for target_doc in target_doc_ids:
                time_range_init = datetime.strptime(target_doc, "%Y-%m-%d__%H-%M-%S__%z")
                time_range_fin = datetime.strptime(target_doc, "%Y-%m-%d__%H-%M-%S__%z") + timedelta(hours=1)

                logger.info(f"\n\nSummarizing data from {time_range_init} to {time_range_fin}...")
                
                # Fetch raw documents to aggregate in hourly collection
                found_raw_doc_ids = (
                    self.db.collection(self.collection_name_raw)
                    .where(filter=FieldFilter("timestamp", ">=", time_range_init))
                    .where(filter=FieldFilter("timestamp", "<=", time_range_fin))
                    .order_by("timestamp")
                    .stream()
                )

                raw_data_list = []
                for raw_doc in found_raw_doc_ids:
                    data = raw_doc.to_dict()
                    raw_data_list.append(data)
                if not raw_data_list:
                    logger.info(f"No data found for {target_doc}. Skipping summary for this one.")
                    continue

                # Create dataframe and remove invalid rows
                df = pd.DataFrame(raw_data_list)
                cols = ["data_valid_bmp280", "data_valid_dht22"]  
                df = df[df[cols].all(axis=1)]

                # Calculate Stats                
                summary = {
                    "timestamp_processed": now,
                    "window_start": time_range_init,
                    "window_end": time_range_fin,
                    "sample_count": len(df),
                    "list_data_timestamps": list(df["timestamp"])
                }

                for col in df.columns:
                    if col.startswith("data_"):
                        if col.startswith("data_valid"):
                            pass # Ignore validities since already filtered in dataframe
                        else:
                            summary[f"{col}_avg"] = float(df[col].mean())
                            summary[f"{col}_min"] = float(df[col].min())
                            summary[f"{col}_max"] = float(df[col].max())
                            summary[f"{col}_std"] = float(df[col].std())
                            summary[f"list_{col}_values"] = list(df[col])
                    else:
                        if col == "timestamp":
                            pass # Ignore timestamp
                        else:
                            # Take the first non-null value from the column
                            summary[col] = df[col].dropna().iloc[0]

                # Save to Firestore
                # Using the hour's start time as the Document ID keeps things organized
                self.db.collection("weather-data-hourly").document(target_doc).set(summary)
                logger.info(f"Successfully saved summary to 'weather-data-hourly' with ID: {target_doc}")
    
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
        backend.process_hourly_summary()
        time.sleep(PROCESS_HOURLY_SUMMARY_INTERVAL_SEC)

    logger.info("Weather Station Backend stopped / shutdown")

if __name__ == "__main__":
    # --- DEBUGGING / TESTING BLOCK ---
    logger.info("Starting Weather Station Backend Debug Mode...")
    
    try:
        # Initialize Service
        backend = WeatherStationBackend()
        logger.info("Firebase Connection Initialized.")

        # Test Fetching
        test_timeframe = "5m"
        logger.info(f"Fetching data for: {test_timeframe}...")
        
        df = backend.fetch_raw_data(test_timeframe)
        
        # Infer current timezone and correct timestamps
        local_tz = datetime.now().astimezone().tzinfo
        df["timestamp"] = (pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(local_tz))   

        # 3. Validation
        if df.empty:
            logger.info("Warning: No data found in the specified range.")
            logger.info("Check your Collection Name and Document ID format.")
        else:
            logger.info("\n --- DATA SUMMARY ---")
            logger.info(f"Total Rows: {len(df)}")
            logger.info(f"Columns: {list(df.columns)}")
            logger.info("\nFirst 5 entries:")
            logger.info(df[['timestamp', 'data_temp_bmp280', 'data_humi_dht22']].head())
            
            logger.info("\n --- STATS ---")
            logger.info(f"Average Temp (BMP280): {df['data_temp_bmp280'].mean():.2f}°C")
            logger.info(f"Latest Timestamp: {df['timestamp'].max()}")
            
        backend.process_hourly_summary()

    except Exception as e:
        logger.info(f"DEBUG FAILED: {e}")