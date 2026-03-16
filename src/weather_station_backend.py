import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import firestore as google_firestore 
from google.cloud.firestore_v1 import FieldFilter
from datetime import datetime, timedelta

class WeatherStationBackend:
    def __init__(self):
        # Use the same key path as your weather_service.py
        self.db_key = "./database_key.json"
        self.collection_name_raw = "weather-data-raw"
        self.collection_name_hourly = "weather-data-hourly"
        
        # Initialize Firebase only if not already initialized
        if not firebase_admin._apps:
            cred = credentials.Certificate(self.db_key)
            firebase_admin.initialize_app(cred)
        
        self.db = firestore.client()

    def fetch_raw_data(self, timeframe: str) -> pd.DataFrame:
        now = datetime.now().astimezone()
        lookback_map = {
            "5m": timedelta(minutes=5),
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "1 month": timedelta(days=30),
            "1 year": timedelta(days=365),
            "all": timedelta(days=36500) # 100 years
        }
        start_time = now - lookback_map.get(timeframe, timedelta(hours=24))

        # Example to get collection size only
        query = (
            self.db.collection(self.collection_name_raw)
            .where(filter=FieldFilter("timestamp", ">=", start_time))
            .order_by("timestamp")
        )

        # This returns an aggregation query, then you get() the result
        total_count = query.count().get()[0][0].value

        # Get pointer to data stream
        docs = (
            self.db.collection(self.collection_name_raw)
            .where(filter=FieldFilter("timestamp", ">=", start_time))
            .order_by("timestamp")
            .stream()
        )
        
        print(f"Total documents: {total_count}")

        data_list = []
        for doc in docs:
            data = doc.to_dict()
            data_list.append(data)

        if not data_list:
            return pd.DataFrame()

        return pd.DataFrame(data_list).sort_values("timestamp")
    
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

                print(f"Summarizing data from {time_range_init} to {time_range_fin}...")
                
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
                    print(f"No data found for {target_doc}. Skipping summary for this one.")
                    continue

                # Create dataframe and remove invalid rows
                df = pd.DataFrame(raw_data_list)
                cols = ["data_valid_bmp280", "data_valid_dht22"]  
                df = df[df[cols].all(axis=1)]

                # 2. Calculate Stats                
                summary = {
                    "timestamp_processed": now,
                    "window_start": time_range_init,
                    "window_end": time_range_fin,
                    "sample_count": len(df),
                    "data_timestamps": list(df["timestamp"])
                }

                for col in df.columns:
                    if col.startswith("data_"):
                        if col.startswith("data_valid"):
                            pass # Ignore validities since already filtered in dataframe
                        else:
                            summary[f"{col}_avg"] = float(df[col].mean())
                            summary[f"{col}_min"] = float(df[col].min())
                            summary[f"{col}_max"] = float(df[col].max())
                            summary[f"{col}_values"] = list(df[col])
                    else:
                        if col == "timestamp":
                            pass # Ignore timestamp
                        else:
                            # Take the first non-null value from the column
                            summary[col] = df[col].dropna().iloc[0]

                # 3. Save to Firestore
                # Using the hour's start time as the Document ID keeps things organized
                self.db.collection("weather-data-hourly").document(target_doc).set(summary)
                
                print(f"Successfully saved summary to 'weather-data-hourly' with ID: {target_doc}")
    
if __name__ == "__main__":
    # --- DEBUGGING / TESTING BLOCK ---
    print("Starting DataService Debug Mode...")
    
    try:
        # 1. Initialize Service
        service = WeatherStationBackend()
        print("Firebase Connection Initialized.")

        # 2. Test Fetching
        test_timeframe = "5m"
        print(f"Fetching data for: {test_timeframe}...")
        
        df = service.fetch_raw_data(test_timeframe)
        
        # Infer current timezone and correct timestamps
        local_tz = datetime.now().astimezone().tzinfo
        df["timestamp"] = (
                            pd.to_datetime(df["timestamp"], utc=True)
                            .dt.tz_convert(local_tz)
                           )   

        # 3. Validation
        if df.empty:
            print("Warning: No data found in the specified range.")
            print("Check your Collection Name and Document ID format.")
        else:
            print("\n --- DATA SUMMARY ---")
            print(f"Total Rows: {len(df)}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 5 entries:")
            print(df[['timestamp', 'data_temp_bmp280', 'data_humi_dht22']].head())
            
            print("\n --- STATS ---")
            print(f"Average Temp (BMP280): {df['data_temp_bmp280'].mean():.2f}°C")
            print(f"Latest Timestamp: {df['timestamp'].max()}")
            
        service.process_hourly_summary()

    except Exception as e:
        print(f"DEBUG FAILED: {e}")