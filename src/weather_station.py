import threading
import time
import sys
import os
import logging
from weather_station_controller import weather_station_controller_worker

# ==============================
# Configuration
# ==============================
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

# Individual handler
if not logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

# ==============================
# Threads / Workers setup
# ==============================

def start_workers():
    workers = {
        "controller": threading.Thread(target=weather_station_controller_worker, daemon=True),
    }
    for name, t in workers.items():
        logger.info(f"Starting worker '{name}' for Weather Station")        
        t.start()
    return workers

def supervise(workers):
    while True:
        for name, t in workers.items():
            if not t.is_alive():
                logger.error(f"Worker {name} died. Restarting service.")
                sys.exit(1)  # systemd restarts the whole service
        time.sleep(1)


if __name__ == "__main__":
    logger.info("Starting workers for Weather Station")
    workers = start_workers()
    supervise(workers)
