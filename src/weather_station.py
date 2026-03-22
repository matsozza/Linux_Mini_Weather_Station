import threading
import time
import sys
import os
import logging
import getpass
import signal
import fcntl
import subprocess
from weather_station_controller import weather_station_controller_worker
from weather_station_backend import weather_station_backend_worker

# ==============================
# Configuration
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"weather_station_{getpass.getuser()}.log")

# ==============================
# Setup Logging
# ==============================
# Open / Create log file
with open(LOG_FILE, "w") as f:
    f.write("Log started\n")

logger = logging.getLogger("WeatherStation")
logger.setLevel(logging.DEBUG)
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
shutdown_event = threading.Event()
def handle_signal(sig, frame):
    logger.info(f"Received signal {sig}, shutting down workers...")
    shutdown_event.set()

if threading.current_thread() is threading.main_thread():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

def start_workers():
    # Start thread based workers
    workers = {
        "backend": threading.Thread(target=weather_station_backend_worker, args=(shutdown_event,), daemon=True, name="backend"),
        "controller": threading.Thread(target=weather_station_controller_worker, args=(shutdown_event,), daemon=True, name="controller"),
    }
    for name, t in workers.items():
        logger.info(f"Starting worker '{name}' for Weather Station")        
        t.start()        
        
    # Start process based workers
    logger.info("Starting worker 'frontend' (Streamlit Process)")
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    STREAMLIT_BIN = os.path.join(BASE_DIR, "venv/bin/streamlit")
    workers["frontend"] = subprocess.Popen([STREAMLIT_BIN, "run", "./weather_station_frontend.py", "--server.port", "8501", "--server.headless", "true"])   

    return workers

def supervise(workers):
    while not shutdown_event.is_set():
        for name, w in workers.items():
            # Logic for Subprocess (Streamlit)
            if isinstance(w, subprocess.Popen):
                is_running = (w.poll() is None)
            # Logic for Threads (Backend/Controller)
            else:
                is_running = w.is_alive()

            if not is_running:
                logger.error(f"Worker '{name}' died. Restarting service.")
                # Kill remaining processes before exiting so we don't leave 'zombies'
                cleanup_workers(workers, logger)
                sys.exit(1)
        
        time.sleep(10)

    logger.info("Supervisor shutting down, cleaning up workers...")
    cleanup_workers(workers, logger)
    logger.info("All workers stopped. Exiting.")
    sys.exit(0)

def cleanup_workers(workers, logger):
    """Helper to stop both threads and processes safely."""
    for name, w in workers.items():
        if isinstance(w, subprocess.Popen):
            if w.poll() is None: # If still running
                logger.info(f"Terminating process: {name}")
                w.terminate()
                w.wait(timeout=5)
        elif isinstance(w, threading.Thread):
            if w.is_alive():
                logger.info(f"Waiting for thread: {name}")
                w.join(timeout=5)

if __name__ == "__main__":
    # Allow only one instance of the process to run
    lock_file = open(f'/tmp/{os.path.splitext(os.path.basename(__file__))[0]}.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error(f"Service {__file__} already running - Unable to run another instance.")
        sys.exit(1)
    
    logger.info("Starting workers for Weather Station")
    workers = start_workers()
    supervise(workers)
