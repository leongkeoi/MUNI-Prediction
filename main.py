import os
import time
import json
import threading
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

# Import the Pydantic models we built
from models import TransitAPIResponse

# Load environment variables from .env file
load_dotenv()

# ==========================================
# CONFIGURATION & SETTINGS
# ==========================================
USE_MOCK_DATA = False  # Set to False to hit the live 511 API

# Fetch variables from .env
API_KEY = os.getenv("API_KEY", "f71959ec-37b4-4e53-a5db-0d90073b94de")
API_BASE_URL = os.getenv("API_BASE_URL", "http://511.org/transit/StopMonitoring")

# Endpoint Configurations
STOPS = {
    "L": {"stopcode": "16641", "mock_file": "mock_L.json"},
    "29_NB": {"stopcode": "16556", "mock_file": "mock_29NB.json"},
    "29_SB": {"stopcode": "16555", "mock_file": "mock_29SB.json"},
}

# Timing Intervals (in seconds)
API_FETCH_INTERVAL = 181
APPROXIMATION_INTERVAL = 10
DISPLAY_SWITCH_INTERVAL = 10

# ==========================================
# THREAD-SAFE STATE STORAGE (The Bridge)
# ==========================================
state_lock = threading.Lock()
shared_display_strings = {
    "L": "L Taraval: Loading...",
    "29_NB": "29 Sunset: Loading...",
    "29_SB": "29 Sunset: Loading...",
}

last_fetched_payloads = {
    "L": None,
    "29_NB": None,
    "29_SB": None,
}

def fetch_transit_data(line_id: str):
    """Handles network request or reads from disk based on environment toggle."""
    if USE_MOCK_DATA:
        filename = STOPS[line_id]["mock_file"]
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Static testing file '{filename}' missing in directory.")
            return None
    else:
        params = {
            "api_key": API_KEY,
            "agency": "SF",
            "stopcode": STOPS[line_id]["stopcode"],
            "format": "json"
        }
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=10)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "30")
                wait_time = int(retry_after)
                print(f"⚠️ [429 Throttled] Rate limit hit for line {line_id}. Retrying after {wait_time} seconds.")
                time.sleep(wait_time)
                return None
            if response.status_code == 200:
                return response.json()
            else:
                print(f"API Error ({response.status_code}) for line {line_id}")
                return None
        except Exception as e:
            print(f"Network exception hitting 511 servers: {e}")
            return None
        
# ==========================================
# TRANSIT DATA LOGIC
# ==========================================

def format_minutes_list(minutes: list) -> str:
    """Formats a list of integers into a clean 'x, y, and z' string pattern."""
    if not minutes:
        return "No predictions"
    if len(minutes) == 1:
        return f"{minutes[0]} minute" if minutes[0] == 1 else f"{minutes[0]} minutes"
    if len(minutes) == 2:
        return f"{minutes[0]} and {minutes[1]} minutes"
    return ", ".join(map(str, minutes[:-1])) + f", and {minutes[-1]} minutes"

def process_transit_payload(line_id: str, raw_data: dict):
    """Parses JSON data via Pydantic and calculates countdown deltas thread-safely."""
    try:
        # Validate data structure using Pydantic
        parsed_response = TransitAPIResponse(**raw_data)
        
        delivery = parsed_response.service_delivery.delivery
        stop_visits = delivery.stop_visits
        
        if not stop_visits:
            with state_lock:
                shared_display_strings[line_id] = f"{line_id} line: No buses active"
            return

        # Extract baseline info from the primary journey
        first_journey = stop_visits[0].journey
        line_ref = first_journey.line_ref
        line_name = first_journey.line_name.title()
        destination = first_journey.monitored_call.destination_display
        
        # Calculate arrival windows against current timezone-aware clock
        now = datetime.now(timezone.utc)
        minutes_until_arrival = []
        
        for visit in stop_visits:
            # Skip records missing foundational components
            if not visit or not visit.journey:
                continue

            #skip 1970 ghost schedules
            if visit.recorded_at.year == 1970:
                continue

            call = visit.journey.monitored_call
            arrival_time = call.expected_arrival_time or call.aimed_arrival_time

            # Ensure arrival time is timezone aware
            if arrival_time.tzinfo is None:
                arrival_time = arrival_time.replace(tzinfo=timezone.utc)
                
            delta = arrival_time - now
            mins = int(delta.total_seconds() / 60)
            # Filter out vehicles that have already technically passed the stop
            if mins >= 0:
                minutes_until_arrival.append(mins)
                
        # Sort values chronologically
        minutes_until_arrival.sort()
        minutes_str = format_minutes_list(minutes_until_arrival)
        
        # Construct your exact requested sentence format
        final_sentence = f"{line_ref} {line_name} to {destination} in {minutes_str}"
        
        # Safely push string across the data bridge
        with state_lock:
            shared_display_strings[line_id] = final_sentence
            
    except Exception as e:
        print(f"Error parsing data for line {line_id}: {e}")

def fetch_transit_data(line_id: str):
    """Handles network request or reads from disk based on environment toggle."""
    if USE_MOCK_DATA:
        filename = STOPS[line_id]["mock_file"]
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Static testing file '{filename}' missing in directory.")
            return None
    else:
        # Construct live URL parameters using protected variables
        params = {
            "api_key": API_KEY,
            "agency": "SF",
            "stopcode": STOPS[line_id]["stopcode"],
            "format": "json"
        }
        try:
            # 511 API payload text sometimes starts with a BOM indicator; requests resolves it
            response = requests.get(API_BASE_URL, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"API Error ({response.status_code}) for line {line_id}")
                return None
        except Exception as e:
            print(f"Network exception hitting 511 servers: {e}")
            return None

# ==========================================
# BACKGROUND DATA THREAD LOOPS (Non-blocking)
# ==========================================

def data_pipeline_worker():
    """Background engine managing network calls (181s) and time approximations (10s)."""
    last_api_fetch = 0
    last_approximation = 0
    
    print("🚀 Starting background data pipeline engine...")
    
    while True:
        current_time = time.time()
        
        # Rule 1: Hit API / Refresh Disk cache every 181 seconds
        if current_time - last_api_fetch >= API_FETCH_INTERVAL:
            for line_id in STOPS.keys():
                payload = fetch_transit_data(line_id)
                if payload:
                    # Save the download cache to memory for future approximations
                    last_fetched_payloads[line_id] = payload
                    process_transit_payload(line_id, payload)
                time.sleep(5)  # Stagger requests to avoid hitting API burst block rate limits
            last_api_fetch = time.time()
            last_approximation = last_api_fetch  # Reset approximation timer after a fresh fetch
            
        # Rule 2: Update clock calculations using local memory data every 10 seconds
        elif current_time - last_approximation >= APPROXIMATION_INTERVAL:
            for line_id in STOPS.keys():
                # Read directly from local variables instead of calling fetch_transit_data()
                cached_payload = last_fetched_payloads.get(line_id)
                if cached_payload:
                    process_transit_payload(line_id, cached_payload)
                time.sleep(5)  # Stagger requests to avoid hitting API burst block rate limits
            last_approximation = time.time()
            
        # Keep execution loops light without freezing system resources
        time.sleep(0.5)

# ==========================================
# INTERFACE EXECUTION LOGIC (Simulation Thread)
# ==========================================

if __name__ == "__main__":
    # Launch the non-blocking background network data processor
    network_thread = threading.Thread(target=data_pipeline_worker, daemon=True)
    network_thread.start()
    
    # Wait briefly for baseline extraction data to populate initial state variables
    time.sleep(2)
    
    print("\n🖥️ Simulating LED Frame Engine Output (Press Ctrl+C to terminate):")
    print("-" * 70)
    
    # Run a simulated display switching circuit loop (alternates active key every 15s)
    lines_pool = list(STOPS.keys())
    pool_index = 0
    
    try:
        while True:
            active_line = lines_pool[pool_index]
            
            # Pull data safely across the locked bridge to avoid tearing
            with state_lock:
                current_text = shared_display_strings[active_line]
                
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] [LED Row Display Active] ➡️  {current_text}")
            
            # Switch target pointer key for the next loop pass iteration
            pool_index = (pool_index + 1) % len(lines_pool)
            
            # Wait for display visibility window before toggling matrix display frame
            time.sleep(DISPLAY_SWITCH_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nStopping data loop execution. Clean exit complete.")
