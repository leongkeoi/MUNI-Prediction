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
    # Initialize timers in the past so they trigger immediately upon boot
    last_api_fetch = 0
    last_approximation = 0
    
    print("Gathering transit data...")
    
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
                
                # CRITICAL: Your 5-second protective safety delay to prevent API burst blocking
                time.sleep(5)  
            
            # Reset timers cleanly after the full 15-second loop cycle completes
            last_api_fetch = time.time()
            last_approximation = last_api_fetch  
            
        # Rule 2: Update clock calculations using local memory data every 10 seconds
        # CHANGED: Using a clean 'if' instead of 'elif' so it checks independently
        if current_time - last_approximation >= APPROXIMATION_INTERVAL:
            for line_id in STOPS.keys():
                # Read directly from local variables instead of calling fetch_transit_data()
                cached_payload = last_fetched_payloads.get(line_id)
                if cached_payload:
                    process_transit_payload(line_id, cached_payload)
            
            # REMOVED: The 5-second sleep from the local calculation pass.
            # Because this rule reads pure text from your Pi's memory cache, 
            # it hits 0% network traffic and cannot trigger API burst limits.
            last_approximation = time.time()
            
        # Keep execution loops light without freezing system resources
        time.sleep(0.5)

# =========================
# INTERFACE EXECUTION LOGIC
# =========================

if __name__ == "__main__":
    # Import Henner Zeller's library inside the main thread wrapper
    from rgbmatrix import RGBMatrix, RGBMatrixOptions, graphics # type: ignore

    # 1. Launch the non-blocking background network data processor
    network_thread = threading.Thread(target=data_pipeline_worker, daemon=True)
    network_thread.start()
    
    # Wait briefly for baseline extraction data to populate initial state variables
    time.sleep(2)
    
    print("\nMUNI Prediction App Started. Initializing LED Matrix hardware...")
    print("-" * 70)

    # 2. Configure Henner Zeller's Core Hardware Options
    options = RGBMatrixOptions()
    options.rows = 32
    options.cols = 64
    options.chain_length = 1
    options.parallel = 1
    options.hardware_mapping = 'adafruit-hat'  # Change to 'adafruit-hat-pwm' if using Quality mode
    options.gpio_slowdown = 1                  # Minimizes text flickering on Raspberry Pi 4/5

    matrix = RGBMatrix(options = options)
    canvas = matrix.CreateFrameCanvas()

    # 3. Load Your Local 9-Pixel Tall Font Asset
    font = graphics.Font()
    font.LoadFont("fonts/6x9.bdf")

    # 4. Define Visual Cosmetics (Colors for your MUNI lines)
    color_l = graphics.Color(255, 215, 0)      # MUNI Yellow for L Taraval
    color_nb = graphics.Color(0, 255, 100)     # Mint Green for 29 Sunset NB
    color_sb = graphics.Color(0, 200, 255)     # Sky Blue for 29 Sunset SB

    # 5. Initialize Horizontal Positions Starting at the Right Wall (64px)
    x_l = 64.0
    x_nb = 64.0
    x_sb = 64.0

    # 6. Customize Scroll Speeds (Pixels per frame)
    speed_l = 1.0
    speed_nb = 1.2
    speed_sb = 0.8

    # 7. Your Exact 0-Indexed Layout Coordinate Plan
    y_l = 8
    y_nb = 19
    y_sb = 30

    # 8. Main Concurrent Animation Frame Loop
    try:
        while True:
            # Wipe the frame clean before painting the new positions
            canvas.Clear()

            # Thread-safe read: Snap a fast copy of the strings behind the lock
            with state_lock:
                str_l = shared_display_strings["L"]
                str_nb = shared_display_strings["29_NB"]
                str_sb = shared_display_strings["29_SB"]

            # Draw all three text elements to our offscreen buffer frame
            len_l = graphics.DrawText(canvas, font, int(x_l), y_l, color_l, str_l)
            len_nb = graphics.DrawText(canvas, font, int(x_nb), y_nb, color_nb, str_nb)
            len_sb = graphics.DrawText(canvas, font, int(x_sb), y_sb, color_sb, str_sb)

            # Shift tracking positions leftward for the next render pass
            x_l -= speed_l
            x_nb -= speed_nb
            x_sb -= speed_sb

            # Reset each sentence to the right side if it scrolls off the left edge
            if x_l < -len_l: x_l = canvas.width
            if x_nb < -len_nb: x_nb = canvas.width
            if x_sb < -len_sb: x_sb = canvas.width

            # Flip the offscreen canvas to the hardware screen on next vertical sync
            canvas = matrix.SwapOnVSync(canvas)
            
            # Smooth 30FPS clock pacing delay
            time.sleep(0.03)
            
    except KeyboardInterrupt:
        print("\nStopping display loop. Matrix cleared. Clean exit complete.")
