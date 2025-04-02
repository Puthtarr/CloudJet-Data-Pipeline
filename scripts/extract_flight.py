import requests
import json
import os
import pytz
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import logging
from datetime import datetime, timedelta

# Load path
from settings import RAW_FLIGHT_DATA_DIR, LOGS_DATA_DIR

# Load api key from .env
load_dotenv()
AVIATION_API_KEY = os.getenv("AVIATIONSTACK_API_KEY")

def extract_flight_data(date):
    # Check API usable
    if not AVIATION_API_KEY:
        raise ValueError("API Key not found. Check in .env file")

    # Setup Logger
    day = date.strftime("%Y-%m-%d")
    log_file = LOGS_DATA_DIR / f'flight_extract_{day}.log'
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f"Starting flight data extraction for {day}")
    # API Documentary >> https://aviationstack.com/documentation
    # Scope = out from donmuang, suvanaphom
    departure_airport = ["BKK"]
    arrival_airports = ["NRT", "HND", "ICN", "SIN", "KUL", "TPE", "SGN", "HKG"]

    print(f'Get Flight Data on date: {date}')

    all_result = []

    for dep in departure_airport:
        for arr in arrival_airports:
            url = f"http://api.aviationstack.com/v1/flights"
            params = {
                "access_key": AVIATION_API_KEY,
                "dep_iata": dep,
                "arr_iata": arr,
                "limit": 100
            }

            try:
                response = requests.get(url=url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data:
                        count = len(data["data"])
                        all_result.extend(data["data"])
                        log_msg = f"{dep} → {arr}: {count} flights"
                        print(f"{log_msg}")
                        logging.info(log_msg)
                    else:
                        msg = f"No data for {dep} → {arr}"
                        print(f"{msg}")
                        logging.warning(msg)
                else:
                    err = f"HTTP ERROR {response.status_code} for {dep} → {arr}: {response.text}"
                    print(f"{err}")
                    logging.error(err)

            except requests.exceptions.RequestException as e:
                err = f"Request failed for {dep} → {arr}: {e}"
                print(f"{err}")
                logging.error(err)

    save_path = RAW_FLIGHT_DATA_DIR / day
    save_path.mkdir(parents=True, exist_ok=True)

    with open(save_path / "data.json", "w", encoding="utf-8") as f:
        json.dump(all_result, f, ensure_ascii=False, indent=2)

    logging.info(f"Saved {len(all_result)} records to {save_path / 'data.json'}")
    print(f"\nSaved {len(all_result)} records to {save_path / 'data.json'}")
    logging.info("Extraction completed\n")

if __name__ == "__main__":
    date = datetime.now(pytz.timezone("Asia/Bangkok")) - timedelta(days=0)
    # print(f'Get Flight Data on date: {date}')
    extract_flight_data(date)
