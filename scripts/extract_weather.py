import requests
import json
import os
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Load Path
from settings import RAW_WEATHER_DATA_DIR, LOGS_DATA_DIR

# Load API KEY
load_dotenv()
WEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

def extract_weather_data(date):
    if not WEATHER_API_KEY:
        raise ValueError("API Key not found. Check in .env file")

    # Setup logger
    day = date.strftime("%Y-%m-%d")
    log_file = LOGS_DATA_DIR / f'weather_extract_{day}.log'
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f"Starting weather data extraction for {day}")

    # Destination City list
    cities = {
        "Tokyo": "Tokyo,JP",
        "Seoul": "Seoul,KR",
        "Singapore": "Singapore,SG",
        "Kuala Lumpur": "Kuala Lumpur,MY",
        "Taipei": "Taipei,TW",
        "Ho Chi Minh": "Ho Chi Minh City,VN",
        "Hong Kong": "Hong Kong,HK"
    }

    weather_results = []
    print(f'Get Weather Data on date: {date}')

    for city_name, query in cities.items():
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": query,
            "appid": WEATHER_API_KEY,
            "units": "metric"
        }

        try:
            response = requests.get(url=url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                weather_results.append({
                    "city": city_name,
                    "query": query,
                    "data": data
                })
                log_msg = f"Fetched weather at {city_name} Successed"
                print(log_msg)
                logging.info(log_msg)
            else:
                err = f"HTTP ERROR {response.status_code} for {city_name}:{response.text}"
                print(err)
                logging.info(err)
        except requests.exceptions.RequestException as e:
            err = f"Request failed for {city_name}: {e}"
            print(err)
            logging.info(err)


    # Save to file
    save_path = RAW_WEATHER_DATA_DIR / day
    save_path.mkdir(parents=True, exist_ok=True)

    with open(save_path / "data.json", "w", encoding="utf-8") as f:
        json.dump(weather_results, f, ensure_ascii=False, indent=2)

    logging.info(f"Saved {len(weather_results)} records to {save_path / 'data.json'}")
    print(f"Saved {len(weather_results)} records to {save_path / 'data.json'}")
    logging.info("🏁 Weather extraction completed\n")


if __name__ == "__main__":
    date = datetime.now(pytz.timezone("Asia/Bangkok"))
    # print(f'Get Weather Data on date: {date}')
    extract_weather_data(date)