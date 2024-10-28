# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import requests
import time
import json
import logging
from requests.exceptions import RequestException

# Define the API endpoint and parameters
with open('./config/config.json') as config_file:
    config = json.load(config_file)
    api_key = config['api_key']

# Set up logging
logging.basicConfig(level=logging.INFO)
BASE_URL = "http://api.openweathermap.org/data/2.5/forecast"
CITY = "San Diego"
RATE_LIMIT = 60  # Max 60 requests per minute for the free tier

def get_weather_data(city, api_key, max_retries):
    params = {
        "q": city,
        "appid": api_key,
        "units": "imperial"
    }
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get("Retry-After", 60))
                logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
            elif response.status_code == 200:
                return response.json()
            else:
                logging.error(f"Failed to fetch data. Status code: {response.status_code}")
                return None
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            retries += 1
            time.sleep(2 ** retries)  # Exponential backoff
    logging.error("Max retries reached. Could not fetch data.")
    return None

def save_raw_data_to_json(data, filename="./data/raw_weather_data.json"):
    with open(filename, 'w') as file:
        json.dump(data, file)

# cities_to_check = ["San Diego", "Los Angeles", "New York"]  # Add more cities as needed
#
# weather_data = get_weather_data(cities_to_check, api_key, max_retries=3)

