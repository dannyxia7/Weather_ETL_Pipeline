import os
import requests
import time
import json
import logging
from datetime import datetime

# Define the API endpoint and parameters
api_key = os.getenv('API_KEY')

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(filename=f'weather_etl_log',
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger()

base_url = "http://api.openweathermap.org/data/2.5/"
os.makedirs("./data", exist_ok=True)

# Max 60 requests per minute for the free tier


def fetch_api_data(endpoint, params, max_retries=3):
    """Fetch data from the OpenWeather API with error handling and exponential backoff."""
    retry_count = 0
    backoff_time = 1  # initial backoff time in seconds

    while retry_count < max_retries:
        try:
            response = requests.get(endpoint, params=params, timeout=10)

            if response.status_code == 200:
                logger.info("Successfully fetched data from %s", endpoint)
                return response.json()

            elif response.status_code == 403:
                logger.error("Error 403: Forbidden – Check API key and permissions.")
                return None

            elif response.status_code == 404:
                logger.error("Error 404: Not Found – Verify the endpoint URL.")
                return None

            else:
                logger.warning("Unexpected response code %d received from the API", response.status_code)

        except requests.exceptions.Timeout:
            logger.error("Timeout error on attempt %d", retry_count + 1)
        except requests.exceptions.ConnectionError:
            logger.error("Connection error on attempt %d", retry_count + 1)
        except Exception as e:
            logger.error("Unexpected error on attempt %d: %s", retry_count + 1, str(e))

        # Increment retry count, apply exponential backoff, and try again
        retry_count += 1
        time.sleep(backoff_time)
        backoff_time *= 2  # Exponential increase (1s, 2s, 4s, etc.)

    logger.error("Failed to fetch data from %s after %d attempts", endpoint, max_retries)
    return None


def extract_weather_data(cities, units='imperial'):
    """Extract data from both the weather and forecast endpoints."""

    cities_data = {
        "weather": {},
        "forecast": {}
    }

    for city in cities:
        logger.info(f"Fetching data for city: {city}")

        # Define parameters for weather and forecast endpoints
        params = {
            'q': city,  # example city
            'appid': api_key,
            'units': units
        }

        # Fetch current weather data
        weather_data = fetch_api_data(f"{base_url}weather", params)
        forecast_data = fetch_api_data(f"{base_url}forecast", params)

        # Only save data if both requests were successful
        if weather_data and forecast_data:
            cities_data["weather"][city] = weather_data
            cities_data["forecast"][city] = forecast_data
        else:
            logger.error(f"Data extraction failed for city: {city}")
            raise ValueError(f"Failed to extract both weather and forecast data for city: {city}")

    # Save all cities' data to a single JSON file
    output_file = f"./data/all_cities_weather_data_{timestamp}.json"
    with open(output_file, "w") as f:
        json.dump(cities_data, f, indent=4)

    logger.info(f"All cities' data saved to {output_file}")

    return cities_data

