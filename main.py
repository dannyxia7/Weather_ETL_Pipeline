from extract import get_weather_data, save_raw_data_to_json
from transform import transform_weather_data, save_transformed_data_to_json
from load import load_data_to_db
import json

with open('./config/config.json') as config_file:
    config = json.load(config_file)
    api_key = config['api_key']

cities_to_check = ["San Diego", "Los Angeles", "New York"]

# Extract
raw_data = get_weather_data(cities_to_check, api_key, max_retries=3)
save_raw_data_to_json(raw_data, f"./data/raw_weather_data.json")

# Transform
transformed_data = transform_weather_data(raw_data)
save_transformed_data_to_json(transformed_data, f"./data/transformed_weather_data.json")

# Load
load_data_to_db(transformed_data, "weather_data")