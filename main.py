import psycopg2
from extract import extract_weather_data
from transform import transform_weather_data
from load import load_data_to_db

# List of cities to fetch weather data for
city_list = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Diego"]


def main():
    raw_weather_data = extract_weather_data(city_list)
    transformed_data = transform_weather_data(raw_weather_data)
    load_data_to_db(transformed_data)

if __name__ == "__main__":
    main()