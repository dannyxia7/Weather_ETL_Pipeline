import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_weather_data(weather_data):
    transformed_data = {"weather": [], "forecast": []}

    for city, weather_info in weather_data["weather"].items():
        transformed_data["weather"].append({
            "city": city,
            "country": weather_info.get("sys", {}).get("country", ""),  # Extracting country code
            "latitude": weather_info.get("coord", {}).get("lat"),  # Extracting latitude
            "longitude": weather_info.get("coord", {}).get("lon"),  # Extracting longitude
            "timestamp": weather_info["dt"],
            "temperature": weather_info["main"]["temp"],
            "humidity": weather_info["main"]["humidity"],
            "weather_condition": weather_info["weather"][0]["description"]
        })

    for city, forecast_info in weather_data["forecast"].items():
        for forecast in forecast_info["list"]:
            transformed_data["forecast"].append({
                "city": city,
                "timestamp": forecast["dt"],
                "temperature": forecast["main"]["temp"],
                "humidity": forecast["main"]["humidity"],
                "weather_condition": forecast["weather"][0]["description"]
            })

    # Extract alerts if available
    for city, alerts_info in weather_data.get("alerts", {}).items():
        for alert in alerts_info:
            transformed_data["alerts"].append({
                "city_id": alert["city_id"],  # Assuming city_id is part of the alert
                "alert_type": alert["event"],
                "alert_description": alert["description"],
                "timestamp": alert["start"],  # Using alert start time
            })

    logger.info("Data transformation completed.")
    logger.debug(f"Transformed Data: {json.dumps(transformed_data, indent=4)}")  # Log the entire transformed data

    return transformed_data
