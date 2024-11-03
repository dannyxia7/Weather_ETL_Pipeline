import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_weather_data(weather_data):
    transformed_data = {"weather": [], "forecast": []}

    for city, weather_info in weather_data["weather"].items():
        transformed_data["weather"].append({
            "city": city,
            "country": weather_info.get("sys", {}).get("country", ""),
            "latitude": weather_info.get("coord", {}).get("lat", None),
            "longitude": weather_info.get("coord", {}).get("lon", None),
            "timestamp": datetime.fromtimestamp(weather_info["dt"], None),
            "temperature": weather_info.get("main", {}).get("temp", None),
            "humidity": weather_info.get("main", {}).get("humidity", None),
            "weather_condition": (weather_info.get("weather", [{}])[0].get("description", "Unknown"))
        })

    for city, forecast_info in weather_data["forecast"].items():
        for forecast in forecast_info["list"]:
            transformed_data["forecast"].append({
                "city": city,
                "timestamp": datetime.fromtimestamp(forecast["dt"], None),
                "temperature": forecast.get("main", {}).get("temp", None),
                "humidity": forecast.get("main", {}).get("humidity", None),
                "weather_condition": (forecast.get("weather", [{}])[0].get("description", "Unknown"))
            })

    # # Extract alerts if available
    # for city, alerts_info in weather_data.get("alerts", {}).items():
    #     for alert in alerts_info:
    #         transformed_data["alerts"].append({
    #             "city_id": alert["city_id"],  # Assuming city_id is part of the alert
    #             "alert_type": alert["event"],
    #             "alert_description": alert["description"],
    #             "timestamp": datetime.fromtimestamp(alert["start"]),  # Using alert start time
    #         })

    # Convert data for JSON serialization (ISO 8601 string format for timestamp)
    serializable_data = {
        "weather": [
            {**entry, "timestamp": entry["timestamp"].isoformat()} for entry in transformed_data["weather"]
        ],
        "forecast": [
            {**entry, "timestamp": entry["timestamp"].isoformat()} for entry in transformed_data["forecast"]
        ]
    }

    logger.info("Data transformation completed.")
    logger.debug(f"Transformed Data: {json.dumps(serializable_data, indent=4)}")  # Log the entire transformed data

    return serializable_data
