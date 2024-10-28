import json

def transform_weather_data(weather_data):
    transformed_data = []
    for entry in weather_data['list']:
        weather_info = {
            "city": weather_data['city']['name'],
            "timestamp": entry['dt_txt'],
            "temperature": entry['main']['temp'],
            "humidity": entry['main']['humidity'],
            "weather_description": entry['weather'][0]['description'],
            "wind_speed": entry['wind']['speed']
        }
        transformed_data.append(weather_info)
    return transformed_data

def save_transformed_data_to_json(transformed_data, filename="./data/transformed_weather_data.json"):
    with open(filename, 'w') as file:
        json.dump(transformed_data, file)
