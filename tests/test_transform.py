import unittest
from unittest.mock import patch
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from transform import transform_weather_data  # Make sure to import correctly

class TestTransformWeatherData(unittest.TestCase):

    @patch("transform.logger")  # Mock logger to prevent log output during testing
    def test_transform_weather_data(self, mock_logger):
        # Sample input for testing
        weather_data = {
            "weather": {
                "San Diego": {
                    "sys": {"country": "US"},
                    "coord": {"lat": 32.7157, "lon": -117.1611},
                    "dt": 1609459200,
                    "main": {"temp": 70, "humidity": 50},
                    "weather": [{"description": "clear sky"}]
                }
            },
            "forecast": {
                "San Diego": {
                    "list": [
                        {
                            "dt": 1609462800,
                            "main": {"temp": 72, "humidity": 45},
                            "weather": [{"description": "partly cloudy"}]
                        }
                    ]
                }
            }
        }

        expected_output = {
            "weather": [
                {
                    "city": "San Diego",
                    "country": "US",
                    "latitude": 32.7157,
                    "longitude": -117.1611,
                    "timestamp": "2020-12-31T16:00:00",
                    "temperature": 70,
                    "humidity": 50,
                    "weather_condition": "clear sky"
                }
            ],
            "forecast": [
                {
                    "city": "San Diego",
                    "timestamp": "2020-12-31T17:00:00",
                    "temperature": 72,
                    "humidity": 45,
                    "weather_condition": "partly cloudy"
                }
            ]
        }

        # Call the function to test
        result = transform_weather_data(weather_data)

        # Assert that the result matches the expected output
        self.assertEqual(result, expected_output)

    def test_transform_weather_data_empty_input(self):
        # Test with empty input
        weather_data = {
            "weather": {},
            "forecast": {}
        }

        expected_output = {
            "weather": [],
            "forecast": []
        }

        result = transform_weather_data(weather_data)
        self.assertEqual(result, expected_output)

if __name__ == "__main__":
    unittest.main()