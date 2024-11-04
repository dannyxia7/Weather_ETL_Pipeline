import unittest
from unittest.mock import patch, MagicMock
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from extract import extract_weather_data


class TestExtractWeatherData(unittest.TestCase):

    @patch("extract.fetch_api_data")  # Mock fetch_api_data
    def test_extract_weather_data_success(self, mock_fetch):
        # Mock successful API responses
        mock_fetch.side_effect = [
            {"name": "San Diego", "main": {"temp": 285.32}, "weather": [{"description": "clear sky"}]},  # Weather data
            {"city": "San Diego", "list": [{"weather": [{"description": "clear sky"}]}]}  # Forecast data
        ]

        result = extract_weather_data(["San Diego"])

        self.assertIn("weather", result)
        self.assertIn("forecast", result)
        self.assertEqual(len(result["weather"]), 1)
        self.assertEqual(len(result["forecast"]), 1)
        self.assertEqual(result["weather"]["San Diego"]["name"], "San Diego")
        self.assertEqual(result["forecast"]["San Diego"]["list"][0]["weather"][0]["description"], "clear sky")

    @patch("extract.fetch_api_data")  # Mock fetch_api_data
    def test_extract_weather_data_api_failure(self, mock_fetch):
        # Mock an API failure (e.g., server down)
        mock_fetch.side_effect = [None, None]  # Both fetch calls fail

        with self.assertRaises(ValueError) as context:
            extract_weather_data(["San Diego"])  # Call the function

        # Assert that the correct error message is raised
        self.assertEqual(str(context.exception), "Failed to extract both weather and forecast data for city: San Diego")


    @patch("extract.fetch_api_data")
    def test_extract_weather_data_both_data_required(self, mock_fetch):
        # Mock an API response where one of the data fetch fails
        mock_fetch.side_effect = [None, {"city": "San Diego",
                                         "list": [{"weather": [{"description": "clear sky"}]}]}]  # Only forecast data

        with self.assertRaises(ValueError) as context:
            extract_weather_data(["San Diego"])  # Ensure you're calling the function correctly

        self.assertEqual(str(context.exception), "Failed to extract both weather and forecast data for city: San Diego")

if __name__ == "__main__":
    unittest.main()
