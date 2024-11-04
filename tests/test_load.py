# import unittest
# from unittest.mock import MagicMock, patch
# import psycopg2
# import sys
# import os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
# from load import load_data_to_db, create_tables
#
# class TestDatabaseFunctions(unittest.TestCase):
#
#     @patch('psycopg2.connect')
#     def test_create_tables(self, mock_connect):
#         # Mock the cursor and connection
#         mock_conn = MagicMock()
#         mock_cursor = MagicMock()
#         mock_connect.return_value = mock_conn
#         mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
#
#         # Call the function to test
#         create_tables(mock_cursor)
#
#         # Expected SQL command with normalized whitespace
#         expected_city_table_sql = '''CREATE TABLE IF NOT EXISTS cities (
#             id SERIAL PRIMARY KEY,
#             name VARCHAR(255) UNIQUE NOT NULL,
#             country VARCHAR(255),
#             latitude FLOAT,
#             longitude FLOAT
#         )'''
#
#         # Verify that each table creation command was executed
#         mock_cursor.execute.assert_any_call(
#             expected_city_table_sql.strip()
#         )
#         mock_cursor.execute.assert_any_call(
#             '''CREATE TABLE IF NOT EXISTS weather_conditions (
#                 id SERIAL PRIMARY KEY,
#                 description VARCHAR(255) UNIQUE NOT NULL
#             )'''.strip()
#         )
#         mock_cursor.execute.assert_any_call(
#             '''CREATE TABLE IF NOT EXISTS weather_data (
#                 id SERIAL PRIMARY KEY,
#                 city VARCHAR(255) NOT NULL,
#                 timestamp TIMESTAMP NOT NULL,
#                 temperature FLOAT,
#                 humidity INTEGER,
#                 weather_condition INT,
#                 FOREIGN KEY (weather_condition) REFERENCES weather_conditions(id),
#                 UNIQUE (city, timestamp)
#             )'''.strip()
#         )
#         mock_cursor.execute.assert_any_call(
#             '''CREATE TABLE IF NOT EXISTS forecast_data (
#                 id SERIAL PRIMARY KEY,
#                 city VARCHAR(255) NOT NULL,
#                 timestamp TIMESTAMP NOT NULL,
#                 temperature FLOAT,
#                 humidity INTEGER,
#                 weather_condition INT,
#                 FOREIGN KEY (weather_condition) REFERENCES weather_conditions(id),
#                 UNIQUE (city, timestamp)
#             )'''.strip()
#         )
#
#     @patch('psycopg2.connect')
#     def test_load_data_to_db(self, mock_connect):
#         # Setup
#         mock_conn = MagicMock()
#         mock_cursor = MagicMock()
#         mock_connect.return_value = mock_conn
#         mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
#
#         transformed_data = {
#             "weather": [
#                 {
#                     "city": "San Diego",
#                     "country": "US",
#                     "latitude": None,
#                     "longitude": None,
#                     "timestamp": "2021-01-01T00:00:00",
#                     "temperature": 70,
#                     "humidity": None,
#                     "weather_condition": "clear sky"
#                 }
#             ],
#             "forecast": [
#                 {
#                     "city": "San Diego",
#                     "timestamp": "2021-01-01T01:00:00",
#                     "temperature": None,
#                     "humidity": 45,
#                     "weather_condition": "partly cloudy"
#                 }
#             ]
#         }
#
#         # Call the function to test
#         load_data_to_db(transformed_data)
#
#         # Print the actual calls made to mock_cursor
#         print(mock_cursor.execute.call_args_list)
#
#         # Assert the table creation calls
#         mock_cursor.execute.assert_any_call(
#             '''CREATE TABLE IF NOT EXISTS cities (
#                 id SERIAL PRIMARY KEY,
#                 name VARCHAR(255) UNIQUE NOT NULL,
#                 country VARCHAR(255),
#                 latitude FLOAT,
#                 longitude FLOAT
#             )'''.strip()
#         )
#
# if __name__ == '__main__':
#     unittest.main()