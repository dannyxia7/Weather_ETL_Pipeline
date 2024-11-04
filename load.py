import logging
import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tables(cursor):
    """Create tables if they do not exist."""
    create_cities_table = """
    CREATE TABLE IF NOT EXISTS cities (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        country VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT
    )
    """

    create_weather_conditions_table = """
    CREATE TABLE IF NOT EXISTS weather_conditions (
        id SERIAL PRIMARY KEY,
        description VARCHAR(255) UNIQUE NOT NULL
    )
    """

    create_weather_data_table = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(255) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        temperature FLOAT,
        humidity INTEGER,
        weather_condition INT,
        FOREIGN KEY (weather_condition) REFERENCES weather_conditions(id),
        UNIQUE (city, timestamp)  -- To prevent duplicate entries
    )
    """

    create_forecast_data_table = """
    CREATE TABLE IF NOT EXISTS forecast_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(255) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        temperature FLOAT,
        humidity INTEGER,
        weather_condition INT,
        FOREIGN KEY (weather_condition) REFERENCES weather_conditions(id),
        UNIQUE (city, timestamp)  -- To prevent duplicate entries
    )
    """

    # create_alerts_table = """
    # CREATE TABLE IF NOT EXISTS alerts (
    #     id SERIAL PRIMARY KEY,
    #     city_id INT NOT NULL,
    #     alert_type VARCHAR(255) NOT NULL,
    #     description TEXT NOT NULL,
    #     issued_at TIMESTAMP NOT NULL,
    #     expires_at TIMESTAMP,
    #     FOREIGN KEY (city_id) REFERENCES cities(id)
    # )
    # """

    # Execute table creation queries
    cursor.execute(create_cities_table)
    cursor.execute(create_weather_conditions_table)
    cursor.execute(create_weather_data_table)
    cursor.execute(create_forecast_data_table)
    # cursor.execute(create_alerts_table)
    logger.info("Tables created successfully (if they didn't exist).")


def load_data_to_db(transformed_data):
    conn = None  # Initialize connection to None
    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="4976111",
            host="localhost",
            port="5432"
        )

        with conn:
            with conn.cursor() as cursor:
                # Create tables if they do not exist
                create_tables(cursor)

                # Insert city data
                for weather in transformed_data["weather"]:
                    cursor.execute("""
                        INSERT INTO cities (name, country, latitude, longitude)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                    """, (weather['city'], weather['country'], weather['latitude'], weather['longitude']))
                    logger.info(f"Inserted city: {weather['city']} into the database.")


                # Insert unique weather conditions from forecast data
                unique_conditions = set(forecast["weather_condition"] for forecast in transformed_data["forecast"])
                condition_ids = {}

                for condition_description in unique_conditions:
                    cursor.execute("""
                        INSERT INTO weather_conditions (description) VALUES (%s)
                        ON CONFLICT (description) DO NOTHING
                        RETURNING id
                    """, (condition_description,))
                    condition_id = cursor.fetchone()  # Fetch the condition ID if it was newly inserted

                    # If the weather condition already existed, retrieve the existing ID
                    if condition_id is None:
                        cursor.execute("SELECT id FROM weather_conditions WHERE description = %s", (condition_description,))
                        condition_id = cursor.fetchone()

                    # Store the condition ID if it was found
                    condition_ids[condition_description] = condition_id[0]

                # Insert weather data
                for weather in transformed_data["weather"]:
                    weather_condition_id = condition_ids.get(weather["weather_condition"])

                    # If the condition ID does not exist, try to insert it
                    if weather_condition_id is None:
                        cursor.execute("""
                            INSERT INTO weather_conditions (description) VALUES (%s)
                            ON CONFLICT (description) DO NOTHING
                            RETURNING id
                        """, (weather["weather_condition"],))

                        # Fetch the new condition ID
                        weather_condition_id = cursor.fetchone()  # This returns (id,) or None

                        # If a new ID was inserted, store it
                        if weather_condition_id is not None:
                            condition_ids[weather["weather_condition"]] = weather_condition_id[0]
                            weather_condition_id = weather_condition_id[0]  # Use the actual ID directly

                    # If we still do not have a weather_condition_id, try to retrieve it
                    if weather_condition_id is None:  # This means the condition was not inserted
                        cursor.execute("SELECT id FROM weather_conditions WHERE description = %s",
                                       (weather["weather_condition"],))
                        weather_condition_id = cursor.fetchone()  # This also returns (id,) or None

                        # Check if we found an existing ID
                        if weather_condition_id is not None:
                            condition_ids[weather["weather_condition"]] = weather_condition_id[0]
                            weather_condition_id = weather_condition_id[0]  # Use the actual ID directly

                    # Insert into weather_data
                    if weather_condition_id is not None:
                        cursor.execute("""
                            INSERT INTO weather_data (city, timestamp, temperature, humidity, weather_condition)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (city, timestamp) DO NOTHING
                        """, (
                            weather["city"],
                            weather["timestamp"],
                            weather["temperature"],
                            weather["humidity"],
                            weather_condition_id  # This is now guaranteed to be an integer if not None
                        ))
                    else:
                        # Handle case where weather_condition_id is still None
                        cursor.execute("""
                            INSERT INTO weather_data (city, timestamp, temperature, humidity)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (city, timestamp) DO NOTHING
                        """, (
                            weather["city"],
                            weather["timestamp"],
                            weather["temperature"],
                            weather["humidity"]
                        ))
                        logger.warning(f"Weather condition '{weather['weather_condition']}' not found; row inserted without it.")

                # Insert forecast data
                for forecast in transformed_data["forecast"]:
                    weather_condition_id = condition_ids.get(forecast["weather_condition"])
                    if weather_condition_id is not None:
                        cursor.execute("""
                            INSERT INTO forecast_data (city, timestamp, temperature, humidity, weather_condition)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (city, timestamp) DO NOTHING
                        """, (
                            forecast["city"],
                            forecast["timestamp"],
                            forecast["temperature"],
                            forecast["humidity"],
                            weather_condition_id
                        ))
                    else:
                        # Insert without weather_condition
                        cursor.execute("""
                            INSERT INTO forecast_data (city, timestamp, temperature, humidity)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (city, timestamp) DO NOTHING
                        """, (
                            forecast["city"],
                            forecast["timestamp"],
                            forecast["temperature"],
                            forecast["humidity"]
                        ))
                        logger.warning(f"Forecast condition '{forecast['weather_condition']}' not found; row inserted without it.")

                # # Insert alerts data
                # for alert in transformed_data["alerts"]:
                #     cursor.execute("""
                #         INSERT INTO alerts (city_id, alert_type, alert_description, timestamp)
                #         VALUES (%s, %s, %s, %s)
                #     """, (
                #         alert["city_id"],
                #         alert["alert_type"],
                #         alert["alert_description"],
                #         alert["timestamp"]
                #     ))

            # Commit the transaction
            conn.commit()
            logger.info("Data successfully loaded into the database.")

    except psycopg2.Error as e:
        if conn:  # Check if conn was established
            conn.rollback()
        logger.error(f"Error loading data to database: {e}")
        raise Exception("Data loading failed.") from e  # Raise a new exception

    finally:
        # Close the database connection
        if conn:
            conn.close()
