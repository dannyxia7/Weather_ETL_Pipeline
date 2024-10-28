import psycopg2
from psycopg2 import sql


def load_data_to_db(data, table_name="weather_data"):
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="4976111",
        host="localhost",
        port="5433"
    )
    cursor = conn.cursor()

    # Define the CREATE TABLE statement
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        city VARCHAR(50),
        timestamp TIMESTAMP,
        temperature FLOAT,
        humidity INT,
        wind_speed FLOAT,
        weather_description VARCHAR(255)
    );
    """

    # Execute the query
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'weather_data' created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table: {e}")

    # Define SQL insert statement dynamically
    insert_query = sql.SQL("""
        INSERT INTO {table} (city, timestamp, temperature, humidity, wind_speed, weather_description)
        VALUES (%s, %s, %s, %s, %s, %s)
    """).format(table=sql.Identifier(table_name))

    # Execute batch insert
    try:
        data_tuples = [
            (row['city'], row['timestamp'], row['temperature'], row['humidity'],
             row['wind_speed'], row['weather_description'])
            for row in data
        ]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        print("Data loaded successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
    finally:
        cursor.close()
        conn.close()