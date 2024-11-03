from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import extract_weather_data
from transform import transform_weather_data
from load import load_data_to_db

# List of cities to fetch weather data for
city_list = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Toronto",
    "London",
    "Paris",
    "Berlin",
    "Tokyo",
    "Sydney",
    "Cape Town",
    "São Paulo",
    "Mumbai",
    "Dubai"
]

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

raw_weather_data = extract_weather_data(city_list)
transformed_data = transform_weather_data(raw_weather_data)
load_data_to_db(transformed_data)

with DAG(
        "weather_etl_dag",
        default_args=default_args,
        description="A weather ETL DAG",
        schedule=timedelta(hours=1),  # Set your desired schedule
        start_date=datetime(2023, 10, 1),
        catchup=False
) as dag:
    # Step 1: Extract data
    def extract_task(**kwargs):
        json_file_path = extract_weather_data(city_list)
        kwargs['ti'].xcom_push(key='json_file_path', value=json_file_path)


    extract_weather = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_task,
    )


    # Step 2: Transform data
    def transform_task(**kwargs):
        json_file_path = kwargs['ti'].xcom_pull(key='json_file_path', task_ids='extract_weather')
        transformed_data = transform_weather_data(json_file_path)
        kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)


    transform_weather = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_task,
    )


    # Step 3: Load data
    def load_task(**kwargs):
        transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_weather')
        load_data_to_db(transformed_data)


    load_weather = PythonOperator(
        task_id="load_weather",
        python_callable=load_task,
    )

    # Define task dependencies
    extract_weather >> transform_weather >> load_weather