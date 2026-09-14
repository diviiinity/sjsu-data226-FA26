from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests


@task
def extract():
    """
    Extract the last 60 days of weather data from Open-Meteo.
    """

    latitude = float(Variable.get("latitude"))
    longitude = float(Variable.get("longitude"))

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": (
            "temperature_2m_max,"
            "temperature_2m_min,"
            "precipitation_sum,"
            "weather_code"
        ),
        "past_days": 60,
        "forecast_days": 0,
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    print("Weather data extracted successfully")

    return response.json()


@task
def transform(data):
    """
    Transform the API response into rows for Snowflake.
    """

    latitude = float(Variable.get("latitude"))
    longitude = float(Variable.get("longitude"))

    daily = data["daily"]

    dates = daily["time"]
    temp_max_values = daily["temperature_2m_max"]
    temp_min_values = daily["temperature_2m_min"]
    precipitation_values = daily["precipitation_sum"]
    weather_code_values = daily["weather_code"]

    rows = []

    for i in range(len(dates)):
        rows.append((
            latitude,
            longitude,
            dates[i],
            temp_max_values[i],
            temp_min_values[i],
            precipitation_values[i],
            weather_code_values[i]
        ))

    print("Rows transformed:", len(rows))

    return rows


def get_snowflake_connection():
    """
    Get a Snowflake connection using the Airflow Connection.
    """

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn"
    )

    return hook.get_conn()


@task
def load(rows):
    """
    Full refresh load into Snowflake using a transaction.
    """

    database = "DEMO_DB"
    schema = "RAW"
    target_table = "WEATHER_TEMPERATURE"

    full_table_name = (
        f"{database}.{schema}.{target_table}"
    )

    connection = None
    cursor = None

    try:
        connection = get_snowflake_connection()
        cursor = connection.cursor()

        # Create the table if it does not exist
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                latitude FLOAT,
                longitude FLOAT,
                "date" DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INTEGER,
                PRIMARY KEY (latitude, longitude, "date")
            )
        """

        cursor.execute(create_table_sql)

        # Start transaction for the data refresh
        cursor.execute("BEGIN")

        # Full refresh: remove existing records
        cursor.execute(
            f"DELETE FROM {full_table_name}"
        )

        insert_sql = f"""
            INSERT INTO {full_table_name}
            (
                latitude,
                longitude,
                "date",
                temp_max,
                temp_min,
                precipitation,
                weather_code
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, rows)

        # Save the delete and insert operations
        cursor.execute("COMMIT")

        print("Transaction committed successfully")
        print("Rows loaded:", len(rows))

    except Exception as error:
        if cursor is not None:
            cursor.execute("ROLLBACK")

        print("Transaction failed. Changes rolled back.")
        print(error)

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


with DAG(
    dag_id="WeatherTemperatureETL",
    start_date=datetime(2026, 9, 14),
    schedule="@daily",
    catchup=False,
    tags=["homework", "weather"]
) as dag:

    weather_data = extract()

    weather_rows = transform(weather_data)

    load(weather_rows)