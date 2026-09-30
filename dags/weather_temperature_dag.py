from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import requests


@task
def extract():
    """
    Extract weather data for Portland and Austin from Open-Meteo.
    """

    cities = Variable.get(
        "weather_cities",
        deserialize_json=True
    )

    extracted_data = []

    for city_info in cities:
        city = city_info["city"]
        latitude = float(city_info["latitude"])
        longitude = float(city_info["longitude"])

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
            "timezone": "auto"
        }

        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        extracted_data.append({
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "weather": response.json()
        })

        print(f"Weather data extracted for {city}")

    return extracted_data


@task
def transform(extracted_data):
    """
    Transform both city responses into Snowflake rows.
    """

    rows = []

    for city_data in extracted_data:
        city = city_data["city"]
        latitude = city_data["latitude"]
        longitude = city_data["longitude"]
        daily = city_data["weather"]["daily"]

        dates = daily["time"]
        temp_max_values = daily["temperature_2m_max"]
        temp_min_values = daily["temperature_2m_min"]
        precipitation_values = daily["precipitation_sum"]
        weather_code_values = daily["weather_code"]

        for i in range(len(dates)):
            rows.append((
                city,
                latitude,
                longitude,
                dates[i],
                temp_max_values[i],
                temp_min_values[i],
                precipitation_values[i],
                weather_code_values[i]
            ))

    print("Total rows transformed:", len(rows))

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
    Full-refresh load into Snowflake using a transaction.
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

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                city VARCHAR,
                latitude FLOAT,
                longitude FLOAT,
                "date" DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INTEGER,
                PRIMARY KEY (city, latitude, longitude, "date")
            )
        """

        cursor.execute(create_table_sql)

        # Add city to the existing Portland-only table if needed.
        cursor.execute(
            f"""
            ALTER TABLE {full_table_name}
            ADD COLUMN IF NOT EXISTS city VARCHAR
            """
        )

        cursor.execute("BEGIN")

        # Full refresh makes reruns idempotent.
        cursor.execute(
            f"DELETE FROM {full_table_name}"
        )

        insert_sql = f"""
            INSERT INTO {full_table_name}
            (
                city,
                latitude,
                longitude,
                "date",
                temp_max,
                temp_min,
                precipitation,
                weather_code
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, rows)

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
    tags=["lab", "weather"]
) as dag:

    weather_data = extract()
    weather_rows = transform(weather_data)

    load_task = load(weather_rows)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="WeatherAnalyticsDBT",
        wait_for_completion=True,
        poke_interval=30,
    )

    load_task >> trigger_dbt
