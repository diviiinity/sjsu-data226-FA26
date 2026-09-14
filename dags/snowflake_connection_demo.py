from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime


with DAG(
    dag_id="snowflake_connection_demo",
    start_date=datetime(2026, 9, 14),
    schedule=None,
    catchup=False,
    tags=["demo", "snowflake"]
) as dag:

    @task
    def test_snowflake_connection():
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn"
        )

        result = hook.get_first("""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_VERSION()
        """)

        print("Snowflake connection successful:")
        print(result)

    test_snowflake_connection()