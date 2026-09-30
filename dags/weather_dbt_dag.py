from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


dbt_environment = {
    "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson['account'] }}",
    "DBT_USER": "{{ conn.snowflake_conn.login }}",
    "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson['role'] }}",
    "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson['warehouse'] }}",
    "DBT_PRIVATE_KEY_PASSPHRASE": "{{ conn.snowflake_conn.password }}",
}


with DAG(
    dag_id="WeatherAnalyticsDBT",
    start_date=datetime(2026, 9, 14),
    schedule=None,
    catchup=False,
    tags=["lab", "dbt", "weather"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        ),
        env=dbt_environment,
        append_env=True,
        cwd="/opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        ),
        env=dbt_environment,
        append_env=True,
        cwd="/opt/airflow/dbt",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "dbt snapshot "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        ),
        env=dbt_environment,
        append_env=True,
        cwd="/opt/airflow/dbt",
    )

    dbt_run >> dbt_test >> dbt_snapshot
