from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="Daily stock data ingestion and transformation pipeline",
    schedule_interval="0 19 * * 1-5", 
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["stocks", "elt", "dbt"],
) as dag:
    # Task 1: Ingest stock data from Alpha Vantage
    ingest = BashOperator(
        task_id="ingest_stock_data",
        bash_command=(
            "cd /opt/airflow/ingest &&"
            "python ingest.py"
        ),
    )

    # Task 2: Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_project &&"
            "dbt run --profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt_project &&"
            "dbt test --profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # Task 4: Generate dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            "cd /opt/airflow/dbt_project &&"
            "dbt docs generate --profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # pipeline order
    ingest >> dbt_run >> dbt_test >> dbt_docs