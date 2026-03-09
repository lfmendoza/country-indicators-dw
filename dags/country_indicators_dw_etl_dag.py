from datetime import datetime, timedelta
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "country_indicators_dw",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_country_indicators_dw_etl():
    from etl.run import run_pipeline
    return run_pipeline()


with DAG(
    dag_id="country_indicators_dw_etl",
    default_args=default_args,
    description="SQL + NoSQL to warehouse",
    schedule_interval=timedelta(hours=24),
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["country-indicators-dw", "etl", "warehouse"],
) as dag:
    run_etl = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_country_indicators_dw_etl,
    )
