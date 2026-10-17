import sys
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="products_etl_dag",
    description="ETL DummyJSON vers PostgreSQL",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "etl", "postgresql"],
)
def products_etl_pipeline():

    @task(retries=1)
    def execute_etl():
        sys.path.insert(0, "/opt/airflow/etl_src")

        from pipeline import run_pipeline

        run_pipeline()

    execute_etl()


products_etl_pipeline()