
import sys
import json
import time
from pathlib import Path
from datetime import datetime

from airflow.sdk import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

# Existing ETL modules
sys.path.insert(0, "/opt/airflow/etl_src")

DATA_DIR = Path("/opt/airflow/data")


# =====================================================
# SHARED STORAGE HELPERS
# =====================================================

def save_json(data, path):
    """Save intermediate data to shared Docker storage."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, default=str)


def read_json(path):
    """Read intermediate data from shared storage."""
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


# =====================================================
# AIRFLOW DAG
# =====================================================

@dag(
    dag_id="products_etl_dag",
    description="End-to-end DummyJSON ETL with PostgreSQL monitoring",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "etl", "postgresql", "docker"],
)
def products_etl_pipeline():

    # -------------------------------------------------
    # TASK 1: START MONITORING
    # -------------------------------------------------

    @task(retries=0)
    def start_monitoring():
        from monitor import start_run

        run_id = start_run()

        print(f"ETL RUN {run_id} started")

        return {
            "run_id": run_id,
            "start_time": time.time()
        }

    # -------------------------------------------------
    # TASK 2: EXTRACT
    # -------------------------------------------------

    @task(retries=1)
    def extract(context):
        from extract import extract_products

        start = time.perf_counter()

        products = extract_products()

        path = DATA_DIR / str(context["run_id"]) / "raw.json"
        save_json(products, path)

        print(
            f"EXTRACT | products={len(products)} | "
            f"duration={time.perf_counter() - start:.3f}s"
        )

        return {
            **context,
            "raw_path": str(path),
            "extracted_count": len(products)
        }

    # -------------------------------------------------
    # TASK 3: TRANSFORM
    # -------------------------------------------------

    @task(retries=1)
    def transform(context):
        from transform import transform_products

        start = time.perf_counter()

        raw_products = read_json(context["raw_path"])
        products = transform_products(raw_products)

        path = DATA_DIR / str(context["run_id"]) / "transformed.json"
        save_json(products, path)

        print(
            f"TRANSFORM | products={len(products)} | "
            f"duration={time.perf_counter() - start:.3f}s"
        )

        return {
            **context,
            "transformed_path": str(path)
        }

    # -------------------------------------------------
    # TASK 4: VALIDATE
    # -------------------------------------------------

    @task(retries=1)
    def validate(context):
        from validate import validate_products

        start = time.perf_counter()

        products = read_json(context["transformed_path"])

        valid_products, invalid_products = validate_products(products)

        path = DATA_DIR / str(context["run_id"]) / "valid.json"
        save_json(valid_products, path)

        print(
            f"VALIDATE | valid={len(valid_products)} | "
            f"invalid={len(invalid_products)} | "
            f"duration={time.perf_counter() - start:.3f}s"
        )

        return {
            **context,
            "valid_path": str(path),
            "valid_count": len(valid_products),
            "invalid_count": len(invalid_products)
        }

    # -------------------------------------------------
    # TASK 5: LOAD
    # -------------------------------------------------

    @task(retries=1)
    def load(context):
        from load import load_products

        start = time.perf_counter()

        valid_products = read_json(context["valid_path"])

        load_products(valid_products)

        print(
            f"LOAD | products={len(valid_products)} | "
            f"duration={time.perf_counter() - start:.3f}s"
        )

        return {
            **context,
            "loaded_count": len(valid_products)
        }

    # -------------------------------------------------
    # TASK 6: POST-LOAD QUALITY CHECK
    # -------------------------------------------------

    @task(retries=1)
    def quality_check(context):
        from load import connect_db
        from monitor import complete_run

        valid_products = read_json(context["valid_path"])

        product_ids = [
            product["product_id"]
            for product in valid_products
        ]

        # Check duplicates in validated data
        if len(product_ids) != len(set(product_ids)):
            raise AirflowException(
                "Data quality check failed: duplicate product IDs"
            )

        # Verify that loaded products exist in PostgreSQL
        if product_ids:
            connection = connect_db()

            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM products
                        WHERE product_id = ANY(%s);
                        """,
                        (product_ids,)
                    )

                    stored_count = cursor.fetchone()[0]

            finally:
                connection.close()

            if stored_count != len(product_ids):
                raise AirflowException(
                    f"Post-load quality check failed: "
                    f"expected={len(product_ids)}, "
                    f"found={stored_count}"
                )

        duration = time.time() - context["start_time"]

        complete_run(
            run_id=context["run_id"],
            extracted_count=context["extracted_count"],
            valid_count=context["valid_count"],
            invalid_count=context["invalid_count"],
            loaded_count=context["loaded_count"],
            duration_seconds=duration
        )

        print(
            f"ETL RUN {context['run_id']} | SUCCESS | "
            f"duration={duration:.3f}s"
        )

    
    # -------------------------------------------------
    # TASK 7: FAILURE MONITORING
    # -------------------------------------------------

    @task(
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0
    )
    def record_failure(context):
        from monitor import fail_run

        duration = time.time() - context["start_time"]

        fail_run(
            run_id=context["run_id"],
            error_message=(
                "An ETL task failed. Check Apache Airflow logs."
            ),
            duration_seconds=duration
        )

        raise AirflowException(
            "ETL pipeline failed. Check upstream task logs."
        )

    # =================================================
    # DAG DEPENDENCIES
    # =================================================

    context = start_monitoring()

    extracted = extract(context)
    transformed = transform(extracted)
    validated = validate(transformed)
    loaded = load(validated)
    checked = quality_check(loaded)

    failure = record_failure(context)

    # Trigger failure monitoring if an ETL task fails
    [
        extracted,
        transformed,
        validated,
        loaded,
        checked
    ] >> failure


# Register the DAG
products_etl_pipeline()
