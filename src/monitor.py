import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def start_run():
    """Create a new ETL execution record."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_runs (status)
                VALUES ('RUNNING')
                RETURNING run_id;
                """
            )
            run_id = cursor.fetchone()[0]

    return run_id


def complete_run(
    run_id,
    extracted_count,
    valid_count,
    invalid_count,
    loaded_count,
    duration_seconds
):
    """Mark an ETL execution as successful."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE etl_runs
                SET
                    end_time = NOW(),
                    status = 'SUCCESS',
                    extracted_count = %s,
                    valid_count = %s,
                    invalid_count = %s,
                    loaded_count = %s,
                    duration_seconds = %s
                WHERE run_id = %s;
                """,
                (
                    extracted_count,
                    valid_count,
                    invalid_count,
                    loaded_count,
                    duration_seconds,
                    run_id
                )
            )


def fail_run(run_id, error_message, duration_seconds):
    """Mark an ETL execution as failed."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE etl_runs
                SET
                    end_time = NOW(),
                    status = 'FAILED',
                    error_message = %s,
                    duration_seconds = %s
                WHERE run_id = %s;
                """,
                (str(error_message), duration_seconds, run_id)
            )