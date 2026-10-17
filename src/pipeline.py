import time

from extract import extract_products
from transform import transform_products
from validate import validate_products
from load import load_products
from logger import setup_logger
from monitor import start_run, complete_run, fail_run

logger = setup_logger()


def run_pipeline():
    pipeline_start = time.perf_counter()
    run_id = None

    extracted_count = 0
    valid_count = 0
    invalid_count = 0
    loaded_count = 0

    logger.info("Starting ETL pipeline")

    try:
        # START MONITORING
        run_id = start_run()
        logger.info("ETL RUN ID | %s", run_id)

        # EXTRACT
        start = time.perf_counter()
        raw_products = extract_products()
        extracted_count = len(raw_products)

        logger.info(
            "EXTRACT | products=%s | duration=%.3fs",
            extracted_count,
            time.perf_counter() - start
        )

        # TRANSFORM
        start = time.perf_counter()
        clean_products = transform_products(raw_products)

        logger.info(
            "TRANSFORM | products=%s | duration=%.3fs",
            len(clean_products),
            time.perf_counter() - start
        )

        # VALIDATE
        start = time.perf_counter()
        valid_products, invalid_products = validate_products(clean_products)

        valid_count = len(valid_products)
        invalid_count = len(invalid_products)

        logger.info(
            "VALIDATE | valid=%s | invalid=%s | duration=%.3fs",
            valid_count,
            invalid_count,
            time.perf_counter() - start
        )

        if invalid_count > 0:
            logger.warning(
                "DATA QUALITY | rejected=%s",
                invalid_count
            )

        # LOAD
        start = time.perf_counter()
        load_products(valid_products)
        loaded_count = valid_count

        logger.info(
            "LOAD | products=%s | duration=%.3fs",
            loaded_count,
            time.perf_counter() - start
        )

        # COMPLETE MONITORING
        duration = time.perf_counter() - pipeline_start

        complete_run(
            run_id=run_id,
            extracted_count=extracted_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            loaded_count=loaded_count,
            duration_seconds=duration
        )

        logger.info("ETL RUN %s | SUCCESS", run_id)

    except Exception as error:
        duration = time.perf_counter() - pipeline_start

        logger.exception("ETL pipeline failed")

        if run_id is not None:
            try:
                fail_run(run_id, str(error), duration)
            except Exception:
                logger.exception("Failed to record ETL failure")

        raise

    finally:
        logger.info(
            "TOTAL PIPELINE DURATION | %.3fs",
            time.perf_counter() - pipeline_start
        )


if __name__ == "__main__":
    run_pipeline()