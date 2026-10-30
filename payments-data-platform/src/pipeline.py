import argparse
from datetime import date, timedelta

from src.common.config import Settings
from src.common.log import get_logger
from src.common.spark import get_spark
from src.generator import generate
from src.transforms import load_warehouse, reconciliation, silver

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the daily pipeline for a range of dates")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    spark = get_spark("payments-pipeline", settings)
    end_date = args.end_date or args.start_date

    run_date = args.start_date
    while run_date <= end_date:
        logger.info("running pipeline for %s", run_date)
        generate.run(settings, run_date)
        silver.run(spark, settings, run_date)
        reconciliation.run(spark, settings, run_date)
        if not args.skip_load:
            load_warehouse.run(spark, settings, run_date)
        run_date += timedelta(days=1)

    spark.stop()


if __name__ == "__main__":
    main()
