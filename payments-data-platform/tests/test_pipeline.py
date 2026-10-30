from datetime import date, timedelta

from src.common.config import Settings
from src.generator import generate
from src.transforms import reconciliation, silver

START = date(2026, 10, 1)


def _run_days(spark, settings, days):
    summaries = []
    for offset in range(days):
        run_date = START + timedelta(days=offset)
        generate.run(settings, run_date)
        silver.run(spark, settings, run_date)
        summaries.append(reconciliation.run(spark, settings, run_date))
    return summaries


def test_pipeline_end_to_end_is_idempotent(spark, tmp_path):
    settings = Settings(
        data_root=str(tmp_path),
        pipeline_start_date=START,
        transactions_per_day=400,
        merchant_count=10,
    )
    first = _run_days(spark, settings, 5)
    final = spark.read.parquet(settings.path("gold", "reconciliation")).groupBy("status").count()
    first_counts = {row["status"]: row["count"] for row in final.collect()}

    assert {"MATCHED", "PENDING"} <= set(first[-1])
    assert first_counts.get("UNMATCHED", 0) + first_counts.get("MATCHED", 0) > 0

    silver.run(spark, settings, START + timedelta(days=4))
    reconciliation.run(spark, settings, START + timedelta(days=4))
    rerun = spark.read.parquet(settings.path("gold", "reconciliation")).groupBy("status").count()
    assert {row["status"]: row["count"] for row in rerun.collect()} == first_counts

    transactions = spark.read.parquet(settings.path("silver", "transactions"))
    assert transactions.count() == transactions.select("transaction_id").distinct().count()
    assert spark.read.parquet(settings.path("quarantine", "transactions")).count() > 0
