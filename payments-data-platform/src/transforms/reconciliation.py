from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import Settings, run_date_arg
from src.common.log import get_logger
from src.common.spark import get_spark
from src.transforms.schemas import MONEY, SILVER_SETTLEMENTS, SILVER_TRANSACTIONS

logger = get_logger(__name__)

MATCHED = "MATCHED"
LATE = "LATE"
AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
DUPLICATE = "DUPLICATE"
PENDING = "PENDING"
UNMATCHED = "UNMATCHED"
ORPHAN_SETTLEMENT = "ORPHAN_SETTLEMENT"

STATUSES = [MATCHED, LATE, AMOUNT_MISMATCH, DUPLICATE, PENDING, UNMATCHED, ORPHAN_SETTLEMENT]


def reconcile(
    transactions: DataFrame,
    settlements: DataFrame,
    run_date: date,
    late_window_days: int,
    late_threshold_days: int,
) -> DataFrame:
    window_start = run_date - timedelta(days=late_window_days)
    lookback_start = window_start - timedelta(days=late_window_days)

    known_transactions = transactions.filter(
        F.col("auth_date").between(lookback_start, run_date)
    ).select("transaction_id")
    approved = transactions.filter(
        (F.col("auth_status") == "APPROVED") & F.col("auth_date").between(window_start, run_date)
    )
    in_window = settlements.filter(F.col("settlement_date").between(window_start, run_date))

    settled = in_window.groupBy("transaction_id").agg(
        F.count("*").alias("settlement_count"),
        F.sum("settled_amount").alias("settled_amount"),
        F.min("settlement_date").alias("settlement_date"),
        F.concat_ws(",", F.sort_array(F.collect_list("settlement_id"))).alias("settlement_ids"),
    )

    days_to_settle = F.datediff("settlement_date", "auth_date")
    age_days = F.datediff(F.lit(run_date), "auth_date")
    status = (
        F.when(F.col("settlement_count").isNull() & (age_days >= late_window_days), UNMATCHED)
        .when(F.col("settlement_count").isNull(), PENDING)
        .when(F.col("settlement_count") > 1, DUPLICATE)
        .when(F.col("settled_amount") != F.col("amount"), AMOUNT_MISMATCH)
        .when(days_to_settle > late_threshold_days, LATE)
        .otherwise(MATCHED)
    )

    transaction_view = approved.join(settled, "transaction_id", "left").select(
        F.col("auth_date").alias("business_date"),
        "transaction_id",
        "settlement_ids",
        "merchant_id",
        "card_scheme",
        "currency",
        F.col("amount").alias("auth_amount"),
        "settled_amount",
        F.coalesce("settlement_count", F.lit(0)).cast(T.IntegerType()).alias("settlement_count"),
        "settlement_date",
        days_to_settle.alias("days_to_settle"),
        status.alias("status"),
    )

    orphan_view = in_window.join(known_transactions, "transaction_id", "left_anti").select(
        F.col("settlement_date").alias("business_date"),
        "transaction_id",
        F.col("settlement_id").alias("settlement_ids"),
        "merchant_id",
        "card_scheme",
        "currency",
        F.lit(None).cast(MONEY).alias("auth_amount"),
        "settled_amount",
        F.lit(1).alias("settlement_count"),
        "settlement_date",
        F.lit(None).cast(T.IntegerType()).alias("days_to_settle"),
        F.lit(ORPHAN_SETTLEMENT).alias("status"),
    )

    return (
        transaction_view.unionByName(orphan_view)
        .withColumn("run_date", F.lit(run_date))
        .withColumn("reconciled_at", F.current_timestamp())
    )


def run(spark: SparkSession, settings: Settings, run_date: date) -> dict[str, int]:
    lookback = timedelta(days=settings.late_window_days * 2)
    transactions = (
        spark.read.schema(SILVER_TRANSACTIONS)
        .parquet(settings.path("silver", "transactions"))
        .filter(F.col("auth_date").between(run_date - lookback, run_date))
    )
    settlements = (
        spark.read.schema(SILVER_SETTLEMENTS)
        .parquet(settings.path("silver", "settlements"))
        .filter(F.col("settlement_date").between(run_date - lookback, run_date))
    )

    result = reconcile(
        transactions,
        settlements,
        run_date,
        settings.late_window_days,
        settings.late_threshold_days,
    ).cache()

    (
        result.write.mode("overwrite")
        .partitionBy("business_date")
        .parquet(settings.path("gold", "reconciliation"))
    )

    summary = {row["status"]: row["count"] for row in result.groupBy("status").count().collect()}
    logger.info("reconciliation for %s: %s", run_date, summary)
    return summary


if __name__ == "__main__":
    run_date = run_date_arg("Reconcile authorisations against settlements")
    settings = Settings.from_env()
    run(get_spark("payments-reconciliation", settings), settings, run_date)
