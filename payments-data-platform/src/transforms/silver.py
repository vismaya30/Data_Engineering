from dataclasses import asdict
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import Settings, run_date_arg
from src.common.log import get_logger
from src.common.spark import get_spark
from src.common.storage import write_json
from src.quality.checks import (
    check_thresholds,
    evaluate,
    merchant_rules,
    settlement_rules,
    transaction_rules,
)
from src.transforms.schemas import (
    BRONZE_MERCHANTS,
    BRONZE_SETTLEMENTS,
    BRONZE_TRANSACTIONS,
    MONEY,
)

logger = get_logger(__name__)


def read_bronze(
    spark: SparkSession, settings: Settings, entity: str, run_date: date, schema: T.StructType
) -> DataFrame:
    path = settings.path("bronze", entity, f"ingest_date={run_date.isoformat()}")
    return spark.read.schema(schema).json(path).withColumn("ingest_date", F.lit(run_date))


def _clean_string(name: str, upper: bool = False) -> Column:
    value = F.trim(F.col(name))
    value = F.upper(value) if upper else value
    return F.when(value == "", None).otherwise(value).alias(name)


def clean_transactions(df: DataFrame) -> DataFrame:
    return df.select(
        _clean_string("transaction_id"),
        _clean_string("merchant_id", upper=True),
        _clean_string("card_scheme", upper=True),
        _clean_string("masked_pan"),
        F.col("amount").cast(MONEY).alias("amount"),
        _clean_string("currency", upper=True),
        _clean_string("auth_code"),
        _clean_string("auth_status", upper=True),
        F.to_timestamp("auth_timestamp").alias("auth_timestamp"),
        "ingest_date",
    ).withColumn("auth_date", F.to_date("auth_timestamp"))


def clean_settlements(df: DataFrame) -> DataFrame:
    return df.select(
        _clean_string("settlement_id"),
        _clean_string("transaction_id"),
        _clean_string("merchant_id", upper=True),
        _clean_string("card_scheme", upper=True),
        F.col("settled_amount").cast(MONEY).alias("settled_amount"),
        _clean_string("currency", upper=True),
        F.to_date("settlement_date").alias("settlement_date"),
        _clean_string("batch_id"),
        "ingest_date",
    )


def clean_merchants(df: DataFrame) -> DataFrame:
    return df.select(
        _clean_string("merchant_id", upper=True),
        _clean_string("merchant_name"),
        _clean_string("mcc"),
        _clean_string("country", upper=True),
        _clean_string("settlement_currency", upper=True),
        F.to_date("onboarded_date").alias("onboarded_date"),
        "ingest_date",
    )


def deduplicate(df: DataFrame, key: str, order_by: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(F.col(order_by).desc_nulls_last())
    keyed = (
        df.filter(F.col(key).isNotNull())
        .withColumn("_row", F.row_number().over(window))
        .filter("_row = 1")
        .drop("_row")
    )
    return keyed.unionByName(df.filter(F.col(key).isNull()))


def flag_known_merchants(transactions: DataFrame, merchants: DataFrame) -> DataFrame:
    known = merchants.select("merchant_id").distinct().withColumn("merchant_known", F.lit(True))
    return transactions.join(F.broadcast(known), "merchant_id", "left").withColumn(
        "merchant_known", F.coalesce("merchant_known", F.lit(False))
    )


def write_quarantine(settings: Settings, entity: str, invalid: DataFrame) -> None:
    (
        invalid.withColumn("quarantined_at", F.current_timestamp())
        .write.mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(settings.path("quarantine", entity))
    )


def run(spark: SparkSession, settings: Settings, run_date: date) -> list[dict]:
    merchants = deduplicate(
        clean_merchants(read_bronze(spark, settings, "merchants", run_date, BRONZE_MERCHANTS)),
        "merchant_id",
        "onboarded_date",
    )
    transactions = deduplicate(
        clean_transactions(
            read_bronze(spark, settings, "transactions", run_date, BRONZE_TRANSACTIONS)
        ),
        "transaction_id",
        "auth_timestamp",
    )
    settlements = deduplicate(
        clean_settlements(
            read_bronze(spark, settings, "settlements", run_date, BRONZE_SETTLEMENTS)
        ),
        "settlement_id",
        "settlement_date",
    )

    valid_merchants, bad_merchants, merchant_result = evaluate(
        "merchants", merchants, merchant_rules()
    )
    valid_transactions, bad_transactions, transaction_result = evaluate(
        "transactions", flag_known_merchants(transactions, valid_merchants), transaction_rules()
    )
    valid_settlements, bad_settlements, settlement_result = evaluate(
        "settlements", settlements, settlement_rules()
    )

    write_quarantine(settings, "merchants", bad_merchants)
    write_quarantine(settings, "transactions", bad_transactions.drop("merchant_known"))
    write_quarantine(settings, "settlements", bad_settlements)

    results = [merchant_result, transaction_result, settlement_result]
    report = [asdict(result) | {"reject_ratio": result.reject_ratio} for result in results]
    write_json(
        settings.path("reports", "data_quality", f"ingest_date={run_date.isoformat()}.json"),
        {"run_date": run_date.isoformat(), "results": report},
    )
    for entry in report:
        logger.info("data quality %s", entry)

    first_day = run_date == settings.pipeline_start_date
    allow_empty = frozenset({"settlements"}) if first_day else frozenset()
    check_thresholds(results, settings.max_reject_ratio, allow_empty)

    valid_merchants.drop("ingest_date").write.mode("overwrite").parquet(
        settings.path("silver", "merchants")
    )
    (
        valid_transactions.drop("merchant_known", "ingest_date")
        .write.mode("overwrite")
        .partitionBy("auth_date")
        .parquet(settings.path("silver", "transactions"))
    )
    (
        valid_settlements.drop("ingest_date")
        .write.mode("overwrite")
        .partitionBy("settlement_date")
        .parquet(settings.path("silver", "settlements"))
    )
    logger.info("silver written for %s", run_date)
    return report


if __name__ == "__main__":
    run_date = run_date_arg("Clean, deduplicate and validate bronze into silver")
    settings = Settings.from_env()
    run(get_spark("payments-silver", settings), settings, run_date)
