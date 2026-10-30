import io
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from psycopg2.extensions import connection
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import Settings, run_date_arg
from src.common.db import connect
from src.common.log import get_logger
from src.common.spark import get_spark
from src.transforms.schemas import SILVER_SETTLEMENTS, SILVER_TRANSACTIONS

logger = get_logger(__name__)

DDL_PATH = Path(__file__).resolve().parents[2] / "sql" / "ddl.sql"
PIPELINE = "payments_daily"


@dataclass(frozen=True)
class Target:
    table: str
    source: tuple[str, ...]
    date_column: str | None
    columns: tuple[str, ...]
    schema: T.StructType | None = None


MERCHANTS = Target(
    "raw.merchants",
    ("silver", "merchants"),
    None,
    ("merchant_id", "merchant_name", "mcc", "country", "settlement_currency", "onboarded_date"),
)

INCREMENTAL_TARGETS = (
    Target(
        "raw.transactions",
        ("silver", "transactions"),
        "auth_date",
        (
            "transaction_id",
            "merchant_id",
            "card_scheme",
            "masked_pan",
            "amount",
            "currency",
            "auth_code",
            "auth_status",
            "auth_timestamp",
            "auth_date",
        ),
        SILVER_TRANSACTIONS,
    ),
    Target(
        "raw.settlements",
        ("silver", "settlements"),
        "settlement_date",
        (
            "settlement_id",
            "transaction_id",
            "merchant_id",
            "card_scheme",
            "settled_amount",
            "currency",
            "settlement_date",
            "batch_id",
        ),
        SILVER_SETTLEMENTS,
    ),
    Target(
        "raw.reconciliation",
        ("gold", "reconciliation"),
        "business_date",
        (
            "business_date",
            "transaction_id",
            "settlement_ids",
            "merchant_id",
            "card_scheme",
            "currency",
            "auth_amount",
            "settled_amount",
            "settlement_count",
            "settlement_date",
            "days_to_settle",
            "status",
            "run_date",
            "reconciled_at",
        ),
    ),
)


def dates_to_load(
    watermark: date | None, run_date: date, late_window_days: int, start_date: date
) -> list[date]:
    first = run_date - timedelta(days=late_window_days)
    if watermark is not None:
        first = min(first, watermark + timedelta(days=1))
    first = max(first, start_date)
    return [first + timedelta(days=offset) for offset in range((run_date - first).days + 1)]


def read_watermark(conn: connection) -> date | None:
    with conn.cursor() as cursor:
        cursor.execute(
            "select last_loaded_date from raw.load_watermark where pipeline = %s", (PIPELINE,)
        )
        row = cursor.fetchone()
    return row[0] if row else None


def write_watermark(conn: connection, run_date: date) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            insert into raw.load_watermark (pipeline, last_loaded_date, updated_at)
            values (%s, %s, now())
            on conflict (pipeline) do update
            set last_loaded_date = greatest(raw.load_watermark.last_loaded_date,
                                            excluded.last_loaded_date),
                updated_at = now()
            """,
            (PIPELINE, run_date),
        )


def _copy(conn: connection, spark: SparkSession, settings: Settings, target: Target, dates) -> int:
    reader = spark.read.schema(target.schema) if target.schema else spark.read
    df = reader.parquet(settings.path(*target.source))
    if target.date_column:
        df = df.filter(F.col(target.date_column).isin(dates))
    frame = df.select(*[F.col(column).cast("string") for column in target.columns]).toPandas()

    with conn.cursor() as cursor:
        if target.date_column:
            cursor.execute(
                f"delete from {target.table} where {target.date_column} = any(%s)", (dates,)
            )
        else:
            cursor.execute(f"truncate {target.table}")
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor.copy_expert(
            f"copy {target.table} ({', '.join(target.columns)}) from stdin with (format csv)",
            buffer,
        )
    return len(frame)


def run(spark: SparkSession, settings: Settings, run_date: date) -> dict[str, int]:
    loaded = {}
    with connect(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("set time zone 'UTC'")
            cursor.execute(DDL_PATH.read_text())
        watermark = read_watermark(conn)
        dates = dates_to_load(
            watermark, run_date, settings.late_window_days, settings.pipeline_start_date
        )
        logger.info("loading %s to %s (watermark %s)", dates[0], dates[-1], watermark)

        loaded[MERCHANTS.table] = _copy(conn, spark, settings, MERCHANTS, dates)
        for target in INCREMENTAL_TARGETS:
            loaded[target.table] = _copy(conn, spark, settings, target, dates)
        write_watermark(conn, run_date)

    logger.info("warehouse load for %s: %s", run_date, loaded)
    return loaded


if __name__ == "__main__":
    run_date = run_date_arg("Load silver and gold partitions into PostgreSQL")
    settings = Settings.from_env()
    run(get_spark("payments-load", settings), settings, run_date)
