from datetime import date, timedelta

from pyspark.sql import functions as F

from src.transforms.reconciliation import (
    AMOUNT_MISMATCH,
    DUPLICATE,
    LATE,
    MATCHED,
    ORPHAN_SETTLEMENT,
    PENDING,
    UNMATCHED,
    reconcile,
)
from src.transforms.schemas import MONEY

RUN_DATE = date(2026, 10, 20)


def days_ago(n: int) -> date:
    return RUN_DATE - timedelta(days=n)


def _transactions(spark, rows):
    df = spark.createDataFrame(
        [
            (txn_id, "M00001", "VISA", amount, "GBP", status, auth_date)
            for txn_id, amount, status, auth_date in rows
        ],
        "transaction_id string, merchant_id string, card_scheme string, amount string, "
        "currency string, auth_status string, auth_date date",
    )
    return df.withColumn("amount", F.col("amount").cast(MONEY))


def _settlements(spark, rows):
    df = spark.createDataFrame(
        [
            (settlement_id, txn_id, "M00001", "VISA", amount, "GBP", settlement_date)
            for settlement_id, txn_id, amount, settlement_date in rows
        ],
        "settlement_id string, transaction_id string, merchant_id string, card_scheme string, "
        "settled_amount string, currency string, settlement_date date",
    )
    return df.withColumn("settled_amount", F.col("settled_amount").cast(MONEY))


def _statuses(spark, transactions, settlements):
    result = reconcile(
        _transactions(spark, transactions),
        _settlements(spark, settlements),
        RUN_DATE,
        late_window_days=3,
        late_threshold_days=2,
    )
    return {row["transaction_id"]: row["status"] for row in result.collect()}


def test_each_status_is_assigned(spark):
    statuses = _statuses(
        spark,
        [
            ("T_MATCH", "10.00", "APPROVED", days_ago(2)),
            ("T_LATE", "10.00", "APPROVED", days_ago(3)),
            ("T_MISMATCH", "10.00", "APPROVED", days_ago(2)),
            ("T_DUP", "10.00", "APPROVED", days_ago(2)),
            ("T_PENDING", "10.00", "APPROVED", days_ago(1)),
            ("T_UNMATCHED", "10.00", "APPROVED", days_ago(3)),
            ("T_DECLINED", "10.00", "DECLINED", days_ago(1)),
        ],
        [
            ("S1", "T_MATCH", "10.00", days_ago(1)),
            ("S2", "T_LATE", "10.00", days_ago(0)),
            ("S3", "T_MISMATCH", "9.50", days_ago(1)),
            ("S4", "T_DUP", "10.00", days_ago(1)),
            ("S5", "T_DUP", "10.00", days_ago(1)),
            ("S6", "T_GHOST", "25.00", days_ago(1)),
        ],
    )
    assert statuses == {
        "T_MATCH": MATCHED,
        "T_LATE": LATE,
        "T_MISMATCH": AMOUNT_MISMATCH,
        "T_DUP": DUPLICATE,
        "T_PENDING": PENDING,
        "T_UNMATCHED": UNMATCHED,
        "T_GHOST": ORPHAN_SETTLEMENT,
    }


def test_older_authorisations_are_not_orphans(spark):
    statuses = _statuses(
        spark,
        [("T_OLD", "10.00", "APPROVED", days_ago(5))],
        [("S1", "T_OLD", "10.00", days_ago(2))],
    )
    assert statuses == {}


def test_future_settlements_are_ignored(spark):
    statuses = _statuses(
        spark,
        [("T1", "10.00", "APPROVED", days_ago(1))],
        [("S1", "T1", "10.00", RUN_DATE + timedelta(days=1))],
    )
    assert statuses == {"T1": PENDING}


def test_output_columns(spark):
    result = reconcile(
        _transactions(spark, [("T1", "10.00", "APPROVED", days_ago(1))]),
        _settlements(spark, [("S1", "T1", "10.00", RUN_DATE)]),
        RUN_DATE,
        3,
        2,
    )
    row = result.first()
    assert row["business_date"] == days_ago(1)
    assert row["settlement_ids"] == "S1"
    assert row["days_to_settle"] == 1
    assert row["run_date"] == RUN_DATE
