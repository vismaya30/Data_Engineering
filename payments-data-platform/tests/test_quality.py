from datetime import date
from decimal import Decimal

import pytest

from src.quality.checks import (
    DataQualityError,
    GateResult,
    apply_rules,
    check_thresholds,
    evaluate,
    settlement_rules,
)
from src.transforms.schemas import MONEY

DAY = date(2026, 10, 10)


def _settlements(spark, rows):
    df = spark.createDataFrame(
        rows,
        "settlement_id string, transaction_id string, settled_amount string, "
        "currency string, card_scheme string, settlement_date date",
    )
    return df.withColumn("settled_amount", df["settled_amount"].cast(MONEY))


def test_rules_split_valid_and_invalid(spark):
    df = _settlements(
        spark,
        [
            ("S1", "T1", "10.00", "GBP", "VISA", DAY),
            ("S2", None, "10.00", "GBP", "VISA", DAY),
            ("S3", "T3", "-5.00", "XXX", "VISA", DAY),
            ("S4", "T4", None, "EUR", "AMEX", None),
        ],
    )
    valid, invalid = apply_rules(df, settlement_rules())

    assert [row["settlement_id"] for row in valid.collect()] == ["S1"]
    failures = {row["settlement_id"]: set(row["dq_failures"]) for row in invalid.collect()}
    assert failures["S2"] == {"transaction_id_not_null"}
    assert failures["S3"] == {"settled_amount_positive", "currency_supported"}
    assert failures["S4"] == {"settled_amount_positive", "settlement_date_valid"}
    assert valid.first()["settled_amount"] == Decimal("10.00")


def test_evaluate_counts_failures_by_rule(spark):
    df = _settlements(
        spark,
        [
            ("S1", "T1", "10.00", "GBP", "VISA", DAY),
            ("S2", "T2", "0", "GBP", "VISA", DAY),
        ],
    )
    _, _, result = evaluate("settlements", df, settlement_rules())
    assert (result.total, result.passed, result.rejected) == (2, 1, 1)
    assert result.failures_by_rule == {"settled_amount_positive": 1}


def test_threshold_breach_raises():
    results = [GateResult("transactions", total=100, passed=90, rejected=10)]
    with pytest.raises(DataQualityError, match="transactions"):
        check_thresholds(results, max_reject_ratio=0.05)


def test_empty_feed_raises():
    with pytest.raises(DataQualityError, match="no records"):
        check_thresholds([GateResult("settlements", 0, 0, 0)], max_reject_ratio=0.05)


def test_within_threshold_passes():
    check_thresholds([GateResult("merchants", 100, 99, 1)], max_reject_ratio=0.05)
