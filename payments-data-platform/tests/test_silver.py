from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from src.transforms.schemas import BRONZE_TRANSACTIONS
from src.transforms.silver import clean_transactions, deduplicate, flag_known_merchants

DAY = date(2026, 10, 10)


def _bronze(spark, rows):
    return spark.createDataFrame(rows, BRONZE_TRANSACTIONS).withColumn("ingest_date", F.lit(DAY))


def test_clean_casts_and_normalises(spark):
    df = _bronze(
        spark,
        [
            (
                " T1 ",
                "m00001",
                "visa",
                "4123******0001",
                "12.50",
                "gbp",
                "123456",
                "approved",
                "2026-10-10T08:15:00+00:00",
            ),
            ("T2", "M00001", "VISA", None, "abc", "GBP", None, "DECLINED", "not-a-timestamp"),
        ],
    )
    rows = {row["transaction_id"]: row for row in clean_transactions(df).collect()}

    assert rows["T1"]["merchant_id"] == "M00001"
    assert rows["T1"]["card_scheme"] == "VISA"
    assert rows["T1"]["currency"] == "GBP"
    assert rows["T1"]["amount"] == Decimal("12.50")
    assert rows["T1"]["auth_date"] == DAY
    assert rows["T2"]["amount"] is None
    assert rows["T2"]["auth_timestamp"] is None


def test_deduplicate_keeps_one_row_per_key_and_all_null_keys(spark):
    df = spark.createDataFrame(
        [("A", 1), ("A", 2), ("B", 1), (None, 1), (None, 2)], "id string, version int"
    )
    result = deduplicate(df, "id", "version").collect()

    keyed = {row["id"]: row["version"] for row in result if row["id"] is not None}
    assert keyed == {"A": 2, "B": 1}
    assert sum(1 for row in result if row["id"] is None) == 2


def test_flag_known_merchants(spark):
    transactions = spark.createDataFrame(
        [("T1", "M1"), ("T2", "M9")], "transaction_id string, merchant_id string"
    )
    merchants = spark.createDataFrame([("M1",)], "merchant_id string")
    flags = {
        row["transaction_id"]: row["merchant_known"]
        for row in flag_known_merchants(transactions, merchants).collect()
    }
    assert flags == {"T1": True, "T2": False}
