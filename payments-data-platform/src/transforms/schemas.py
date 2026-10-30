from pyspark.sql import types as T

MONEY = T.DecimalType(18, 2)


def _strings(*names: str) -> T.StructType:
    return T.StructType([T.StructField(name, T.StringType()) for name in names])


BRONZE_TRANSACTIONS = _strings(
    "transaction_id",
    "merchant_id",
    "card_scheme",
    "masked_pan",
    "amount",
    "currency",
    "auth_code",
    "auth_status",
    "auth_timestamp",
)

BRONZE_SETTLEMENTS = _strings(
    "settlement_id",
    "transaction_id",
    "merchant_id",
    "card_scheme",
    "settled_amount",
    "currency",
    "settlement_date",
    "batch_id",
)

BRONZE_MERCHANTS = _strings(
    "merchant_id",
    "merchant_name",
    "mcc",
    "country",
    "settlement_currency",
    "onboarded_date",
)

SILVER_TRANSACTIONS = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("card_scheme", T.StringType()),
        T.StructField("masked_pan", T.StringType()),
        T.StructField("amount", MONEY),
        T.StructField("currency", T.StringType()),
        T.StructField("auth_code", T.StringType()),
        T.StructField("auth_status", T.StringType()),
        T.StructField("auth_timestamp", T.TimestampType()),
        T.StructField("auth_date", T.DateType()),
    ]
)

SILVER_SETTLEMENTS = T.StructType(
    [
        T.StructField("settlement_id", T.StringType()),
        T.StructField("transaction_id", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("card_scheme", T.StringType()),
        T.StructField("settled_amount", MONEY),
        T.StructField("currency", T.StringType()),
        T.StructField("batch_id", T.StringType()),
        T.StructField("settlement_date", T.DateType()),
    ]
)
