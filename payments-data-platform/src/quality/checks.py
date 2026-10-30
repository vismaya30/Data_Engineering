from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SUPPORTED_CURRENCIES = ["GBP", "EUR", "USD"]
SUPPORTED_SCHEMES = ["VISA", "MASTERCARD", "AMEX"]
AUTH_STATUSES = ["APPROVED", "DECLINED"]


class DataQualityError(Exception):
    pass


@dataclass(frozen=True)
class Rule:
    name: str
    condition: Column


@dataclass
class GateResult:
    entity: str
    total: int
    passed: int
    rejected: int
    failures_by_rule: dict[str, int] = field(default_factory=dict)

    @property
    def reject_ratio(self) -> float:
        return self.rejected / self.total if self.total else 0.0


def transaction_rules() -> list[Rule]:
    return [
        Rule("transaction_id_not_null", F.col("transaction_id").isNotNull()),
        Rule("merchant_id_not_null", F.col("merchant_id").isNotNull()),
        Rule("merchant_known", F.col("merchant_known")),
        Rule("amount_positive", F.col("amount") > 0),
        Rule("currency_supported", F.col("currency").isin(SUPPORTED_CURRENCIES)),
        Rule("card_scheme_supported", F.col("card_scheme").isin(SUPPORTED_SCHEMES)),
        Rule("auth_status_valid", F.col("auth_status").isin(AUTH_STATUSES)),
        Rule("auth_timestamp_valid", F.col("auth_timestamp").isNotNull()),
    ]


def settlement_rules() -> list[Rule]:
    return [
        Rule("settlement_id_not_null", F.col("settlement_id").isNotNull()),
        Rule("transaction_id_not_null", F.col("transaction_id").isNotNull()),
        Rule("settled_amount_positive", F.col("settled_amount") > 0),
        Rule("currency_supported", F.col("currency").isin(SUPPORTED_CURRENCIES)),
        Rule("card_scheme_supported", F.col("card_scheme").isin(SUPPORTED_SCHEMES)),
        Rule("settlement_date_valid", F.col("settlement_date").isNotNull()),
    ]


def merchant_rules() -> list[Rule]:
    return [
        Rule("merchant_id_not_null", F.col("merchant_id").isNotNull()),
        Rule("merchant_name_not_null", F.col("merchant_name").isNotNull()),
        Rule("mcc_format", F.col("mcc").rlike(r"^\d{4}$")),
        Rule("country_format", F.col("country").rlike(r"^[A-Z]{2}$")),
        Rule("currency_supported", F.col("settlement_currency").isin(SUPPORTED_CURRENCIES)),
    ]


def apply_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    failed = [F.when(~F.coalesce(rule.condition, F.lit(False)), F.lit(rule.name)) for rule in rules]
    flagged = df.withColumn(
        "dq_failures", F.filter(F.array(*failed), lambda name: name.isNotNull())
    )
    valid = flagged.filter(F.size("dq_failures") == 0).drop("dq_failures")
    invalid = flagged.filter(F.size("dq_failures") > 0)
    return valid, invalid


def evaluate(
    entity: str, df: DataFrame, rules: list[Rule]
) -> tuple[DataFrame, DataFrame, GateResult]:
    df = df.cache()
    valid, invalid = apply_rules(df, rules)
    invalid = invalid.cache()
    total = df.count()
    rejected = invalid.count()
    failures = {
        row["rule"]: row["count"]
        for row in invalid.select(F.explode("dq_failures").alias("rule"))
        .groupBy("rule")
        .count()
        .collect()
    }
    result = GateResult(entity, total, total - rejected, rejected, failures)
    return valid, invalid, result


def check_thresholds(
    results: list[GateResult], max_reject_ratio: float, allow_empty: frozenset[str] = frozenset()
) -> None:
    problems = []
    for result in results:
        if result.total == 0 and result.entity in allow_empty:
            continue
        if result.total == 0:
            problems.append(f"{result.entity}: no records received")
        elif result.reject_ratio > max_reject_ratio:
            problems.append(
                f"{result.entity}: reject ratio {result.reject_ratio:.2%} "
                f"exceeds {max_reject_ratio:.2%} {result.failures_by_rule}"
            )
    if problems:
        raise DataQualityError("; ".join(problems))
