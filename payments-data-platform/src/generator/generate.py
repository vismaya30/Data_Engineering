import hashlib
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from src.common.config import Settings, run_date_arg
from src.common.log import get_logger
from src.common.storage import write_jsonl

logger = get_logger(__name__)

CENT = Decimal("0.01")

CARD_SCHEMES = {"VISA": 0.55, "MASTERCARD": 0.35, "AMEX": 0.10}
SCHEME_PAN_PREFIX = {"VISA": "4", "MASTERCARD": "5", "AMEX": "3"}
CURRENCIES = {"GBP": 0.6, "EUR": 0.3, "USD": 0.1}
SETTLEMENT_LAGS = {1: 0.82, 2: 0.14, 3: 0.04}
MAX_SETTLEMENT_LAG = max(SETTLEMENT_LAGS)
MISMATCH_FACTORS = [Decimal("0.90"), Decimal("0.95"), Decimal("1.05"), Decimal("1.10")]

MCC_CODES = ["5411", "5812", "5999", "4111", "5732", "7011", "5541", "5691", "5912", "4121"]
COUNTRIES = ["GB", "IE", "FR", "DE", "NL", "ES"]
NAME_PREFIXES = [
    "Northgate",
    "Riverside",
    "Harbour",
    "Oakwood",
    "Kingsway",
    "Meridian",
    "Bluebell",
    "Highfield",
    "Granite",
    "Willow",
    "Station",
    "Market",
]
NAME_SUFFIXES = [
    "Grocers",
    "Coffee",
    "Electronics",
    "Pharmacy",
    "Taxis",
    "Hotel",
    "Fuel",
    "Outfitters",
    "Bakery",
    "Books",
    "Kitchen",
    "Hardware",
]


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    merchant_count: int = 60
    transactions_per_day: int = 5000
    start_date: date = date(2026, 10, 1)
    decline_rate: float = 0.06
    never_settle_rate: float = 0.015
    amount_mismatch_rate: float = 0.012
    duplicate_settlement_rate: float = 0.008
    orphan_settlement_rate: float = 0.002
    bad_record_rate: float = 0.008
    duplicate_record_rate: float = 0.01

    @classmethod
    def from_settings(cls, settings: Settings) -> "GeneratorConfig":
        return cls(
            seed=settings.generator_seed,
            merchant_count=settings.merchant_count,
            transactions_per_day=settings.transactions_per_day,
            start_date=settings.pipeline_start_date,
        )


def _rng(*parts: object) -> random.Random:
    digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


def _pick(rng: random.Random, weights: dict) -> object:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def _amount(rng: random.Random) -> Decimal:
    value = Decimal(str(rng.lognormvariate(3.2, 0.9))).quantize(CENT)
    return max(value, CENT)


def generate_merchants(cfg: GeneratorConfig) -> list[dict]:
    rng = _rng(cfg.seed, "merchants")
    merchants = []
    for number in range(1, cfg.merchant_count + 1):
        onboarded = date(2022, 1, 1) + timedelta(days=rng.randint(0, 900))
        merchants.append(
            {
                "merchant_id": f"M{number:05d}",
                "merchant_name": f"{rng.choice(NAME_PREFIXES)} {rng.choice(NAME_SUFFIXES)}",
                "mcc": rng.choice(MCC_CODES),
                "country": rng.choice(COUNTRIES),
                "settlement_currency": _pick(rng, CURRENCIES),
                "onboarded_date": onboarded.isoformat(),
            }
        )
    return merchants


def generate_authorisations(
    cfg: GeneratorConfig, business_date: date, merchants: list[dict]
) -> list[dict]:
    rng = _rng(cfg.seed, "auth", business_date.isoformat())
    day_start = datetime.combine(business_date, time.min, tzinfo=UTC)
    records = []
    for sequence in range(cfg.transactions_per_day):
        merchant = rng.choice(merchants)
        scheme = _pick(rng, CARD_SCHEMES)
        approved = rng.random() >= cfg.decline_rate
        timestamp = day_start + timedelta(seconds=rng.randint(0, 86_399))
        bin_digits = rng.randint(10_000, 99_999)
        records.append(
            {
                "transaction_id": f"T{business_date:%Y%m%d}{sequence:07d}",
                "merchant_id": merchant["merchant_id"],
                "card_scheme": scheme,
                "masked_pan": f"{SCHEME_PAN_PREFIX[scheme]}{bin_digits}******"
                f"{rng.randint(0, 9999):04d}",
                "amount": str(_amount(rng)),
                "currency": merchant["settlement_currency"],
                "auth_code": f"{rng.randint(0, 999_999):06d}" if approved else None,
                "auth_status": "APPROVED" if approved else "DECLINED",
                "auth_timestamp": timestamp.isoformat(),
            }
        )
    return records


def settlement_plan(cfg: GeneratorConfig, transaction_id: str) -> tuple[int | None, bool, bool]:
    rng = _rng(cfg.seed, "settlement", transaction_id)
    lag = None if rng.random() < cfg.never_settle_rate else _pick(rng, SETTLEMENT_LAGS)
    mismatch = rng.random() < cfg.amount_mismatch_rate
    duplicate = rng.random() < cfg.duplicate_settlement_rate
    return lag, mismatch, duplicate


def generate_settlements(
    cfg: GeneratorConfig, file_date: date, merchants: list[dict]
) -> list[dict]:
    records = []
    for lag in range(1, MAX_SETTLEMENT_LAG + 1):
        auth_date = file_date - timedelta(days=lag)
        if auth_date < cfg.start_date:
            continue
        for auth in generate_authorisations(cfg, auth_date, merchants):
            if auth["auth_status"] != "APPROVED":
                continue
            planned_lag, mismatch, duplicate = settlement_plan(cfg, auth["transaction_id"])
            if planned_lag != lag:
                continue
            amount = Decimal(auth["amount"])
            if mismatch:
                factor = _rng(cfg.seed, "mismatch", auth["transaction_id"]).choice(MISMATCH_FACTORS)
                amount = (amount * factor).quantize(CENT)
            for copy in range(2 if duplicate else 1):
                records.append(
                    {
                        "settlement_id": f"S{auth['transaction_id'][1:]}{copy}",
                        "transaction_id": auth["transaction_id"],
                        "merchant_id": auth["merchant_id"],
                        "card_scheme": auth["card_scheme"],
                        "settled_amount": str(amount),
                        "currency": auth["currency"],
                        "settlement_date": file_date.isoformat(),
                        "batch_id": f"{auth['card_scheme']}-{file_date:%Y%m%d}",
                    }
                )
    records.extend(_orphan_settlements(cfg, file_date, merchants))
    return records


def _orphan_settlements(cfg: GeneratorConfig, file_date: date, merchants: list[dict]) -> list[dict]:
    rng = _rng(cfg.seed, "orphan", file_date.isoformat())
    count = int(cfg.transactions_per_day * cfg.orphan_settlement_rate)
    previous_day = file_date - timedelta(days=1)
    records = []
    for number in range(count):
        merchant = rng.choice(merchants)
        scheme = _pick(rng, CARD_SCHEMES)
        records.append(
            {
                "settlement_id": f"S{previous_day:%Y%m%d}9{number:06d}0",
                "transaction_id": f"T{previous_day:%Y%m%d}9{number:06d}",
                "merchant_id": merchant["merchant_id"],
                "card_scheme": scheme,
                "settled_amount": str(_amount(rng)),
                "currency": merchant["settlement_currency"],
                "settlement_date": file_date.isoformat(),
                "batch_id": f"{scheme}-{file_date:%Y%m%d}",
            }
        )
    return records


def _corrupt(rng: random.Random, record: dict, number: int) -> dict:
    broken = dict(record)
    broken["transaction_id"] = f"{record['transaction_id']}X{number}"
    defect = rng.choice(
        ["missing_merchant", "unknown_merchant", "negative_amount", "bad_currency", "bad_timestamp"]
    )
    if defect == "missing_merchant":
        broken["merchant_id"] = None
    elif defect == "unknown_merchant":
        broken["merchant_id"] = "M99999"
    elif defect == "negative_amount":
        broken["amount"] = f"-{record['amount']}"
    elif defect == "bad_currency":
        broken["currency"] = "XXX"
    else:
        broken["auth_timestamp"] = "not-a-timestamp"
    return broken


def add_noise(cfg: GeneratorConfig, business_date: date, records: list[dict]) -> list[dict]:
    rng = _rng(cfg.seed, "noise", business_date.isoformat())
    bad_count = int(len(records) * cfg.bad_record_rate)
    duplicate_count = int(len(records) * cfg.duplicate_record_rate)
    bad = [_corrupt(rng, record, n) for n, record in enumerate(rng.sample(records, bad_count))]
    duplicates = [dict(record) for record in rng.sample(records, duplicate_count)]
    noisy = records + bad + duplicates
    rng.shuffle(noisy)
    return noisy


def add_duplicates(cfg: GeneratorConfig, file_date: date, records: list[dict]) -> list[dict]:
    rng = _rng(cfg.seed, "settlement-noise", file_date.isoformat())
    duplicate_count = int(len(records) * cfg.duplicate_record_rate)
    duplicates = [dict(record) for record in rng.sample(records, duplicate_count)]
    combined = records + duplicates
    rng.shuffle(combined)
    return combined


def run(settings: Settings, run_date: date) -> dict:
    cfg = GeneratorConfig.from_settings(settings)
    merchants = generate_merchants(cfg)
    transactions = add_noise(cfg, run_date, generate_authorisations(cfg, run_date, merchants))
    settlements = add_duplicates(cfg, run_date, generate_settlements(cfg, run_date, merchants))

    partition = f"ingest_date={run_date.isoformat()}"
    write_jsonl(settings.path("bronze", "merchants", partition, "merchants.jsonl"), merchants)
    write_jsonl(
        settings.path("bronze", "transactions", partition, "transactions.jsonl"), transactions
    )
    write_jsonl(settings.path("bronze", "settlements", partition, "settlements.jsonl"), settlements)

    counts = {
        "merchants": len(merchants),
        "transactions": len(transactions),
        "settlements": len(settlements),
    }
    logger.info("bronze written for %s: %s", run_date, counts)
    return counts


if __name__ == "__main__":
    run(Settings.from_env(), run_date_arg("Generate synthetic payments data into bronze"))
