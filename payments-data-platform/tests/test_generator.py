from collections import Counter
from datetime import date, timedelta

from src.generator.generate import (
    GeneratorConfig,
    add_noise,
    generate_authorisations,
    generate_merchants,
    generate_settlements,
    settlement_plan,
)

CFG = GeneratorConfig(transactions_per_day=500, merchant_count=10, start_date=date(2026, 10, 1))
DAY = date(2026, 10, 10)


def test_generation_is_deterministic():
    merchants = generate_merchants(CFG)
    assert generate_authorisations(CFG, DAY, merchants) == generate_authorisations(
        CFG, DAY, merchants
    )
    assert generate_settlements(CFG, DAY, merchants) == generate_settlements(CFG, DAY, merchants)


def test_transaction_ids_are_unique_and_dated():
    records = generate_authorisations(CFG, DAY, generate_merchants(CFG))
    ids = [record["transaction_id"] for record in records]
    assert len(ids) == len(set(ids)) == CFG.transactions_per_day
    assert all(record["auth_timestamp"].startswith("2026-10-10") for record in records)


def test_settlements_follow_their_plan():
    merchants = generate_merchants(CFG)
    settlements = generate_settlements(CFG, DAY, merchants)
    auths = {
        auth["transaction_id"]: auth
        for lag in (1, 2, 3)
        for auth in generate_authorisations(CFG, DAY - timedelta(days=lag), merchants)
    }
    for settlement in settlements:
        auth = auths.get(settlement["transaction_id"])
        if auth is None:
            continue
        lag, _, _ = settlement_plan(CFG, auth["transaction_id"])
        auth_day = date.fromisoformat(auth["auth_timestamp"][:10])
        assert auth["auth_status"] == "APPROVED"
        assert (DAY - auth_day).days == lag


def test_no_settlements_before_pipeline_start():
    cfg = GeneratorConfig(transactions_per_day=1000, merchant_count=5, start_date=DAY)
    settlements = generate_settlements(cfg, DAY, generate_merchants(cfg))
    assert settlements
    assert all(record["transaction_id"][9] == "9" for record in settlements)


def test_settlement_anomalies_are_present():
    merchants = generate_merchants(CFG)
    settlements = []
    for offset in range(5):
        settlements += generate_settlements(CFG, DAY + timedelta(days=offset), merchants)
    per_transaction = Counter(record["transaction_id"] for record in settlements)
    assert any(count > 1 for count in per_transaction.values())
    assert any(record["transaction_id"][9] == "9" for record in settlements)


def test_noise_adds_bad_and_duplicate_rows():
    clean = generate_authorisations(CFG, DAY, generate_merchants(CFG))
    noisy = add_noise(CFG, DAY, clean)
    assert len(noisy) > len(clean)
    assert any("X" in (record["transaction_id"] or "") for record in noisy)
    assert len({record["transaction_id"] for record in noisy}) < len(noisy)
