from datetime import date

from src.transforms.load_warehouse import dates_to_load

START = date(2026, 10, 1)


def test_first_run_loads_late_window():
    assert dates_to_load(None, date(2026, 10, 10), 3, START) == [
        date(2026, 10, 7),
        date(2026, 10, 8),
        date(2026, 10, 9),
        date(2026, 10, 10),
    ]


def test_never_loads_before_pipeline_start():
    assert dates_to_load(None, date(2026, 10, 2), 3, START) == [
        date(2026, 10, 1),
        date(2026, 10, 2),
    ]


def test_catches_up_after_missed_runs():
    dates = dates_to_load(date(2026, 10, 3), date(2026, 10, 10), 3, START)
    assert dates[0] == date(2026, 10, 4)
    assert dates[-1] == date(2026, 10, 10)


def test_rerun_of_old_date_reloads_its_window():
    assert dates_to_load(date(2026, 10, 20), date(2026, 10, 10), 3, START) == [
        date(2026, 10, 7),
        date(2026, 10, 8),
        date(2026, 10, 9),
        date(2026, 10, 10),
    ]
