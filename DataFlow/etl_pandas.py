import logging
import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(BASE_DIR / "etl.log")],
)
log = logging.getLogger("etl_pandas")


def extract(path):
    """EXTRACT: read the CSV into a DataFrame (everything as text)."""
    return pd.read_csv(path, dtype=str)


def transform(df):
    """TRANSFORM: convert types, flag bad rows, clean text, add 'total'."""
    df = df.copy()
    # errors="coerce" turns unconvertible values into NaN/NaT instead of raising
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["order_date"] = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")

    # a row is bad if any of those three columns failed to convert
    bad = df[["quantity", "price", "order_date"]].isna().any(axis=1)
    rejected = df[bad].copy()
    rejected["reason"] = rejected.apply(
        lambda r: "invalid " + ", ".join(
            c for c in ["quantity", "price", "order_date"] if pd.isna(r[c])
        ),
        axis=1,
    )

    clean = df[~bad].copy()
    clean["order_id"] = clean["order_id"].astype(int)
    clean["quantity"] = clean["quantity"].astype(int)
    clean["customer"] = clean["customer"].str.strip().str.title()
    clean["product"] = clean["product"].str.strip()
    clean["order_date"] = clean["order_date"].dt.strftime("%Y-%m-%d")
    clean["total"] = (clean["quantity"] * clean["price"]).round(2)
    return clean, rejected


def load(df, db_path):
    """LOAD: write the DataFrame to a SQLite table (replaces it each run)."""
    with sqlite3.connect(db_path) as conn:
        df.to_sql("sales", conn, if_exists="replace", index=False)


def run_pipeline():
    log.info("Pipeline started")
    raw = extract(BASE_DIR / "sales.csv")
    log.info("Extracted %d rows", len(raw))
    clean, rejected = transform(raw)
    for _, row in rejected.iterrows():
        log.warning("Rejected order_id=%s: %s", row["order_id"], row["reason"])
    load(clean, BASE_DIR / "sales_pandas.db")
    log.info("Loaded %d rows into sales_pandas.db", len(clean))
    if not rejected.empty:
        rejected.to_csv(BASE_DIR / "rejected_pandas.csv", index=False)
    log.info("Pipeline finished: %d loaded, %d rejected", len(clean), len(rejected))


if __name__ == "__main__":
    run_pipeline()
