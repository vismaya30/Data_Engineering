import csv
import logging
import sqlite3
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                       # print to terminal
        logging.FileHandler(BASE_DIR / "etl.log"),     # also save to file
    ],
)
log = logging.getLogger("etl")


def extract(path):
    """EXTRACT: read raw rows from a CSV file into a list of dicts."""
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def transform(rows):
    """TRANSFORM: clean types, fix formatting, drop bad rows, add 'total'."""
    clean, rejected = [], []
    for row in rows:
        try:
            quantity = int(row["quantity"])  # '' raises ValueError
            price = float(row["price"])
            order_date = datetime.strptime(row["order_date"], "%Y-%m-%d").date()
        except ValueError as e:
            log.warning("Rejected order_id=%s: %s", row["order_id"], e)
            rejected.append({**row, "reason": str(e)})
            continue

        clean.append({
            "order_id": int(row["order_id"]),
            "customer": row["customer"].strip().title(),
            "product": row["product"].strip(),
            "quantity": quantity,
            "price": price,
            "order_date": order_date.isoformat(),
            "total": round(quantity * price, 2),
        })
    return clean, rejected


def load(rows, db_path):
    """LOAD: write clean rows into a SQLite table."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            order_id   INTEGER PRIMARY KEY,
            customer   TEXT,
            product    TEXT,
            quantity   INTEGER,
            price      REAL,
            order_date TEXT,
            total      REAL
        )
    """)
    conn.executemany(
        "INSERT OR REPLACE INTO sales VALUES "
        "(:order_id, :customer, :product, :quantity, :price, :order_date, :total)",
        rows,
    )
    conn.commit()
    conn.close()


def save_rejected(rows, path):
    """Write rejected rows (with the reason) to a CSV for review."""
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def run_pipeline():
    log.info("Pipeline started")
    raw = extract(BASE_DIR / "sales.csv")
    log.info("Extracted %d rows", len(raw))
    clean, rejected = transform(raw)
    load(clean, BASE_DIR / "sales.db")
    log.info("Loaded %d rows into sales.db", len(clean))
    save_rejected(rejected, BASE_DIR / "rejected.csv")
    log.info("Pipeline finished: %d loaded, %d rejected", len(clean), len(rejected))


if __name__ == "__main__":
    run_pipeline()
