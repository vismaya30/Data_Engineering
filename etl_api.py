import logging
import sqlite3
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).parent
API_URL = "https://dummyjson.com/products"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(BASE_DIR / "etl.log")],
)
log = logging.getLogger("etl_api")


def extract(url):
    """EXTRACT: call the API and return the products as a DataFrame."""
    params = {"limit": 100, "select": "title,category,price,stock,rating"}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()          # raises an error on 4xx/5xx responses
    return pd.DataFrame(response.json()["products"])


def transform(df):
    """TRANSFORM: clean text, validate numbers, add 'stock_value'."""
    df = df.copy()
    df["title"] = df["title"].str.strip()
    df["category"] = df["category"].str.strip().str.title()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce")

    bad = df["price"].isna() | (df["price"] <= 0) | df["stock"].isna() | (df["stock"] < 0)
    rejected = df[bad].copy()
    rejected["reason"] = "invalid price or stock"

    clean = df[~bad].copy()
    clean["stock"] = clean["stock"].astype(int)
    clean["stock_value"] = (clean["price"] * clean["stock"]).round(2)
    return clean, rejected


def load(df, db_path):
    with sqlite3.connect(db_path) as conn:
        df.to_sql("products", conn, if_exists="replace", index=False)


def run_pipeline():
    log.info("Pipeline started")
    try:
        raw = extract(API_URL)
    except requests.RequestException as e:
        log.error("Extract failed: %s", e)
        return
    log.info("Extracted %d rows from API", len(raw))
    clean, rejected = transform(raw)
    load(clean, BASE_DIR / "api_products.db")
    if not rejected.empty:
        rejected.to_csv(BASE_DIR / "rejected_api.csv", index=False)
    log.info("Pipeline finished: %d loaded, %d rejected", len(clean), len(rejected))


if __name__ == "__main__":
    run_pipeline()
