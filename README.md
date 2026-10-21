# ETL Demo

A small project showing the same Extract → Transform → Load pattern built three ways.

| File | Source | Tools |
|---|---|---|
| `etl.py` | `sales.csv` | plain Python (csv, sqlite3) |
| `etl_pandas.py` | `sales.csv` | pandas |
| `etl_api.py` | [dummyjson.com](https://dummyjson.com) products API | pandas + requests |

## What each pipeline does

1. **Extract** – read raw data from a CSV or an API.
2. **Transform** – fix types, clean text, add a derived column, and set aside invalid rows with a reason.
3. **Load** – write clean rows to a SQLite database. Rejected rows go to a `rejected*.csv` file.

Progress and warnings are logged to the terminal and to `etl.log`.

## Run

```
pip install -r requirements.txt
python3 etl.py
python3 etl_pandas.py
python3 etl_api.py
```

Generated files (`*.db`, `etl.log`, `rejected*.csv`) are ignored by git.
