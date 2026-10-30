import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

st.set_page_config(page_title="Payments Reconciliation", layout="wide")

STATUS_ORDER = [
    "MATCHED",
    "LATE",
    "PENDING",
    "AMOUNT_MISMATCH",
    "DUPLICATE",
    "UNMATCHED",
    "ORPHAN_SETTLEMENT",
]


@st.cache_resource
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('PG_USER', 'payments')}:"
        f"{os.getenv('PG_PASSWORD', 'payments')}@{os.getenv('PG_HOST', 'localhost')}:"
        f"{os.getenv('PG_PORT', '15432')}/{os.getenv('PG_DATABASE', 'payments')}"
    )
    return create_engine(url)


@st.cache_data(ttl=300)
def query(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@st.cache_data(ttl=300)
def date_bounds() -> tuple[date, date]:
    bounds = query("select min(auth_date) as lo, max(auth_date) as hi from marts.fact_transactions")
    lo, hi = bounds.iloc[0]["lo"], bounds.iloc[0]["hi"]
    if pd.isna(lo):
        today = date.today()
        return today - timedelta(days=30), today
    return lo, hi


st.title("Payments Reconciliation")

lo, hi = date_bounds()
with st.sidebar:
    st.header("Filters")
    selected = st.date_input("Business date", value=(max(lo, hi - timedelta(days=13)), hi))
    start, end = selected if len(selected) == 2 else (selected[0], selected[0])
    currencies = query("select currency_code from marts.dim_currency order by 1")["currency_code"]
    currency = st.multiselect("Currency", list(currencies), default=list(currencies))
    schemes = query("select scheme_code from marts.dim_card_scheme order by 1")["scheme_code"]
    scheme = st.multiselect("Card scheme", list(schemes), default=list(schemes))

filters = {"start": start, "end": end, "currency": currency or [""], "scheme": scheme or [""]}

volumes = query(
    """
    select auth_date, card_scheme, currency, merchant_id,
           transaction_count, approved_count, declined_count, approved_amount
    from marts.agg_daily_volumes
    where auth_date between :start and :end
      and currency = any(:currency) and card_scheme = any(:scheme)
    """,
    **filters,
)
recon = query(
    """
    select business_date, merchant_id, card_scheme, currency, status, is_exception,
           record_count, amount_difference
    from marts.agg_daily_reconciliation
    where business_date between :start and :end
      and currency = any(:currency) and card_scheme = any(:scheme)
    """,
    **filters,
)

if volumes.empty:
    st.info("No data for the selected filters yet. Run the pipeline and dbt first.")
    st.stop()

authorisations = recon.loc[recon["status"] != "ORPHAN_SETTLEMENT", "record_count"].sum()
matched = recon.loc[recon["status"] == "MATCHED", "record_count"].sum()
exceptions = recon.loc[recon["is_exception"], "record_count"].sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Authorisations", f"{int(volumes['transaction_count'].sum()):,}")
col2.metric("Approved value", f"{volumes['approved_amount'].sum():,.2f}")
col3.metric("Match rate", f"{(matched / authorisations if authorisations else 0):.2%}")
col4.metric("Open exceptions", f"{int(exceptions):,}")

left, right = st.columns(2)

daily_volume = volumes.groupby(["auth_date", "card_scheme"], as_index=False)[
    "approved_amount"
].sum()
left.subheader("Approved value by scheme")
left.plotly_chart(
    px.bar(daily_volume, x="auth_date", y="approved_amount", color="card_scheme"),
    use_container_width=True,
)

daily_status = recon.groupby(["business_date", "status"], as_index=False)["record_count"].sum()
right.subheader("Reconciliation status by day")
right.plotly_chart(
    px.bar(
        daily_status,
        x="business_date",
        y="record_count",
        color="status",
        category_orders={"status": STATUS_ORDER},
    ),
    use_container_width=True,
)

st.subheader("Exceptions by merchant")
merchants = query("select merchant_id, merchant_name, country_code from marts.dim_merchant")
by_merchant = (
    recon[recon["is_exception"]]
    .pivot_table(index="merchant_id", columns="status", values="record_count", aggfunc="sum")
    .fillna(0)
    .astype(int)
    .reset_index()
    .merge(merchants, on="merchant_id", how="left")
)
st.dataframe(by_merchant, use_container_width=True, hide_index=True)

st.subheader("Exception detail")
detail = query(
    """
    select transaction_id, auth_date, merchant_id, card_scheme, currency, amount,
           settled_amount, settlement_count, days_to_settle, reconciliation_status
    from marts.fact_transactions
    where auth_date between :start and :end
      and currency = any(:currency) and card_scheme = any(:scheme)
      and reconciliation_status in ('AMOUNT_MISMATCH', 'DUPLICATE', 'UNMATCHED')
    order by auth_date desc, transaction_id
    limit 500
    """,
    **filters,
)
st.dataframe(detail, use_container_width=True, hide_index=True)
