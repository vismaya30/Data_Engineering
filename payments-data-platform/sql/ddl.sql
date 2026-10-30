create schema if not exists raw;

create table if not exists raw.merchants (
    merchant_id          text primary key,
    merchant_name        text not null,
    mcc                  char(4) not null,
    country              char(2) not null,
    settlement_currency  char(3) not null,
    onboarded_date       date
);

create table if not exists raw.transactions (
    transaction_id  text primary key,
    merchant_id     text not null,
    card_scheme     text not null,
    masked_pan      text,
    amount          numeric(18, 2) not null,
    currency        char(3) not null,
    auth_code       text,
    auth_status     text not null,
    auth_timestamp  timestamptz not null,
    auth_date       date not null
);

create index if not exists transactions_auth_date_idx on raw.transactions (auth_date);

create table if not exists raw.settlements (
    settlement_id    text primary key,
    transaction_id   text not null,
    merchant_id      text not null,
    card_scheme      text not null,
    settled_amount   numeric(18, 2) not null,
    currency         char(3) not null,
    settlement_date  date not null,
    batch_id         text
);

create index if not exists settlements_settlement_date_idx on raw.settlements (settlement_date);
create index if not exists settlements_transaction_id_idx on raw.settlements (transaction_id);

create table if not exists raw.reconciliation (
    business_date     date not null,
    transaction_id    text not null,
    settlement_ids    text,
    merchant_id       text not null,
    card_scheme       text not null,
    currency          char(3) not null,
    auth_amount       numeric(18, 2),
    settled_amount    numeric(18, 2),
    settlement_count  integer not null,
    settlement_date   date,
    days_to_settle    integer,
    status            text not null,
    run_date          date not null,
    reconciled_at     timestamptz not null
);

create index if not exists reconciliation_business_date_idx on raw.reconciliation (business_date);

create table if not exists raw.load_watermark (
    pipeline          text primary key,
    last_loaded_date  date not null,
    updated_at        timestamptz not null
);
