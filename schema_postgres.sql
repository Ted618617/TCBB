-- ===== Schemas =====
create schema if not exists raw;
create schema if not exists stg;
create schema if not exists mart;
create schema if not exists meta;

-- ===== Meta: ETL state & run log =====
create table if not exists meta.etl_state (
  pipeline_name text primary key,
  last_watermark timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists meta.etl_run_log (
  run_id uuid primary key,
  pipeline_name text not null,
  start_time timestamptz not null,
  end_time timestamptz,
  status text not null,
  watermark_start timestamptz,
  watermark_end timestamptz,
  rows_raw int default 0,
  rows_stg int default 0,
  rows_mart int default 0,
  rows_quarantine int default 0,
  error_message text,
  created_at timestamptz not null default now()
);

create table if not exists meta.dq_results (
  run_id uuid not null references meta.etl_run_log(run_id),
  rule_id text not null,
  rule_name text not null,
  total_cnt int not null,
  fail_cnt int not null,
  fail_ratio numeric(6,3) not null,
  created_at timestamptz not null default now(),
  primary key(run_id, rule_id)
);

create table if not exists meta.quarantine_transactions (
  run_id uuid not null references meta.etl_run_log(run_id),
  rule_id text not null,
  fail_reason text not null,
  txn_id text,
  txn_time timestamptz,
  amount numeric(18,2),
  currency text,
  account_id text,
  customer_id text,
  channel_id text,
  merchant_id text,
  txn_type text,
  status text,
  source_system text,
  ingestion_time timestamptz,
  created_at timestamptz not null default now()
);

-- ===== Raw tables (no uniqueness constraints; allow dirty/duplicate) =====
create table if not exists raw.customers (
  customer_id text,
  name text,
  id_hash text,
  phone text,
  email text,
  risk_level text,
  source_system text,
  ingestion_time timestamptz not null,
  run_id uuid not null
);

create table if not exists raw.accounts (
  account_id text,
  customer_id text,
  account_type text,
  open_date date,
  status text,
  source_system text,
  ingestion_time timestamptz not null,
  run_id uuid not null
);

create table if not exists raw.merchants (
  merchant_id text,
  merchant_name text,
  mcc text,
  status text,
  source_system text,
  ingestion_time timestamptz not null,
  run_id uuid not null
);

create table if not exists raw.channels (
  channel_id text,
  channel_name text,
  channel_type text,
  source_system text,
  ingestion_time timestamptz not null,
  run_id uuid not null
);

create table if not exists raw.transactions (
  txn_id text,
  txn_time timestamptz,
  amount numeric(18,2),
  currency text,
  account_id text,
  customer_id text,
  channel_id text,
  merchant_id text,
  txn_type text,
  status text,
  source_system text,
  ingestion_time timestamptz not null,
  run_id uuid not null
);

create index if not exists ix_raw_txn_time on raw.transactions(txn_time);

-- ===== Staging (standardized) =====
create table if not exists stg.transactions (
  run_id uuid not null,
  txn_id text,
  txn_time timestamptz,
  amount numeric(18,2),
  currency_std text,
  account_id text,
  customer_id text,
  channel_id text,
  merchant_id text,
  txn_type text,
  status text,
  source_system text,
  is_duplicate_suspect boolean not null default false,
  created_at timestamptz not null default now()
);

create index if not exists ix_stg_txn_time on stg.transactions(txn_time);

-- ===== Mart dims (simple upsert; SCD2 optional later) =====
create table if not exists mart.dim_customer (
  customer_sk bigserial primary key,
  customer_id text unique not null,
  name_std text,
  risk_level text,
  updated_at timestamptz not null default now()
);

create table if not exists mart.dim_account (
  account_sk bigserial primary key,
  account_id text unique not null,
  customer_id text,
  account_type text,
  status text,
  updated_at timestamptz not null default now()
);

create table if not exists mart.dim_merchant (
  merchant_sk bigserial primary key,
  merchant_id text unique not null,
  merchant_name text,
  mcc text,
  status text,
  updated_at timestamptz not null default now()
);

create table if not exists mart.dim_channel (
  channel_sk bigserial primary key,
  channel_id text unique not null,
  channel_name text,
  channel_type text,
  updated_at timestamptz not null default now()
);

-- ===== Mart fact (idempotent key: txn_id + source_system) =====
create table if not exists mart.fact_transactions (
  txn_id text not null,
  source_system text not null,
  txn_time timestamptz,
  amount numeric(18,2),
  currency_std text,
  account_sk bigint,
  customer_sk bigint,
  channel_sk bigint,
  merchant_sk bigint,
  txn_type text,
  status text,
  run_id uuid,
  created_at timestamptz not null default now(),
  primary key (txn_id, source_system)
);

create index if not exists ix_fact_txn_time on mart.fact_transactions(txn_time);