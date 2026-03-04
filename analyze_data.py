# analyze_data.py
# Bank Data Pipeline (PostgreSQL) - Full ETL + Incremental + DQ + Quarantine + Run Log + Mart Upsert
#
# Reads:
#   raw.transactions (+ raw.accounts/raw.channels/raw.merchants/raw.customers for FK validation & dims)
# Writes:
#   stg.transactions
#   meta.etl_run_log / meta.etl_state / meta.dq_results / meta.quarantine_transactions
#   mart.dim_* / mart.fact_transactions
#
# Usage:
#   python analyze_data.py
#   python analyze_data.py --pipeline transactions
#   python analyze_data.py --mode full --reset-state
#   python analyze_data.py --run-id 11111111-2222-3333-4444-555555555555  (re-run same batch idempotently)
#
# Env:
#   DATABASE_URL=postgresql+psycopg2://bank:bankpass@localhost:5432/bank_demo

from __future__ import annotations

#(1) 讀 last watermark（meta.etl_state）

import argparse
import os
import uuid
import numpy as np
import pandas as pd

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from db import get_engine, exec_sql


# -----------------------------
# DB helpers
# -----------------------------
load_dotenv()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_engine():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set. Example: postgresql+psycopg2://bank:bankpass@localhost:5432/bank_demo")
    return create_engine(url, pool_pre_ping=True)


def exec_sql(engine, sql: str, params: Optional[dict] = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def read_sql_df(engine, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), engine, params=params or {})


def write_df(df: pd.DataFrame, engine, schema: str, table: str, chunksize: int = 5000) -> None:
    if df is None or df.empty:
        return
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=chunksize,
    )


# -----------------------------
# Config
# -----------------------------
ALLOWED_CURRENCY = {"TWD", "USD", "JPY", "EUR"}
AMOUNT_MAX = 5_000_000  # demo upper bound


@dataclass
class RunContext:
    run_id: str
    pipeline_name: str
    start_time: datetime
    watermark_start: Optional[datetime] = None
    watermark_end: Optional[datetime] = None


# -----------------------------
# ETL steps
# -----------------------------
def get_last_watermark(engine, pipeline_name: str) -> Optional[datetime]:
    df = read_sql_df(
        engine,
        "select last_watermark from meta.etl_state where pipeline_name = :p",
        {"p": pipeline_name},
    )
    if df.empty:
        return None
    return df["last_watermark"].iloc[0]


def set_watermark(engine, pipeline_name: str, watermark: Optional[datetime]) -> None:
    exec_sql(
        engine,
        """
        insert into meta.etl_state(pipeline_name, last_watermark)
        values (:p, :wm)
        on conflict (pipeline_name) do update
        set last_watermark = excluded.last_watermark,
            updated_at = now()
        """,
        {"p": pipeline_name, "wm": watermark},
    )


def init_run_log(engine, ctx: RunContext) -> None:
    # Idempotent: if same run_id is reused, overwrite into "running" state
    exec_sql(
        engine,
        """
        insert into meta.etl_run_log(run_id, pipeline_name, start_time, status)
        values (:run_id, :pipeline, :start_time, 'running')
        on conflict (run_id) do update
        set pipeline_name = excluded.pipeline_name,
            start_time = excluded.start_time,
            status = 'running',
            end_time = null,
            watermark_start = null,
            watermark_end = null,
            rows_raw = 0,
            rows_stg = 0,
            rows_mart = 0,
            rows_quarantine = 0,
            error_message = null
        """,
        {"run_id": ctx.run_id, "pipeline": ctx.pipeline_name, "start_time": ctx.start_time},
    )


def finalize_run_log_success(engine, ctx: RunContext, rows_raw: int, rows_stg: int, rows_quarantine: int, rows_mart: int) -> None:
    exec_sql(
        engine,
        """
        update meta.etl_run_log
        set end_time = :end_time,
            status = 'success',
            watermark_start = :wm_start,
            watermark_end = :wm_end,
            rows_raw = :rows_raw,
            rows_stg = :rows_stg,
            rows_quarantine = :rows_quarantine,
            rows_mart = :rows_mart
        where run_id = :run_id
        """,
        {
            "end_time": utc_now(),
            "wm_start": ctx.watermark_start,
            "wm_end": ctx.watermark_end,
            "rows_raw": rows_raw,
            "rows_stg": rows_stg,
            "rows_quarantine": rows_quarantine,
            "rows_mart": rows_mart,
            "run_id": ctx.run_id,
        },
    )


def finalize_run_log_failed(engine, ctx: RunContext, err: str) -> None:
    exec_sql(
        engine,
        """
        update meta.etl_run_log
        set end_time = :end_time,
            status = 'failed',
            error_message = :err
        where run_id = :run_id
        """,
        {"end_time": utc_now(), "err": err[:4000], "run_id": ctx.run_id},
    )


def cleanup_for_rerun(engine, ctx: RunContext) -> None:
    # If user reuses same run_id, remove prior artifacts for that run_id (stg/quarantine/dq_results)
    exec_sql(engine, "delete from stg.transactions where run_id = :rid", {"rid": ctx.run_id})
    exec_sql(engine, "delete from meta.quarantine_transactions where run_id = :rid", {"rid": ctx.run_id})
    exec_sql(engine, "delete from meta.dq_results where run_id = :rid", {"rid": ctx.run_id})
    # fact is upserted by pk; optional to remove rows tagged by run_id for cleaner rerun metrics
    exec_sql(engine, "delete from mart.fact_transactions where run_id = :rid", {"rid": ctx.run_id})


def extract_raw_transactions(engine, ctx: RunContext, mode: str) -> pd.DataFrame:
    base_sql = """
    select
      txn_id, txn_time, amount, currency, account_id, customer_id, channel_id,
      merchant_id, txn_type, status, source_system, ingestion_time, run_id
    from raw.transactions
    """
    params: Dict[str, object] = {}
    where = ""

    if mode == "incremental" and ctx.watermark_start is not None:
        where = " where txn_time > :wm"
        params["wm"] = ctx.watermark_start

    df = read_sql_df(engine, base_sql + where, params)
    return df


def standardize_to_staging(raw_txn: pd.DataFrame, ctx: RunContext) -> pd.DataFrame:
    if raw_txn.empty:
        return pd.DataFrame()

    df = raw_txn.copy()

    # Currency standardization
    df["currency_std"] = df["currency"].astype("string").str.strip().str.upper()
    df["currency_std"] = df["currency_std"].replace({"NTD": "TWD"})

    # Ensure txn_time as datetime (pandas should parse from timestamptz already)
    # Mark duplicate-suspect by (account_id, txn_time, amount)
    df["is_duplicate_suspect"] = df.duplicated(subset=["account_id", "txn_time", "amount"], keep=False).fillna(False)

    # Staging payload
    stg = pd.DataFrame({
        "run_id": ctx.run_id,
        "txn_id": df["txn_id"],
        "txn_time": df["txn_time"],
        "amount": df["amount"],
        "currency_std": df["currency_std"],
        "account_id": df["account_id"],
        "customer_id": df["customer_id"],
        "channel_id": df["channel_id"],
        "merchant_id": df["merchant_id"],
        "txn_type": df["txn_type"],
        "status": df["status"],
        "source_system": df["source_system"],
        "is_duplicate_suspect": df["is_duplicate_suspect"].astype(bool),
    })

    return stg


def dq_evaluate(
    stg_txn: pd.DataFrame,
    valid_accounts: set,
    valid_channels: set,
    valid_merchants: set,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - dq_results_df: per-rule summary (for meta.dq_results)
      - quarantine_df: one row per failed transaction (first-fail priority) for meta.quarantine_transactions

    Notes:
      - We compute rule fail counts independently (good for reporting)
      - Quarantine stores ONLY the first failing rule per row (good for "one row -> one reason")
    """
    if stg_txn.empty:
        dq_results = pd.DataFrame(columns=["rule_id", "rule_name", "total_cnt", "fail_cnt", "fail_ratio"])
        quarantine = pd.DataFrame()
        return dq_results, quarantine

    total = len(stg_txn)

    # Helpers
    now = utc_now()

    # Rule conditions (True means PASS)
    rules: List[Tuple[str, str, pd.Series]] = []

    # R001 required fields
    req_cols = ["txn_id", "txn_time", "amount", "account_id", "channel_id", "source_system"]
    r001 = stg_txn[req_cols].notna().all(axis=1)
    rules.append(("R001", "Required fields not null", r001))

    # R002 amount range
    r002 = stg_txn["amount"].notna() & (stg_txn["amount"] > 0) & (stg_txn["amount"] < AMOUNT_MAX)
    rules.append(("R002", "Amount within valid range", r002))

    # R003 currency allowlist
    r003 = stg_txn["currency_std"].notna() & stg_txn["currency_std"].isin(list(ALLOWED_CURRENCY))
    rules.append(("R003", "Currency in allowlist", r003))

    # R004 txn_time not in future
    r004 = stg_txn["txn_time"].notna() & (stg_txn["txn_time"] <= now)
    rules.append(("R004", "Transaction time not in future", r004))

    # R005 account_id exists
    r005 = stg_txn["account_id"].notna() & stg_txn["account_id"].isin(list(valid_accounts))
    rules.append(("R005", "Account FK exists", r005))

    # R006 channel_id exists
    r006 = stg_txn["channel_id"].notna() & stg_txn["channel_id"].isin(list(valid_channels))
    rules.append(("R006", "Channel FK exists", r006))

    # R007 merchant required for card purchases (simple heuristic)
    # If source_system == 'CC' OR txn_type == 'purchase', then merchant_id must exist in merchants.
    needs_merchant = (stg_txn["source_system"] == "CC") | (stg_txn["txn_type"] == "purchase")
    has_merchant_ok = stg_txn["merchant_id"].notna() & stg_txn["merchant_id"].isin(list(valid_merchants))
    r007 = (~needs_merchant) | has_merchant_ok
    rules.append(("R007", "Merchant FK exists for card purchase", r007))

    # R008 duplicate txn_id+source_system within this batch should be flagged (fail)
    # This is "data correctness" rule often asked in interviews.
    key = stg_txn[["txn_id", "source_system"]].astype("string")
    dup_mask = key.duplicated(keep=False).fillna(False)
    r008 = ~dup_mask
    rules.append(("R008", "No duplicate txn_id+source in batch", r008))

    # Build dq_results
    dq_rows = []
    for rule_id, rule_name, pass_mask in rules:
        fail_cnt = int((~pass_mask).sum())
        dq_rows.append({
            "rule_id": rule_id,
            "rule_name": rule_name,
            "total_cnt": total,
            "fail_cnt": fail_cnt,
            "fail_ratio": round(fail_cnt / total, 6) if total > 0 else 0,
        })
    dq_results = pd.DataFrame(dq_rows)

    # Quarantine: first-fail priority (order of rules matters)
    # Create an array of first failing rule_id and reason
    fail_rule_id = np.array([None] * total, dtype=object)
    fail_reason = np.array([None] * total, dtype=object)

    for rule_id, rule_name, pass_mask in rules:
        fail_here = (~pass_mask).to_numpy()
        # only fill where still None
        take = fail_here & (fail_rule_id == None)
        fail_rule_id[take] = rule_id
        fail_reason[take] = rule_name

    # Rows that failed any rule
    failed_any = fail_rule_id != None
    quarantine = stg_txn.loc[failed_any].copy()
    quarantine.insert(0, "rule_id", fail_rule_id[failed_any])
    quarantine.insert(1, "fail_reason", fail_reason[failed_any])

    return dq_results, quarantine


def upsert_dims(engine, ctx: RunContext, stg_txn: pd.DataFrame) -> None:
    """
    Minimal viable dim upsert:
    - dim_customer from raw.customers (only those customer_ids in this batch)
    - dim_account  from raw.accounts  (only those account_ids in this batch)
    - dim_channel  from raw.channels  (only those channel_ids in this batch; small)
    - dim_merchant from raw.merchants (only those merchant_ids in this batch)
    Note:
      raw.* may contain multiple versions per key across runs (different ingestion_time),
      so we must de-duplicate within the INSERT statement to avoid Postgres ON CONFLICT
      cardinality violations.
    """
    if stg_txn.empty:
        return

    cust_ids = stg_txn["customer_id"].dropna().astype(str).unique().tolist()
    acct_ids = stg_txn["account_id"].dropna().astype(str).unique().tolist()
    chan_ids = stg_txn["channel_id"].dropna().astype(str).unique().tolist()
    merch_ids = stg_txn["merchant_id"].dropna().astype(str).unique().tolist()

    def arr_param(values: List[str]) -> Dict[str, object]:
        return {"ids": values}

    # dim_channel
    if chan_ids:
        exec_sql(
            engine,
            """
            insert into mart.dim_channel(channel_id, channel_name, channel_type, updated_at)
            select x.channel_id, x.channel_name, x.channel_type, now()
            from (
              select distinct on (c.channel_id)
                c.channel_id, c.channel_name, c.channel_type, c.ingestion_time
              from raw.channels c
              where c.channel_id = any(:ids)
              order by c.channel_id, c.ingestion_time desc
            ) x
            on conflict (channel_id) do update
            set channel_name = excluded.channel_name,
                channel_type = excluded.channel_type,
                updated_at = now()
            """,
            arr_param(chan_ids),
        )

    # dim_customer (distinct on customer_id)
    if cust_ids:
        exec_sql(
            engine,
            r"""
            insert into mart.dim_customer(customer_id, name_std, risk_level, updated_at)
            select x.customer_id,
                   trim(regexp_replace(coalesce(x.name,''), '\s+', ' ', 'g')) as name_std,
                   x.risk_level,
                   now()
            from (
              select distinct on (c.customer_id)
                c.customer_id, c.name, c.risk_level, c.ingestion_time
              from raw.customers c
              where c.customer_id = any(:ids)
              order by c.customer_id, c.ingestion_time desc
            ) x
            on conflict (customer_id) do update
            set name_std = excluded.name_std,
                risk_level = excluded.risk_level,
                updated_at = now()
            """,
            arr_param(cust_ids),
        )

    # dim_account (distinct on account_id)
    if acct_ids:
        exec_sql(
            engine,
            """
            insert into mart.dim_account(account_id, customer_id, account_type, status, updated_at)
            select x.account_id, x.customer_id, x.account_type, x.status, now()
            from (
              select distinct on (a.account_id)
                a.account_id, a.customer_id, a.account_type, a.status, a.ingestion_time
              from raw.accounts a
              where a.account_id = any(:ids)
              order by a.account_id, a.ingestion_time desc
            ) x
            on conflict (account_id) do update
            set customer_id = excluded.customer_id,
                account_type = excluded.account_type,
                status = excluded.status,
                updated_at = now()
            """,
            arr_param(acct_ids),
        )

    # dim_merchant (distinct on merchant_id)
    if merch_ids:
        exec_sql(
            engine,
            """
            insert into mart.dim_merchant(merchant_id, merchant_name, mcc, status, updated_at)
            select x.merchant_id, x.merchant_name, x.mcc, x.status, now()
            from (
              select distinct on (m.merchant_id)
                m.merchant_id, m.merchant_name, m.mcc, m.status, m.ingestion_time
              from raw.merchants m
              where m.merchant_id = any(:ids)
              order by m.merchant_id, m.ingestion_time desc
            ) x
            on conflict (merchant_id) do update
            set merchant_name = excluded.merchant_name,
                mcc = excluded.mcc,
                status = excluded.status,
                updated_at = now()
            """,
            arr_param(merch_ids),
        )
        

def load_fact(engine, ctx: RunContext) -> int:
    sql = """
    with q as (
      select distinct txn_id, source_system
      from meta.quarantine_transactions
      where run_id = cast(:rid as uuid)
    ),
    s as (
      select *
      from stg.transactions
      where run_id = cast(:rid as uuid)
    ),
    pass_rows as (
      select s.*
      from s
      left join q on q.txn_id = s.txn_id and q.source_system = s.source_system
      where q.txn_id is null
    )
    insert into mart.fact_transactions(
      txn_id, source_system, txn_time, amount, currency_std,
      account_sk, customer_sk, channel_sk, merchant_sk,
      txn_type, status, run_id
    )
    select
      p.txn_id,
      p.source_system,
      p.txn_time,
      p.amount,
      p.currency_std,
      da.account_sk,
      dc.customer_sk,
      dch.channel_sk,
      dm.merchant_sk,
      p.txn_type,
      p.status,
      cast(:rid as uuid)
    from pass_rows p
    left join mart.dim_account  da  on da.account_id = p.account_id
    left join mart.dim_customer dc  on dc.customer_id = p.customer_id
    left join mart.dim_channel  dch on dch.channel_id = p.channel_id
    left join mart.dim_merchant dm  on dm.merchant_id = p.merchant_id
    on conflict (txn_id, source_system)
    do update set
      txn_time = excluded.txn_time,
      amount = excluded.amount,
      currency_std = excluded.currency_std,
      account_sk = excluded.account_sk,
      customer_sk = excluded.customer_sk,
      channel_sk = excluded.channel_sk,
      merchant_sk = excluded.merchant_sk,
      txn_type = excluded.txn_type,
      status = excluded.status,
      run_id = excluded.run_id,
      created_at = now()
    returning 1;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"rid": ctx.run_id}).fetchall()
    return len(rows)


def reset_state(engine, pipeline_name: str) -> None:
    exec_sql(engine, "delete from meta.etl_state where pipeline_name = :p", {"p": pipeline_name})


# -----------------------------
# Main
# -----------------------------
def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bank ETL Pipeline (Postgres): incremental + DQ + quarantine + mart upsert")
    p.add_argument("--pipeline", type=str, default="transactions")
    p.add_argument("--mode", type=str, choices=["incremental", "full"], default="incremental")
    p.add_argument("--run-id", type=str, default="", help="Reuse a run_id for idempotent rerun. Default: auto-generate.")
    p.add_argument("--reset-state", action="store_true", help="Reset meta.etl_state watermark for this pipeline (start from scratch).")
    p.add_argument("--chunksize", type=int, default=5000)
    return p.parse_args()


def main() -> None:
    args = build_args()
    engine = get_engine()

    pipeline_name = args.pipeline
    run_id = args.run_id.strip() if args.run_id.strip() else str(uuid.uuid4())
    ctx = RunContext(
        run_id=run_id,
        pipeline_name=pipeline_name,
        start_time=utc_now(),
    )

    if args.reset_state:
        print(f"[reset-state] Removing watermark for pipeline={pipeline_name}")
        reset_state(engine, pipeline_name)

    # Watermark start
    if args.mode == "incremental":
        ctx.watermark_start = get_last_watermark(engine, pipeline_name)
    else:
        ctx.watermark_start = None

    print("=== Bank ETL (Postgres) ===")
    print(f"pipeline: {pipeline_name}")
    print(f"mode: {args.mode}")
    print(f"run_id: {ctx.run_id}")
    print(f"watermark_start: {ctx.watermark_start}")

    init_run_log(engine, ctx)
    cleanup_for_rerun(engine, ctx)

    rows_raw = rows_stg = rows_quarantine = rows_mart = 0

    try:
        # 1) Extract raw
        raw_txn = extract_raw_transactions(engine, ctx, args.mode)
        rows_raw = int(len(raw_txn))
        if rows_raw == 0:
            print("No new raw transactions to process.")
            # still finalize run log success with zero counts
            ctx.watermark_end = ctx.watermark_start
            finalize_run_log_success(engine, ctx, 0, 0, 0, 0)
            return

        # watermark end
        ctx.watermark_end = raw_txn["txn_time"].max()
        print(f"raw rows: {rows_raw}, watermark_end: {ctx.watermark_end}")

        # 2) Standardize to staging
        stg_txn = standardize_to_staging(raw_txn, ctx)
        rows_stg = int(len(stg_txn))
        write_df(stg_txn, engine, "stg", "transactions", chunksize=args.chunksize)
        print(f"stg rows inserted: {rows_stg}")

        # 3) Prepare FK sets (from raw master data)
        #    (In real world: use clean master tables; here raw.* acts as reference)
        valid_accounts = set(read_sql_df(engine, "select distinct account_id from raw.accounts where account_id is not null")["account_id"].astype(str).tolist())
        valid_channels = set(read_sql_df(engine, "select distinct channel_id from raw.channels where channel_id is not null")["channel_id"].astype(str).tolist())
        valid_merchants = set(read_sql_df(engine, "select distinct merchant_id from raw.merchants where merchant_id is not null")["merchant_id"].astype(str).tolist())

        # 4) DQ evaluate -> dq_results + quarantine (first-fail)
        dq_results_df, quarantine_df = dq_evaluate(stg_txn, valid_accounts, valid_channels, valid_merchants)

        # Write dq_results
        if not dq_results_df.empty:
            dq_results_df = dq_results_df.copy()
            if "run_id" in dq_results_df.columns:
                dq_results_df = dq_results_df.drop(columns=["run_id"])
            dq_results_df.insert(0, "run_id", ctx.run_id)
            dq_results_df = dq_results_df[["run_id", "rule_id", "rule_name", "total_cnt", "fail_cnt", "fail_ratio"]]
            write_df(dq_results_df, engine, "meta", "dq_results", chunksize=1000)

        # Write quarantine rows
        if quarantine_df is not None and not quarantine_df.empty:
            q = quarantine_df.copy()
            if "run_id" in q.columns:
                q = q.drop(columns=["run_id"])
            q.insert(0, "run_id", ctx.run_id)
            # Map staging column names to quarantine schema
            # quarantine schema expects txn_id/txn_time/amount/currency/account_id/customer_id/channel_id/merchant_id/txn_type/status/source_system/ingestion_time
            # We don't have ingestion_time in staging; set null (or re-join raw if you want).
            q["currency"] = q["currency_std"]
            q["ingestion_time"] = pd.NaT

            q = q[[
                "run_id", "rule_id", "fail_reason",
                "txn_id", "txn_time", "amount", "currency",
                "account_id", "customer_id", "channel_id", "merchant_id",
                "txn_type", "status", "source_system",
                "ingestion_time"
            ]]
            write_df(q, engine, "meta", "quarantine_transactions", chunksize=2000)
            rows_quarantine = int(len(q))

        print(f"dq quarantine rows: {rows_quarantine}")

        # 5) Upsert dims based on batch keys
        upsert_dims(engine, ctx, stg_txn)

        # 6) Load fact (exclude quarantined)
        rows_mart = load_fact(engine, ctx)
        print(f"mart fact rows upserted: {rows_mart}")

        # 7) Persist watermark (incremental mode)
        if args.mode == "incremental":
            set_watermark(engine, pipeline_name, ctx.watermark_end)

        # 8) finalize run log success
        finalize_run_log_success(engine, ctx, rows_raw, rows_stg, rows_quarantine, rows_mart)

        print("=== SUCCESS ===")
        print(f"run_id={ctx.run_id}")
        print(f"rows_raw={rows_raw}, rows_stg={rows_stg}, rows_quarantine={rows_quarantine}, rows_mart={rows_mart}")
        print(f"watermark_start={ctx.watermark_start}, watermark_end={ctx.watermark_end}")

    except Exception as e:
        err = repr(e)
        finalize_run_log_failed(engine, ctx, err)
        print("=== FAILED ===")
        print(err)
        raise


if __name__ == "__main__":
    main()