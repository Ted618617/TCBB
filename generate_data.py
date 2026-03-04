# generate_data.py (Postgres, Bank Raw Data Generator)

import argparse
import os
import random
import string
import uuid
from datetime import datetime, timezone, timedelta, date

import numpy as np
import pandas as pd

from db import get_engine, exec_sql


def utc_now():
    return datetime.now(timezone.utc)


def rand_str(n: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def safe_email(seed: str) -> str:
    base = "".join(ch for ch in seed.lower() if ch.isalnum()) or "user"
    return f"{base}.{rand_str(6).lower()}@example.test"


def safe_phone() -> str:
    return f"09{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(100,999)}"


def parse_date(s: str) -> date:
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


def generate_channels(run_id: str, ingestion_time: datetime) -> pd.DataFrame:
    rows = [
        ("CH_ATM", "ATM", "ATM"),
        ("CH_APP", "Mobile App", "APP"),
        ("CH_WEB", "Internet Banking", "WEB"),
        ("CH_BR",  "Branch", "BRANCH"),
        ("CH_POS", "Card POS", "POS"),
    ]
    df = pd.DataFrame(rows, columns=["channel_id", "channel_name", "channel_type"])
    df["source_system"] = "REF"
    df["ingestion_time"] = ingestion_time
    df["run_id"] = run_id
    return df


def generate_customers(n: int, run_id: str, ingestion_time: datetime, name_noise_rate: float) -> pd.DataFrame:
    first_pool = ["Wei", "Yu", "Jia", "Hao", "Yun", "Ting", "Chen", "Lin", "Wang", "Hsu"]
    last_pool = ["Chen", "Lin", "Wang", "Huang", "Chang", "Liu", "Wu", "Tsai", "Yang", "Chou"]

    customer_ids = [f"C{str(i).zfill(8)}" for i in range(1, n + 1)]
    names = []
    for _ in range(n):
        name = f"{random.choice(first_pool)} {random.choice(last_pool)}"
        if random.random() < name_noise_rate:
            name = (" " + name + "  ") if random.random() < 0.5 else (name.upper() if random.random() < 0.5 else name.lower())
        names.append(name)

    risk_levels = np.random.choice(["low", "medium", "high"], size=n, p=[0.75, 0.20, 0.05]).tolist()
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "name": names,
        "id_hash": [rand_str(18) for _ in range(n)],
        "phone": [safe_phone() for _ in range(n)],
        "email": [safe_email(nm) for nm in names],
        "risk_level": risk_levels,
        "source_system": "CORE",
        "ingestion_time": ingestion_time,
        "run_id": run_id,
    })
    return df


def generate_accounts(n: int, customers: pd.DataFrame, run_id: str, ingestion_time: datetime) -> pd.DataFrame:
    cust_ids = customers["customer_id"].tolist()
    account_ids = [f"A{str(i).zfill(10)}" for i in range(1, n + 1)]
    account_type = np.random.choice(["checking", "savings"], size=n, p=[0.55, 0.45]).tolist()
    status = np.random.choice(["active", "frozen", "closed"], size=n, p=[0.92, 0.06, 0.02]).tolist()

    base = date.today()
    open_dates = [(base - timedelta(days=random.randint(0, 365 * 5))) for _ in range(n)]

    df = pd.DataFrame({
        "account_id": account_ids,
        "customer_id": [random.choice(cust_ids) for _ in range(n)],
        "account_type": account_type,
        "open_date": open_dates,
        "status": status,
        "source_system": "CORE",
        "ingestion_time": ingestion_time,
        "run_id": run_id,
    })
    return df


def generate_merchants(n: int, run_id: str, ingestion_time: datetime) -> pd.DataFrame:
    merchant_ids = [f"M{str(i).zfill(8)}" for i in range(1, n + 1)]
    mcc_pool = ["5411", "5812", "5912", "5311", "4111", "5541", "5732", "7995", "4899"]
    df = pd.DataFrame({
        "merchant_id": merchant_ids,
        "merchant_name": [f"Merchant_{rand_str(6)}" for _ in range(n)],
        "mcc": [random.choice(mcc_pool) for _ in range(n)],
        "status": np.random.choice(["active", "inactive"], size=n, p=[0.96, 0.04]).tolist(),
        "source_system": "CARD",
        "ingestion_time": ingestion_time,
        "run_id": run_id,
    })
    return df


def random_txn_time(start: datetime, end: datetime) -> datetime:
    sec = random.randint(0, int((end - start).total_seconds()))
    return start + timedelta(seconds=sec)


def generate_transactions(
    n: int,
    customers: pd.DataFrame,
    accounts: pd.DataFrame,
    merchants: pd.DataFrame,
    channels: pd.DataFrame,
    run_id: str,
    ingestion_time: datetime,
    start_dt: datetime,
    end_dt: datetime,
    dup_rate: float,
    missing_fk_rate: float,
    invalid_currency_rate: float,
    bad_amount_rate: float,
    future_time_rate: float,
) -> pd.DataFrame:

    cust_ids = customers["customer_id"].tolist()
    acct_ids = accounts["account_id"].tolist()
    merch_ids = merchants["merchant_id"].tolist()
    chan_ids = channels["channel_id"].tolist()

    source_systems = ["ATM", "IB", "CC", "BRANCH"]
    txn_types_map = {
        "ATM": ["withdraw", "deposit"],
        "IB": ["transfer", "billpay"],
        "CC": ["purchase"],
        "BRANCH": ["cash_deposit", "counter_transfer"],
    }
    currency_good = ["TWD", "USD", "JPY", "EUR"]

    txn_ids = [f"T{str(i).zfill(12)}" for i in range(1, n + 1)]
    src = np.random.choice(source_systems, size=n, p=[0.28, 0.32, 0.30, 0.10]).tolist()
    status = np.random.choice(["success", "failed"], size=n, p=[0.965, 0.035]).tolist()

    rows = []
    for i in range(n):
        source = src[i]
        txn_type = random.choice(txn_types_map[source])

        account_id = random.choice(acct_ids)
        customer_id = random.choice(cust_ids)
        channel_id = random.choice(chan_ids)

        merchant_id = None
        if source == "CC":
            merchant_id = random.choice(merch_ids)

        currency = "TWD" if random.random() < 0.88 else random.choice(currency_good)

        if txn_type in ("withdraw", "deposit", "cash_deposit"):
            amount = round(random.uniform(100, 50000), 2)
        elif txn_type in ("transfer", "counter_transfer"):
            amount = round(random.uniform(100, 300000), 2)
        elif txn_type == "billpay":
            amount = round(random.uniform(50, 20000), 2)
        else:
            amount = round(random.uniform(20, 12000), 2)

        txn_time = random_txn_time(start_dt, end_dt)

        rows.append({
            "txn_id": txn_ids[i],
            "txn_time": txn_time,
            "amount": amount,
            "currency": currency,
            "account_id": account_id,
            "customer_id": customer_id,
            "channel_id": channel_id,
            "merchant_id": merchant_id,
            "txn_type": txn_type,
            "status": status[i],
            "source_system": source,
            "ingestion_time": ingestion_time,
            "run_id": run_id,
        })

    df = pd.DataFrame(rows)
    n_rows = len(df)

    # Dirty injections
    dup_cnt = int(n_rows * dup_rate)
    if dup_cnt > 0:
        dup_idx = np.random.choice(df.index, size=dup_cnt, replace=False)
        victim_idx = np.random.choice(df.index, size=dup_cnt, replace=True)
        df.loc[dup_idx, "txn_id"] = df.loc[victim_idx, "txn_id"].values

    fk_cnt = int(n_rows * missing_fk_rate)
    if fk_cnt > 0:
        fk_idx = np.random.choice(df.index, size=fk_cnt, replace=False)
        fk_cols = ["account_id", "customer_id", "channel_id", "merchant_id"]
        for idx in fk_idx:
            col = random.choice(fk_cols)
            if col == "merchant_id":
                df.at[idx, col] = None if random.random() < 0.5 else "M_BAD_" + rand_str(6)
            else:
                df.at[idx, col] = random.choice([None, col[:2].upper() + "_BAD_" + rand_str(6)])

    cur_cnt = int(n_rows * invalid_currency_rate)
    if cur_cnt > 0:
        cur_idx = np.random.choice(df.index, size=cur_cnt, replace=False)
        bad_currencies = ["NTD", "twd", "TWD ", "ABC", "", None]
        df.loc[cur_idx, "currency"] = np.random.choice(bad_currencies, size=cur_cnt, replace=True)

    bad_amt_cnt = int(n_rows * bad_amount_rate)
    if bad_amt_cnt > 0:
        bad_amt_idx = np.random.choice(df.index, size=bad_amt_cnt, replace=False)
        for idx in bad_amt_idx:
            df.at[idx, "amount"] = random.choice([0, -round(random.uniform(1, 1000), 2), round(random.uniform(5_000_000, 50_000_000), 2)])

    future_cnt = int(n_rows * future_time_rate)
    if future_cnt > 0:
        future_idx = np.random.choice(df.index, size=future_cnt, replace=False)
        df.loc[future_idx, "txn_time"] = utc_now() + pd.to_timedelta(np.random.randint(1, 30, size=future_cnt), unit="D")

    return df


def write_df(df: pd.DataFrame, engine, schema: str, table: str, chunksize: int = 5000):
    if df.empty:
        return
    df.to_sql(table, engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=chunksize)


def reset_raw(engine):
    exec_sql(engine, "truncate table raw.transactions, raw.customers, raw.accounts, raw.merchants, raw.channels;")


def build_args():
    p = argparse.ArgumentParser()
    p.add_argument("--n-customers", type=int, default=2000)
    p.add_argument("--n-accounts", type=int, default=2500)
    p.add_argument("--n-merchants", type=int, default=500)
    p.add_argument("--n-transactions", type=int, default=100000)

    p.add_argument("--start-date", type=str, default="2025-03-01")
    p.add_argument("--months", type=int, default=12)

    p.add_argument("--dup-rate", type=float, default=0.015)
    p.add_argument("--missing-fk-rate", type=float, default=0.010)
    p.add_argument("--invalid-currency-rate", type=float, default=0.008)
    p.add_argument("--bad-amount-rate", type=float, default=0.006)
    p.add_argument("--future-time-rate", type=float, default=0.004)
    p.add_argument("--name-noise-rate", type=float, default=0.08)

    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--reset", action="store_true")
    p.add_argument("--chunksize", type=int, default=5000)
    return p.parse_args()


def main():
    args = build_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    engine = get_engine()
    run_id = str(uuid.uuid4())
    ingestion_time = utc_now()

    start_date = parse_date(args.start_date)
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=int(args.months * 30.5))

    print("=== Bank Raw Data Generator (Postgres) ===")
    print(f"run_id: {run_id}")
    print(f"ingestion_time: {ingestion_time.isoformat()}")
    print(f"time_range: {start_dt.isoformat()} ~ {end_dt.isoformat()}")
    print(f"scale: customers={args.n_customers}, accounts={args.n_accounts}, merchants={args.n_merchants}, txns={args.n_transactions}")
    print(f"dirty: dup={args.dup_rate}, missing_fk={args.missing_fk_rate}, invalid_cur={args.invalid_currency_rate}, bad_amt={args.bad_amount_rate}, future_time={args.future_time_rate}")

    if args.reset:
        print("[reset] truncating raw tables...")
        reset_raw(engine)

    channels_df = generate_channels(run_id, ingestion_time)
    customers_df = generate_customers(args.n_customers, run_id, ingestion_time, args.name_noise_rate)
    accounts_df = generate_accounts(args.n_accounts, customers_df, run_id, ingestion_time)
    merchants_df = generate_merchants(args.n_merchants, run_id, ingestion_time)

    txns_df = generate_transactions(
        args.n_transactions,
        customers_df,
        accounts_df,
        merchants_df,
        channels_df,
        run_id,
        ingestion_time,
        start_dt,
        end_dt,
        args.dup_rate,
        args.missing_fk_rate,
        args.invalid_currency_rate,
        args.bad_amount_rate,
        args.future_time_rate,
    )

    print("Writing raw.channels ...")
    write_df(channels_df, engine, "raw", "channels", chunksize=args.chunksize)

    print("Writing raw.customers ...")
    write_df(customers_df, engine, "raw", "customers", chunksize=args.chunksize)

    print("Writing raw.accounts ...")
    write_df(accounts_df, engine, "raw", "accounts", chunksize=args.chunksize)

    print("Writing raw.merchants ...")
    write_df(merchants_df, engine, "raw", "merchants", chunksize=args.chunksize)

    print("Writing raw.transactions ...")
    write_df(txns_df, engine, "raw", "transactions", chunksize=args.chunksize)

    print("=== Done ===")
    print(f"Inserted rows: channels={len(channels_df)}, customers={len(customers_df)}, accounts={len(accounts_df)}, merchants={len(merchants_df)}, transactions={len(txns_df)}")
    print("Tip: keep this run_id for ETL/DQ demo.")


if __name__ == "__main__":
    main()