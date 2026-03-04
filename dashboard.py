# dashboard.py
# Streamlit Dashboard for Bank Data Pipeline (Postgres)
# Pages:
# 1) Run Monitor (etl_run_log + watermark)
# 2) Data Quality (dq_results + quarantine)
# 3) Business KPI (fact_transactions)
#
# Usage:
#   streamlit run dashboard.py
#
# Env:
#   DATABASE_URL=postgresql+psycopg2://bank:bankpass@localhost:5432/bank_demo

from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# -----------------------------
# DB helpers
# -----------------------------
@st.cache_resource
def get_engine():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set. Put it in .env or env var.")
    return create_engine(url, pool_pre_ping=True)


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(text(sql), engine, params=params or {})


# -----------------------------
# UI helpers
# -----------------------------
def fmt_ts(ts):
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or pd.isna(ts):
        return ""
    if isinstance(ts, str):
        return ts
    return ts.strftime("%Y-%m-%d %H:%M:%S%z")


def pct(x):
    if x is None or pd.isna(x):
        return None
    return float(x)


# -----------------------------
# Queries
# -----------------------------
SQL_RUN_LOG = """
select
  run_id::text,
  pipeline_name,
  start_time,
  end_time,
  status,
  rows_raw,
  rows_stg,
  rows_quarantine,
  rows_mart,
  watermark_start,
  watermark_end,
  error_message
from meta.etl_run_log
order by start_time desc
limit :limit
"""

SQL_ETL_STATE = """
select pipeline_name, last_watermark, updated_at
from meta.etl_state
order by pipeline_name
"""

SQL_DQ_RESULTS_BY_RUN = """
select
  rule_id,
  rule_name,
  total_cnt,
  fail_cnt,
  round((fail_ratio * 100.0)::numeric, 2) as fail_pct
from meta.dq_results
where run_id = cast(:run_id as uuid)
order by fail_cnt desc, rule_id
"""

SQL_QUARANTINE_REASON_BY_RUN = """
select
  rule_id,
  fail_reason,
  count(*) as cnt
from meta.quarantine_transactions
where run_id = cast(:run_id as uuid)
group by rule_id, fail_reason
order by cnt desc
"""

SQL_QUARANTINE_SAMPLE_BY_RUN = """
select
  rule_id,
  fail_reason,
  txn_id,
  txn_time,
  amount,
  currency,
  account_id,
  customer_id,
  channel_id,
  merchant_id,
  txn_type,
  status,
  source_system
from meta.quarantine_transactions
where run_id = cast(:run_id as uuid)
order by created_at desc
limit :limit
"""

SQL_FACT_KPI = """
select
  count(*) as txn_cnt,
  sum(amount) as total_amount,
  round(avg(case when status='failed' then 1 else 0 end) * 100.0, 2) as fail_rate_pct,
  min(txn_time) as min_txn_time,
  max(txn_time) as max_txn_time
from mart.fact_transactions
"""

SQL_FACT_BY_SOURCE = """
select
  source_system,
  count(*) as cnt,
  round(avg(case when status='failed' then 1 else 0 end) * 100.0, 2) as fail_rate_pct
from mart.fact_transactions
group by source_system
order by cnt desc
"""

SQL_FACT_DAILY_TREND = """
select
  date_trunc('day', txn_time) as day,
  count(*) as txn_cnt,
  sum(amount) as total_amount,
  round(avg(case when status='failed' then 1 else 0 end) * 100.0, 2) as fail_rate_pct
from mart.fact_transactions
where txn_time is not null
group by 1
order by 1
"""

SQL_FACT_TYPE_TOP = """
select
  txn_type,
  count(*) as cnt,
  round(avg(case when status='failed' then 1 else 0 end) * 100.0, 2) as fail_rate_pct
from mart.fact_transactions
group by txn_type
order by cnt desc
limit 12
"""


# -----------------------------
# Pages
# -----------------------------
def page_run_monitor():
    st.subheader("ETL Run Monitor")
    col1, col2 = st.columns([2, 1])

    with col2:
        limit = st.number_input("Show last N runs", min_value=5, max_value=200, value=30, step=5)

    run_log = read_df(SQL_RUN_LOG, {"limit": int(limit)})
    if run_log.empty:
        st.info("No runs yet.")
        return

    # --- add derived metrics ---
    run_log = run_log.copy()
    run_log["duration_seconds"] = (
        pd.to_datetime(run_log["end_time"]) - pd.to_datetime(run_log["start_time"])
    ).dt.total_seconds()
    run_log["duration_seconds"] = pd.to_numeric(run_log["duration_seconds"], errors="coerce")

    # ensure numeric
    run_log["rows_raw"] = pd.to_numeric(run_log["rows_raw"], errors="coerce").fillna(0)
    run_log["rows_mart"] = pd.to_numeric(run_log["rows_mart"], errors="coerce").fillna(0)

    # pass_rate (%) = rows_mart / rows_raw
    run_log["pass_rate_pct"] = np.divide(
        run_log["rows_mart"].to_numpy(),
        run_log["rows_raw"].to_numpy(),
        out=np.full(len(run_log), np.nan, dtype=float),
        where=(run_log["rows_raw"].to_numpy() != 0),
    ) * 100.0


    # Latest run (prefer latest success with data)        
    preferred = run_log[(run_log["status"] == "success") & (run_log["rows_raw"] > 0)].sort_values("rows_raw", ascending=False).head(1)
    latest = preferred.iloc[0] if not preferred.empty else run_log.iloc[0]
    
    latest_pass_rate = float(latest["pass_rate_pct"]) if pd.notna(latest["pass_rate_pct"]) else 0.0
    latest_duration = float(latest["duration_seconds"]) if pd.notna(latest["duration_seconds"]) else float("nan")

    # Metrics from latest run (6 cards)
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Latest Status", str(latest["status"]))
    m2.metric("rows_raw", int(latest["rows_raw"] or 0))
    m3.metric("rows_quarantine", int(latest["rows_quarantine"] or 0))
    m4.metric("rows_mart", int(latest["rows_mart"] or 0))
    m5.metric("pass_rate", f"{latest_pass_rate:.2f}%")

    # duration with '-' for NaN
    if pd.isna(latest_duration):
        m6.metric("duration", "-")
    else:
        m6.metric("duration", f"{latest_duration:.1f}s")

    # Watermark state
    with col1:
        st.caption("Watermark state")
        state = read_df(SQL_ETL_STATE)
        if not state.empty:
            state["last_watermark"] = state["last_watermark"].apply(fmt_ts)
            state["updated_at"] = state["updated_at"].apply(fmt_ts)
            st.dataframe(state, use_container_width=True)
        else:
            st.write("(no etl_state yet)")

    # Run log table
    st.caption("Run log (latest first)")
    show_cols = [
        "start_time", "end_time", "status",
        "rows_raw", "rows_stg", "rows_quarantine", "rows_mart",
        "pass_rate_pct", "duration_seconds",
        "watermark_start", "watermark_end", "run_id"
    ]
    table = run_log[show_cols].copy()
    for c in ["start_time", "end_time", "watermark_start", "watermark_end"]:
        table[c] = table[c].apply(fmt_ts)

    table["pass_rate_pct"] = table["pass_rate_pct"].map(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
    table["duration_seconds"] = table["duration_seconds"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "")

    st.dataframe(table, use_container_width=True)

    # Trend chart
    st.caption("Trend")
    trend = run_log.copy()
    trend = trend.sort_values("start_time")
    trend["start_time"] = pd.to_datetime(trend["start_time"])
    trend = trend.set_index("start_time")

    # rows chart
    st.line_chart(trend[["rows_raw", "rows_quarantine", "rows_mart"]].fillna(0))

    # pass rate chart (optional but nice)
    st.line_chart(trend[["pass_rate_pct"]].fillna(0))
    
    # --- Focused trend: last 10 runs (avoid scale distortion) ---
    st.caption("Trend (Last 10 runs)")
    last10 = run_log.head(10).copy()          # run_log 已經是 start_time desc
    last10 = last10.sort_values("start_time") # 轉成時間由舊到新
    last10["start_time"] = pd.to_datetime(last10["start_time"])
    last10 = last10.set_index("start_time")

    if len(run_log) > 10:        
        st.caption("Trend (Last 10 runs)")
        last10 = run_log.head(10).copy()          # run_log 已是 start_time desc
        last10 = last10.sort_values("start_time") # 舊→新
        last10["start_time"] = pd.to_datetime(last10["start_time"])
        last10 = last10.set_index("start_time")
        st.line_chart(last10[["rows_raw", "rows_quarantine", "rows_mart"]].fillna(0))
        st.line_chart(last10[["pass_rate_pct"]].fillna(0))
        
    st.line_chart(last10[["pass_rate_pct"]].fillna(0))


def page_data_quality():
    st.subheader("Data Quality (DQ)")
    
    # pick run_id
    runs_all = read_df(
        "select run_id::text, start_time, status, rows_raw "
        "from meta.etl_run_log "
        "order by start_time desc limit 200"
    )

    if runs_all.empty:
        st.info("No runs yet.")
        return

    show_failed = st.toggle("Show failed runs", value=False)

    runs = runs_all.copy()
    if not show_failed:
        runs = runs[(runs["status"] == "success") & (runs["rows_raw"].fillna(0) > 0)]
        # 排序更新
        runs = runs.sort_values(["rows_raw", "start_time"], ascending=[False, False])

    if runs.empty:
        st.info("No runs to show (toggle on 'Show failed runs' to include failed runs).")
        return

    runs["label"] = runs.apply(
        lambda r: f"{fmt_ts(r['start_time'])} | {r['status']} | rows_raw={int(r['rows_raw'] or 0)} | {r['run_id']}",
        axis=1
    )

    selected = st.selectbox("Select a run", runs["label"].tolist(), index=0)
    run_id = selected.split("|")[-1].strip()
    
    
    # DQ results
    dq = read_df(SQL_DQ_RESULTS_BY_RUN, {"run_id": run_id})
    if dq.empty:
        # check run summary (rows_raw) to explain why DQ is empty
        run_summary = read_df(
            "select rows_raw, status from meta.etl_run_log where run_id = cast(:run_id as uuid) limit 1",
            {"run_id": run_id},
        )
        if not run_summary.empty and int(run_summary.iloc[0]["rows_raw"] or 0) == 0:
            st.info("No new data in this run (skipped DQ).")
        else:
            st.warning("No dq_results for this run.")
        # (optional) still show quarantine sample as empty, then return
        # return # 暫時先不 return，讓 UI 一致。
    else:
        c1, c2 = st.columns([1, 1])
        with c1:
            st.caption("DQ results (by rule)")
            st.dataframe(dq, use_container_width=True)

        with c2:
            st.caption("Fail count by rule")
            chart = dq[["rule_id", "fail_cnt"]].set_index("rule_id")
            st.bar_chart(chart)

        st.caption("Fail % by rule")
        chart2 = dq[["rule_id", "fail_pct"]].set_index("rule_id")
        st.bar_chart(chart2)

    # Quarantine reason distribution
    qr = read_df(SQL_QUARANTINE_REASON_BY_RUN, {"run_id": run_id})
    if not qr.empty:
        st.caption("Quarantine reasons (grouped)")
        st.dataframe(qr, use_container_width=True)

    # Quarantine sample
    st.caption("Quarantine sample rows")
    limit = st.number_input("Sample size", min_value=10, max_value=500, value=50, step=10)
    sample = read_df(SQL_QUARANTINE_SAMPLE_BY_RUN, {"run_id": run_id, "limit": int(limit)})
    if sample.empty:
        st.write("(no quarantine rows)")
    else:
        sample = sample.copy()
        sample["txn_time"] = sample["txn_time"].apply(fmt_ts)
        st.dataframe(sample, use_container_width=True)


def page_business_kpi():
    st.subheader("Business KPI (Mart Fact)")

    kpi = read_df(SQL_FACT_KPI)
    if kpi.empty:
        st.info("No fact data yet.")
        return

    k = kpi.iloc[0]
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Transactions", int(k["txn_cnt"] or 0))
    m2.metric("Total Amount", f"{float(k['total_amount'] or 0):,.2f}")
    m3.metric("Fail Rate %", f"{float(k['fail_rate_pct'] or 0):.2f}%")
    # ORI m4.metric("Date Range Min", f"{fmt_ts(k['min_txn_time'])}")
    # ORI m5.metric("Date Range Max", f"{fmt_ts(k['max_txn_time'])}")
    
    min_ts = fmt_ts(k["min_txn_time"])
    max_ts = fmt_ts(k["max_txn_time"])
    # 只顯示日期（不會被截斷）
    m4.metric("Date Range Min", f"{min_ts[5:19]}")
    m5.metric("Date Range Max", f"{max_ts[5:19]}")
    # 完整時間放下面（不截、也更清楚）
    st.caption(f"Date Range (full): {min_ts}  →  {max_ts}")

    st.caption("By source_system")
    by_src = read_df(SQL_FACT_BY_SOURCE)
    if not by_src.empty:
        st.dataframe(by_src, use_container_width=True)
        st.bar_chart(by_src.set_index("source_system")[["cnt"]])

    st.caption("Daily trend")
    trend = read_df(SQL_FACT_DAILY_TREND)
    if not trend.empty:
        trend = trend.copy()
        trend["day"] = pd.to_datetime(trend["day"])
        trend = trend.set_index("day")
        st.line_chart(trend[["txn_cnt", "total_amount"]].fillna(0))
        st.line_chart(trend[["fail_rate_pct"]].fillna(0))

    st.caption("Top txn_type")
    top_type = read_df(SQL_FACT_TYPE_TOP)
    if not top_type.empty:
        st.dataframe(top_type, use_container_width=True)
        st.bar_chart(top_type.set_index("txn_type")[["cnt"]])


# -----------------------------
# App
# -----------------------------
st.set_page_config(page_title="Bank Data Pipeline Dashboard", layout="wide")
st.title("🏦 Bank Data Integration & Quality Pipeline Dashboard")

st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Run Monitor", "Data Quality", "Business KPI"], index=0)

st.sidebar.markdown("---")
st.sidebar.caption("Tip: DATABASE_URL must be set in .env")

try:
    if page == "Run Monitor":
        page_run_monitor()
    elif page == "Data Quality":
        page_data_quality()
    else:
        page_business_kpi()
except Exception as e:
    st.error("Dashboard error")
    st.exception(e)