# db.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set. Put it in .env or env var.")
    return create_engine(url, pool_pre_ping=True)

def exec_sql(engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})