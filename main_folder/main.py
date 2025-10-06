#!/usr/bin/env python3
import os, time, pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from binance import Client
from binance.exceptions import BinanceAPIException
import logging 

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_HOST = "192.168.1.49"
DB_NAME = "criptodb"
DB_USER = "admincar"
DB_PASSWORD = "1234car"
DB_PORT = "5432"
SSL_MODE = 'require' 
API_KEY, API_SECRET = os.getenv("API_KEY",""), os.getenv("API_SECRET","")
SCHEMA, TABLE, SYMBOL = "crypto", "perp_metrics_1m", os.getenv("SYMBOL","BTCUSDT")


# Crear el "motor" de SQLAlchemy para conectar con la base de datos
try:
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    logging.info(f"Conexión a PostgreSQL establecida exitosamente con SQLAlchemy en {DB_HOST}.")
except Exception as e:
    logging.error(f"Error al crear el motor de SQLAlchemy: {e}")
    exit() # Salimos si no podemos conectar


client = Client(api_key=API_KEY, api_secret=API_SECRET, testnet=False)

def floor5(dt): m=(dt.minute//5)*5; return dt.replace(minute=m, second=0, microsecond=0, tzinfo=timezone.utc)
def call(fn, **kw):
    back=1.0
    for _ in range(5):
        try: return fn(**kw)
        except BinanceAPIException as e:
            time.sleep(back); back=min(back*1.8, 30)
    return []

def fetch_oi_5m(start_ts, end_ts):
    # usar sólo endTime+limit y paginar hacia atrás si se desea; aquí pedimos el tramo reciente
    data = call(client.futures_open_interest_hist,
                symbol=SYMBOL, period="5m",
                startTime=int(start_ts.timestamp()*1000),
                endTime=int(end_ts.timestamp()*1000), limit=500)
    if not data: return pd.DataFrame(columns=["ts","open_interest","open_interest_value"])
    df = pd.DataFrame(data)
    tscol = "timestamp" if "timestamp" in df.columns else "time"
    df["ts"] = pd.to_datetime(df[tscol], unit="ms", utc=True)
    df["open_interest"] = pd.to_numeric(df.get("sumOpenInterest", df.get("openInterest")), errors="coerce")
    df["open_interest_value"] = pd.to_numeric(df.get("sumOpenInterestValue"), errors="coerce")
    return df[["ts","open_interest","open_interest_value"]].drop_duplicates("ts")

def fetch_toptrader_ratio_5m(start_ts, end_ts):
    data = call(client.futures_top_long_short_account_ratio,
                symbol=SYMBOL, period="5m",
                startTime=int(start_ts.timestamp()*1000),
                endTime=int(end_ts.timestamp()*1000), limit=500)
    if not data: return pd.DataFrame(columns=["ts","toptrader_long_short_ratio"])
    df = pd.DataFrame(data); df["ts"]=pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["toptrader_long_short_ratio"]=pd.to_numeric(df["longShortRatio"], errors="coerce")
    return df[["ts","toptrader_long_short_ratio"]].drop_duplicates("ts")

def fetch_taker_ratio_5m(start_ts, end_ts):
    data = call(client.futures_taker_long_short_ratio,
                symbol=SYMBOL, period="5m",
                startTime=int(start_ts.timestamp()*1000),
                endTime=int(end_ts.timestamp()*1000), limit=500)
    if not data: return pd.DataFrame(columns=["ts","taker_long_short_vol_ratio"])
    df = pd.DataFrame(data); df["ts"]=pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["taker_long_short_vol_ratio"]=pd.to_numeric(df["buySellRatio"], errors="coerce")
    return df[["ts","taker_long_short_vol_ratio"]].drop_duplicates("ts")

def upsert_1m(df):
    if df.empty: return
    with engine.begin() as conn:
        tmp="_tmp_perp5m_enrich"
        df.to_sql(tmp, con=conn, schema=SCHEMA, if_exists="replace", index=False)
        conn.execute(text(f"""
        INSERT INTO {SCHEMA}.{TABLE} (ts,symbol,open_interest,open_interest_value,toptrader_long_short_ratio,taker_long_short_vol_ratio)
        SELECT ts, :sym, open_interest, open_interest_value, toptrader_long_short_ratio, taker_long_short_vol_ratio
        FROM {SCHEMA}.{tmp}
        ON CONFLICT (ts, symbol) DO UPDATE
        SET open_interest = COALESCE(EXCLUDED.open_interest, {SCHEMA}.{TABLE}.open_interest),
            open_interest_value = COALESCE(EXCLUDED.open_interest_value, {SCHEMA}.{TABLE}.open_interest_value),
            toptrader_long_short_ratio = COALESCE(EXCLUDED.toptrader_long_short_ratio, {SCHEMA}.{TABLE}.toptrader_long_short_ratio),
            taker_long_short_vol_ratio = COALESCE(EXCLUDED.taker_long_short_vol_ratio, {SCHEMA}.{TABLE}.taker_long_short_vol_ratio);
        """), {"sym": SYMBOL})
        conn.execute(text(f"DROP TABLE {SCHEMA}.{tmp}"))

def main():
    now = datetime.now(timezone.utc)
    end5 = floor5(now)                 # borde superior alineado
    start5 = end5 - timedelta(minutes=30)  # 30m de margen
    oi = fetch_oi_5m(start5, end5)
    tt = fetch_toptrader_ratio_5m(start5, end5)
    tk = fetch_taker_ratio_5m(start5, end5)

    # merge 5m → grilla 1m con ffill
    grid = pd.date_range(start=start5, end=end5, freq="1min", tz=timezone.utc, inclusive="left")
    base = pd.DataFrame({"ts": grid})
    for part in (oi, tt, tk):
        if not part.empty:
            part = part.set_index("ts").sort_index()
            part = part.reindex(pd.date_range(start=start5, end=end5, freq="5min", tz=timezone.utc, inclusive="left")).ffill()
            part1m = part.reindex(grid, method="ffill").reset_index().rename(columns={"index":"ts"})
            base = base.merge(part1m, on="ts", how="left")
    base["symbol"] = SYMBOL
    upsert_1m(base)

if __name__ == "__main__":
    main()