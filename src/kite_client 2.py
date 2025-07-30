# src/kite_client.py
import os, json, io
from datetime import timedelta
import pandas as pd
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

def get_kite():
    """Create a Kite client using today's access_token saved in tokens.json."""
    with open("tokens.json") as f:
        data = json.load(f)
    api_key = data.get("api_key") or os.getenv("KITE_API_KEY")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(data["access_token"])
    return kite

def instruments_nse_eq(kite):
    """Return instruments master for NSE equities as a DataFrame."""
    recs = kite.instruments("NSE")
    df = pd.DataFrame(recs)
    df = df[(df["segment"] == "NSE") & (df["instrument_type"] == "EQ")]
    return df

def get_hist_daily(kite, instrument_token, start_date, end_date):
    """Fetch daily OHLCV for a single instrument (no cache)."""
    candles = kite.historical_data(
        instrument_token,
        from_date=start_date,
        to_date=end_date,
        interval="day",
        continuous=False,
        oi=False,
    )
    df = pd.DataFrame(candles)
    # Ensure 'date' is datetime for downstream logic
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df

# --------------------- Cached fetch (for speed at NIFTY-500 scale) ---------------------

def _cache_path(symbol: str) -> str:
    os.makedirs("cache", exist_ok=True)
    fname = symbol.replace("/", "_").replace(" ", "_")
    return os.path.join("cache", f"{fname}.csv")

def _read_cache(symbol: str) -> pd.DataFrame:
    path = _cache_path(symbol)
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["date"])
    df.sort_values("date", inplace=True)
    df.drop_duplicates(subset=["date"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def _write_cache(symbol: str, df: pd.DataFrame) -> None:
    if not df.empty:
        df = df.sort_values("date").drop_duplicates(subset=["date"])
    df.to_csv(_cache_path(symbol), index=False)

def get_hist_daily_cached(kite, instrument_token, symbol, start_date, end_date):
    """
    Read daily history for SYMBOL from local cache; if missing or old,
    fetch only the missing window from Kite historical_data() and append.
    """
    cached = _read_cache(symbol)
    if cached.empty:
        fetch_start = start_date
    else:
        last_dt = pd.to_datetime(cached["date"].iloc[-1]).date()
        if last_dt >= end_date:
            return cached
        fetch_start = last_dt + timedelta(days=1)

    if fetch_start <= end_date:
        fresh = get_hist_daily(kite, instrument_token, fetch_start, end_date)
        if not fresh.empty:
            fresh["date"] = pd.to_datetime(fresh["date"])
            merged = fresh if cached.empty else pd.concat([cached, fresh], ignore_index=True)
            _write_cache(symbol, merged)
            return merged

    return cached

