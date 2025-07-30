#!/usr/bin/env python3
"""
Ranked swing scanner (NSE): "trend first, then tight consolidation".

Outputs
-------
• out/YYYY-MM-DD/todays_scan.csv  → ranked candidates (highest score first)
• out/YYYY-MM-DD/debug_checks.csv → per-symbol metrics behind the score

Notes
-----
• Historical candles are fetched per instrument via Kite historical_data().
• Access token must be set on the client before API calls (we read tokens.json).
"""

import os
import sys
import time
from typing import List, Dict, Any
import pandas as pd
from datetime import date, timedelta
import logging

from kite_client import get_kite, instruments_nse_eq, get_hist_daily_cached
from database import store_ohlc_data, store_scan_metadata
from screener import (
    atr,                 # ATR(14)
    is_uptrend,
    within_52w_high,
    tight_box,
    tag_nr7,
    tag_inside_day,
)

# -------------------- Tunables (env overrides supported) ----------------------

UNIVERSE_PATH = os.environ.get("UNIVERSE_PATH", "data/universe_nifty500.txt")

PRICE_FLOOR = float(os.environ.get("PRICE_FLOOR", 100.0))            # ₹ minimum price
MIN_DV_CR   = float(os.environ.get("MIN_DV_CR", 5.0))                # 20D avg traded value, ₹ crore

BOX_LEN     = int(os.environ.get("BOX_LEN", 12))                     # consolidation window (days)
BOX_SPAN    = float(os.environ.get("BOX_SPAN", 0.10))                # allowed span fraction (e.g., 0.10 = 10%)

BOX_CAP     = float(os.environ.get("BOX_CAP", 0.12))                 # <=12% fully "good"
ATR_CAP     = float(os.environ.get("ATR_CAP", 1.20))                 # ATR_now / ATR_base <= 1.2
VOL_CAP     = float(os.environ.get("VOL_CAP", 1.20))                 # Vol5 / Vol50 <= 1.2
PIVOT_TOL   = float(os.environ.get("PIVOT_TOL", 0.03))               # <=3% from pivot fully "good"

W_BOX   = float(os.environ.get("W_BOX",   0.30))
W_ATR   = float(os.environ.get("W_ATR",   0.25))
W_VOL   = float(os.environ.get("W_VOL",   0.20))
W_PIVOT = float(os.environ.get("W_PIVOT", 0.25))
BONUS_UPTREND  = float(os.environ.get("BONUS_UPTREND",  0.05))
BONUS_NEARHIGH = float(os.environ.get("BONUS_NEARHIGH", 0.05))

TOP_N = int(os.environ.get("TOP_N", 50))                              # just for console display
PAUSE_SECS = float(os.environ.get("PAUSE_SECS", 0.12))                # gentle pacing for API

# -----------------------------------------------------------------------------

def clamp01(x: float) -> float:
    return 0.0 if x < 0 else (1.0 if x > 1 else x)

def load_universe(path: str) -> List[str]:
    if not os.path.exists(path):
        print(f"Universe file not found: {path}")
        print("Run: python scripts/update_universe.py")
        sys.exit(1)
    with open(path) as f:
        syms = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    return syms

def box_span_fraction(df: pd.DataFrame, look: int, last_close: float) -> float:
    box = df.tail(look)
    if box.empty:
        return 9.99
    return (box["high"].max() - box["low"].min()) / last_close

def compute_score(metrics: Dict[str, Any]) -> float:
    """Turn raw metrics into a 0..1+ score."""
    box_frac   = min(metrics["box_span_frac"], BOX_CAP) / BOX_CAP
    atr_ratio  = min(metrics["atr_ratio"],     ATR_CAP) / ATR_CAP
    vol_ratio  = min(metrics["vol_ratio"],     VOL_CAP) / VOL_CAP
    dist_ratio = min(metrics["dist_to_pivot"], PIVOT_TOL) / PIVOT_TOL

    s_box   = clamp01(1.0 - box_frac)     # tighter better
    s_atr   = clamp01(1.0 - atr_ratio)    # lower ATR better
    s_vol   = clamp01(1.0 - vol_ratio)    # lower volume better
    s_pivot = clamp01(1.0 - dist_ratio)   # closer to pivot better

    score = (W_BOX * s_box) + (W_ATR * s_atr) + (W_VOL * s_vol) + (W_PIVOT * s_pivot)
    if metrics.get("uptrend"):            score += BONUS_UPTREND
    if metrics.get("within_20pct_high"):  score += BONUS_NEARHIGH
    return score

def main() -> None:
    scan_start_time = time.time()
    
    kite = get_kite()  # reads tokens.json; sets access_token

    inst_df = instruments_nse_eq(kite)  # tradingsymbol -> instrument_token
    by_symbol = {r["tradingsymbol"]: r["instrument_token"] for _, r in inst_df.iterrows()}

    universe = load_universe(UNIVERSE_PATH)
    end = date.today()
    start = end - timedelta(days=600)

    ranked_rows: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, Any]] = []
    stored_count = 0

    total = len(universe)
    for i, sym in enumerate(universe, 1):
        print(f"[{i}/{total}] {sym}", flush=True)
        token = by_symbol.get(sym)
        if not token:
            debug_rows.append({"symbol": sym, "reason": "not_found_in_instruments"})
            continue

        try:
            df = get_hist_daily_cached(kite, token, sym, start, end)  # cached daily OHLCV
        except Exception as e:
            debug_rows.append({"symbol": sym, "reason": f"fetch_failed:{e}"})
            continue

        if len(df) < 220:
            debug_rows.append({"symbol": sym, "reason": "insufficient_history"})
            continue
        
        # Store OHLC data in database (store all historical data)
        try:
            rows_stored = store_ohlc_data(sym, df)
            if rows_stored > 0:
                stored_count += 1
        except Exception as e:
            logging.error(f"Failed to store OHLC data for {sym}: {e}")

        last = df.iloc[-1]
        close = float(last["close"])

        # Liquidity guards
        if close < PRICE_FLOOR:
            debug_rows.append({"symbol": sym, "reason": "below_price_floor", "close": close})
            continue

        df["traded_value"] = df["close"] * df["volume"]
        tv20 = df["traded_value"].rolling(20).mean().iat[-1]
        tv20_cr = (tv20 / 1e7) if pd.notna(tv20) else 0.0
        if tv20_cr < MIN_DV_CR:
            debug_rows.append({"symbol": sym, "reason": "below_traded_value", "tv20_cr": round(tv20_cr, 2)})
            continue

        # Metrics for scoring
        up = is_uptrend(df)
        near_high = within_52w_high(df, 0.20)

        box_frac = box_span_fraction(df, BOX_LEN, close)
        box = df.tail(BOX_LEN)
        pivot = float(box["high"].max()) if len(box) else close
        dist_to_pivot = abs(pivot - close) / pivot

        a = atr(df)
        atr_now = float(a.iat[-1])
        atr_base = float(a.iloc[-60:-1].median()) if len(a) >= 61 else atr_now
        atr_ratio = (atr_now / atr_base) if atr_base > 0 else 9.99

        v5  = float(df["volume"].rolling(5).mean().iat[-1])
        v50 = float(df["volume"].rolling(50).mean().iat[-1]) if pd.notna(df["volume"].rolling(50).mean().iat[-1]) else 0.0
        vol_ratio = (v5 / v50) if v50 > 0 else 9.99

        # Tags
        df["nr7"] = tag_nr7(df)
        df["inside"] = tag_inside_day(df)

        metrics = {
            "box_span_frac": box_frac,
            "atr_ratio": atr_ratio,
            "vol_ratio": vol_ratio,
            "dist_to_pivot": dist_to_pivot,
            "uptrend": up,
            "within_20pct_high": near_high,
        }
        score = compute_score(metrics)

        ranked_rows.append({
            "symbol": sym,
            "score": round(score, 4),
            "close": round(close, 2),
            "pivot": round(pivot, 2),
            "dist_to_pivot_pct": round(dist_to_pivot * 100, 2),
            "box_span_pct": round(box_frac * 100, 2),
            "atr_ratio": round(atr_ratio, 2),
            "vol5_to_vol50": round(vol_ratio, 2) if vol_ratio < 9 else None,
            "uptrend": bool(up),
            "within_20pct_high": bool(near_high),
            "nr7_today": bool(df["nr7"].iat[-1]),
            "inside_today": bool(df["inside"].iat[-1]),
            "tv20_cr": round(tv20_cr, 2),
        })

        debug_rows.append({
            "symbol": sym,
            "reason": "ranked",
            "score": round(score, 4),
            "close": round(close, 2),
            "pivot": round(pivot, 2),
            "dist_to_pivot_pct": round(dist_to_pivot * 100, 2),
            "box_span_pct": round(box_frac * 100, 2),
            "atr_ratio": round(atr_ratio, 2),
            "vol5_to_vol50": round(vol_ratio, 2) if vol_ratio < 9 else None,
            "uptrend": bool(up),
            "within_20pct_high": bool(near_high),
            "nr7_today": bool(df["nr7"].iat[-1]),
            "inside_today": bool(df["inside"].iat[-1]),
            "tv20_cr": round(tv20_cr, 2),
        })

        time.sleep(PAUSE_SECS)

    # -------- Write outputs --------
    out_dir = f"out/{end.isoformat()}"
    os.makedirs(out_dir, exist_ok=True)

    ranked_df = pd.DataFrame(ranked_rows).sort_values(["score", "box_span_pct"], ascending=[False, True])
    debug_df  = pd.DataFrame(debug_rows)

    ranked_csv = f"{out_dir}/todays_scan.csv"
    debug_csv  = f"{out_dir}/debug_checks.csv"

    ranked_df.to_csv(ranked_csv, index=False)
    debug_df.to_csv(debug_csv, index=False)

    # Store scan metadata
    scan_duration = time.time() - scan_start_time
    try:
        store_scan_metadata(
            scan_date=end.isoformat(),
            total_symbols=total,
            ranked_symbols=len(ranked_df),
            scan_duration=scan_duration
        )
        print(f"Stored OHLC data for {stored_count} symbols in database")
    except Exception as e:
        logging.error(f"Failed to store scan metadata: {e}")
    
    print(f"Wrote {len(ranked_df)} ranked rows → {ranked_csv}")
    print(f"Top {min(TOP_N, len(ranked_df))}:")
    with pd.option_context("display.max_rows", 100, "display.width", 140):
        print(ranked_df.head(TOP_N)[["symbol","score","close","dist_to_pivot_pct","box_span_pct","atr_ratio","vol5_to_vol50","uptrend","within_20pct_high"]])
    print(f"Wrote debug → {debug_csv}")
    print(f"Scan completed in {scan_duration:.1f} seconds")

if __name__ == "__main__":
    main()



