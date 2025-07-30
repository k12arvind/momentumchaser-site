# scripts/update_universe.py
import os
import sys
import time
import io
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Prefer the NSE archives/CDN host that serves the CSV; fall back to Nifty Indices.
NSE_HOME = "https://www.nseindia.com"
NSE_PAGE = "https://www.nseindia.com/products-services/indices-nifty500-index"
NSE_CSV  = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"   # archives/CDN
NI_CSV   = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"      # fallback

OUT_TXT  = "data/universe_nifty500.txt"

def make_session():
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125.0.0.0 Safari/537.36"),
        "Accept": "text/csv,application/octet-stream,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    return s

def fetch_nse_csv_text() -> str:
    """Fetch from NSE with a browser-like session + preflight to get cookies."""
    s = make_session()
    s.get(NSE_HOME, timeout=10)
    s.get(NSE_PAGE, timeout=15)
    r = s.get(NSE_CSV, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text

def fetch_ni_csv_text() -> str:
    """Fallback: Nifty Indices CSV."""
    s = make_session()
    s.headers.update({"Referer": "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-500"})
    r = s.get(NI_CSV, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text

def parse_symbols(csv_text: str) -> list[str]:
    df = pd.read_csv(io.StringIO(csv_text))
    for col in ("Symbol", "SYMBOL", "symbol"):
        if col in df.columns:
            syms = [str(x).strip().upper() for x in df[col].dropna().tolist()]
            syms = [s for s in syms if s and s.isascii()]
            return sorted(set(syms))
    raise RuntimeError(f"'Symbol' column not found. Columns: {list(df.columns)}")

def main():
    os.makedirs("data", exist_ok=True)
    print("Downloading NIFTY-500 constituents…")
    csv_text = None
    try:
        csv_text = fetch_nse_csv_text()
        print("✓ Fetched from NSE archives.")
    except Exception as e:
        print(f"⚠ NSE fetch failed: {e}")
        time.sleep(1.0)
    if csv_text is None:
        try:
            csv_text = fetch_ni_csv_text()
            print("✓ Fetched from Nifty Indices (fallback).")
        except Exception as e:
            print(f"✗ Fallback also failed: {e}")
            sys.exit(2)

    try:
        syms = parse_symbols(csv_text)
    except Exception as e:
        print(f"✗ Could not parse CSV: {e}")
        sys.exit(3)

    with open(OUT_TXT, "w") as f:
        f.write("\n".join(syms) + "\n")
    print(f"Wrote {len(syms)} symbols → {OUT_TXT}")

if __name__ == "__main__":
    main()


