import numpy as np
import pandas as pd

def ma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = np.maximum(h - l, np.maximum((h - c.shift(1)).abs(), (l - c.shift(1)).abs()))
    return tr.rolling(n).mean()

def is_uptrend(df: pd.DataFrame) -> bool:
    s50 = ma(df["close"], 50)
    s200 = ma(df["close"], 200)
    return bool((df["close"].iat[-1] > s50.iat[-1] > s200.iat[-1]) and (s50.iat[-1] > s50.iat[-5]))

def within_52w_high(df: pd.DataFrame, max_dd=0.20) -> bool:
    high_52w = df["high"].rolling(252).max().iat[-1]
    return (high_52w - df["close"].iat[-1]) / high_52w <= max_dd

def tight_box(df: pd.DataFrame, look=12, max_span=0.07) -> bool:
    box = df.tail(look)
    span = (box["high"].max() - box["low"].min()) / df["close"].iat[-1]
    return span <= max_span

def volume_dryup(df: pd.DataFrame) -> bool:
    v5 = df["volume"].rolling(5).mean().iat[-1]
    v50 = df["volume"].rolling(50).mean().iat[-1]
    return v5 <= 0.8 * v50

def atr_contracting(df: pd.DataFrame) -> bool:
    a = atr(df)
    base = a.iloc[-60:-1].median()
    return a.iat[-1] <= 0.8 * base

def tag_nr7(df: pd.DataFrame) -> pd.Series:
    rng = df["high"] - df["low"]
    return rng == rng.rolling(7).min()

def tag_inside_day(df: pd.DataFrame) -> pd.Series:
    h, l = df["high"], df["low"]
    return (h < h.shift(1)) & (l > l.shift(1))

def near_pivot(df: pd.DataFrame, look=12, tol=0.02):
    box = df.tail(look)
    pivot = float(box["high"].max())
    ok = (pivot - df["close"].iat[-1]) / pivot <= tol
    return ok, pivot
