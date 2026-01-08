import os
import time
import pandas as pd
import mplfinance as mpf
import requests
from datetime import datetime, timedelta, timezone
from typing import Tuple
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# --- Config ---
load_dotenv()
API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_KEY") or "").strip()
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"

if not API_KEY:
    raise RuntimeError("Missing Polygon key. Set POLYGON_API_KEY in .env")

# Simple in-memory cache
_CACHE = {}

def _fetch_history(ticker: str, days_back=365) -> pd.DataFrame:
    """Fetches daily OHLCV from Polygon (Blocking/Sync)."""
    
    # 1. Check Cache
    utc_now = datetime.now(timezone.utc)
    today_str = utc_now.strftime("%Y-%m-%d")
    cache_key = (ticker, today_str)
    
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    # 2. Setup Dates
    start_dt = utc_now - timedelta(days=days_back)
    start_fmt = start_dt.strftime("%Y-%m-%d")
    end_fmt = today_str

    # 3. Request
    url = BASE_URL.format(ticker=ticker, start=start_fmt, end=end_fmt)
    params = {"adjusted": "true", "sort": "asc", "apiKey": API_KEY}
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 429:
            print(f"   ⚠️ Chart rate limit ({ticker}). Sleeping 15s...")
            time.sleep(15)
            return _fetch_history(ticker, days_back) # Retry once
            
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"   ⚠️ Chart fetch failed for {ticker}: {e}")
        return pd.DataFrame()
    finally:
        # PROACTIVE safety net: Always sleep 13s after a request
        time.sleep(13) 

    results = data.get("results", [])
    if not results:
        return pd.DataFrame()

    # 4. Parse
    df = pd.DataFrame(results)
    df["Date"] = pd.to_datetime(df["t"], unit="ms").dt.tz_localize(None) # UTC -> Naive
    df = df.set_index("Date").sort_index()
    
    # Rename for mplfinance
    df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    
    # Cache and Return
    _CACHE[cache_key] = df
    return df

def plot_stock_chart(ticker: str, save_path: str = None, benchmark_ticker="VOO"):
    """
    Generates a candle chart with VOO overlay.
    Returns: (fig, axes) tuple.
    """
    # 1. Get Data
    df = _fetch_history(ticker)
    if df.empty or len(df) < 20:
        raise ValueError(f"Not enough data for {ticker}")
    
    bench = _fetch_history(benchmark_ticker)
    
    # 2. Align Benchmark (Normalize VOO to start at Ticker's price)
    common_idx = df.index.intersection(bench.index)
    
    addplots = []
    
    if not common_idx.empty:
        # Start comparison from the 10th common point (to avoid noise at very start)
        anchor_pos = min(10, len(common_idx) - 1) 
        anchor_date = common_idx[anchor_pos]
        
        # Calculate scaling factor
        t_price = df.loc[anchor_date, "Close"]
        b_price = bench.loc[anchor_date, "Close"]
        scale = t_price / b_price
        
        # Create normalized series
        bench_norm = bench.loc[common_idx, "Close"] * scale
        
        # Reindex to match the main dataframe exactly
        bench_aligned = bench_norm.reindex(df.index) 
        
        addplots.append(
            mpf.make_addplot(
                bench_aligned.values,   # <--- FIX: Pass .values (Numpy Array) instead of Series
                color="orange", 
                linestyle="dashed", 
                width=1.5,
                label=f"{benchmark_ticker} (Comp)"
            )
        )

    # 3. Plot Style
    mc = mpf.make_marketcolors(up="#00b300", down="#ff3333", edge="inherit", wick="inherit", volume="in")
    s = mpf.make_mpf_style(base_mpf_style="yahoo", marketcolors=mc, gridstyle=":", rc={"font.size": 10})

    # 4. Generate Plot
    fig, axes = mpf.plot(
        df,
        type="candle",
        volume=True,
        mav=(20, 50),
        addplot=addplots,
        style=s,
        title=f"\n{ticker} vs {benchmark_ticker} (1Y)",
        returnfig=True,
        figsize=(10, 5),
        tight_layout=True,
        datetime_format="%b %Y"
    )
    
    # Legend
    axes[0].legend(loc="upper left")
    
    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
        
    return fig, axes

if __name__ == "__main__":
    print("Testing Chart Module...")
    try:
        plot_stock_chart("NVDA", "test_chart.png")
        print("✅ Chart saved to test_chart.png")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()