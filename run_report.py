#!/usr/bin/env python
# run_report.py - The Orchestrator

import matplotlib
matplotlib.use("Agg") # Force non-interactive backend

import asyncio
import sys
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

# Local Modules
from universe import UniverseService
from prices import PriceService
from ranking import RankingService
from report import ReportService
from tracker import TradeTracker   # Updated Tracker
from build_site import build_website # Static Site Generator

# Config
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)
COHORTS = ["megacap", "sp500", "sp400"]

async def build_report(run_date: date):
    print(f"🚀 Starting Momentum Report for {run_date}")
    
    # 0. Initialize Tracker (Now includes Options Engine)
    tracker = TradeTracker()

    # 1. Tracker Maintenance (Auto-Fill missing prices for Stocks AND Options)
    print("📋 Checking for missing trade prices...")
    await tracker.resolve_prices()

    # 2. Universe Sync
    u_service = UniverseService()
    await u_service.sync(as_of=run_date)

    # 3. Prices & Dates
    p_service = PriceService()
    target_dates = await p_service.resolve_target_dates(run_date)

    # 4. Ranking & Signal Generation
    r_service = RankingService()
    top_picks = {} 

    for cohort in COHORTS:
        print(f"📊 Processing {cohort.upper()}...")
        
        # Get Tickers & Prices
        cohort_df = u_service.get_cohort(cohort)
        tickers = cohort_df['symbol'].tolist()
        prices_df = await p_service.get_snapshots(tickers, target_dates)
        
        # Rank
        ranked_df = r_service.calculate_ranks(prices_df, target_dates)
        top_10 = r_service.extract_top_picks(ranked_df, cohort, run_date)
        top_picks[cohort] = top_10
        
        # --- TRACKER: Process New Signals (Stocks + Options) ---
        # This will now trigger the OptionPicker to find contracts
        tracker.process_signals(top_10, prices_df, cohort, run_date)
        # -------------------------------------------------------

    # 5. Momentum Report (Main HTML)
    print("📝 Generating Momentum HTML...")
    rep_service = ReportService()
    
    all_winners = []
    for df in top_picks.values():
        if not df.empty:
            all_winners.extend(df['ticker'].tolist())
    
    # Prefetch news/metadata with progress bars
    await rep_service.cache_metadata(list(set(all_winners)))
    
    # Generate Main Report
    momentum_html = rep_service.generate_html(top_picks, target_dates, run_date)
    mom_file = REPORT_DIR / f"momentum_{run_date.isoformat()}.html"
    mom_file.write_text(momentum_html, encoding="utf-8")
    
    # 6. Performance Report (Tracker HTML)
    print("📝 Generating Performance HTML...")
    perf_html = tracker.render_html_report()
    perf_file = REPORT_DIR / f"performance_{run_date.isoformat()}.html"
    perf_file.write_text(perf_html, encoding="utf-8")
    
    return mom_file, perf_file

def main():
    load_dotenv()
    
    if len(sys.argv) > 1:
        run_date = date.fromisoformat(sys.argv[1])
    else:
        run_date = date.today()

    try:
        # Run Pipeline
        mom_file, perf_file = asyncio.run(build_report(run_date))
        
        print(f"\n✅ SUCCESS!")
        print(f"   1. Momentum Report:  {mom_file.absolute()}")
        print(f"   2. Perf Dashboard:   {perf_file.absolute()}")

        # --- NEW: Build the Website ---
        build_website()
        # ------------------------------
        
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()