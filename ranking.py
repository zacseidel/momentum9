import sqlite3
import pandas as pd
from pathlib import Path
from datetime import date
from typing import Dict, List

# --- Configuration ---
DB_PATH = Path("data/market_data.sqlite")

class RankingService:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def calculate_ranks(self, prices_df: pd.DataFrame, date_map: Dict[str, str]) -> pd.DataFrame:
        """
        Takes raw price data, calculates momentum returns, and ranks them.
        Returns a DataFrame sorted by best performance.
        """
        if prices_df.empty:
            return pd.DataFrame()

        # 1. Pivot Long Data to Wide (Index=Ticker, Columns=Date)
        #    This makes vector math easy (e.g., Col_A - Col_B)
        pivoted = prices_df.pivot(index="ticker", columns="date", values="close")
        
        # 2. Map friendly names (prices.py) to the actual dates in the data
        #    We safely extract columns; if a date is missing, pandas will raise KeyError
        try:
            c_now      = pivoted[date_map["latest_trading"]]
            c_1week    = pivoted[date_map["minus_1_week"]]
            c_1month   = pivoted[date_map["minus_1_month"]]
            c_1year    = pivoted[date_map["minus_1_year"]]
            c_13months = pivoted[date_map["minus_13_months"]]
        except KeyError as e:
            print(f"❌ Ranking Error: Missing price column for {e}")
            return pd.DataFrame()

        # 3. Calculate Returns
        #    Primary Metric: 12-month momentum (skipping most recent month if you wanted standard momentum, 
        #    but your old code used direct 1-year: (Now - 1Y) / 1Y)
        current_return = (c_now - c_1year) / c_1year
        
        #    Trend Validation: Previous period (1mo ago vs 13mo ago)
        previous_return = (c_1month - c_13months) / c_13months
        
        #    Short-term Context
        last_week_return = (c_now - c_1week) / c_1week

        # 4. Ranking (Lower rank is better, e.g. #1)
        current_rank  = current_return.rank(ascending=False, method="min")
        previous_rank = previous_return.rank(ascending=False, method="min")
        
        #    Rank Change: If Prev was #10 and Curr is #5, change is +5 (Positive is good)
        rank_change = previous_rank - current_rank

        # 5. Assemble Results
        df = pd.DataFrame({
            "current_return":    current_return,
            "last_week_return":  last_week_return,
            "last_month_return": previous_return,
            "current_rank":      current_rank,
            "last_month_rank":   previous_rank,
            "rank_change":       rank_change
        })

        # 6. Filter: "Improving or Steady"
        #    We only want stocks that are maintaining or improving their rank.
        df = df.dropna()
        df = df[df["current_rank"] <= df["last_month_rank"]]
        
        # Sort by raw return (Highest first)
        return df.sort_values("current_return", ascending=False)

    def extract_top_picks(self, ranked_df: pd.DataFrame, cohort: str, run_date: date) -> pd.DataFrame:
        """
        Slices the Top 10, calculates 'Streak' (consecutive weeks on list), and saves to DB.
        """
        if ranked_df.empty:
            print(f"⚠️  No ranked results for {cohort}.")
            return pd.DataFrame()

        # 1. Select Top 10
        top_10 = ranked_df.head(10).copy()
        top_10.index.name = "ticker"
        top_10 = top_10.reset_index()

        # 2. Calculate Streaks (The "Tweetable" Metric)
        top_10 = self._calculate_streaks(top_10, cohort, run_date)

        # 3. Format for Display/Storage
        #    Convert floats -> strings (e.g. 0.25 -> "25.0%") to match DB schema TEXT types
        display_df = top_10.copy()
        pct_cols = ["current_return", "last_week_return", "last_month_return"]
        for c in pct_cols:
            display_df[c] = display_df[c].apply(lambda x: f"{x:.1%}")

        #    Add Date for the Primary Key
        display_df["date"] = run_date.isoformat()

        # 4. Save to Database
        self._save_to_db(display_df, cohort, run_date)
        
        return display_df

    def _calculate_streaks(self, current_df: pd.DataFrame, cohort: str, run_date: date) -> pd.DataFrame:
        """
        Calculates consecutive streaks AND preserves the original start date of the streak.
        """
        table_name = f"top10_{cohort}"
        
        with sqlite3.connect(self.db_path) as conn:
            # A. Find the most recent previous entry
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT MAX(date) FROM {table_name} WHERE date < ?", (run_date.isoformat(),))
                last_date = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                last_date = None

            # B. If no history, everyone starts today
            if not last_date:
                current_df["streak"] = 1
                current_df["streak_start"] = run_date.isoformat()
                return current_df

            # C. Get history (streak count AND start date)
            prev_df = pd.read_sql(
                f"SELECT ticker, streak, streak_start FROM {table_name} WHERE date = ?", 
                conn, 
                params=(last_date,)
            )
        
        # D. Merge History
        #    suffixes: '_new' (current run), '_old' (last run)
        merged = current_df.merge(prev_df, on="ticker", how="left", suffixes=("", "_old"))
        
        # E. Logic
        #    Streak: if old exists, old + 1. Else 1.
        merged["streak"] = merged["streak"].fillna(0).astype(int) + 1
        
        #    Start Date: if old exists, keep old start. Else use today.
        #    (We use run_date.isoformat() for the new ones)
        today_str = run_date.isoformat()
        merged["streak_start"] = merged["streak_start"].fillna(today_str)
        
        # Cleanup
        cols_to_drop = [c for c in merged.columns if "_old" in c]
        return merged.drop(columns=cols_to_drop)

    def _save_to_db(self, df: pd.DataFrame, cohort: str, run_date: date):
        table_name = f"top10_{cohort}"
        run_iso = run_date.isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            # 1. Clean slate for this specific date (allows re-running report safely)
            conn.execute(f"DELETE FROM {table_name} WHERE date = ?", (run_iso,))
            
            # 2. Insert
            #    We rely on DataFrame column names matching the DB schema.
            #    (ticker, date, current_return, ..., streak)
            df.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"   💾 Saved {len(df)} top picks for {cohort} (Streaks updated).")