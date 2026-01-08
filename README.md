# Momentum Strategy Engine

**Project Status:** Active / v4 Multi-Strategy Architecture
**Last Update:** January 2026
**Primary Goal:** Automated weekly stock market momentum reporting, performance tracking, and static website generation.

## 📖 Project Overview
This project implements a quantitative momentum strategy focused on three cohorts: **S&P 500**, **S&P 400 (MidCap)**, and **MegaCap** (Top 25 S&P 500 by weight).

It runs a weekly pipeline that:
1.  **Syncs** the universe of stocks from State Street (SSGA).
2.  **Downloads** price history using Polygon.io (with robust rate-limiting and local SQLite caching).
3.  **Ranks** stocks based on 12-month momentum, volatility-adjusted returns, and rank stability.
4.  **Tracks** "Streaks" and logs "Dropped" tickers.
5.  **Generates** a static website (`/docs`) with historical reports, performance dashboards, and a trends blog.
6.  **Executes** a multi-strategy backtest:
    * **Stocks:** Tracks active Buy/Sell signals for the Top 5 picks.
    * **Options:** "Shadow tracks" three specific option strategies (100d Call, LEAP, Short Put) for every stock pick to evaluate leverage vs. pure equity performance.

---

## 🏗 System Architecture (AI Context)

### Data Flow
`Universe` → `Prices` → `Ranking` → `Top 5 Signal` → `Option Picker` → `CSV Logs` → `Site Builder` → `GitHub Pages`

### Core Modules

#### 1. Orchestration
* **`run_report.py`**: The entry point.
    * **Role:** Async orchestrator.
    * **Logic:** Syncs universe → Resolves dates → Fetches Prices → Calculates Ranks → Generates Report → **Triggers Option Picker** → Updates Tracker → Builds Website.
    * **Key Flag:** Uses `matplotlib.use("Agg")` to prevent memory leaks on headless servers.

#### 2. Data Ingestion
* **`universe.py`**:
    * **Source:** Direct Excel downloads from SSGA (SPY, MDY).
    * **Logic:** Dynamic header detection to handle SSGA format changes.
    * **Output:** `data/universe/{cohort}.csv` and `change_log.csv`.
* **`prices.py`**:
    * **Source:** Polygon.io.
    * **Logic:** Uses **Grouped Daily** endpoint for bulk efficiency.
    * **Resiliency:** Auto-backtracks if a target date is a holiday.

#### 3. Analytics & Strategy
* **`ranking.py`**:
    * **Strategy:** (Current Close - 1Y Close) / 1Y Close.
    * **Filter:** `Current Rank <= Previous Month Rank` (Momentum Persistence).
    * **Storage:** `top10_{cohort}` tables in SQLite.
* **`strategies.py` (The Option Picker)**:
    * **Role:** Finds the "Best Fit" option contract for a specific stock signal.
    * **Logic:** Scans the option chain to minimize distance to target parameters (e.g., "Find call closest to 100 DTE and 105% Strike").
    * **Strategies Tracked:**
        1.  **100d Call:** ~100 DTE, 5% OTM.
        2.  **500d LEAP:** ~500 DTE, 10% OTM.
        3.  **Short Put:** ~30 DTE, ATM.

#### 4. Visualization & Reporting
* **`report.py`**:
    * **Role:** Generates HTML reports with Metadata + News.
    * **Safe Mode:** Proactive rate-limiting (13s sleep) for Polygon Free Tier.
    * **Features:** Universe updates table, Dropped Ticker stats, Lightbox charts.

#### 5. Portfolio Tracking (`tracker.py`)
* **Role:** The State Machine for the portfolio.
* **Dual-Log System:**
    * **`data/trade_log.csv`**: Tracks the underlying **Stock** trades (Top 5).
    * **`data/option_log.csv`**: Shadow tracks the 3 associated **Option** strategies for each stock signal.
* **Price Resolution:** Asynchronously fills "Entry" and "Exit" prices for both stocks (OHLC) and options (Daily Close) using historical snapshots.
* **Metrics:** Calculates **Average Annualized Log Returns** vs SPY.

#### 6. Static Site Generator (`build_site.py`)
* **Role:** Converts raw data into a deployable website.
* **Output:** `docs/` folder (Configured for GitHub Pages).
* **Logic:** Archives reports, renders Markdown trend blogs, and builds the `index.html` dashboard.

---

## 💾 Database & Storage

1.  **SQLite (`data/market_data.sqlite`)**:
    * `daily_prices`: `(ticker, date)` PK.
    * `top10_{cohort}`: Historical rankings.
    * `company_metadata` & `company_news`: Cached API responses.
2.  **CSV Logs**:
    * `data/trade_log.csv`: Master record of Stock Buy/Sell signals.
    * `data/option_log.csv`: Detailed record of specific option contract performance.

---

## 🚀 Usage Guide

### 1. Setup
```bash
# Install Dependencies
pip install pandas numpy matplotlib mplfinance httpx requests jinja2 python-dotenv openpyxl markdown

# Set Environment Variables (.env)
POLYGON_API_KEY=your_key_here

# Initialize Database
python init_db.py


2. Weekly Routine (Run on Fridays)

Bash
python run_report.py
Process:

Downloads prices/news & generates HTML report.

Auto-Picks Options: Identifies specific contract symbols (e.g., NVDA260515C...) for new Top 5 entrants.

Backfills Prices: Checks Polygon for the historical prices of any pending trades.

Builds Site: Regenerates docs/.

Action: Commit and push the docs/ folder to GitHub to update your live dashboard.

3. Active Trading

Stock Signals: Check data/trade_log.csv. Change user_action to BOUGHT if executed.

Option Ideas: Check data/option_log.csv to see which specific contracts the algorithm selected.

4. Writing Trends

Create a markdown file in trends/ (e.g., 2026-01-08-volatility.md).

Run the report to publish it to the website.

🧠 Key Insights & "Gotchas"
Data Gaps: Option data is ephemeral. The system "Forward Tracks" (logs the symbol today, tracks it moving forward) because retrieving historical option chains usually requires expensive paid data tiers.

Polygon Rate Limits: The script is optimized for the Free Tier (5 calls/min) but includes sleep timers. Be patient during news fetching.

Site Hosting: Ensure GitHub Pages is set to serve from the /docs folder on the main branch.