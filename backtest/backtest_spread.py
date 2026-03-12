"""
NIFTY Triple Calendar Spread – Backtesting Engine
==================================================
Reads data/nifty_futures_aligned.csv (built by data_downloader.py).

Strategy simulated:
  - Entry:  Z-score of curve_diff exceeds threshold
  - Exit:   Z-score reverts below exit_threshold  OR  stoploss hit  OR  time stop

Reports:
  - Total P&L, Win rate, Avg trade, Max drawdown, Sharpe ratio
  - Per-trade log CSV

Usage:
    python backtest_spread.py
    python backtest_spread.py --capital 300000 --zscore_entry 2.0 --sl 20 --tgt 15
"""

import os
import argparse
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

DATA_FILE  = os.path.join(os.path.dirname(__file__), "..", "data", "nifty_futures_aligned.csv")
REPORT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "backtest_reports")
os.makedirs(REPORT_DIR, exist_ok=True)


# ─── Parameters ─────────────────────────────────────────────────────────────

@dataclass
class BacktestParams:
    capital:        float = 500_000
    lot_size:       int   = 75
    margin_per_set: float = 100_000      # approx hedged margin per butterfly set
    zscore_entry:   float = 2.0
    zscore_exit:    float = 0.5
    lookback:       int   = 20           # rolling window (trading days)
    sl_points:      float = 20.0         # curve_diff stoploss
    tgt_points:     float = 15.0         # curve_diff target
    max_daily_loss: float = 5_000        # halt day if exceeded
    max_positions:  int   = 3            # max concurrent sets


# ─── Trade record ────────────────────────────────────────────────────────────

@dataclass
class Trade:
    entry_date:       str
    signal:           str      # SELL_BUTTERFLY | BUY_BUTTERFLY
    entry_diff:       float
    stoploss_diff:    float
    target_diff:      float
    qty:              int
    exit_date:        str  = ""
    exit_diff:        float = 0.0
    exit_reason:      str  = ""
    pnl_points:       float = 0.0
    pnl_inr:          float = 0.0


# ─── Engine ──────────────────────────────────────────────────────────────────

class Backtester:

    def __init__(self, params: BacktestParams):
        self.p = params

    def _compute_qty(self) -> int:
        sets = max(1, int(self.p.capital // self.p.margin_per_set))
        sets = min(sets, self.p.max_positions)
        return sets * self.p.lot_size

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        p = self.p

        # Spread features
        df = df.copy()
        df["spread1"]    = df["next_close"] - df["near_close"]
        df["spread2"]    = df["far_close"]  - df["next_close"]
        df["curve_diff"] = df["spread1"]    - df["spread2"]

        # Rolling Z-score
        rolling = df["curve_diff"].rolling(p.lookback)
        df["mean"]   = rolling.mean()
        df["std"]    = rolling.std()
        df["zscore"] = (df["curve_diff"] - df["mean"]) / df["std"].replace(0, np.nan)
        df = df.dropna(subset=["zscore"]).reset_index(drop=True)

        trades: list[Trade] = []
        open_trade: Optional[Trade] = None
        daily_pnl  = 0.0
        equity     = p.capital
        equity_curve = []

        for _, row in df.iterrows():
            date       = row["date"]
            cdiff      = row["curve_diff"]
            zscore     = row["zscore"]

            # ── Check exit on open trade ────────────────────────────────────
            if open_trade is not None:
                closed = False
                exit_reason = ""

                if open_trade.signal == "SELL_BUTTERFLY":
                    hit_sl  = cdiff >= open_trade.stoploss_diff
                    hit_tgt = cdiff <= open_trade.target_diff
                    exit_z  = zscore <=  p.zscore_exit
                else:
                    hit_sl  = cdiff <= open_trade.stoploss_diff
                    hit_tgt = cdiff >= open_trade.target_diff
                    exit_z  = zscore >= -p.zscore_exit

                if hit_sl:
                    exit_reason = "STOPLOSS"
                    closed = True
                elif hit_tgt:
                    exit_reason = "TARGET"
                    closed = True
                elif exit_z:
                    exit_reason = "ZSCORE_REVERT"
                    closed = True

                if closed:
                    if open_trade.signal == "SELL_BUTTERFLY":
                        pnl_pts = open_trade.entry_diff - cdiff
                    else:
                        pnl_pts = cdiff - open_trade.entry_diff

                    pnl_inr = pnl_pts * open_trade.qty
                    open_trade.exit_date    = date
                    open_trade.exit_diff    = cdiff
                    open_trade.exit_reason  = exit_reason
                    open_trade.pnl_points   = round(pnl_pts, 2)
                    open_trade.pnl_inr      = round(pnl_inr, 2)
                    trades.append(open_trade)
                    open_trade = None
                    daily_pnl  += pnl_inr
                    equity     += pnl_inr

            equity_curve.append(equity)

            # ── Daily loss check ────────────────────────────────────────────
            if daily_pnl <= -p.max_daily_loss:
                continue

            # ── Entry signal ────────────────────────────────────────────────
            if open_trade is not None:
                continue   # one position at a time in basic backtest

            qty = self._compute_qty()

            if zscore > p.zscore_entry:
                signal = "SELL_BUTTERFLY"
                sl     = cdiff + p.sl_points
                tgt    = cdiff - p.tgt_points
            elif zscore < -p.zscore_entry:
                signal = "BUY_BUTTERFLY"
                sl     = cdiff - p.sl_points
                tgt    = cdiff + p.tgt_points
            else:
                signal = None

            if signal:
                open_trade = Trade(
                    entry_date    = date,
                    signal        = signal,
                    entry_diff    = cdiff,
                    stoploss_diff = sl,
                    target_diff   = tgt,
                    qty           = qty,
                )

        # Force-close any open position at end
        if open_trade is not None:
            last = df.iloc[-1]
            cdiff = last["curve_diff"]
            if open_trade.signal == "SELL_BUTTERFLY":
                pnl_pts = open_trade.entry_diff - cdiff
            else:
                pnl_pts = cdiff - open_trade.entry_diff
            pnl_inr = pnl_pts * open_trade.qty
            open_trade.exit_date   = last["date"]
            open_trade.exit_diff   = cdiff
            open_trade.exit_reason = "END_OF_DATA"
            open_trade.pnl_points  = round(pnl_pts, 2)
            open_trade.pnl_inr     = round(pnl_inr, 2)
            trades.append(open_trade)
            equity += pnl_inr

        trades_df = pd.DataFrame([t.__dict__ for t in trades])
        if not trades_df.empty:
            trades_df["equity_after"] = p.capital + trades_df["pnl_inr"].cumsum()

        return trades_df, equity_curve

    @staticmethod
    def report(trades_df: pd.DataFrame, equity_curve: list, capital: float):
        if trades_df.empty:
            log.info("No trades generated.")
            return

        wins  = trades_df[trades_df["pnl_inr"] > 0]
        loss  = trades_df[trades_df["pnl_inr"] <= 0]

        total_pnl    = trades_df["pnl_inr"].sum()
        win_rate     = len(wins) / len(trades_df) * 100
        avg_win      = wins["pnl_inr"].mean()  if len(wins)  else 0
        avg_loss     = loss["pnl_inr"].mean()  if len(loss)  else 0
        best_trade   = trades_df["pnl_inr"].max()
        worst_trade  = trades_df["pnl_inr"].min()

        # Max drawdown
        equity_arr = np.array(equity_curve, dtype=float)
        peak       = np.maximum.accumulate(equity_arr)
        drawdown   = (peak - equity_arr) / peak * 100
        max_dd     = drawdown.max()

        # Sharpe (daily returns on equity)
        returns    = np.diff(equity_arr) / equity_arr[:-1]
        sharpe     = (returns.mean() / returns.std() * np.sqrt(252)
                      if returns.std() > 0 else 0)

        print("\n" + "=" * 60)
        print("  NIFTY TRIPLE CALENDAR SPREAD – BACKTEST REPORT")
        print("=" * 60)
        print(f"  Total Trades   : {len(trades_df)}")
        print(f"  Win Rate       : {win_rate:.1f}%")
        print(f"  Total P&L      : ₹{total_pnl:,.0f}")
        print(f"  Avg Win        : ₹{avg_win:,.0f}")
        print(f"  Avg Loss       : ₹{avg_loss:,.0f}")
        print(f"  Best Trade     : ₹{best_trade:,.0f}")
        print(f"  Worst Trade    : ₹{worst_trade:,.0f}")
        print(f"  Max Drawdown   : {max_dd:.1f}%")
        print(f"  Sharpe Ratio   : {sharpe:.2f}")
        print(f"  Final Capital  : ₹{capital + total_pnl:,.0f}")
        print("=" * 60)

        # Exit reason breakdown
        print("\n  Exit Reasons:")
        print(trades_df["exit_reason"].value_counts().to_string())
        print()

        # Save trade log
        out = os.path.join(REPORT_DIR, "trades.csv")
        trades_df.to_csv(out, index=False)
        log.info(f"Trade log saved to {out}")

        # Equity curve CSV
        ec_df = pd.DataFrame({"equity": equity_curve})
        ec_out = os.path.join(REPORT_DIR, "equity_curve.csv")
        ec_df.to_csv(ec_out, index=False)
        log.info(f"Equity curve saved to {ec_out}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital",      type=float, default=500_000)
    ap.add_argument("--zscore_entry", type=float, default=2.0)
    ap.add_argument("--zscore_exit",  type=float, default=0.5)
    ap.add_argument("--lookback",     type=int,   default=20)
    ap.add_argument("--sl",           type=float, default=20.0)
    ap.add_argument("--tgt",          type=float, default=15.0)
    ap.add_argument("--max_daily_loss", type=float, default=5_000)
    args = ap.parse_args()

    if not os.path.exists(DATA_FILE):
        log.error(f"Data file not found: {DATA_FILE}")
        log.error("Run: python data_downloader.py first")
        return

    df = pd.read_csv(DATA_FILE)
    log.info(f"Loaded {len(df)} rows from {DATA_FILE}")

    params = BacktestParams(
        capital        = args.capital,
        zscore_entry   = args.zscore_entry,
        zscore_exit    = args.zscore_exit,
        lookback       = args.lookback,
        sl_points      = args.sl,
        tgt_points     = args.tgt,
        max_daily_loss = args.max_daily_loss,
    )

    bt             = Backtester(params)
    trades_df, ec  = bt.run(df)
    Backtester.report(trades_df, ec, params.capital)


if __name__ == "__main__":
    main()
