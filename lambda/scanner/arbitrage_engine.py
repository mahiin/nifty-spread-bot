"""
Synthetic Futures Arbitrage Engine.

Exploits Put-Call Parity:
    Synthetic Future = Call − Put + Strike

If synthetic ≠ actual future by more than threshold → arbitrage trade.

Case 1: Synthetic > Actual → BUY Future, SELL Call, BUY Put
Case 2: Synthetic < Actual → SELL Future, BUY Call, SELL Put
"""
import os
import pandas as pd

THRESHOLD = float(os.environ.get("ARBITRAGE_THRESHOLD", "15"))
UNDERLYING = os.environ.get("UNDERLYING", "NIFTY")


def get_atm_strike(spot_price: float, step: int = 50) -> int:
    """Round spot to nearest 50 to get ATM strike."""
    return int(round(spot_price / step) * step)


def get_atm_option_symbols(broker, spot_price: float, expiry_date) -> dict:
    """
    Fetch ATM call & put symbols for the given expiry.
    Returns dict with call_symbol, put_symbol, strike.
    """
    df = broker.get_instruments("NFO")
    # Angel One expiry format: "30MAR2026"; expiry_date may be a Timestamp
    expiry_str = pd.Timestamp(expiry_date).strftime("%d%b%Y").upper()
    strike = get_atm_strike(spot_price)
    # Angel One stores strike * 100 as float string e.g. "2200000.000000"
    strike_val = float(strike * 100)

    opts = df[
        (df["name"] == UNDERLYING)
        & (df["expiry"] == expiry_str)
        & (df["strike"].astype(float) == strike_val)
        & (df["instrumenttype"].isin(["OPTIDX", "OPTSTK"]))
    ]

    call_rows = opts[opts["symbol"].str.endswith("CE")][["symbol", "token"]].values
    put_rows  = opts[opts["symbol"].str.endswith("PE")][["symbol", "token"]].values

    if not len(call_rows) or not len(put_rows):
        return {}

    return {
        "strike":       strike,
        "call_symbol":  call_rows[0][0],
        "call_token":   call_rows[0][1],
        "put_symbol":   put_rows[0][0],
        "put_token":    put_rows[0][1],
    }


def check_parity(broker, futures: dict, spot_price: float, quote_data: dict) -> dict:
    """
    Main arbitrage check.
    Uses near-month expiry options (most liquid).
    """
    try:
        opt_info = get_atm_option_symbols(broker, spot_price, futures["near_expiry"])
        if not opt_info:
            return {"arbitrage_signal": "NONE", "error": "Options not found"}

        call_sym   = opt_info["call_symbol"]
        put_sym    = opt_info["put_symbol"]
        call_token = opt_info["call_token"]
        put_token  = opt_info["put_token"]

        # Use token IDs for quote request; response keyed by symbol
        opt_quotes = broker.get_quote([f"NFO:{call_token}", f"NFO:{put_token}"])
        call_price = float(opt_quotes.get(f"NFO:{call_sym}", {}).get("ltp", 0) or 0)
        put_price  = float(opt_quotes.get(f"NFO:{put_sym}",  {}).get("ltp", 0) or 0)

        if call_price == 0 or put_price == 0:
            return {"arbitrage_signal": "NONE", "error": "Option prices zero"}

        strike = opt_info["strike"]
        near_key = f"NFO:{futures['near']}"
        actual_future = float(quote_data.get(near_key, {}).get("ltp", 0) or 0)

        synthetic = call_price - put_price + strike
        mispricing = synthetic - actual_future

        signal = "NONE"
        if mispricing > THRESHOLD:
            signal = "BUY_FUT_SELL_CALL_BUY_PUT"
        elif mispricing < -THRESHOLD:
            signal = "SELL_FUT_BUY_CALL_SELL_PUT"

        return {
            "arbitrage_signal": signal,
            "call_symbol":      call_sym,
            "put_symbol":       put_sym,
            "strike":           strike,
            "call_price":       call_price,
            "put_price":        put_price,
            "synthetic_future": round(synthetic, 2),
            "actual_future":    actual_future,
            "arb_mispricing":   round(mispricing, 2),
            "arb_stoploss":     round(mispricing + (12 if mispricing > 0 else -12), 2),
            "arb_target":       round(mispricing * 0.2, 2),  # target ~80% reversion
        }

    except Exception as exc:
        return {"arbitrage_signal": "NONE", "error": str(exc)}
