import os
from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import FastAPI, Query
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

load_dotenv()

app = FastAPI(title="Trader Zero Market Data API PRO")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)


# ---------------- TIMEFRAME ----------------
def get_timeframe(tf: str):
    if tf == "1m":
        return TimeFrame.Minute
    if tf == "5m":
        return TimeFrame(5, TimeFrameUnit.Minute)
    if tf == "15m":
        return TimeFrame(15, TimeFrameUnit.Minute)
    if tf == "1h":
        return TimeFrame.Hour
    return TimeFrame.Day


def get_date_range(timeframe: str):
    end = datetime.now(timezone.utc)

    if timeframe in ["1m", "5m", "15m"]:
        start = end - timedelta(days=5)
    elif timeframe == "1h":
        start = end - timedelta(days=30)
    else:
        start = end - timedelta(days=160)

    return start, end


# ---------------- CALCOLI ----------------
def calculate_atr(candles):
    if len(candles) < 15:
        return None

    trs = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        trs.append(tr)

    return round(sum(trs[-14:]) / 14, 4)


def calculate_momentum(candles, lookback=5):
    if len(candles) <= lookback:
        return 0

    return round(candles[-1]["close"] - candles[-lookback]["close"], 4)


def detect_trend(candles):
    closes = [c["close"] for c in candles]

    if len(closes) < 20:
        return "neutral"

    short_avg = sum(closes[-5:]) / 5
    mid_avg = sum(closes[-20:]) / 20

    if short_avg > mid_avg:
        return "up"
    if short_avg < mid_avg:
        return "down"

    return "neutral"


def detect_compression(candles, atr):
    if len(candles) < 20 or not atr:
        return False

    last_10 = candles[-10:]
    range_10 = max(c["high"] for c in last_10) - min(c["low"] for c in last_10)

    return range_10 < atr * 3


def get_levels(candles):
    recent = candles[-20:]

    support_near = min(c["low"] for c in recent)
    resistance_near = max(c["high"] for c in recent)

    return round(support_near, 4), round(resistance_near, 4)


def classify_setup(trend, momentum, compression, price, support, resistance, atr):
    if not atr:
        return "insufficient_data", "neutral", 0

    distance_to_support = abs(price - support)
    distance_to_resistance = abs(resistance - price)

    near_support = distance_to_support <= atr * 1.2
    near_resistance = distance_to_resistance <= atr * 1.2

    if compression and trend == "up" and momentum > 0:
        return "compression_breakout_long", "long", 7

    if compression and trend == "down" and momentum < 0:
        return "compression_breakdown_short", "short", 7

    if trend == "up" and near_support and momentum >= 0:
        return "pullback_long_near_support", "long", 6

    if trend == "down" and near_resistance and momentum <= 0:
        return "pullback_short_near_resistance", "short", 6

    if trend == "up" and near_resistance:
        return "possible_breakout_or_bull_trap", "long_conditional", 5

    if trend == "down" and near_support:
        return "possible_breakdown_or_bear_trap", "short_conditional", 5

    return "watchlist_only", "neutral", 3


def score_asset(trend, momentum, compression, price, support, resistance, atr, setup_base_score):
    score = setup_base_score

    if atr:
        distance_to_support = abs(price - support)
        distance_to_resistance = abs(resistance - price)

        if distance_to_support <= atr * 1.2 or distance_to_resistance <= atr * 1.2:
            score += 1.2

        if abs(momentum) >= atr:
            score += 1.0

        if compression:
            score += 1.3

    if trend in ["up", "down"]:
        score += 0.8

    return round(min(score, 10), 2)


# ---------------- FETCH DATI ----------------
def fetch_symbol_snapshot(symbol: str, timeframe: str):
    symbol = symbol.upper()
    start, end = get_date_range(timeframe)

    trade_request = StockLatestTradeRequest(
        symbol_or_symbols=symbol,
        feed=DataFeed.IEX
    )

    latest_trade = client.get_stock_latest_trade(trade_request)

    if symbol not in latest_trade:
        return {
            "symbol": symbol,
            "error": "Prezzo non disponibile"
        }

    price = latest_trade[symbol].price

    bars_request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=get_timeframe(timeframe),
        start=start,
        end=end,
        limit=100,
        feed=DataFeed.IEX
    )

    bars = client.get_stock_bars(bars_request)

    if symbol not in bars.data:
        return {
            "symbol": symbol,
            "price": price,
            "error": "Candele non disponibili"
        }

    candles = []
    for bar in bars.data[symbol]:
        candles.append({
            "time": str(bar.timestamp),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })

    if len(candles) < 20:
        return {
            "symbol": symbol,
            "price": price,
            "error": "Candele insufficienti"
        }

    atr = calculate_atr(candles)
    momentum = calculate_momentum(candles)
    trend = detect_trend(candles)
    compression = detect_compression(candles, atr)

    support, resistance = get_levels(candles)

    setup, bias, base_score = classify_setup(
        trend=trend,
        momentum=momentum,
        compression=compression,
        price=price,
        support=support,
        resistance=resistance,
        atr=atr
    )

    score = score_asset(
        trend=trend,
        momentum=momentum,
        compression=compression,
        price=price,
        support=support,
        resistance=resistance,
        atr=atr,
        setup_base_score=base_score
    )

    last_10 = candles[-10:]
    range_10 = round(max(c["high"] for c in last_10) - min(c["low"] for c in last_10), 4)

    return {
        "symbol": symbol,
        "price": price,
        "timeframe": timeframe,
        "trend": trend,
        "bias": bias,
        "setup": setup,
        "score": score,
        "momentum": momentum,
        "atr": atr,
        "compression": compression,
        "range_10": range_10,
        "levels": {
            "support_near": support,
            "resistance_near": resistance
        },
        "last_candle": candles[-1],
        "last_candles": candles[-10:]
    }


# ---------------- HOME ----------------
@app.get("/")
def home():
    return {
        "status": "Trader Zero PRO server online",
        "endpoints": ["/market-snapshot", "/scan-market"]
    }


# ---------------- SNAPSHOT SINGOLO ----------------
@app.get("/market-snapshot")
def market_snapshot(
    symbol: str = Query(...),
    timeframe: str = Query("1d")
):
    return fetch_symbol_snapshot(symbol, timeframe)


# ---------------- SCANNER MULTI-ASSET ----------------
@app.get("/scan-market")
def scan_market(
    symbols: str = Query("MSFT,AAPL,TSLA,NVDA,AMD,SPY"),
    timeframe: str = Query("1d"),
    top: int = Query(3)
):
    symbol_list = [
        s.strip().upper()
        for s in symbols.split(",")
        if s.strip()
    ]

    results = []

    for symbol in symbol_list:
        try:
            snapshot = fetch_symbol_snapshot(symbol, timeframe)

            if "error" not in snapshot:
                results.append(snapshot)
            else:
                results.append({
                    "symbol": symbol,
                    "error": snapshot["error"],
                    "score": 0
                })

        except Exception as e:
            results.append({
                "symbol": symbol,
                "error": str(e),
                "score": 0
            })

    ranked = sorted(
        results,
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    valid = [r for r in ranked if "error" not in r]
    errors = [r for r in ranked if "error" in r]

    return {
        "timeframe": timeframe,
        "symbols_scanned": symbol_list,
        "top": valid[:top],
        "all_ranked": valid,
        "errors": errors,
        "logic": {
            "score_factors": [
                "trend",
                "momentum",
                "compression",
                "vicinanza a supporto/resistenza",
                "ATR"
            ],
            "warning": "Lo scanner seleziona setup probabilistici, non segnali di ingresso."
        }
    }
