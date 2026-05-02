import os
from datetime import datetime, timedelta, timezone

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


# ---------------- ATR ----------------
def calculate_atr(candles):
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

    return sum(trs[-14:]) / 14 if len(trs) >= 14 else None


# ---------------- HOME ----------------
@app.get("/")
def home():
    return {"status": "Trader Zero PRO server online"}


# ---------------- SNAPSHOT ----------------
@app.get("/market-snapshot")
def market_snapshot(
    symbol: str = Query(...),
    timeframe: str = Query("1d")
):
    symbol = symbol.upper()

    end = datetime.now(timezone.utc)

    if timeframe in ["1m", "5m", "15m"]:
        start = end - timedelta(days=5)
    elif timeframe == "1h":
        start = end - timedelta(days=30)
    else:
        start = end - timedelta(days=120)

    # -------- PREZZO --------
    trade_request = StockLatestTradeRequest(
        symbol_or_symbols=symbol,
        feed=DataFeed.IEX
    )
    latest_trade = client.get_stock_latest_trade(trade_request)

    if symbol not in latest_trade:
        return {"error": "Prezzo non disponibile"}

    price = latest_trade[symbol].price

    # -------- CANDELE --------
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
        return {"error": "Candele non disponibili"}

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

    # -------- TREND --------
    closes = [c["close"] for c in candles]

    trend = "neutral"
    if closes[-1] > closes[0]:
        trend = "up"
    elif closes[-1] < closes[0]:
        trend = "down"

    # -------- SUPPORTI VICINI --------
    recent_lows = [c["low"] for c in candles[-20:]]
    recent_highs = [c["high"] for c in candles[-20:]]

    support_near = min(recent_lows)
    resistance_near = max(recent_highs)

    # -------- RANGE --------
    last_10 = candles[-10:]
    range_10 = max(c["high"] for c in last_10) - min(c["low"] for c in last_10)

    # -------- MOMENTUM --------
    momentum = closes[-1] - closes[-5] if len(closes) >= 5 else 0

    # -------- ATR --------
    atr = calculate_atr(candles)

    # -------- ULTIMA CANDELA --------
    last_candle = candles[-1]

    return {
        "symbol": symbol,
        "price": price,
        "timeframe": timeframe,
        "trend": trend,

        "momentum": momentum,
        "atr": atr,
        "range_10": range_10,

        "last_candle": last_candle,

        "levels": {
            "support_near": support_near,
            "resistance_near": resistance_near
        },

        "last_candles": candles[-10:]
    }
