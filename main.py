import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Query
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

load_dotenv()

app = FastAPI(title="Trader Zero Market Data API")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)


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


@app.get("/")
def home():
    return {"status": "Trader Zero server online"}


@app.get("/market-snapshot")
def market_snapshot(
    symbol: str = Query(..., examples=["AAPL"]),
    timeframe: str = Query("1d", examples=["1d"])
):
    symbol = symbol.upper()

    end = datetime.now(timezone.utc)

    if timeframe in ["1m", "5m", "15m"]:
        start = end - timedelta(days=5)
    elif timeframe == "1h":
        start = end - timedelta(days=30)
    else:
        start = end - timedelta(days=120)

    trade_request = StockLatestTradeRequest(
        symbol_or_symbols=symbol,
        feed=DataFeed.IEX
    )
    latest_trade = client.get_stock_latest_trade(trade_request)

    if symbol not in latest_trade:
        return {
            "symbol": symbol,
            "error": "Nessun ultimo prezzo trovato per questo simbolo."
        }

    price = latest_trade[symbol].price

    bars_request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=get_timeframe(timeframe),
        start=start,
        end=end,
        limit=50,
        feed=DataFeed.IEX
    )

    bars = client.get_stock_bars(bars_request)

    candles = []

    if symbol not in bars.data:
        return {
            "symbol": symbol,
            "price": price,
            "error": "Nessuna candela trovata per questo simbolo/timeframe.",
            "timeframe": timeframe,
            "start": str(start),
            "end": str(end)
        }

    for bar in bars.data[symbol]:
        candles.append({
            "time": str(bar.timestamp),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })

    closes = [c["close"] for c in candles]

    trend = "neutral"
    if len(closes) >= 2:
        if closes[-1] > closes[0]:
            trend = "up"
        elif closes[-1] < closes[0]:
            trend = "down"

    support = min(c["low"] for c in candles) if candles else None
    resistance = max(c["high"] for c in candles) if candles else None

    return {
        "symbol": symbol,
        "price": price,
        "timeframe": timeframe,
        "trend": trend,
        "last_candles": candles[-10:],
        "volume": candles[-1]["volume"] if candles else None,
        "levels": {
            "support": support,
            "resistance": resistance
        }
    }
