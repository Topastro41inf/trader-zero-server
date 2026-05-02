import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Query
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

load_dotenv()

app = FastAPI(title="Trader Zero Market Data API v3")

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
        start = end - timedelta(days=7)
    elif timeframe == "1h":
        start = end - timedelta(days=45)
    else:
        start = end - timedelta(days=180)

    return start, end


# ---------------- FRESHNESS ----------------
def parse_candle_time(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def freshness_status(last_time: Optional[datetime], timeframe: str):
    now = datetime.now(timezone.utc)

    if last_time is None:
        return {
            "status": "unknown",
            "age_minutes": None,
            "warning": "Timestamp non leggibile. Non considerare i dati realtime."
        }

    age_minutes = round((now - last_time).total_seconds() / 60, 2)

    if timeframe in ["1m", "5m"]:
        if age_minutes <= 20:
            status = "fresh"
            warning = None
        elif age_minutes <= 240:
            status = "delayed"
            warning = "Dati intraday non freschissimi. Usare solo per contesto, non per scalping aggressivo."
        else:
            status = "stale"
            warning = "Dati intraday vecchi. Non usare per decisioni operative realtime."

    elif timeframe == "15m":
        if age_minutes <= 60:
            status = "fresh"
            warning = None
        elif age_minutes <= 360:
            status = "delayed"
            warning = "Dati 15m ritardati. Validità operativa ridotta."
        else:
            status = "stale"
            warning = "Dati 15m vecchi. Non usarli come realtime."

    elif timeframe == "1h":
        if age_minutes <= 180:
            status = "fresh"
            warning = None
        elif age_minutes <= 1440:
            status = "delayed"
            warning = "Dati orari ritardati. Buoni per contesto, meno per timing."
        else:
            status = "stale"
            warning = "Dati orari vecchi. Setup da ricontrollare."

    else:
        if age_minutes <= 2880:
            status = "fresh"
            warning = None
        elif age_minutes <= 10080:
            status = "delayed"
            warning = "Dati daily non recentissimi. Verificare sessione corrente."
        else:
            status = "stale"
            warning = "Dati daily vecchi. Analisi non operativa."

    return {
        "status": status,
        "age_minutes": age_minutes,
        "warning": warning
    }


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


def score_asset(trend, momentum, compression, price, support, resistance, atr, setup_base_score, freshness):
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

    if freshness["status"] == "delayed":
        score -= 1.0

    if freshness["status"] == "stale":
        score -= 3.0

    return round(max(min(score, 10), 0), 2)


# ---------------- FETCH DATI ----------------
def fetch_symbol_snapshot(symbol: str, timeframe: str):
    symbol = symbol.upper()
    start, end = get_date_range(timeframe)

    server_timestamp = datetime.now(timezone.utc).isoformat()

    trade_request = StockLatestTradeRequest(
        symbol_or_symbols=symbol,
        feed=DataFeed.IEX
    )

    latest_trade = client.get_stock_latest_trade(trade_request)

    if symbol not in latest_trade:
        return {
            "symbol": symbol,
            "server_timestamp": server_timestamp,
            "error": "Prezzo non disponibile"
        }

    price = latest_trade[symbol].price
    trade_timestamp = getattr(latest_trade[symbol], "timestamp", None)

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
            "server_timestamp": server_timestamp,
            "trade_timestamp": str(trade_timestamp),
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
            "server_timestamp": server_timestamp,
            "trade_timestamp": str(trade_timestamp),
            "error": "Candele insufficienti"
        }

    last_candle_time = parse_candle_time(candles[-1]["time"])
    freshness = freshness_status(last_candle_time, timeframe)

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
        setup_base_score=base_score,
        freshness=freshness
    )

    last_10 = candles[-10:]
    range_10 = round(max(c["high"] for c in last_10) - min(c["low"] for c in last_10), 4)

    warnings = []

    if freshness["warning"]:
        warnings.append(freshness["warning"])

    if timeframe in ["1m", "5m"] and freshness["status"] != "fresh":
        warnings.append("Blocco scalping consigliato: dati non abbastanza freschi.")

    return {
        "symbol": symbol,
        "price": price,
        "timeframe": timeframe,

        "server_timestamp": server_timestamp,
        "trade_timestamp": str(trade_timestamp),
        "last_candle_timestamp": candles[-1]["time"],

        "freshness": freshness,
        "warnings": warnings,

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


# ---------------- HOME / HEALTH ----------------
@app.get("/")
def home():
    return {
        "status": "Trader Zero PRO server online",
        "version": "v3",
        "endpoints": ["/health", "/market-snapshot", "/scan-market"]
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "alpaca_keys_loaded": bool(API_KEY and SECRET_KEY)
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
                    "score": 0,
                    "warnings": snapshot.get("warnings", [])
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

    global_warnings = []

    stale_count = sum(
        1 for r in valid
        if r.get("freshness", {}).get("status") == "stale"
    )

    delayed_count = sum(
        1 for r in valid
        if r.get("freshness", {}).get("status") == "delayed"
    )

    if stale_count > 0:
        global_warnings.append(f"{stale_count} asset hanno dati vecchi.")

    if delayed_count > 0:
        global_warnings.append(f"{delayed_count} asset hanno dati ritardati.")

    if timeframe in ["1m", "5m"] and (stale_count > 0 or delayed_count > 0):
        global_warnings.append("Scanner intraday non execution-grade: evitare scalping aggressivo.")

    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "timeframe": timeframe,
        "symbols_scanned": symbol_list,
        "top": valid[:top],
        "all_ranked": valid,
        "errors": errors,
        "global_warnings": global_warnings,
        "logic": {
            "score_factors": [
                "trend",
                "momentum",
                "compression",
                "vicinanza a supporto/resistenza",
                "ATR",
                "freshness penalty"
            ],
            "warning": "Lo scanner seleziona setup probabilistici, non segnali di ingresso."
        }
    }
