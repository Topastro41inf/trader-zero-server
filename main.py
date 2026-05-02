import os
import requests
from datetime import datetime, timedelta, timezone, time
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

load_dotenv()

app = FastAPI(title="Trader Zero Market Data API v6 Multi-Asset")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

NY_TZ = ZoneInfo("America/New_York")


# ============================================================
# MARKET HOURS USA
# ============================================================
def get_us_market_status():
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(NY_TZ)

    weekday = now_ny.weekday()
    current_time = now_ny.time()

    regular_open = time(9, 30)
    regular_close = time(16, 0)
    premarket_open = time(4, 0)
    afterhours_close = time(20, 0)

    if weekday >= 5:
        status = "weekend_closed"
        tradable_context = "market_closed"
        scalping_allowed = False
        message = "Mercato USA chiuso per weekend. Dati validi come ultimo riferimento disponibile, non per realtime."
    elif premarket_open <= current_time < regular_open:
        status = "premarket"
        tradable_context = "limited_liquidity"
        scalping_allowed = False
        message = "Premarket USA: liquidità ridotta. Evitare scalping aggressivo."
    elif regular_open <= current_time <= regular_close:
        status = "regular_open"
        tradable_context = "market_open"
        scalping_allowed = True
        message = "Mercato USA aperto in sessione regolare."
    elif regular_close < current_time <= afterhours_close:
        status = "afterhours"
        tradable_context = "limited_liquidity"
        scalping_allowed = False
        message = "Afterhours USA: liquidità ridotta. Evitare scalping aggressivo."
    else:
        status = "closed"
        tradable_context = "market_closed"
        scalping_allowed = False
        message = "Mercato USA chiuso. Dati validi come ultimo riferimento disponibile."

    return {
        "status": status,
        "tradable_context": tradable_context,
        "scalping_allowed": scalping_allowed,
        "now_utc": now_utc.isoformat(),
        "now_new_york": now_ny.isoformat(),
        "message": message
    }


# ============================================================
# TIMEFRAME
# ============================================================
def get_alpaca_timeframe(tf: str):
    if tf == "1m":
        return TimeFrame.Minute
    if tf == "5m":
        return TimeFrame(5, TimeFrameUnit.Minute)
    if tf == "15m":
        return TimeFrame(15, TimeFrameUnit.Minute)
    if tf == "1h":
        return TimeFrame.Hour
    return TimeFrame.Day


def get_twelve_interval(tf: str):
    mapping = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
        "1d": "1day"
    }
    return mapping.get(tf, "1day")


def get_date_range(timeframe: str):
    end = datetime.now(timezone.utc)

    if timeframe in ["1m", "5m", "15m"]:
        start = end - timedelta(days=10)
    elif timeframe == "1h":
        start = end - timedelta(days=60)
    else:
        start = end - timedelta(days=220)

    return start, end


# ============================================================
# GENERIC CALCOLI
# ============================================================
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


def score_asset(trend, momentum, compression, price, support, resistance, atr, setup_base_score, penalty=0):
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

    score -= penalty

    return round(max(min(score, 10), 0), 2)


# ============================================================
# FINNHUB EQUITY QUOTE
# ============================================================
def quote_freshness_status(quote_time: Optional[datetime], market_status):
    if quote_time is None:
        return {
            "status": "unknown",
            "age_minutes": None,
            "data_context": "unknown",
            "warning": "Timestamp Finnhub non disponibile."
        }

    now = datetime.now(timezone.utc)
    age_minutes = round((now - quote_time).total_seconds() / 60, 2)

    market_state = market_status["status"]

    if market_state in ["weekend_closed", "closed"]:
        return {
            "status": "market_closed_reference",
            "age_minutes": age_minutes,
            "data_context": "last_available_quote",
            "warning": "Mercato chiuso: quote valida come ultimo riferimento disponibile, non come realtime."
        }

    if market_state in ["premarket", "afterhours"]:
        if age_minutes <= 60:
            return {
                "status": "extended_hours_reference",
                "age_minutes": age_minutes,
                "data_context": "thin_liquidity",
                "warning": "Sessione estesa: liquidità ridotta. Non usare per scalping aggressivo."
            }

        return {
            "status": "delayed_extended_hours",
            "age_minutes": age_minutes,
            "data_context": "delayed",
            "warning": "Quote non fresca in sessione estesa."
        }

    if age_minutes <= 20:
        return {
            "status": "fresh",
            "age_minutes": age_minutes,
            "data_context": "realtime_context",
            "warning": None
        }

    if age_minutes <= 240:
        return {
            "status": "delayed",
            "age_minutes": age_minutes,
            "data_context": "delayed",
            "warning": "Quote Finnhub ritardata. Prudenza su timing intraday."
        }

    return {
        "status": "stale",
        "age_minutes": age_minutes,
        "data_context": "stale",
        "warning": "Quote Finnhub vecchia. Non usarla per realtime."
    }


def fetch_finnhub_quote(symbol: str, market_status):
    if not FINNHUB_API_KEY:
        return {
            "source": "finnhub",
            "error": "FINNHUB_API_KEY non configurata"
        }

    try:
        response = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={
                "symbol": symbol.upper(),
                "token": FINNHUB_API_KEY
            },
            timeout=8
        )
        response.raise_for_status()
        data = response.json()

        current_price = data.get("c")
        timestamp = data.get("t")

        if not current_price or current_price == 0:
            return {
                "source": "finnhub",
                "symbol": symbol.upper(),
                "error": "Quote Finnhub non disponibile o zero",
                "raw": data
            }

        quote_time = (
            datetime.fromtimestamp(timestamp, tz=timezone.utc)
            if timestamp
            else None
        )

        freshness = quote_freshness_status(quote_time, market_status)

        return {
            "source": "finnhub",
            "symbol": symbol.upper(),
            "price": current_price,
            "change": data.get("d"),
            "change_percent": data.get("dp"),
            "high": data.get("h"),
            "low": data.get("l"),
            "open": data.get("o"),
            "previous_close": data.get("pc"),
            "timestamp": quote_time.isoformat() if quote_time else None,
            "freshness": freshness,
            "raw": data
        }

    except Exception as e:
        return {
            "source": "finnhub",
            "symbol": symbol.upper(),
            "error": str(e)
        }


# ============================================================
# EQUITY CANDLE FRESHNESS
# ============================================================
def parse_candle_time(value: str) -> Optional[datetime]:
    try:
        clean = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def candle_freshness_status(last_time: Optional[datetime], timeframe: str, market_status):
    if last_time is None:
        return {
            "status": "unknown",
            "age_minutes": None,
            "data_context": "unknown",
            "warning": "Timestamp candela non leggibile."
        }

    now = datetime.now(timezone.utc)
    age_minutes = round((now - last_time).total_seconds() / 60, 2)

    market_state = market_status["status"]

    if market_state in ["weekend_closed", "closed"]:
        return {
            "status": "market_closed_reference",
            "age_minutes": age_minutes,
            "data_context": "last_session_candles",
            "warning": "Mercato chiuso: candele valide come ultima sessione disponibile, non realtime."
        }

    if market_state in ["premarket", "afterhours"]:
        return {
            "status": "extended_hours_context",
            "age_minutes": age_minutes,
            "data_context": "thin_liquidity",
            "warning": "Sessione estesa: candele da usare con prudenza."
        }

    if timeframe in ["1m", "5m"]:
        if age_minutes <= 20:
            status = "fresh"
            warning = None
        elif age_minutes <= 240:
            status = "delayed"
            warning = "Candele intraday non freschissime. Usare solo per contesto."
        else:
            status = "stale"
            warning = "Candele intraday vecchie. Non usare per decisioni operative realtime."
    elif timeframe == "15m":
        if age_minutes <= 60:
            status = "fresh"
            warning = None
        elif age_minutes <= 360:
            status = "delayed"
            warning = "Candele 15m ritardate. Validità operativa ridotta."
        else:
            status = "stale"
            warning = "Candele 15m vecchie."
    elif timeframe == "1h":
        if age_minutes <= 180:
            status = "fresh"
            warning = None
        elif age_minutes <= 1440:
            status = "delayed"
            warning = "Candele orarie ritardate. Buone per contesto, meno per timing."
        else:
            status = "stale"
            warning = "Candele orarie vecchie."
    else:
        if age_minutes <= 2880:
            status = "fresh"
            warning = None
        elif age_minutes <= 10080:
            status = "delayed"
            warning = "Candele daily non recentissime. Verificare sessione corrente."
        else:
            status = "stale"
            warning = "Candele daily vecchie."

    return {
        "status": status,
        "age_minutes": age_minutes,
        "data_context": status,
        "warning": warning
    }


def combined_freshness(candle_freshness, quote_freshness, timeframe: str, market_status):
    warnings = []

    if market_status.get("message"):
        warnings.append(market_status["message"])

    if candle_freshness.get("warning"):
        warnings.append(candle_freshness["warning"])

    if quote_freshness and quote_freshness.get("warning"):
        warnings.append(quote_freshness["warning"])

    market_state = market_status["status"]
    candle_status = candle_freshness.get("status")
    quote_status = quote_freshness.get("status") if quote_freshness else "unknown"

    if market_state in ["weekend_closed", "closed"]:
        return {
            "status": "market_closed",
            "data_context": "last_session_reference",
            "candle_status": candle_status,
            "quote_status": quote_status,
            "scalping_allowed": False,
            "daytrading_allowed": False,
            "watchlist_allowed": True,
            "warnings": warnings + [
                "Operatività realtime bloccata: mercato chiuso.",
                "Consentita solo analisi watchlist/preparazione."
            ]
        }

    if market_state in ["premarket", "afterhours"]:
        return {
            "status": "extended_hours",
            "data_context": "limited_liquidity",
            "candle_status": candle_status,
            "quote_status": quote_status,
            "scalping_allowed": False,
            "daytrading_allowed": "conditional",
            "watchlist_allowed": True,
            "warnings": warnings + [
                "Sessione estesa: evitare scalping aggressivo.",
                "Setup validi solo come monitoraggio condizionale."
            ]
        }

    if timeframe in ["1m", "5m"]:
        if candle_status == "fresh" and quote_status == "fresh":
            return {
                "status": "execution_context",
                "data_context": "realtime_candidate",
                "candle_status": candle_status,
                "quote_status": quote_status,
                "scalping_allowed": True,
                "daytrading_allowed": True,
                "watchlist_allowed": True,
                "warnings": warnings
            }

        return {
            "status": "not_execution_grade",
            "data_context": "intraday_context_only",
            "candle_status": candle_status,
            "quote_status": quote_status,
            "scalping_allowed": False,
            "daytrading_allowed": "conditional",
            "watchlist_allowed": True,
            "warnings": warnings + [
                "Dati non execution-grade per scalping.",
                "Consentiti solo setup condizionali o watchlist."
            ]
        }

    if candle_status in ["fresh", "delayed"] or quote_status in ["fresh", "delayed"]:
        return {
            "status": "context_ok",
            "data_context": "operational_context",
            "candle_status": candle_status,
            "quote_status": quote_status,
            "scalping_allowed": False,
            "daytrading_allowed": True,
            "watchlist_allowed": True,
            "warnings": warnings
        }

    return {
        "status": "context_weak",
        "data_context": "weak_data_context",
        "candle_status": candle_status,
        "quote_status": quote_status,
        "scalping_allowed": False,
        "daytrading_allowed": False,
        "watchlist_allowed": True,
        "warnings": warnings + [
            "Contesto dati debole: usare solo come watchlist."
        ]
    }


# ============================================================
# EQUITY DATA — ALPACA + FINNHUB
# ============================================================
def fetch_alpaca_candles(symbol: str, timeframe: str):
    symbol = symbol.upper()
    start, end = get_date_range(timeframe)

    trade_request = StockLatestTradeRequest(
        symbol_or_symbols=symbol,
        feed=DataFeed.IEX
    )

    latest_trade = client.get_stock_latest_trade(trade_request)

    alpaca_price = None
    alpaca_trade_timestamp = None

    if symbol in latest_trade:
        alpaca_price = latest_trade[symbol].price
        alpaca_trade_timestamp = getattr(latest_trade[symbol], "timestamp", None)

    bars_request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=get_alpaca_timeframe(timeframe),
        start=start,
        end=end,
        limit=100,
        feed=DataFeed.IEX
    )

    bars = client.get_stock_bars(bars_request)

    if symbol not in bars.data:
        return {
            "symbol": symbol,
            "alpaca_price": alpaca_price,
            "alpaca_trade_timestamp": str(alpaca_trade_timestamp),
            "error": "Candele Alpaca non disponibili"
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

    return {
        "symbol": symbol,
        "alpaca_price": alpaca_price,
        "alpaca_trade_timestamp": str(alpaca_trade_timestamp),
        "candles": candles
    }


def fetch_symbol_snapshot(symbol: str, timeframe: str):
    symbol = symbol.upper()
    server_timestamp = datetime.now(timezone.utc).isoformat()
    market_status = get_us_market_status()

    finnhub_quote = fetch_finnhub_quote(symbol, market_status)
    alpaca_data = fetch_alpaca_candles(symbol, timeframe)

    if "error" in alpaca_data:
        return {
            "symbol": symbol,
            "server_timestamp": server_timestamp,
            "market_status": market_status,
            "finnhub_quote": finnhub_quote,
            "error": alpaca_data["error"]
        }

    candles = alpaca_data["candles"]

    if len(candles) < 20:
        return {
            "symbol": symbol,
            "server_timestamp": server_timestamp,
            "market_status": market_status,
            "finnhub_quote": finnhub_quote,
            "error": "Candele insufficienti"
        }

    quote_price = finnhub_quote.get("price") if "error" not in finnhub_quote else None
    alpaca_price = alpaca_data.get("alpaca_price")
    price = quote_price or alpaca_price or candles[-1]["close"]

    last_candle_time = parse_candle_time(candles[-1]["time"])

    candle_freshness = candle_freshness_status(
        last_candle_time,
        timeframe,
        market_status
    )

    quote_freshness = finnhub_quote.get("freshness") if "error" not in finnhub_quote else {
        "status": "unknown",
        "age_minutes": None,
        "data_context": "unknown",
        "warning": finnhub_quote.get("error", "Quote Finnhub non disponibile")
    }

    combined = combined_freshness(
        candle_freshness,
        quote_freshness,
        timeframe,
        market_status
    )

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

    penalty = 0
    if combined.get("status") in ["market_closed", "extended_hours"]:
        penalty = 1.0
    elif combined.get("status") == "not_execution_grade":
        penalty = 1.5
    elif combined.get("status") == "context_weak":
        penalty = 2.5

    score = score_asset(
        trend=trend,
        momentum=momentum,
        compression=compression,
        price=price,
        support=support,
        resistance=resistance,
        atr=atr,
        setup_base_score=base_score,
        penalty=penalty
    )

    last_10 = candles[-10:]
    range_10 = round(max(c["high"] for c in last_10) - min(c["low"] for c in last_10), 4)

    price_sources = {
        "primary": "finnhub" if quote_price else "alpaca_or_candle",
        "finnhub_price": quote_price,
        "alpaca_price": alpaca_price,
        "last_candle_close": candles[-1]["close"]
    }

    price_discrepancy = None
    if quote_price and candles[-1]["close"]:
        price_discrepancy = round(quote_price - candles[-1]["close"], 4)

    return {
        "asset_class": "equity",
        "symbol": symbol,
        "price": price,
        "timeframe": timeframe,
        "server_timestamp": server_timestamp,

        "market_status": market_status,

        "data_sources": {
            "quote": "finnhub",
            "candles": "alpaca_iex"
        },

        "price_sources": price_sources,
        "price_discrepancy_vs_last_candle": price_discrepancy,

        "finnhub_quote": finnhub_quote,
        "alpaca_trade_timestamp": alpaca_data.get("alpaca_trade_timestamp"),
        "last_candle_timestamp": candles[-1]["time"],

        "candle_freshness": candle_freshness,
        "quote_freshness": quote_freshness,
        "combined_freshness": combined,
        "warnings": combined.get("warnings", []),

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


# ============================================================
# COMMODITIES — TWELVE DATA
# ============================================================
COMMODITY_SYMBOLS = {
    "GOLD": "XAU/USD",
    "ORO": "XAU/USD",
    "XAU": "XAU/USD",
    "XAUUSD": "XAU/USD",
    "XAU/USD": "XAU/USD",

    "SILVER": "XAG/USD",
    "ARGENTO": "XAG/USD",
    "XAG": "XAG/USD",
    "XAGUSD": "XAG/USD",
    "XAG/USD": "XAG/USD",

    "OIL": "WTI/USD",
    "WTI": "WTI/USD",
    "CRUDE": "WTI/USD",
    "PETROLIO": "WTI/USD",

    "BRENT": "BRENT/USD",
    "NATGAS": "NATURAL_GAS/USD",
    "NATURALGAS": "NATURAL_GAS/USD",
    "GAS": "NATURAL_GAS/USD",
    "GAS_NATURALE": "NATURAL_GAS/USD",

    "COPPER": "COPPER/USD",
    "RAME": "COPPER/USD",

    "WHEAT": "WHEAT/USD",
    "GRANO": "WHEAT/USD",

    "CORN": "CORN/USD",
    "MAIS": "CORN/USD"
}


def normalize_commodity_symbol(symbol: str):
    raw = symbol.strip().upper()
    return COMMODITY_SYMBOLS.get(raw, symbol.strip())


def fetch_twelve_time_series(symbol: str, timeframe: str):
    if not TWELVE_DATA_API_KEY:
        return {
            "source": "twelve_data",
            "error": "TWELVE_DATA_API_KEY non configurata"
        }

    normalized = normalize_commodity_symbol(symbol)
    interval = get_twelve_interval(timeframe)

    try:
        response = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": normalized,
                "interval": interval,
                "outputsize": 100,
                "apikey": TWELVE_DATA_API_KEY
            },
            timeout=12
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "error":
            return {
                "source": "twelve_data",
                "symbol": symbol,
                "normalized_symbol": normalized,
                "error": data.get("message", "Errore Twelve Data"),
                "raw": data
            }

        values = data.get("values", [])

        if not values:
            return {
                "source": "twelve_data",
                "symbol": symbol,
                "normalized_symbol": normalized,
                "error": "Nessuna candela commodity disponibile",
                "raw": data
            }

        candles_desc = []

        for item in values:
            try:
                candles_desc.append({
                    "time": item.get("datetime"),
                    "open": float(item.get("open")),
                    "high": float(item.get("high")),
                    "low": float(item.get("low")),
                    "close": float(item.get("close")),
                    "volume": float(item.get("volume", 0)) if item.get("volume") is not None else None
                })
            except Exception:
                continue

        candles = list(reversed(candles_desc))

        return {
            "source": "twelve_data",
            "symbol": symbol,
            "normalized_symbol": normalized,
            "interval": interval,
            "meta": data.get("meta", {}),
            "candles": candles,
            "raw_status": data.get("status")
        }

    except Exception as e:
        return {
            "source": "twelve_data",
            "symbol": symbol,
            "normalized_symbol": normalized,
            "error": str(e)
        }


def fetch_twelve_quote(symbol: str):
    if not TWELVE_DATA_API_KEY:
        return {
            "source": "twelve_data",
            "error": "TWELVE_DATA_API_KEY non configurata"
        }

    normalized = normalize_commodity_symbol(symbol)

    try:
        response = requests.get(
            "https://api.twelvedata.com/quote",
            params={
                "symbol": normalized,
                "apikey": TWELVE_DATA_API_KEY
            },
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "error":
            return {
                "source": "twelve_data",
                "symbol": symbol,
                "normalized_symbol": normalized,
                "error": data.get("message", "Errore Twelve Data quote"),
                "raw": data
            }

        close_value = data.get("close")
        price = float(close_value) if close_value not in [None, ""] else None

        return {
            "source": "twelve_data",
            "symbol": symbol,
            "normalized_symbol": normalized,
            "price": price,
            "name": data.get("name"),
            "exchange": data.get("exchange"),
            "currency": data.get("currency"),
            "datetime": data.get("datetime"),
            "timestamp": data.get("timestamp"),
            "open": float(data["open"]) if data.get("open") not in [None, ""] else None,
            "high": float(data["high"]) if data.get("high") not in [None, ""] else None,
            "low": float(data["low"]) if data.get("low") not in [None, ""] else None,
            "previous_close": float(data["previous_close"]) if data.get("previous_close") not in [None, ""] else None,
            "change": float(data["change"]) if data.get("change") not in [None, ""] else None,
            "percent_change": float(data["percent_change"]) if data.get("percent_change") not in [None, ""] else None,
            "raw": data
        }

    except Exception as e:
        return {
            "source": "twelve_data",
            "symbol": symbol,
            "normalized_symbol": normalized,
            "error": str(e)
        }


def commodity_freshness(last_time: Optional[datetime], timeframe: str):
    if last_time is None:
        return {
            "status": "unknown",
            "age_minutes": None,
            "scalping_allowed": False,
            "warning": "Timestamp commodity non leggibile."
        }

    now = datetime.now(timezone.utc)
    age_minutes = round((now - last_time).total_seconds() / 60, 2)

    if timeframe in ["1m", "5m"]:
        if age_minutes <= 20:
            return {
                "status": "fresh",
                "age_minutes": age_minutes,
                "scalping_allowed": True,
                "warning": None
            }
        if age_minutes <= 180:
            return {
                "status": "delayed",
                "age_minutes": age_minutes,
                "scalping_allowed": False,
                "warning": "Commodity intraday ritardata. Vietato scalping, consentito solo setup condizionale."
            }
        return {
            "status": "stale",
            "age_minutes": age_minutes,
            "scalping_allowed": False,
            "warning": "Commodity intraday vecchia. Non usare per operatività realtime."
        }

    if timeframe == "15m":
        if age_minutes <= 60:
            return {
                "status": "fresh",
                "age_minutes": age_minutes,
                "scalping_allowed": False,
                "warning": None
            }
        if age_minutes <= 360:
            return {
                "status": "delayed",
                "age_minutes": age_minutes,
                "scalping_allowed": False,
                "warning": "Commodity 15m ritardata. Usare con prudenza."
            }
        return {
            "status": "stale",
            "age_minutes": age_minutes,
            "scalping_allowed": False,
            "warning": "Commodity 15m vecchia."
        }

    if timeframe == "1h":
        if age_minutes <= 180:
            return {
                "status": "fresh",
                "age_minutes": age_minutes,
                "scalping_allowed": False,
                "warning": None
            }
        if age_minutes <= 1440:
            return {
                "status": "delayed",
                "age_minutes": age_minutes,
                "scalping_allowed": False,
                "warning": "Commodity 1h ritardata. Buona per contesto, non per timing aggressivo."
            }
        return {
            "status": "stale",
            "age_minutes": age_minutes,
            "scalping_allowed": False,
            "warning": "Commodity 1h vecchia."
        }

    if age_minutes <= 2880:
        return {
            "status": "fresh_or_last_session",
            "age_minutes": age_minutes,
            "scalping_allowed": False,
            "warning": "Dati daily validi per contesto/preparazione, non per scalping."
        }

    return {
        "status": "stale",
        "age_minutes": age_minutes,
        "scalping_allowed": False,
        "warning": "Commodity daily vecchia. Ricontrollare il feed."
    }


def fetch_commodity_snapshot(symbol: str, timeframe: str):
    server_timestamp = datetime.now(timezone.utc).isoformat()

    series = fetch_twelve_time_series(symbol, timeframe)
    quote = fetch_twelve_quote(symbol)

    if "error" in series:
        return {
            "asset_class": "commodity",
            "symbol": symbol,
            "server_timestamp": server_timestamp,
            "data_source": "twelve_data",
            "quote": quote,
            "error": series["error"],
            "hint": "Prova /commodity-search per verificare il simbolo esatto."
        }

    candles = series.get("candles", [])

    if len(candles) < 20:
        return {
            "asset_class": "commodity",
            "symbol": symbol,
            "normalized_symbol": series.get("normalized_symbol"),
            "server_timestamp": server_timestamp,
            "data_source": "twelve_data",
            "quote": quote,
            "error": "Candele commodity insufficienti"
        }

    last_time = parse_candle_time(candles[-1]["time"])
    freshness = commodity_freshness(last_time, timeframe)

    quote_price = quote.get("price") if "error" not in quote else None
    price = quote_price or candles[-1]["close"]

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

    penalty = 0
    if freshness["status"] in ["delayed", "fresh_or_last_session"]:
        penalty = 0.8
    elif freshness["status"] == "stale":
        penalty = 2.5

    score = score_asset(
        trend=trend,
        momentum=momentum,
        compression=compression,
        price=price,
        support=support,
        resistance=resistance,
        atr=atr,
        setup_base_score=base_score,
        penalty=penalty
    )

    last_10 = candles[-10:]
    range_10 = round(max(c["high"] for c in last_10) - min(c["low"] for c in last_10), 4)

    warnings = []
    if freshness.get("warning"):
        warnings.append(freshness["warning"])

    if not freshness.get("scalping_allowed"):
        warnings.append("Scalping commodity bloccato salvo dati intraday freschi e confermati.")

    return {
        "asset_class": "commodity",
        "symbol": symbol,
        "normalized_symbol": series.get("normalized_symbol"),
        "price": price,
        "timeframe": timeframe,
        "server_timestamp": server_timestamp,

        "data_source": "twelve_data",
        "quote": quote,

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

        "operational_map": {
            "long_activation_above": resistance,
            "short_activation_below": support,
            "no_trade_zone": f"{support} - {resistance}",
            "long_invalid_below": support,
            "short_invalid_above": resistance,
            "note": "Non è un ordine operativo. È una mappa condizionale."
        },

        "last_candle": candles[-1],
        "last_candles": candles[-10:]
    }


def commodity_search(query: str):
    if not TWELVE_DATA_API_KEY:
        return {
            "source": "twelve_data",
            "error": "TWELVE_DATA_API_KEY non configurata"
        }

    try:
        response = requests.get(
            "https://api.twelvedata.com/symbol_search",
            params={
                "symbol": query,
                "apikey": TWELVE_DATA_API_KEY
            },
            timeout=10
        )
        response.raise_for_status()
        return {
            "source": "twelve_data",
            "query": query,
            "result": response.json()
        }
    except Exception as e:
        return {
            "source": "twelve_data",
            "query": query,
            "error": str(e)
        }


# ============================================================
# ROUTES
# ============================================================
@app.get("/")
def home():
    return {
        "status": "Trader Zero PRO server online",
        "version": "v6-multi-asset-commodities",
        "endpoints": [
            "/health",
            "/market-status",
            "/quote-fresh",
            "/market-snapshot",
            "/scan-market",
            "/commodity-search",
            "/commodity-snapshot",
            "/scan-commodities"
        ]
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "v6-multi-asset-commodities",
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "market_status": get_us_market_status(),
        "alpaca_keys_loaded": bool(API_KEY and SECRET_KEY),
        "finnhub_key_loaded": bool(FINNHUB_API_KEY),
        "twelve_data_key_loaded": bool(TWELVE_DATA_API_KEY)
    }


@app.get("/market-status")
def market_status():
    return get_us_market_status()


@app.get("/quote-fresh")
def quote_fresh(symbol: str = Query(...)):
    market_status_data = get_us_market_status()

    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "market_status": market_status_data,
        "quote": fetch_finnhub_quote(symbol, market_status_data)
    }


@app.get("/market-snapshot")
def market_snapshot(
    symbol: str = Query(...),
    timeframe: str = Query("1d")
):
    return fetch_symbol_snapshot(symbol, timeframe)


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

    market_status_data = get_us_market_status()
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
                    "market_status": market_status_data,
                    "finnhub_quote": snapshot.get("finnhub_quote")
                })

        except Exception as e:
            results.append({
                "symbol": symbol,
                "error": str(e),
                "score": 0,
                "market_status": market_status_data
            })

    ranked = sorted(
        results,
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    valid = [r for r in ranked if "error" not in r]
    errors = [r for r in ranked if "error" in r]

    global_warnings = []

    if market_status_data["status"] in ["weekend_closed", "closed"]:
        global_warnings.append(
            "Mercato chiuso: scanner valido solo per watchlist/preparazione, non per operatività immediata."
        )

    if market_status_data["status"] in ["premarket", "afterhours"]:
        global_warnings.append(
            "Sessione estesa: liquidità ridotta, evitare scalping aggressivo."
        )

    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "v6-multi-asset-commodities",
        "asset_class": "equity",
        "market_status": market_status_data,
        "timeframe": timeframe,
        "symbols_scanned": symbol_list,
        "top": valid[:top],
        "all_ranked": valid,
        "errors": errors,
        "global_warnings": global_warnings
    }


@app.get("/commodity-search")
def commodity_search_route(query: str = Query(...)):
    return commodity_search(query)


@app.get("/commodity-snapshot")
def commodity_snapshot(
    symbol: str = Query("GOLD"),
    timeframe: str = Query("1h")
):
    return fetch_commodity_snapshot(symbol, timeframe)


@app.get("/scan-commodities")
def scan_commodities(
    symbols: str = Query("GOLD,SILVER,OIL,BRENT,NATGAS,COPPER"),
    timeframe: str = Query("1h"),
    top: int = Query(3)
):
    symbol_list = [
        s.strip()
        for s in symbols.split(",")
        if s.strip()
    ]

    results = []

    for symbol in symbol_list:
        try:
            snapshot = fetch_commodity_snapshot(symbol, timeframe)

            if "error" not in snapshot:
                results.append(snapshot)
            else:
                results.append({
                    "symbol": symbol,
                    "normalized_symbol": normalize_commodity_symbol(symbol),
                    "error": snapshot["error"],
                    "score": 0,
                    "hint": snapshot.get("hint")
                })

        except Exception as e:
            results.append({
                "symbol": symbol,
                "normalized_symbol": normalize_commodity_symbol(symbol),
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

    if any(r.get("freshness", {}).get("scalping_allowed") is False for r in valid):
        global_warnings.append(
            "Una o più commodity non hanno dati adatti allo scalping. Usare come watchlist/setup condizionale."
        )

    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "v6-multi-asset-commodities",
        "asset_class": "commodity",
        "data_source": "twelve_data",
        "timeframe": timeframe,
        "symbols_scanned": symbol_list,
        "top": valid[:top],
        "all_ranked": valid,
        "errors": errors,
        "global_warnings": global_warnings,
        "logic": {
            "warning": "Non è un segnale operativo. È una selezione probabilistica e condizionale.",
            "supported_aliases": list(COMMODITY_SYMBOLS.keys())
        }
    }
