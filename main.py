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

app = FastAPI(title="Trader Zero Market Data API v5 Market Intelligence")

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

NY_TZ = ZoneInfo("America/New_York")


# ---------------- MARKET HOURS ----------------
def get_us_market_status():
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(NY_TZ)

    weekday = now_ny.weekday()  # Monday=0, Sunday=6
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
        start = end - timedelta(days=10)
    elif timeframe == "1h":
        start = end - timedelta(days=60)
    else:
        start = end - timedelta(days=220)

    return start, end


# ---------------- FINNHUB ----------------
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

    # Regular market open
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


# ---------------- CANDLE FRESHNESS ----------------
def parse_candle_time(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
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

    # Regular market open
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
    scalping_allowed_by_market = market_status["scalping_allowed"]

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

    # Market open
    if timeframe in ["1m", "5m"]:
        if candle_status == "fresh" and quote_status == "fresh" and scalping_allowed_by_market:
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

    # 15m/1h/1d during market open
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


def score_asset(trend, momentum, compression, price, support, resistance, atr, setup_base_score, combined):
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

    status = combined.get("status")

    if status == "execution_context":
        score += 0.5
    elif status == "market_closed":
        score -= 1.0
    elif status == "extended_hours":
        score -= 1.0
    elif status == "not_execution_grade":
        score -= 1.5
    elif status == "context_weak":
        score -= 2.5

    return round(max(min(score, 10), 0), 2)


# ---------------- FETCH DATI ----------------
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

    score = score_asset(
        trend=trend,
        momentum=momentum,
        compression=compression,
        price=price,
        support=support,
        resistance=resistance,
        atr=atr,
        setup_base_score=base_score,
        combined=combined
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


# ---------------- HOME / HEALTH ----------------
@app.get("/")
def home():
    return {
        "status": "Trader Zero PRO server online",
        "version": "v5-market-intelligence",
        "endpoints": [
            "/health",
            "/market-status",
            "/quote-fresh",
            "/market-snapshot",
            "/scan-market"
        ]
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "v5-market-intelligence",
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "market_status": get_us_market_status(),
        "alpaca_keys_loaded": bool(API_KEY and SECRET_KEY),
        "finnhub_key_loaded": bool(FINNHUB_API_KEY)
    }


@app.get("/market-status")
def market_status():
    return get_us_market_status()


# ---------------- QUOTE FRESH ----------------
@app.get("/quote-fresh")
def quote_fresh(symbol: str = Query(...)):
    market_status = get_us_market_status()

    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "market_status": market_status,
        "quote": fetch_finnhub_quote(symbol, market_status)
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

    market_status = get_us_market_status()
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
                    "market_status": market_status,
                    "finnhub_quote": snapshot.get("finnhub_quote")
                })

        except Exception as e:
            results.append({
                "symbol": symbol,
                "error": str(e),
                "score": 0,
                "market_status": market_status
            })

    ranked = sorted(
        results,
        key=lambda x: x.get("score", 0),
        reverse=True
    )

    valid = [r for r in ranked if "error" not in r]
    errors = [r for r in ranked if "error" in r]

    global_warnings = []

    if market_status["status"] in ["weekend_closed", "closed"]:
        global_warnings.append(
            "Mercato chiuso: scanner valido solo per watchlist/preparazione, non per operatività immediata."
        )

    if market_status["status"] in ["premarket", "afterhours"]:
        global_warnings.append(
            "Sessione estesa: liquidità ridotta, evitare scalping aggressivo."
        )

    not_execution_count = sum(
        1 for r in valid
        if r.get("combined_freshness", {}).get("scalping_allowed") is False
        and timeframe in ["1m", "5m"]
    )

    if timeframe in ["1m", "5m"] and not_execution_count > 0:
        global_warnings.append(
            "Scanner intraday non execution-grade per uno o più asset."
        )

    return {
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "v5-market-intelligence",
        "market_status": market_status,
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
                "market status",
                "combined freshness",
                "Finnhub quote freshness",
                "Alpaca candle freshness"
            ],
            "warning": "Quote fresca senza candele fresche non basta per scalping operativo."
        }
    }
