"""
RSICross_ScreenedUniverse — Alpaca Paper Trading
================================================
Trades a FIXED, pre-screened universe of 30 symbols. No daily screener, no
watchlist ranking — the universe is chosen offline by screen_universe.py +
measure_spreads.py and changes only when you re-screen.

Strategy
--------
  09:30 ET  stream starts; intraday RSI warms up from the opening bars
  10:00 ET  entries open
  intraday  ENTER when RSI(14) on 1-minute bars crosses UP through 20
  intraday  EXIT  when RSI crosses UP through 60
            ...or MAX_HOLD_MINUTES elapses
            ...or the disaster stop trips
  re-entry  a symbol can fire again once RSI has climbed back above 60 since
            its last entry, i.e. the oversold -> recovered cycle completed
  15:25 ET  last entry
  15:30 ET  hard close anything still open
  15:32 ET  email summary

Why no quote subscription
-------------------------
Alpaca's free IEX feed allows 30 channels. Bars + quotes = 2 per symbol, capping
you at 15 symbols. Bars only = 1 per symbol = 30 symbols, which is what this bot
wants.

Losing quotes means no live spread guard — and that is fine, because spread
control moved UPSTREAM. The universe was filtered on measured NBBO spreads
offline, which is far better than the old live guard ever was: that guard read
IEX-only quotes, which are wildly wide when IEX has no competitive quote posted.
It was rejecting good trades on bad data (one session: TDC skipped at a reported
"12.75%" spread while ITUB filled at 0.24% slippage and ELAN at 0.07%).

Basis, and how much to trust it
-------------------------------
From a surrogate-null study on 18 days of 1-minute bars: buying new highs had
NEGATIVE forward returns, while fading oversold extremes was the profitable
direction. `rsi_cross` was the only trigger with positive real returns at every
horizon.

The effect was NOT statistically significant — every confidence interval spanned
zero. This bot exists to gather forward, out-of-sample evidence. Judge it over
months, not days.

The entry level started at 15, which produced only ~2 trades/day live — too few
to learn anything at a useful rate. It is now 20. A shallower threshold means
more signals, each individually less extreme; whether that trades away real edge
for sample size is exactly what the forward record will show.

Because the level changed mid-run, trades before and after are NOT one dataset.
The trade log records RSI at entry, so they can be separated later.

Setup
  pip install alpaca-py python-dotenv schedule flask websockets
  .env: ALPACA_API_KEY, ALPACA_SECRET_KEY, EMAIL_ADDRESS, EMAIL_PASSWORD, EMAIL_TO
  universe.csv: a `symbol` column (output of the screener)
"""

import os
import csv
import time
import logging
import schedule
import smtplib
import threading
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, send_file, Response

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

# ── Configuration ─────────────────────────────────────────────────────────────

load_dotenv()

BOT_VERSION = "rsicross-screened-1.0"

API_KEY    = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

EMAIL_ADDRESS  = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_TO       = os.getenv("EMAIL_TO") or os.getenv("EMAIL_ADDRESS")
SMTP_HOST, SMTP_PORT = "smtp.gmail.com", 587

# --- Universe ---
UNIVERSE_FILE = "universe.csv"     # a `symbol` column; replace it to re-screen
MAX_SYMBOLS   = 30                 # free IEX feed: 30 channels, bars only = 30 symbols

# Used only if UNIVERSE_FILE is missing, so a deploy can never silently
# trade nothing. Replace with your screened list.
UNIVERSE_FALLBACK = []

# --- Schedule (ET) ---
STREAM_START_HOUR, STREAM_START_MIN   = 9, 30   # begin warming RSI
MONITOR_START_HOUR, MONITOR_START_MIN = 10, 0   # entries open
LAST_ENTRY_HOUR, LAST_ENTRY_MIN       = 15, 25
EXIT_HOUR, EXIT_MINUTE                = 15, 30  # hard close
SUMMARY_MINUTE                        = 32
MARKET_CLOSE_HOUR, MARKET_CLOSE_MIN   = 16, 5   # stop stream

# --- Signal ---
RSI_PERIOD        = 14      # bars (1-minute), matches the study
# Raised 15 -> 20 on 2026-08-xx: RSI 15 produced only ~2 trades/day live, too
# few to accumulate evidence at any useful rate. 20 is a shallower (less
# selective) oversold reading, so expect more signals of individually weaker
# quality — the point is sample size, not a claim that 20 is better.
# Treat pre-change and post-change trades as SEPARATE datasets when analysing.
ENTRY_CROSS_LEVEL = 20.0    # ENTER on an upward cross through this
EXIT_CROSS_LEVEL  = 60.0    # EXIT on an upward cross through this

# --- Risk / sizing ---
POSITION_SIZE_USD   = 1500
MAX_POSITIONS       = 3     # concurrent
MAX_ENTRIES_PER_DAY = 10
# A symbol may fire again once RSI has risen back above RE_ARM_LEVEL since its
# last entry. That completes the oversold -> recovered cycle, so a later dip is a
# genuinely NEW signal rather than the same one re-triggering while the stock is
# still depressed. Kept equal to EXIT_CROSS_LEVEL so a normal exit re-arms the
# symbol in the same bar; a position closed by MAX HOLD or DISASTER STOP stays
# disarmed until RSI genuinely recovers.
RE_ARM_LEVEL = EXIT_CROSS_LEVEL

# NB the backtest counted only the FIRST signal per symbol-day, so allowing
# re-entry is a deliberate divergence from what was tested. MAX_ENTRIES_PER_DAY
# is now the real brake on over-trading.

# Backstop: RSI may never reach the exit level. Raised 60 -> 120 alongside the
# exit level going 50 -> 60: a higher target takes longer to reach, so a 60m cap
# would have cut many trades short and turned this into a timed exit by stealth.
# Watch the share of exits tagged MAX HOLD — if most trades hit this rather than
# RSI>60, the cap is doing the work, not the signal.
MAX_HOLD_MINUTES = 120

# NOT data-derived: the study measured mean forward returns and never the
# distribution of individual trade paths, so nothing in it says where a stop
# belongs. 0 disables.
#
# Tightened 3% -> 2%, which caps a trade at about -$30 on a $1500 position.
# At this level it is no longer purely catastrophic-loss insurance: 2% is close
# enough to normal intraday noise that it will sometimes cut trades that would
# have recovered — and this entry is a mean-reversion signal, where a second dip
# before the bounce is common. Watch the DISASTER STOP share of exits; if it is
# more than a few percent, the stop is shaping results rather than insuring
# against disaster.
DISASTER_STOP_PCT = 2.0

DATA_FEED = DataFeed.IEX

TRADE_LOG_FILE = "rsicross_trade_log.csv"
PRICE_LOG_FILE = "rsicross_price_log.csv"
PRICE_LOG_INTERVAL = 60      # seconds; bars are 1-minute so 60s loses nothing

ET = ZoneInfo("America/New_York")

class _ETFormatter(logging.Formatter):
    def converter(self, timestamp):
        return datetime.fromtimestamp(timestamp, ET).timetuple()

_handlers = [logging.StreamHandler(), logging.FileHandler("rsicross_bot.log")]
for _h in _handlers:
    _h.setFormatter(_ETFormatter("%(asctime)s ET  %(levelname)s  %(message)s",
                                 datefmt="%Y-%m-%d %H:%M:%S"))
logging.basicConfig(level=logging.INFO, handlers=_handlers)
log = logging.getLogger(__name__)

trading = TradingClient(API_KEY, SECRET_KEY, paper=True)
data    = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# ── State ─────────────────────────────────────────────────────────────────────

universe: list[str] = []
track: dict = {}          # symbol -> RSI state
positions: dict = {}      # symbol -> open position
traded_today: set = set()
session_date: str | None = None
_stream_state = {"stream": None, "running": False, "started": False}
_monitor_open = {"active": False}
_day_counters = {"entries": 0}

# ── Flask ─────────────────────────────────────────────────────────────────────

flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    held = ", ".join(positions) or "none"
    return Response(
        f"<h2>RSICross_ScreenedUniverse</h2><p>version {BOT_VERSION}</p>"
        f"<p>{len(universe)} symbols | entries {_day_counters['entries']}"
        f"/{MAX_ENTRIES_PER_DAY} | holding: {held}</p><ul>"
        "<li><a href='/logs/trades'>trade log</a></li>"
        "<li><a href='/logs/prices'>price log</a></li>"
        "<li><a href='/logs/bot'>bot log</a></li></ul>", mimetype="text/html")

def _send(path, name):
    if os.path.exists(path):
        return send_file(path, as_attachment=True, download_name=name)
    return Response("Not yet.", status=404)

@flask_app.route("/logs/trades")
def dl_t(): return _send(TRADE_LOG_FILE, TRADE_LOG_FILE)
@flask_app.route("/logs/prices")
def dl_p(): return _send(PRICE_LOG_FILE, PRICE_LOG_FILE)
@flask_app.route("/logs/bot")
def dl_b(): return _send("rsicross_bot.log", "rsicross_bot.log")

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)),
                  use_reloader=False)

# ── Universe ──────────────────────────────────────────────────────────────────

def load_universe():
    """Read the screened symbol list. Skips `#` provenance headers written by
    screen_universe.py / measure_spreads.py."""
    syms = []
    if os.path.exists(UNIVERSE_FILE):
        with open(UNIVERSE_FILE, newline="") as f:
            rows = (l for l in f if not l.lstrip().startswith("#"))
            for r in csv.DictReader(rows):
                s = (r.get("symbol") or "").strip().upper()
                if s:
                    syms.append(s)
        log.info(f"Loaded {len(syms)} symbols from {UNIVERSE_FILE}")
    else:
        syms = list(UNIVERSE_FALLBACK)
        log.warning(f"{UNIVERSE_FILE} not found — using the built-in fallback "
                    f"({len(syms)} symbols)")
    seen, out = set(), []
    for s in syms:
        if s not in seen:
            seen.add(s); out.append(s)
    if len(out) > MAX_SYMBOLS:
        log.warning(f"{len(out)} symbols but the IEX feed allows {MAX_SYMBOLS} "
                    f"channels (bars only) — using the first {MAX_SYMBOLS}")
        out = out[:MAX_SYMBOLS]
    if not out:
        log.error("EMPTY UNIVERSE — nothing will trade. Add universe.csv.")
    return out

# ── RSI ───────────────────────────────────────────────────────────────────────

def wilder_seed(closes, period):
    if len(closes) < period + 1:
        return None, None, None
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0)); losses.append(max(-ch, 0.0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    return ag, al, (100.0 if al == 0 else 100 - 100 / (1 + ag / al))


def wilder_step(ag, al, change, period):
    ag = (ag * (period - 1) + max(change, 0.0)) / period
    al = (al * (period - 1) + max(-change, 0.0)) / period
    return ag, al, (100.0 if al == 0 else 100 - 100 / (1 + ag / al))

# ── Session start ─────────────────────────────────────────────────────────────

def job_session_start():
    """At 09:30 — reset state, warm RSI from any available bars, start the stream."""
    global universe, track, traded_today, session_date
    log.info("=" * 62)
    log.info("SESSION START — " + datetime.now(ET).strftime("%Y-%m-%d %H:%M ET"))
    log.info("=" * 62)

    try:
        if not trading.get_clock().is_open:
            log.info("Market closed. Skipping today.")
            return
    except Exception as e:
        log.error(f"Clock check failed: {e}")
        return

    universe = load_universe()
    if not universe:
        return
    track = {s: {"ag": None, "al": None, "rsi": None, "prev_rsi": None,
                 "last_close": None, "bars": 0, "warm": [], "armed": True,
                 "fires": 0} for s in universe}
    traded_today = set()
    _day_counters["entries"] = 0
    _monitor_open["active"] = False
    stop_price_stream()
    _stream_state["started"] = _stream_state["running"] = False
    session_date = datetime.now(ET).strftime("%Y-%m-%d")

    log.info(f"Universe ({len(universe)}): {', '.join(universe)}")
    start_price_stream(universe)


def job_warm_rsi():
    """At 09:55 — seed RSI from the session's bars so the 10:00 open is live.

    Without this the first ~14 minutes of the entry window would be dead while
    the indicator warms. Seeding from 09:30 also matches the backtest, which
    computed RSI fresh from the open each day.
    """
    if not universe:
        return
    now = datetime.now(ET)
    start = now.replace(hour=STREAM_START_HOUR, minute=STREAM_START_MIN,
                        second=0, microsecond=0)
    try:
        bars = data.get_stock_bars(StockBarsRequest(
            symbol_or_symbols=universe, timeframe=TimeFrame.Minute,
            start=start, end=now, feed=DATA_FEED))
    except Exception as e:
        log.warning(f"RSI warm-up fetch failed ({e}); will warm live instead")
        return
    warmed = 0
    for s in universe:
        if s not in track:          # session start has not run yet
            continue
        cl = [float(b.close) for b in bars.data.get(s, [])]
        if len(cl) >= RSI_PERIOD + 1:
            ag, al, rsi = wilder_seed(cl, RSI_PERIOD)
            track[s].update(ag=ag, al=al, rsi=rsi, last_close=cl[-1], bars=len(cl))
            warmed += 1
    log.info(f"RSI warmed for {warmed}/{len(universe)} symbols")
    ready = [f"{s}:{track[s]['rsi']:.0f}" for s in universe
             if track[s]["rsi"] is not None]
    if ready:
        log.info("  " + "  ".join(ready))

# ── Stream ────────────────────────────────────────────────────────────────────

async def _on_bar(bar):
    """One completed 1-minute bar: update RSI, then check exit or entry."""
    try:
        s = bar.symbol
        t = track.get(s)
        if t is None:
            return
        close = float(bar.close)
        now = datetime.now(ET)

        if t["last_close"] is None:
            t["last_close"] = close; t["bars"] += 1
            return
        change = close - t["last_close"]
        t["last_close"] = close
        t["bars"] += 1

        if t["ag"] is None:
            t["warm"].append(change)
            if len(t["warm"]) >= RSI_PERIOD:
                p = RSI_PERIOD
                t["ag"] = sum(max(c, 0.0) for c in t["warm"]) / p
                t["al"] = sum(max(-c, 0.0) for c in t["warm"]) / p
                t["rsi"] = (100.0 if t["al"] == 0
                            else 100 - 100 / (1 + t["ag"] / t["al"]))
            return

        t["prev_rsi"] = t["rsi"]
        t["ag"], t["al"], t["rsi"] = wilder_step(t["ag"], t["al"], change, RSI_PERIOD)

        # ---- re-arm ----
        # Checked before anything else so it applies whether or not a position is
        # open: an RSI-50 exit re-arms in the same bar it closes.
        if (t["prev_rsi"] is not None and t["rsi"] is not None
                and t["prev_rsi"] < RE_ARM_LEVEL <= t["rsi"] and not t["armed"]):
            t["armed"] = True
            log.info(f"  RE-ARM {s:6s} | RSI crossed {RE_ARM_LEVEL:.0f} "
                     f"({t['prev_rsi']:.1f} -> {t['rsi']:.1f}) — eligible again")

        # ---- manage an open position ----
        if s in positions:
            pos = positions[s]
            # 1) primary exit: RSI crosses UP through the exit level
            if (t["prev_rsi"] is not None and t["prev_rsi"] < EXIT_CROSS_LEVEL
                    <= t["rsi"]):
                _exit_position(s, close, f"RSI>{EXIT_CROSS_LEVEL:.0f}")
                return
            # 2) time backstop
            if now >= pos["max_exit_at"]:
                _exit_position(s, close, "MAX HOLD")
                return
            # 3) disaster stop
            if DISASTER_STOP_PCT > 0:
                ref = pos["entry_fill"] or pos["entry"]
                if close <= ref * (1 - DISASTER_STOP_PCT / 100.0):
                    _exit_position(s, close, "DISASTER STOP")
            return

        # ---- entry ----
        if not _monitor_open["active"]:
            return
        if now >= now.replace(hour=LAST_ENTRY_HOUR, minute=LAST_ENTRY_MIN,
                              second=0, microsecond=0):
            return
        if (t["prev_rsi"] is not None and t["rsi"] is not None
                and t["prev_rsi"] < ENTRY_CROSS_LEVEL <= t["rsi"]):
            _check_entry(s, close, now)

    except Exception as e:
        log.debug(f"bar handler error: {e}")


def _check_entry(sym, price, now):
    t = track[sym]
    if not t["armed"]:
        return          # already traded; waits for RSI to recover past RE_ARM_LEVEL
    if len(positions) >= MAX_POSITIONS:
        log.info(f"  SKIP {sym:6s} | RSI cross but {MAX_POSITIONS} positions open")
        return
    if _day_counters["entries"] >= MAX_ENTRIES_PER_DAY:
        log.info(f"  SKIP {sym:6s} | daily entry cap reached")
        return
    log.info(f"  SIGNAL {sym:6s} | RSI {t['prev_rsi']:.1f} -> {t['rsi']:.1f} "
             f"crossed {ENTRY_CROSS_LEVEL:.0f} | ${price:.2f}")
    _enter_position(sym, price, now)

# ── Orders ────────────────────────────────────────────────────────────────────

def _get_fill_price(order_id, timeout_s=3.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            o = trading.get_order_by_id(order_id)
            if o.filled_avg_price is not None:
                return float(o.filled_avg_price)
            if str(o.status).lower().split(".")[-1] in (
                    "canceled", "rejected", "expired"):
                return None
        except Exception:
            pass
        time.sleep(0.4)
    return None


def _enter_position(sym, price, now):
    qty = int(POSITION_SIZE_USD / price)
    if qty < 1:
        traded_today.add(sym)
        track[sym]["armed"] = False
        return
    hard = now.replace(hour=EXIT_HOUR, minute=EXIT_MINUTE, second=0, microsecond=0)
    max_exit = min(now + timedelta(minutes=MAX_HOLD_MINUTES), hard)
    try:
        order = trading.submit_order(MarketOrderRequest(
            symbol=sym, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY))
        positions[sym] = {"entry": price, "qty": qty, "entry_time": now,
                          "max_exit_at": max_exit, "entry_fill": None,
                          "entry_rsi": track[sym]["rsi"]}
        traded_today.add(sym)
        track[sym]["armed"] = False
        track[sym]["fires"] = track[sym].get("fires", 0) + 1
        _day_counters["entries"] += 1
        log.info(f"  ENTER {sym:6s} qty {qty} @ ${price:.2f} | "
                 f"max exit {max_exit.strftime('%H:%M')} | "
                 f"entry {_day_counters['entries']}/{MAX_ENTRIES_PER_DAY} | "
                 f"holding {len(positions)}/{MAX_POSITIONS}")
        fill = _get_fill_price(order.id)
        if fill:
            positions[sym]["entry_fill"] = fill
            log.info(f"    fill ${fill:.4f} (slip {fill - price:+.4f})")
        _log_trade(sym, "ENTER", price, qty, "", now, fill,
                   track[sym]["rsi"])
    except Exception as e:
        log.error(f"  Buy failed {sym}: {e}")


def _exit_position(sym, price, reason):
    pos = positions.get(sym)
    if not pos:
        return
    now = datetime.now(ET)
    qty = pos["qty"]
    ref = pos["entry_fill"] or pos["entry"]
    pnl = (price - ref) * qty
    held = (now - pos["entry_time"]).total_seconds() / 60
    log.info(f"  EXIT  {sym:6s} | {reason} | ${ref:.2f} -> ${price:.2f} | "
             f"held {held:.0f}m | RSI {pos.get('entry_rsi', 0):.0f}->"
             f"{track[sym]['rsi'] if track[sym]['rsi'] else 0:.0f} | "
             f"P&L ${pnl:+.2f}")
    fill = None
    try:
        order = trading.submit_order(MarketOrderRequest(
            symbol=sym, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))
        fill = _get_fill_price(order.id)
        if fill:
            log.info(f"    fill ${fill:.4f} | P&L on fills ${(fill - ref) * qty:+.2f}")
    except Exception as e:
        log.error(f"  Sell failed {sym}: {e}")
    _log_trade(sym, f"EXIT-{reason}", price, qty, f"{pnl:+.2f}", now, fill,
               track[sym]["rsi"], held)
    positions.pop(sym, None)


def _log_trade(sym, action, price, qty, pnl, now, fill=None, rsi=None, held=None):
    cols = ["date", "time_et", "symbol", "action", "price", "fill_price",
            "slippage", "qty", "rsi", "held_min", "pnl"]
    exists = os.path.isfile(TRADE_LOG_FILE)
    with open(TRADE_LOG_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow({"date": now.strftime("%Y-%m-%d"),
                    "time_et": now.strftime("%H:%M:%S"),
                    "symbol": sym, "action": action, "price": round(price, 4),
                    "fill_price": round(fill, 4) if fill else None,
                    "slippage": round(fill - price, 4) if fill else None,
                    "qty": qty, "rsi": round(rsi, 2) if rsi is not None else None,
                    "held_min": round(held, 1) if held is not None else None,
                    "pnl": pnl})

# ── Stream control ────────────────────────────────────────────────────────────

def start_price_stream(symbols):
    if _stream_state["started"] or not symbols:
        return
    _stream_state["started"] = True

    def _run():
        for attempt in range(1, 5):
            try:
                stream = StockDataStream(API_KEY, SECRET_KEY, feed=DATA_FEED)
                _stream_state["stream"] = stream
                # BARS ONLY — 1 channel per symbol, so 30 symbols fit the free
                # IEX cap. Subscribing to quotes as well would halve that to 15.
                stream.subscribe_bars(_on_bar, *symbols)
                _stream_state["running"] = True
                log.info(f"Stream started — {len(symbols)} symbols, minute bars "
                         f"(no quotes: 1 channel/symbol)")
                stream.run()
                return
            except Exception as e:
                _stream_state["running"] = False
                if "connection limit" in str(e).lower() and attempt < 4:
                    wait = attempt * 30
                    log.warning(f"Connection limit; retry in {wait}s")
                    time.sleep(wait); continue
                log.error(f"Stream error: {e}")
                return

    threading.Thread(target=_run, daemon=True).start()


def stop_price_stream():
    s = _stream_state.get("stream")
    if s and _stream_state["running"]:
        try:
            s.stop(); log.info("Stream stopped.")
        except Exception as e:
            log.warning(f"Stop stream: {e}")
    _stream_state["running"] = False

# ── Scheduled jobs ────────────────────────────────────────────────────────────

def job_open_monitor():
    if not universe:
        return
    _monitor_open["active"] = True
    ready = sum(1 for s in universe if track.get(s, {}).get("rsi") is not None)
    log.info("=" * 62)
    log.info(f"ENTRIES OPEN — RSI cross up through {ENTRY_CROSS_LEVEL:.0f} | "
             f"{ready}/{len(universe)} symbols have live RSI")
    log.info(f"  last entry {LAST_ENTRY_HOUR:02d}:{LAST_ENTRY_MIN:02d}, "
             f"exit on RSI>{EXIT_CROSS_LEVEL:.0f} / {MAX_HOLD_MINUTES}m / "
             f"{EXIT_HOUR:02d}:{EXIT_MINUTE:02d}")
    log.info("=" * 62)


def job_safety_check():
    """Bars can stop arriving for a symbol; don't let a hold overrun silently."""
    now = datetime.now(ET)
    for sym in list(positions.keys()):
        pos = positions[sym]
        if now >= pos["max_exit_at"]:
            px = track.get(sym, {}).get("last_close") or pos["entry"]
            _exit_position(sym, px, "MAX HOLD")


def job_exit_all():
    log.info("=" * 62)
    log.info("HARD CLOSE — " + datetime.now(ET).strftime("%Y-%m-%d %H:%M ET"))
    _monitor_open["active"] = False
    for sym in list(positions.keys()):
        px = track.get(sym, {}).get("last_close") or positions[sym]["entry"]
        _exit_position(sym, px, "EOD CLOSE")
    try:
        acct = trading.get_account()
        log.info(f"Portfolio value: ${float(acct.portfolio_value):,.2f}")
    except Exception:
        pass


def job_stop_stream():
    stop_price_stream()
    log.info("Day complete.")


def job_log_prices():
    today = datetime.now(ET).strftime("%Y-%m-%d")
    if not universe or session_date != today:
        return
    now = datetime.now(ET)
    rows = []
    for s in universe:
        t = track.get(s)
        if not t or t.get("last_close") is None:
            continue
        rows.append({"date": today, "time_et": now.strftime("%H:%M:%S"),
                     "symbol": s, "price": round(t["last_close"], 4),
                     "rsi": round(t["rsi"], 2) if t.get("rsi") is not None else None,
                     "bars": t.get("bars", 0),
                     "in_position": "yes" if s in positions else "no"})
    if not rows:
        return
    cols = ["date", "time_et", "symbol", "price", "rsi", "bars", "in_position"]
    exists = os.path.isfile(PRICE_LOG_FILE)
    with open(PRICE_LOG_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerows(rows)

# ── Summary ───────────────────────────────────────────────────────────────────

def build_daily_summary():
    today = datetime.now(ET).strftime("%Y-%m-%d")
    try:
        orders = trading.get_orders(filter=GetOrdersRequest(
            status=QueryOrderStatus.ALL,
            after=datetime.now(ET).replace(hour=0, minute=0, second=0,
                                           microsecond=0),
            limit=500))
    except Exception as e:
        return f"Could not fetch orders: {e}", f"<p>{e}</p>"
    buys, sells = {}, {}
    for o in orders:
        if o.filled_at is None or o.filled_avg_price is None:
            continue
        info = {"qty": float(o.filled_qty or 0), "price": float(o.filled_avg_price)}
        (buys if "buy" in str(o.side).lower() else sells)[o.symbol] = info
    lines, rows_html, total, wins, n = [], "", 0.0, 0, 0
    for sym, b in buys.items():
        s = sells.get(sym)
        if not s:
            lines.append(f"  {sym} qty {b['qty']:.0f} OPEN"); continue
        pnl = (s["price"] - b["price"]) * b["qty"]
        pct = (s["price"] - b["price"]) / b["price"] * 100
        total += pnl; n += 1; wins += pnl > 0
        lines.append(f"  {sym} {b['price']:.2f}->{s['price']:.2f} "
                     f"{pct:+.2f}% ${pnl:+.2f}")
        c = "#3B6D11" if pnl >= 0 else "#A32D2D"
        rows_html += (f"<tr><td><b>{sym}</b></td><td>{b['qty']:.0f}</td>"
                      f"<td>${b['price']:.2f}</td><td>${s['price']:.2f}</td>"
                      f"<td style='color:{c}'>{pct:+.2f}%</td>"
                      f"<td style='color:{c}'>${pnl:+.2f}</td></tr>")
    wr = (wins / n * 100) if n else 0
    text = (f"RSICross Screened Universe — {today}\n" + "=" * 44 +
            f"\nTrades: {n}\nWin rate: {wr:.0f}%\nP&L: ${total:+,.2f}\n\n"
            + ("\n".join(lines) if lines else "  no trades"))
    html = (f"<div style='font-family:system-ui,sans-serif;max-width:640px'>"
            f"<h2>RSICross — Screened Universe</h2>"
            f"<p style='color:#888'>{today} · {BOT_VERSION} · "
            f"entry RSI&gt;{ENTRY_CROSS_LEVEL:.0f}, exit RSI&gt;"
            f"{EXIT_CROSS_LEVEL:.0f}</p>"
            f"<p>Trades <b>{n}</b> · Win <b>{wr:.0f}%</b> · "
            f"P&amp;L <b style='color:{'#3B6D11' if total>=0 else '#A32D2D'}'>"
            f"${total:+,.2f}</b></p>"
            f"<table style='border-collapse:collapse;width:100%;font-size:13px'>"
            f"<tr style='text-align:left'><th>Symbol</th><th>Qty</th><th>Entry</th>"
            f"<th>Exit</th><th>%</th><th>P&amp;L</th></tr>{rows_html}</table></div>")
    return text, html


def send_email(subject, text_body, html_body):
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"], msg["From"], msg["To"] = subject, EMAIL_ADDRESS, EMAIL_TO
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as srv:
            srv.starttls(); srv.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            srv.send_message(msg)
        log.info(f"Summary emailed to {EMAIL_TO}")
    except Exception as e:
        log.error(f"Email failed: {e}")


def job_daily_summary():
    text, html = build_daily_summary()
    log.info("\n" + text)
    send_email(f"RSICross — {datetime.now(ET).strftime('%Y-%m-%d')}", text, html)

def catch_up_on_startup():
    """Bring the bot to the correct state for the CURRENT time.

    The scheduler fires jobs on an exact minute match, so a process that starts
    at 09:32 — or is redeployed at 13:00 — would otherwise miss session start
    and sit dead until the next morning. Redeploys happen often, so this matters
    more than the once-a-day case.
    """
    now = datetime.now(ET)
    try:
        if not trading.get_clock().is_open:
            log.info("Startup: market closed — waiting for the next session.")
            return
    except Exception as e:
        log.warning(f"Startup clock check failed: {e}")
        return

    session_start = now.replace(hour=STREAM_START_HOUR, minute=STREAM_START_MIN,
                                second=0, microsecond=0)
    hard_close = now.replace(hour=EXIT_HOUR, minute=EXIT_MINUTE,
                             second=0, microsecond=0)
    if now < session_start or now >= hard_close:
        log.info("Startup: outside the trading window — waiting for the schedule.")
        return

    log.info("=" * 62)
    log.info(f"STARTUP CATCH-UP — mid-session start at {now.strftime('%H:%M')} ET")
    log.info("=" * 62)
    job_session_start()
    if not universe:
        return

    # Warm RSI from the session's bars so far, rather than waiting ~14 minutes
    # for the live stream to fill the window.
    job_warm_rsi()

    monitor_open = now.replace(hour=MONITOR_START_HOUR, minute=MONITOR_START_MIN,
                               second=0, microsecond=0)
    last_entry = now.replace(hour=LAST_ENTRY_HOUR, minute=LAST_ENTRY_MIN,
                             second=0, microsecond=0)
    if monitor_open <= now < last_entry:
        job_open_monitor()
    elif now >= last_entry:
        log.info("Past the last-entry time — monitoring stays closed today.")
    else:
        log.info(f"Entries open at {MONITOR_START_HOUR:02d}:"
                 f"{MONITOR_START_MIN:02d} as scheduled.")


# ── Scheduler ─────────────────────────────────────────────────────────────────

def run_scheduler():
    def at(h, m, fn):
        def wrap():
            now = datetime.now(ET)
            if now.hour == h and now.minute == m:
                fn()
        return wrap

    schedule.every(1).minutes.do(at(STREAM_START_HOUR, STREAM_START_MIN,
                                    job_session_start))
    schedule.every(1).minutes.do(at(9, 55, job_warm_rsi))
    schedule.every(1).minutes.do(at(MONITOR_START_HOUR, MONITOR_START_MIN,
                                    job_open_monitor))
    schedule.every(1).minutes.do(at(EXIT_HOUR, EXIT_MINUTE, job_exit_all))
    schedule.every(1).minutes.do(at(EXIT_HOUR, SUMMARY_MINUTE, job_daily_summary))
    schedule.every(1).minutes.do(at(MARKET_CLOSE_HOUR, MARKET_CLOSE_MIN,
                                    job_stop_stream))
    schedule.every(30).seconds.do(job_safety_check)
    schedule.every(PRICE_LOG_INTERVAL).seconds.do(job_log_prices)

    log.info(f"RSICross_ScreenedUniverse {BOT_VERSION}")
    log.info(f"  universe: {UNIVERSE_FILE} (max {MAX_SYMBOLS}, bars-only stream)")
    log.info(f"  ENTRY: RSI({RSI_PERIOD}) 1-min cross UP through "
             f"{ENTRY_CROSS_LEVEL:.0f}")
    log.info(f"  EXIT:  RSI cross UP through {EXIT_CROSS_LEVEL:.0f} | "
             f"max hold {MAX_HOLD_MINUTES}m | hard close "
             f"{EXIT_HOUR:02d}:{EXIT_MINUTE:02d}")
    log.info(f"  ${POSITION_SIZE_USD}/position, max {MAX_POSITIONS} concurrent, "
             f"{MAX_ENTRIES_PER_DAY} entries/day")
    log.info(f"  re-entry allowed once RSI recovers above {RE_ARM_LEVEL:.0f}")
    log.info(f"  disaster stop: "
             + (f"{DISASTER_STOP_PCT}%" if DISASTER_STOP_PCT > 0 else "none"))
    log.info(f"  {STREAM_START_HOUR:02d}:{STREAM_START_MIN:02d} stream + RSI warm | "
             f"{MONITOR_START_HOUR:02d}:{MONITOR_START_MIN:02d} entries open | "
             f"{LAST_ENTRY_HOUR:02d}:{LAST_ENTRY_MIN:02d} last entry")
    log.info("  NOTE: RSI 15 is a deep threshold and fires rarely — expect few "
             "trades. The backtested edge was NOT statistically significant; "
             "this run gathers forward evidence.")
    log.info("Waiting...")

    while True:
        schedule.run_pending()
        time.sleep(5)


if __name__ == "__main__":
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
    try:
        acct = trading.get_account()
        log.info(f"Connected to Alpaca paper account. "
                 f"Portfolio ${float(acct.portfolio_value):,.2f}")
    except Exception as e:
        raise RuntimeError(f"Could not connect to Alpaca: {e}")
    universe = load_universe()
    threading.Thread(target=run_flask, daemon=True).start()
    catch_up_on_startup()
    run_scheduler()
