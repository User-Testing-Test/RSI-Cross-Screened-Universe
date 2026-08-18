# RSICross_ScreenedUniverse

Trades a fixed, pre-screened universe of up to 30 symbols. Entry on an intraday
RSI(14) cross **up through 15**; exit on a cross **up through 50**.

Deploy as its own Railway service with its own Alpaca paper keys — Alpaca allows
one websocket per account, so two bots on the same account will fight over it.

## Configure

`.env`:

    ALPACA_API_KEY=...
    ALPACA_SECRET_KEY=...
    EMAIL_ADDRESS=...
    EMAIL_PASSWORD=...
    EMAIL_TO=...

`universe.csv` — a `symbol` column. Paste the screener output straight in; the
extra metric columns are ignored.

## Settings

| | |
|---|---|
| Entry | RSI(14) 1-min cross up through **20** |
| Exit | RSI cross up through **60** |
| Exit backstop | **120 min** max hold |
| Hard close | 15:30 ET |
| Position size | **$1,500** |
| Max concurrent | **3** |
| Max entries/day | **10** |
| Disaster stop | **2%** |
| Entry window | 10:00 – 15:25 ET |
| Re-entry | allowed once RSI recovers above **60** |

## Schedule (ET)

    09:30  stream starts, RSI begins warming
    09:55  RSI seeded from the session's bars
    10:00  entries open
    15:25  last entry
    15:30  hard close
    15:32  summary email
    16:05  stream stops

## Re-entry

A symbol is disarmed after it fires, and re-armed only when RSI climbs back
above 60 (`RE_ARM_LEVEL` is tied to `EXIT_CROSS_LEVEL`, so they move together). That completes the oversold → recovered cycle, so a later dip is a
genuinely new signal rather than the same depressed stock re-triggering.

Because the re-arm level matches the exit level, a normal RSI-50 exit re-arms the
symbol in the same bar it closes. A position closed by `MAX HOLD` or
`DISASTER STOP` — where RSI never recovered — stays disarmed until it does.

Note this diverges from the backtest, which counted only the first signal per
symbol-day. `MAX_ENTRIES_PER_DAY` (10) is now the real brake on over-trading.

## Why bars only, no quotes

The free IEX feed allows 30 channels. Bars + quotes is 2 per symbol, capping the
universe at 15. Bars only is 1 per symbol, so 30 fit.

That removes the live spread guard, which is fine — **spread control moved
upstream**. The universe is filtered on *measured NBBO spreads* offline, which is
strictly better than the old live guard: that read IEX-only quotes, which are
wildly wide whenever IEX has no competitive quote posted, and it was rejecting
good trades on bad data (one session skipped TDC at a reported 12.75% spread
while ITUB filled at 0.24% slippage and ELAN at 0.07%).

## What to expect

**Settings history.** Entry started at 15 — only ~2 trades/day live, too slow to
accumulate evidence — so it was raised to 20. The exit was then raised 50 → 60
and the max hold 60 → 120 minutes, since a higher exit target takes longer to
reach and a 60-minute cap would have cut trades short.

Trades before and after the change are **not one dataset**. The trade log records
RSI at entry, so they can be separated when you analyse.

**The backtested edge was not statistically significant.** Every confidence
interval spanned zero. This runs to gather forward, out-of-sample evidence.
Judge it over months.

## Watch in the logs

- Share of exits tagged `DISASTER STOP` — at 2% this sits close to normal
  intraday noise, so if it is more than a few percent the stop is shaping
  results rather than insuring against disaster. Mean-reversion entries often
  dip again before working.
- Share tagged `MAX HOLD` — if most exits hit the 120-minute backstop rather
  than RSI 60, the exit rule is barely firing and you are effectively running a
  timed exit under another name.
- `slippage` in the trade log — the ground truth on trading costs, and the
  check on whether the offline spread screen picked the right names.

## Re-screening

The universe is fixed until you replace `universe.csv`. Liquidity and signal
rates drift, so re-run the screener periodically (monthly is reasonable) rather
than leaving a year-old list in place.
