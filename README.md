# Folio

A small self-hosted dashboard for a stock portfolio. You record your buys and sells; Folio shows what the portfolio is worth right now, tick by tick.

- Live prices from Yahoo Finance's trade stream, including pre-market and after hours
- Portfolio and per-stock charts (1D, 1W, 1M, 3M, 1Y)
- Holdings with today's move, total return, day and 52-week ranges, and weights
- Market strip (S&P 500, Nasdaq, Dow, VIX, EUR/USD)
- News whose headlines name the companies you hold
- Buy, sell, deposit and withdraw entries, with average-cost and realised profit tracking
- Watchlist for stocks you don't own
- Price alerts with a desktop notification (uses `notify-send` where available), checked every 30 seconds while the server runs
- A recorded history of your portfolio's real value (the **All** chart range), sampled every 5 minutes while markets trade
- Amounts in dollars or euros, and a privacy mode that blurs what you own
- Layouts for a phone, a half-screen window and a full 1440p monitor, in light and dark

No dependencies: one Python file (standard library only) and one HTML page.

## Run

```sh
python3 server.py
```

Then open http://localhost:8791 and use **+ Trade** to record your first deposit and buy.

Settings are environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `FOLIO_PORT` | `8791` | Port to listen on |
| `FOLIO_HOST` | `0.0.0.0` | Address to bind. Use `127.0.0.1` to keep it to this machine |
| `FOLIO_DATA` | `data/portfolio.json` | Where the ledger is stored |

There is no login. With the default `FOLIO_HOST`, anyone on your network can view the dashboard and record trades.

## Keyboard

| Key | Action |
|---|---|
| `T` | Record a trade |
| `1` to `6` | Chart range: 1D, 1W, 1M, 3M, 1Y, All |
| `←` `→` | Cycle the chart through the portfolio and each stock |
| `C` | Switch between dollars and euros |
| `H` | Hide or show your amounts |

## Your data

Everything you enter (trades, watchlist, alerts) is kept in `data/portfolio.json`, and the recorded value history in `data/history.jsonl`. Git ignores the whole `data/` folder. `portfolio.example.json` shows the format; copy it to `data/portfolio.json` to start from a sample.

Folio never talks to your broker. Holdings change only when you record a trade.

## Run at login (Linux, systemd)

```sh
cp folio.service ~/.config/systemd/user/
systemctl --user enable --now folio.service
```

## Notes

- Prices and news come from unofficial Yahoo Finance endpoints, which can change or rate-limit without notice.
- Trades and alerts are entered in US dollars. The euro view converts everything at the current EUR/USD rate, including past amounts.
- The 1W to 1Y portfolio charts show what today's holdings would have been worth, not your actual history.

## Licence

MIT. See [LICENSE](LICENSE).
