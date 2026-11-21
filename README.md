# Folio

A small self-hosted dashboard for a stock portfolio. You record your buys and sells; Folio shows what the portfolio is worth right now, tick by tick.

- Live prices from Yahoo Finance's trade stream, including pre-market and after hours
- Portfolio and per-stock charts (1D, 1W, 1M, 3M, 1Y, 5Y), as a line or as candles with volume, with your buys and sells marked, optional 50-day and 200-day moving averages, and drag-to-measure between any two moments
- Compare view: your portfolio, each holding and the S&P 500 in percent on one chart
- One box (`/` or `Ctrl K`) to search for any stock, fund, index or coin and chart it without owning it, or to run any command
- Details for the charted stock: day and 52-week range, volume, market value, P/E, next earnings date, a short company description, and your own position and break-even
- For companies, three more panels beside the chart: analysts' price targets and recommendations, reported profit against expectations with sales and margins, and each of your own trades measured against today's price
- **Analysis** page: yearly swing (volatility), beta, deepest fall and Sharpe ratio of today's holdings; a risk-against-return chart; which holding causes how much of the swings; how closely your stocks move together; falls from the high against the S&P 500; your money by sector; today's move in each US market sector; and a few plain-language findings at the top
- **Plan** page: a goal, a monthly amount and your assumptions, drawn as the range of two thousand simulated futures with the chance of getting there; and a target mix that shows how far each holding is from its target and the trades (or buys only, with new money) that would close the gap
- Holdings with today's move, total return, day and 52-week ranges, and weights; click a column heading to sort
- Daily results: each trading day's gain or loss, best and worst day, and how far you are from the portfolio's peak
- Market strip (S&P 500, Nasdaq, Dow, VIX, US 10-year yield, gold, EUR/USD); click one to chart it
- Most traded today: the four most traded cryptocurrencies and six most active US stocks (Yahoo Finance rankings, Bitcoin always included), with live prices; click one to chart it
- News whose headlines name the companies you hold, with a filter per stock
- Coming up: earnings reports and dividend payments for the stocks you hold and watch
- Buy, sell, dividend, deposit and withdraw entries with notes, with average-cost and realised profit tracking; entries can be edited
- Watchlist for stocks you don't own
- Alerts with a desktop notification (uses `notify-send` where available): a price reached, or a move of so many percent in a day. Checked every 30 seconds while the server runs
- A recorded history of your portfolio's real value (the **All** chart range), sampled every 5 minutes while markets trade
- Export of every entry as CSV, and a backup copy of the ledger before every change
- Amounts in dollars or euros, and a privacy mode that blurs what you own
- Layouts for a phone, a half-screen window and a full 1440p monitor, in light and dark, with a switch

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
| `/` or `Ctrl K` | Search for any ticker, or run any command |
| `O` `A` `P` | Overview, Analysis, Plan |
| `T` | Record a trade |
| `1` to `7` | Chart range: 1D, 1W, 1M, 3M, 1Y, 5Y, All |
| `←` `→` | Cycle the chart through the portfolio and each stock |
| `Esc` | Back to the portfolio chart, or to the Overview |
| `B` | Compare with the S&P 500 |
| `K` | Line or candles, for a single stock |
| `M` | Moving averages, on a stock's 3M, 1Y and 5Y charts |
| `C` | Switch between dollars and euros |
| `H` | Hide or show your amounts |
| `D` | Switch between dark and light |
| `?` | Show these shortcuts |

## Your data

Everything you enter (trades, watchlist, alerts, plan) is kept in `data/portfolio.json`, and the recorded value history in `data/history.jsonl`. Before any entry is added, edited or removed, the ledger as it was is copied to `data/backups/` (the newest 30 are kept). Git ignores the whole `data/` folder.

**Export** in the Activity card downloads every entry as a CSV file; the shortcuts window (`?`) also offers the full ledger as JSON. `portfolio.example.json` shows the format; copy it to `data/portfolio.json` to start from a sample.

Folio never talks to your broker. Holdings change only when you record a trade.

## Run at login (Linux, systemd)

```sh
cp folio.service ~/.config/systemd/user/
systemctl --user enable --now folio.service
```

## Notes

- Prices, company details and news come from unofficial Yahoo Finance endpoints, which can change or rate-limit without notice. If the details stop answering, the charts and prices carry on without them.
- Trades and alerts are entered in US dollars. The euro view converts everything at the current EUR/USD rate, including past amounts.
- The 1W to 5Y portfolio charts show what today's holdings would have been worth, not your actual history. The **All** range and the daily results are your actual history, from the day Folio started recording.
- Total return is the portfolio's value minus what you put in (deposits less withdrawals), so it counts paper gains, sold gains and dividends together.
- The Analysis page measures today's holdings over the past year of daily prices, or over the days all of them have traded if one is newer. It describes what happened, not what will.
- The Plan page's projection is a simulation from the growth and swings you type in, shown in today's money. It is not a forecast, and nothing in Folio is investment advice.
- Market opening times in the header don't know about US market holidays.

## Licence

MIT. See [LICENSE](LICENSE).
