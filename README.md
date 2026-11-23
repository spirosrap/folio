# Folio

A small self-hosted dashboard for a stock portfolio. You record your buys and sells; Folio shows what the portfolio is worth right now, tick by tick.

- Live prices from Yahoo Finance's trade stream, including pre-market and after hours
- Portfolio and per-stock charts (1D, 1W, 1M, 3M, 1Y, 5Y), as a line or as candles with volume, with your buys and sells marked, optional 50-day and 200-day moving averages, and drag-to-measure between any two moments
- Compare view: your portfolio, each holding and the S&P 500 in percent on one chart
- One box (`/` or `Ctrl K`) to search for any stock, fund, index or coin and chart it without owning it, or to run any command
- Details for the charted stock: day and 52-week range, volume, market value, P/E, next earnings date, a short company description, and your own position and break-even
- For companies, three more panels beside the chart: analysts' price targets and recommendations, reported profit against expectations with sales and margins, and each of your own trades measured against today's price
- **Journey** page: your investment from the first day. A short account of where you stand; a day-by-day chart of the portfolio's value against the money you put in (or of what it was made of) that you can scrub, step through or play back, with what you held and what you did on the chosen day; every purchase scored against today's price and against the S&P 500 since its day; the moments worth remembering (round amounts passed, best and worst day, furthest ahead and behind); each month's result; and every trading day as a coloured square. It can also be saved as a picture to keep or send, with or without amounts
- **Markets** page: indices in the US, Europe and Asia, interest rates, commodities, currencies and coins with live prices; which exchanges are trading right now, on your clock; today's gainers, losers, most traded and trending US stocks; and a ranking of how far each market and each of your holdings has come today, this month, this year and over a year
- **The markets, read**, at the top of that page: the day in plain words, each market's move set against its own ordinary day (so a 3.7% fall reads as "4.1 times an ordinary day, about twice a year"), where each stands against its high and its 200-day average, how nervous traders are (the VIX against ten years of itself), which parts of the market led and lagged with yours marked, what lending to the US pays now and a year ago, and what the day meant for your holdings: how far they usually move against the index, and how far they did. One press asks Claude for a brief of what seems to be behind it, drawn only from the headlines, and what it means for what you hold
- **Analysis** page: yearly swing (volatility), beta, deepest fall and Sharpe ratio of today's holdings; a risk-against-return chart; which holding causes how much of the swings; how closely your stocks move together; falls from the high against the S&P 500; what past market falls (2018, 2020, 2022, 2025 and the past year's worst) would do to today's holdings; your money by sector; today's move in each US market sector; and a few plain-language findings at the top
- **Plan** page: a goal, a monthly amount and your assumptions, drawn as the range of two thousand simulated futures with the chance of getting there; and a target mix that shows how far each holding is from its target and the trades (or buys only, with new money) that would close the gap
- **Lab** page: what if you had bought something else. Starting from your own entries, it draws what you actually did beside any mix of up to 12 tickers, and beside the S&P 500, each given the same money on the same days. Or start from a made-up amount up to ten years back, with a monthly addition and optional rebalancing: growth, swing, deepest fall, each calendar year, and what each part added. The mix starts as what you hold and keeps following it as you buy, until you change a share yourself. One click tests your target mix, or carries the result's growth and swing into the plan
- Holdings with today's move, total return, day and 52-week ranges, and weights; click a column heading to sort
- Daily results: each trading day's gain or loss, best and worst day, and how far you are from the portfolio's peak
- Market strip (S&P 500, Nasdaq, Dow, VIX, US 10-year yield, gold, EUR/USD); click one to chart it
- Most traded today: the four most traded cryptocurrencies and six most active US stocks (Yahoo Finance rankings, Bitcoin always included), with live prices; click one to chart it
- News whose headlines name the companies you hold, with a filter per stock
- Coming up: earnings reports and dividend payments for the stocks you hold and watch
- **Next** page: what to buy next, and how much. It reads your own purchases for the usual size and the usual wait between buys, then shows four ways to place the amount: back to your mix, calm the swings, carry on as before, lower what you paid. Each comes with what it would change (shares of the portfolio, average prices, the yearly swing), and Folio says which it would start with and why. Change the amount, edit the split by hand, weigh other tickers beside your holdings, and open any part as a buy to record. The Overview carries the suggestion as a card
- **Claude's read** on the Next page: Folio does the arithmetic and Claude weighs it. One press sends your holdings, purchases, Folio's figures, analysts' numbers and recent headlines to Claude, which answers with a split, its reasons, what could change its mind and what it wasn't told. You can use its split, ask it follow-up questions, tell it about yourself, or take the mix it suggests as your target. Needs [Claude Code](https://claude.com/claude-code) installed and signed in on the computer that runs Folio; without it the rest of the page works as before
- **Where it leads** on the Next page: the chosen way repeated over your next twelve buys, with prices held still, so you can see when the largest holding drops under a third and what happens to the swing. Each holding's share of the portfolio's swing is shown beside its share of the money, and you can set a target mix for new money to steer toward
- **Bets you hold on purpose** on the Next page: name a group of holdings you keep large deliberately and the range of the portfolio it should stay in. Folio shows where the bet stands now, after this buy and over the next twelve, what the portfolio would lose if the bet halved, and when new money alone brings it inside the range. The rules never take it under its floor, a target that would is flagged with a one-press fix, and Claude is told to keep its split and its suggested mix inside the range
- **Record from a screenshot or text**: drop, choose or paste a screenshot of your broker's orders (or paste the text), and Claude reads the trades out of it. Folio finds the right listing for each ticker (`VUAA` bought in euros becomes `VUAA.DE`), checks the price against the market, marks entries you already have, and records only what you tick. Open it from the trade form, the search box, or by pasting a picture anywhere on the page
- On the Next page, each slice shows what to enter at your broker in the fund's own currency, the target mix shows how far you are from it and about how many buys close the gap, and holdings also count what your funds hold of them
- Entries can be typed into the search box in plain words (`buy 1 tsla at 403.46 on jun 16`, `deposit 200 yesterday`): Folio fills in the trade form for you to check and save. `next 200` shows where $200 could go
- Buy, sell, dividend, deposit and withdraw entries with notes, with average-cost and realised profit tracking; entries can be edited
- Watchlist for stocks you don't own
- Alerts with a desktop notification (uses `notify-send` where available): a price reached, or a move of so many percent in a day. Checked every 30 seconds while the server runs
- An inbox (the bell) where Folio leaves a line when a holding moves sharply, reaches a 52-week high or low, or is about to report earnings, when the portfolio passes a round amount or closes at its highest so far, when your usual wait between buys has passed, and one on how each trading day closed; each kind can be switched off, or also sent to the desktop
- The **All** chart range: your portfolio's real value since your first entry, sampled every 5 minutes while markets trade, and rebuilt from your entries for the days before Folio was running
- Export of every entry as CSV, and a backup copy of the ledger before every change
- Amounts in dollars or euros, and a privacy mode that blurs what you own
- After four hours or more away, a line under the total with what changed since you last looked, and which holdings moved most
- A wall display (`F`): the portfolio full screen with today's line, your holdings, the indices and a clock, for a screen across the room
- Layouts for a phone, a half-screen window and a full 1440p monitor, in light and dark (or following the system), with a choice of accent colour

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
| `FOLIO_CLAUDE` | `claude` | The Claude Code command used for Claude's read |
| `FOLIO_CLAUDE_MODEL` | `claude-opus-5-5` | The model that writes it |

There is no login. With the default `FOLIO_HOST`, anyone on your network can view the dashboard, record trades and ask Claude.

## Keyboard

| Key | Action |
|---|---|
| `/` or `Ctrl K` | Search for any ticker, run any command, or type an entry |
| `O` `J` `X` `W` `A` `P` `L` | Overview, Journey, Next, Markets, Analysis, Plan, Lab |
| `F` | Wall display (`Esc` leaves it) |
| `N` | What Folio noticed for you |
| `T` | Record a trade |
| `1` to `7` | Chart range: 1D, 1W, 1M, 3M, 1Y, 5Y, All |
| `←` `→` | Cycle the chart through the portfolio and each stock; on Journey, step through the days (`Space` plays) |
| `Esc` | Back to the portfolio chart, or to the Overview |
| `B` | Compare with the S&P 500 |
| `K` | Line or candles, for a single stock |
| `M` | Moving averages, on a stock's 3M, 1Y and 5Y charts |
| `C` | Switch between dollars and euros |
| `H` | Hide or show your amounts |
| `D` | Switch between dark and light |
| `?` | Show these shortcuts |

## Your data

Everything you enter (trades, watchlist, alerts, plan, the lab's mix, the Next page's settings) is kept in `data/portfolio.json`, the recorded value history in `data/history.jsonl`, the inbox in `data/inbox.json`, and Claude's latest read in `data/claude.json`. Before any entry is added, edited or removed, the ledger as it was is copied to `data/backups/` (the newest 30 are kept). Git ignores the whole `data/` folder.

**Export** in the Activity card downloads every entry as a CSV file; the shortcuts window (`?`) also offers the full ledger as JSON. `portfolio.example.json` shows the format; copy it to `data/portfolio.json` to start from a sample.

Folio never talks to your broker. Holdings change only when you record a trade.

## Run at login (Linux, systemd)

```sh
cp folio.service ~/.config/systemd/user/
systemctl --user enable --now folio.service
```

## Notes

- Prices, company details and news come from unofficial Yahoo Finance endpoints, which can change or rate-limit without notice. If the details stop answering, the charts and prices carry on without them.
- The ledger is kept in US dollars. A stock or fund listed in another currency (`VUAA.DE` in euros, `VUSA.L` in pence) is priced in dollars at the exchange rate of each moment, and the trade form takes its price in its own currency, as your broker shows it; the entry keeps both that price and the day's rate. Alerts, deposits and dividends are entered in dollars. The euro view converts everything at the current EUR/USD rate, including past amounts.
- A handful of well-known S&P 500 funds (Vanguard VUAA/VUSA/VOO, iShares CSPX/SXR8/IVV, SPDR SPY/SPY5 and a few more) are recognised as trackers: once you hold or weigh one, the Next page uses it wherever it would have said "an S&P 500 fund", and measures its swing by the index, because a European listing closes hours before New York does.
- The 1W to 5Y portfolio charts show what today's holdings would have been worth, not your actual history. The **All** range is your actual history from your first entry: recorded every 5 minutes since Folio started running, and rebuilt day by day from your entries and closing prices for the time before that. The daily results start when recording did.
- Total return is the portfolio's value minus what you put in (deposits less withdrawals), so it counts paper gains, sold gains and dividends together.
- The Analysis page measures today's holdings over the past year of daily prices, or over the days all of them have traded if one is newer. It describes what happened, not what will.
- The Plan page's projection is a simulation from the growth and swings you type in, shown in today's money. It is not a forecast, and nothing in Folio is investment advice.
- The Lab replays past prices with dividends put back in (daily from your own purchases, weekly from a made-up start), without fees, taxes or currency costs. From your purchases, the mix and the index buy at each deposit day's closing price, so entries logged at other prices or on other days than you really bought shift the comparison. A ticker that started trading inside the period joins the mix when it lists. A good past is not a promise.
- The Next page applies fixed rules to your entries and to the past year's daily prices. "Calm the swings" puts the amount, in twenty equal steps, wherever it leaves the portfolio's day-to-day swings smallest; an S&P 500 fund is measured by the index itself. "Back to your mix" aims for the target mix you set on the Next page or on Plan, or, without one, for the shares of your money you gave each holding. None of it predicts prices, and none of it is advice.
- Claude's read is an AI model's judgement, not a rule and not advice. Folio calls Claude Code without tools, from an empty folder, and only when you press the button; it sends nothing but the ledger's figures and market data (Claude Code adds your account's email, which Anthropic already has). The answer is checked before it is shown: only things a buy can go to, amounts made to add up, and every "after" figure on the page is worked out by Folio, not by Claude. Reading a screenshot works the same way: the picture and nothing else is sent, and nothing is recorded until you confirm. At most 40 questions a day are sent, screenshots included
- The Journey page values each past day at its closing prices from your entries, and the last day at live prices. A purchase is compared with the S&P 500 from the close of the day it is dated, dividends counted.
- The stress test measures today's holdings between the S&P 500's high and low in each stretch. A holding that was not trading then is estimated from how it has moved with the market since.
- Market opening times, in the header and on the Markets page, are the usual hours and don't know about holidays.

## Licence

MIT. See [LICENSE](LICENSE).
