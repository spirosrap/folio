#!/usr/bin/env python3
"""Folio: a small self-hosted portfolio dashboard.

Holdings come from a transaction ledger (data/portfolio.json); prices come
from Yahoo Finance's public endpoints. Standard library only.
"""
import csv
import http.cookiejar
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    NY = ZoneInfo("America/New_York")
except Exception:  # no time zone database on this system
    NY = timezone(timedelta(hours=-5))

VERSION = "4.0"
ROOT = Path(__file__).resolve().parent
DATA = Path(os.environ.get("FOLIO_DATA", ROOT / "data" / "portfolio.json"))
PORT = int(os.environ.get("FOLIO_PORT", "8791"))
HOST = os.environ.get("FOLIO_HOST", "0.0.0.0")
UA = {"User-Agent": "Mozilla/5.0"}

RANGES = {  # range -> (yahoo interval, cache seconds)
    "1d": ("1m", 5),  # with pre-market and after-hours trades
    "5d": ("30m", 120),
    "1mo": ("60m", 600),
    "3mo": ("1d", 1800),
    "1y": ("1d", 1800),
    "5y": ("1wk", 3600),
}
SPANS = {**RANGES, "2y": ("1d", 1800), "10y": ("1wk", 3600)}  # longer runs, for moving averages
BENCH = ("^GSPC", "S&P 500")  # what the chart's comparison view measures against

HIST = DATA.with_name("history.jsonl")  # recorded portfolio value over time
BACKUPS = DATA.with_name("backups")
_lock = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=8)


class Bad(Exception):
    pass


def cached(key, ttl, make, keep_on_error=True):
    """A value from the cache, remade once it is older than ttl seconds."""
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        out = make()
    except Exception:
        if hit and keep_on_error:
            return hit[1]
        raise
    with _cache_lock:
        _cache[key] = (now, out)
    return out


def cached_soft(key, ttl, make, miss_ttl=300):
    """Like cached, but a failure answers {} and is only remembered for a few minutes."""
    with _cache_lock:
        miss = _cache.get(("miss", key))
    if miss and time.time() - miss[0] < miss_ttl:
        return miss[1]
    try:
        return cached(key, ttl, make)
    except Exception:
        with _cache_lock:
            _cache[("miss", key)] = (time.time(), {})
        return {}


def fetch(url, timeout=10):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return json.load(r)


# ---------- ledger ----------

def load():
    doc = json.loads(DATA.read_text()) if DATA.exists() else {}
    for key in ("transactions", "watchlist", "alerts"):
        doc.setdefault(key, [])
    doc.setdefault("plan", {})
    doc.setdefault("lab", {})
    return doc


def save(doc, backup=False):
    """Write the ledger. With backup, the copy being replaced is kept in data/backups (the newest 30)."""
    DATA.parent.mkdir(parents=True, exist_ok=True)
    if backup and DATA.exists():
        try:
            BACKUPS.mkdir(exist_ok=True)
            shutil.copy2(DATA, BACKUPS / f"portfolio-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}.json")
            for old in sorted(BACKUPS.glob("portfolio-*.json"))[:-30]:
                old.unlink()
        except OSError as e:
            print("backup:", repr(e), file=sys.stderr)
    tmp = DATA.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2))
    tmp.replace(DATA)


def replay(txs):
    """Average-cost replay. Returns (positions, cash, realized, deposited, dividends)."""
    pos, cash, realized, deposited, income, divs = {}, 0.0, 0.0, 0.0, 0.0, {}
    for t in sorted(txs, key=lambda t: t["date"]):  # stable: same-day keeps entry order
        kind, fee = t["type"], float(t.get("fee") or 0)
        if kind == "deposit":
            cash += t["amount"]
            deposited += t["amount"]
        elif kind == "withdraw":
            cash -= t["amount"]
            deposited -= t["amount"]
        elif kind == "dividend":
            cash += t["amount"]
            income += t["amount"]
            divs[t["symbol"]] = divs.get(t["symbol"], 0.0) + t["amount"]
        elif kind == "buy":
            p = pos.setdefault(t["symbol"], {"symbol": t["symbol"], "name": t.get("name") or t["symbol"], "qty": 0.0, "cost": 0.0})
            p["qty"] += t["qty"]
            p["cost"] += t["qty"] * t["price"] + fee
            cash -= t["qty"] * t["price"] + fee
        elif kind == "sell":
            p = pos.get(t["symbol"])
            if not p or t["qty"] > p["qty"] + 1e-9:
                held = p["qty"] if p else 0
                raise Bad(f"Can't sell {t['qty']:g} {t['symbol']} on {t['date']}: only {held:g} held then.")
            avg = p["cost"] / p["qty"]
            realized += t["qty"] * (t["price"] - avg) - fee
            p["qty"] -= t["qty"]
            p["cost"] -= t["qty"] * avg
            cash += t["qty"] * t["price"] - fee
            if p["qty"] < 1e-9:
                del pos[t["symbol"]]
    for s, p in pos.items():
        p["div"] = divs.get(s, 0.0)
    return pos, cash, realized, deposited, income


# ---------- quotes ----------

def yahoo(symbol, rng):
    interval, ttl = SPANS[rng]
    key = (symbol, rng)
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
           f"?range={rng}&interval={interval}" + ("&includePrePost=true" if rng == "1d" else ""))
    try:
        res = fetch(url)["chart"]["result"][0]
        meta = res["meta"]
        ind = (res.get("indicators", {}).get("quote") or [{}])[0]
        col = lambda k: ind.get(k) or []
        bars = [(t, o if o is not None else c, max(h if h is not None else c, c), min(l if l is not None else c, c), c, v or 0)
                for t, o, h, l, c, v in zip(res.get("timestamp") or [], col("open"), col("high"), col("low"), col("close"), col("volume"))
                if c is not None]
        pts = [(b[0], b[4]) for b in bars]
        tp = meta.get("currentTradingPeriod") or {}
        state = "closed"
        for name in ("pre", "regular", "post"):
            p = tp.get(name) or {}
            if p.get("start", 0) <= now < p.get("end", 0):
                state = "open" if name == "regular" else name
        reg, reg_t = meta["regularMarketPrice"], meta.get("regularMarketTime") or 0
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        price, ext, last_t = reg, None, reg_t
        reg_start = (tp.get("regular") or {}).get("start", 0)
        pre_start = (tp.get("pre") or {}).get("start", 0)
        if reg_t < reg_start and now >= pre_start:
            prev = reg  # today's session hasn't opened: the last regular price is the previous close
        if rng == "1d" and pts and pts[-1][0] > reg_t + 60:  # a trade after the last regular-hours one
            last_t, price = pts[-1]
            ext = "pre" if reg_t < reg_start and last_t >= pre_start else "post"
        out = {
            "regular": reg, "ext": ext,
            "dayHigh": meta.get("regularMarketDayHigh"), "dayLow": meta.get("regularMarketDayLow"),
            "wkHigh": meta.get("fiftyTwoWeekHigh"), "wkLow": meta.get("fiftyTwoWeekLow"),
            "volume": meta.get("regularMarketVolume"), "exchange": meta.get("fullExchangeName"),
            "type": meta.get("instrumentType"),
            "periods": {k: [v.get("start", 0), v.get("end", 0)] for k, v in tp.items()},
            "symbol": meta["symbol"],
            "name": meta.get("longName") or meta.get("shortName") or meta["symbol"],
            "price": price,
            "prevClose": prev,
            "currency": meta.get("currency"),
            "time": last_t,
            "state": state,
            "points": pts,
            "bars": bars,
        }
    except Exception as e:  # keep serving the last good quote if the fetch fails
        if hit:
            return hit[1]
        raise Bad(f"Couldn't get a price for {symbol}. Check the ticker and your connection.")
    with _cache_lock:
        _cache[key] = (now, out)
    return out


def quotes(symbols, rng):
    out = {}
    for s, f in [(s, _pool.submit(yahoo, s, rng)) for s in symbols]:
        try:
            out[s] = f.result()
        except Bad:
            out[s] = None
    return out


# Yahoo's fuller quote records (market cap, volume, earnings dates) need a cookie and a "crumb".
_sess = {"op": None, "crumb": "", "at": 0.0}
_sess_lock = threading.Lock()


def _session(fresh=False):
    with _sess_lock:
        if _sess["op"] and not fresh and time.time() - _sess["at"] < 6 * 3600:
            return _sess["op"], _sess["crumb"]
        op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        try:
            op.open(urllib.request.Request("https://fc.yahoo.com", headers=UA), timeout=10).read()
        except urllib.error.HTTPError:
            pass  # answers 404 but still hands out the cookie
        with op.open(urllib.request.Request("https://query2.finance.yahoo.com/v1/test/getcrumb", headers=UA), timeout=10) as r:
            crumb = r.read().decode()
        _sess.update(op=op, crumb=crumb, at=time.time())
        return op, crumb


def _authed(url):
    for attempt in (0, 1):
        op, crumb = _session(fresh=bool(attempt))
        try:
            with op.open(urllib.request.Request(url + "&crumb=" + urllib.parse.quote(crumb), headers=UA), timeout=10) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if attempt or e.code not in (401, 403):
                raise


def quotes_full(symbols):
    """Fuller quote records by symbol, each kept for a minute. A symbol Yahoo won't answer for maps to {}."""
    now, out, need = time.time(), {}, []
    with _cache_lock:
        for s in symbols:
            hit = _cache.get(("full", s))
            if hit and now - hit[0] < 60:
                out[s] = hit[1]
            else:
                need.append(s)
    if need:
        got = {}
        try:
            url = "https://query2.finance.yahoo.com/v7/finance/quote?symbols=" + urllib.parse.quote(",".join(need))
            got = {r["symbol"]: r for r in _authed(url)["quoteResponse"]["result"]}
        except Exception as e:
            print("quotes_full:", repr(e), file=sys.stderr)
        with _cache_lock:
            for s in need:
                old = _cache.get(("full", s))
                out[s] = got.get(s) or (old[1] if old else {})
                _cache[("full", s)] = (now, out[s])  # failures are remembered for the minute too
    return out


def profile(symbol):
    """Sector, industry and a short description of a company. Kept for a day."""
    def make():
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(symbol)}?modules=assetProfile"
        p = _authed(url)["quoteSummary"]["result"][0]["assetProfile"]
        about = " ".join((p.get("longBusinessSummary") or "").split())
        if len(about) > 340:  # the first sentences that fit
            cut = about[:340]
            about = cut[:cut.rfind(". ") + 1] if ". " in cut[120:] else cut.rsplit(" ", 1)[0] + "…"
        site = p.get("website") or ""
        return {"sector": p.get("sector"), "industry": p.get("industry"), "about": about,
                "website": site if site.startswith("https://") else None}
    return cached_soft(("profile", symbol), 86400, make)


def fundamentals(symbol):
    """What analysts expect of a company and what it has reported. Kept for six hours."""
    def make():
        url = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(symbol)}"
               "?modules=financialData,recommendationTrend,earnings,defaultKeyStatistics")
        r = _authed(url)["quoteSummary"]["result"][0]
        raw = lambda d, k: ((d or {}).get(k) or {}).get("raw")
        fd, ks, er = r.get("financialData") or {}, r.get("defaultKeyStatistics") or {}, r.get("earnings") or {}
        out = {"currency": er.get("financialCurrency") or fd.get("financialCurrency")}
        if raw(fd, "targetMeanPrice") and raw(fd, "targetLowPrice") and raw(fd, "targetHighPrice"):
            out["target"] = {"low": raw(fd, "targetLowPrice"), "mean": raw(fd, "targetMeanPrice"), "median": raw(fd, "targetMedianPrice"),
                             "high": raw(fd, "targetHighPrice"), "n": raw(fd, "numberOfAnalystOpinions")}
        keys = ("strongBuy", "buy", "hold", "sell", "strongSell")
        out["ratings"] = [{"period": t.get("period"), "n": [int(t.get(k) or 0) for k in keys]}
                          for t in (r.get("recommendationTrend") or {}).get("trend") or [] if sum(int(t.get(k) or 0) for k in keys)]
        ch = er.get("earningsChart") or {}
        out["eps"] = [{"q": x.get("date"), "actual": raw(x, "actual"), "estimate": raw(x, "estimate"), "reported": raw(x, "reportedDate")}
                      for x in ch.get("quarterly") or [] if raw(x, "actual") is not None]
        if raw(ch, "currentQuarterEstimate") is not None:
            out["next"] = {"q": f"{ch.get('currentQuarterEstimateDate') or ''}{ch.get('currentQuarterEstimateYear') or ''}", "estimate": raw(ch, "currentQuarterEstimate")}
        fc = er.get("financialsChart") or {}
        for span in ("yearly", "quarterly"):
            out[span] = [{"date": str(x.get("date")), "revenue": raw(x, "revenue"), "earnings": raw(x, "earnings")}
                         for x in fc.get(span) or [] if raw(x, "revenue") is not None][-4:]
        out["margins"] = {k: raw(fd, f) for k, f in (("gross", "grossMargins"), ("operating", "operatingMargins"), ("profit", "profitMargins"))}
        out["growth"] = {"revenue": raw(fd, "revenueGrowth"), "earnings": raw(fd, "earningsGrowth")}
        out["beta"] = raw(ks, "beta")
        return out
    return cached_soft(("fund", symbol), 21600, make)


def detail(symbol):
    """Everything the page shows about one symbol, held or not."""
    if not symbol:
        raise Bad("Enter a ticker symbol.")
    qt = yahoo(symbol, "1d")
    full = quotes_full([symbol]).get(symbol) or {}
    g = full.get
    kind = g("quoteType") or qt["type"]
    prof = profile(symbol) if kind == "EQUITY" else {}
    now = time.time()
    return {
        "symbol": qt["symbol"], "name": qt["name"], "price": qt["price"], "regular": qt["regular"], "prevClose": qt["prevClose"],
        "ext": qt["ext"], "state": qt["state"], "time": qt["time"], "currency": qt["currency"],
        "exchange": g("fullExchangeName") or qt["exchange"], "type": kind,
        "open": g("regularMarketOpen"), "dayHigh": qt["dayHigh"], "dayLow": qt["dayLow"],
        "wkHigh": qt["wkHigh"], "wkLow": qt["wkLow"],
        "volume": g("regularMarketVolume") or qt["volume"], "avgVolume": g("averageDailyVolume3Month"),
        "marketCap": g("marketCap"), "pe": g("trailingPE"), "forwardPe": g("forwardPE"), "eps": g("epsTrailingTwelveMonths"),
        "ma50": g("fiftyDayAverage"), "ma200": g("twoHundredDayAverage"),
        "yield": (g("trailingAnnualDividendYield") or 0) * 100 or None,
        "earnings": next((t for t in (g("earningsTimestampStart"), g("earningsTimestamp")) if t and t > now - 43200), None),
        "earningsEstimate": bool(g("isEarningsDateEstimate")),
        "rating": g("averageAnalystRating"),
        "fund": fundamentals(symbol) if kind == "EQUITY" else {},
        **{k: prof.get(k) for k in ("sector", "industry", "about", "website")},
    }


def search(q):
    """Ticker suggestions for a few typed letters."""
    q = " ".join(q.split())[:40]
    if not q:
        return []

    def make():
        res = fetch("https://query1.finance.yahoo.com/v1/finance/search?q="
                    f"{urllib.parse.quote(q)}&quotesCount=8&newsCount=0")
        return [{"symbol": x["symbol"], "name": x.get("longname") or x.get("shortname") or x["symbol"],
                 "exchange": x.get("exchDisp") or "", "type": x.get("typeDisp") or ""}
                for x in res.get("quotes") or [] if x.get("symbol") and x.get("isYahooFinance")]
    try:
        return cached(("search", q.lower()), 600, make)
    except Exception:
        raise Bad("Search isn't answering right now.")


# ---------- api ----------

def ui_version():
    return str(int((ROOT / "index.html").stat().st_mtime))


def clock(periods, now):
    """When the regular US session next opens, or closes if it is open now. Holidays aren't known here."""
    reg = (periods or {}).get("regular") or [0, 0]
    if now < reg[0]:
        return {"opens": reg[0]}
    if now < reg[1]:
        return {"closes": reg[1]}
    d = (datetime.fromtimestamp(now, NY) + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return {"opens": int(d.timestamp())}


def state():
    with _lock:
        doc = load()
    pos, cash, realized, deposited, income = replay(doc["transactions"])
    q = quotes(list(pos) + [BENCH[0]], "1d")
    bench = q.pop(BENCH[0], None)
    rows, value, cost, day, ext, stale = [], 0.0, 0.0, 0.0, 0.0, []
    for sym, p in pos.items():
        qt = q.get(sym)
        avg = p["cost"] / p["qty"]
        price = qt["price"] if qt else avg
        reg = qt["regular"] if qt else price
        prev = (qt and qt["prevClose"]) or reg
        if not qt:
            stale.append(sym)
        v = p["qty"] * price
        rows.append({
            "symbol": sym, "name": (qt and qt["name"]) or p["name"], "qty": p["qty"],
            "avgCost": avg, "cost": p["cost"], "price": price, "prevClose": prev,
            "value": v, "dayChange": p["qty"] * (reg - prev),
            "dayPct": (reg / prev - 1) * 100 if prev else 0,
            "ext": qt["ext"] if qt else None,
            **{k: (qt[k] if qt else None) for k in ("dayHigh", "dayLow", "wkHigh", "wkLow")}, "extChange": p["qty"] * (price - reg),
            "extPct": (price / reg - 1) * 100 if reg else 0,
            "pl": v - p["cost"], "plPct": (v / p["cost"] - 1) * 100 if p["cost"] else 0,
            "dividends": p["div"],
            "spark": [c for _, c in qt["points"]] if qt else [],
            "state": qt["state"] if qt else "closed", "time": qt and qt["time"],
        })
        value += v
        cost += p["cost"]
        day += p["qty"] * (reg - prev)
        ext += p["qty"] * (price - reg)
    states = {r["state"] for r in rows}
    market = next((s for s in ("open", "pre", "post") if s in states), "closed")
    return {
        "positions": rows, "cash": cash, "total": value + cash, "invested": value, "cost": cost,
        "dayChange": day, "dayPct": day / (value - ext - day) * 100 if value - ext - day else 0,
        "ext": next((e for e in ("pre", "post") if any(r["ext"] == e for r in rows)), None),
        "extChange": ext, "extPct": ext / (value - ext) * 100 if value - ext else 0,
        "pl": value - cost, "plPct": (value / cost - 1) * 100 if cost else 0,
        "realized": realized, "dividends": income, "deposited": deposited, "market": market, "stale": stale,
        "clock": clock(bench and bench["periods"], time.time()),
        "asOf": max([r["time"] or 0 for r in rows], default=0),
        "transactions": sorted(doc["transactions"], key=lambda t: t["date"])[::-1],  # newest first
        "watch": [{"symbol": s, "name": w["name"], "price": w["price"], "prevClose": w["prevClose"],
                   "spark": [c for _, c in w["points"]][::5]}
                  for s, w in quotes(doc["watchlist"], "1d").items() if w],
        "alerts": doc["alerts"], "plan": doc["plan"], "lab": doc["lab"], "inbox": inbox_view(),
        "ui": ui_version(), "version": VERSION,
    }


MARKET = {"^GSPC": "S&P 500", "^IXIC": "Nasdaq", "^DJI": "Dow Jones", "^VIX": "VIX (volatility)",
          "^TNX": "US 10-year yield", "GC=F": "Gold", "EURUSD=X": "EUR / USD"}
POPULAR = {"BTC-USD": ("Bitcoin", "crypto")}  # last good ranking; Bitcoin is always shown


def _screen(scr_id, count):
    return fetch(f"https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds={scr_id}&count={count}")["finance"]["result"][0]["quotes"]


def popular():
    """Today's most traded: top 4 cryptocurrencies by 24-hour volume and top 6 US stocks by shares traded (Yahoo's rankings)."""
    global POPULAR
    now = time.time()
    with _cache_lock:
        hit = _cache.get("popular")
    if hit and now - hit[0] < 300:
        return hit[1]
    try:
        out = {"BTC-USD": ("Bitcoin", "crypto")}
        for q in _screen("most_actives_cryptocurrencies", 25):
            name, price = q.get("shortName") or q["symbol"], q.get("regularMarketPrice") or 0
            pegged = 0.97 < price < 1.03 and abs(q.get("regularMarketChangePercent") or 0) < 0.3  # stablecoins
            if pegged or re.search(r"wrapped|staked|weth|steth", name, re.I):
                continue
            out.setdefault(q["symbol"], (re.sub(r"\s+USD$", "", name), "crypto"))
            if len(out) == 4:
                break
        for q in _screen("most_actives", 6):
            out[q["symbol"]] = (q.get("shortName") or q["symbol"], "stock")
        POPULAR = out
    except Exception:
        out = POPULAR
    with _cache_lock:
        _cache["popular"] = (now, out)
    return out


def market():
    out = []
    pop = popular()
    names = {**{s: n for s, (n, _) in pop.items()}, **MARKET}
    for sym, qt in quotes(list(names), "1d").items():
        if qt:
            out.append({"symbol": sym, "name": names[sym], "group": "index" if sym in MARKET else "popular", "kind": pop[sym][1] if sym in pop else None,
                        "price": qt["price"], "prevClose": qt["prevClose"],
                        "spark": [c for _, c in qt["points"]][::5]})
    return out


def hist_rows():
    """The recorded value history: one {t, v, d} row per snapshot."""
    if not HIST.exists():
        return []
    st = HIST.stat()
    key = (st.st_mtime_ns, st.st_size)
    with _cache_lock:
        hit = _cache.get("hist")
    if hit and hit[0] == key:
        return hit[1]
    rows = []
    for line in HIST.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            pass  # a half-written last line
    with _cache_lock:
        _cache["hist"] = (key, rows)
    return rows


def _rebuilt(until):
    """One row per trading day from the first ledger entry up to the day recording began, from the entries and closing prices."""
    with _lock:
        doc = load()
    txs = sorted(doc["transactions"], key=lambda t: t["date"])
    stop = ny_day(until)
    if not txs or txs[0]["date"] >= stop:
        return []

    def make():
        own = list(dict.fromkeys(t["symbol"] for t in txs if t.get("symbol")))
        got = _dailies(own + [BENCH[0]])
        if BENCH[0] not in got:
            raise Bad("Couldn't load the S&P 500's history.")
        days = [d for d, _, _ in got[BENCH[0]]["rows"] if txs[0]["date"] <= d < stop]
        if not days:
            return []
        px = {}
        for s in own:
            if s in got:
                col, j, last = [], 0, None
                for d in days:
                    while j < len(got[s]["rows"]) and got[s]["rows"][j][0] <= d:
                        last = got[s]["rows"][j][1]
                        j += 1
                    col.append(last)
                px[s] = col
        led = _ledger_on([t for t in txs if t["date"] <= days[-1]], days, px)
        flows, paid, out = dict(led["flows"]), 0.0, []
        for i, d in enumerate(days):
            paid += flows.get(i, 0.0)
            y, m, dd = map(int, d.split("-"))
            out.append({"t": int(datetime(y, m, dd, 16, 0, tzinfo=NY).timestamp()), "v": led["v"][i], "d": round(paid, 2), "b": 1})
        return out
    try:
        return cached(("rebuilt", stop, json.dumps(txs, sort_keys=True)), 21600, make)
    except Exception as e:
        print("rebuilt:", repr(e), file=sys.stderr)
        return []


def full_rows():
    """The value history from your first entry: rebuilt day by day until recording began, then as recorded."""
    rows = hist_rows()
    return _rebuilt(rows[0]["t"] if rows else time.time()) + rows


def growth(rows):
    """The portfolio's growth at each snapshot, starting from 1, with deposits and withdrawals taken out."""
    idx = [1.0] * len(rows)
    for k in range(1, len(rows)):
        prev, flow = rows[k - 1]["v"], rows[k]["d"] - rows[k - 1]["d"]
        idx[k] = idx[k - 1] * ((rows[k]["v"] - flow) / prev if prev > 0 else 1)
    return idx


def insights():
    """Day-by-day results and the high-water mark, from the recorded history."""
    rows = full_rows()
    if len(rows) < 2:
        return {"days": [], "since": rows[0]["t"] if rows else None}
    idx = growth(rows)
    days = {}
    for r, ix in zip(rows, idx):
        dt = day = datetime.fromtimestamp(r["t"], NY)
        if day.hour < 4:  # the small hours and weekends belong to the trading day before them
            day -= timedelta(days=1)
        while day.weekday() >= 5:
            day -= timedelta(days=1)
        own = day.date() == dt.date()
        d = days.setdefault(day.strftime("%Y-%m-%d"), {"first": (r, ix), "n": 0})
        d["last"] = (r, ix)
        d["n"] += own
        d["built"] = d.get("built") or bool(r.get("b"))
        if own and (dt.hour, dt.minute) <= (16, 5):  # a day's result is its value at the regular close
            d["reg"] = (r, ix)
    out, prev = [], None
    for date, d in days.items():
        if d["n"] < 3 and not d["built"]:
            continue  # a stray snapshot or two (one is taken whenever the server starts), not a day's trading
        r, ix = d.get("reg") or d["last"]
        pr, pix = prev or d["first"]
        out.append({"date": date, "v": r["v"], "pl": round(r["v"] - pr["v"] - (r["d"] - pr["d"]), 2),
                    "pct": round((ix / pix - 1) * 100, 3) if pix else 0})
        prev = (r, ix)
    if not out:
        return {"days": [], "since": rows[0]["t"]}
    k = max(range(len(rows)), key=lambda i: idx[i])
    moved = [d for d in out if abs(d["pl"]) >= 0.005]
    streak = 0
    for d in reversed(moved):
        sign = 1 if d["pl"] > 0 else -1
        if streak and (streak > 0) != (sign > 0):
            break
        streak += sign
    return {
        "days": out[-60:], "since": rows[0]["t"],
        "peak": {"v": rows[k]["v"], "t": rows[k]["t"], "now": round(rows[-1]["v"] * idx[k] / idx[-1], 2) if idx[-1] else None},
        "drawdown": round((idx[-1] / idx[k] - 1) * 100, 3), "growth": round((idx[-1] - 1) * 100, 3),
        "best": max(out, key=lambda d: d["pct"]), "worst": min(out, key=lambda d: d["pct"]),
        "up": sum(d["pl"] > 0 for d in moved), "down": sum(d["pl"] < 0 for d in moved), "streak": streak,
    }


def _ffill(ts, pts):
    """A (time, value) series read at each of ts, carrying the last value forward."""
    out, j, last = [], 0, None
    for t in ts:
        while j < len(pts) and pts[j][0] <= t:
            last = pts[j][1]
            j += 1
        out.append(last)
    return out


def _pct(vals, base):
    return [None if v is None or not base else round((v / base - 1) * 100, 3) for v in vals]


def _versus(ts, rng, symbols, names=None):
    """Each symbol's percentage change over the chart's own timeline."""
    out = []
    for sym, qt in quotes(symbols, rng).items():
        if not qt or not qt["points"]:
            continue
        vals = _ffill(ts, qt["points"])
        base = qt["prevClose"] if rng == "1d" else next((v for v in vals if v is not None), None)
        out.append({"symbol": sym, "name": (names or {}).get(sym, sym), "pct": _pct(vals, base)})
    return out


def _candles(bars, limit=110):
    """Bars merged into at most `limit` candles: [time, open, high, low, close, volume]."""
    n = max(1, -(-len(bars) // limit))
    out = []
    for i in range(0, len(bars), n):
        g = bars[i:i + n]
        out.append([g[0][0], g[0][1], max(b[2] for b in g), min(b[3] for b in g), g[-1][4], sum(b[5] for b in g)])
    return out, n


AVERAGES = {"3mo": ("2y", ((50, "50-day"), (200, "200-day"))), "1y": ("2y", ((50, "50-day"), (200, "200-day"))),
            "5y": ("10y", ((10, "10-week"), (40, "40-week")))}  # chart range -> (longer run to average over, the averages)


def _utc_day(t):
    return time.strftime("%Y-%m-%d", time.gmtime(t))


def averages(symbol, rng, ts):
    """Moving averages of the closing price, read at each of the chart's bars."""
    if rng not in AVERAGES or not ts:
        return []
    span, kinds = AVERAGES[rng]
    try:
        pts = yahoo(symbol, span)["points"]
    except Bad:
        return []
    closes, out = [c for _, c in pts], []
    for n, name in kinds:
        by, run = {}, 0.0
        for i, c in enumerate(closes):
            run += c - (closes[i - n] if i >= n else 0)
            if i >= n - 1:
                by[_utc_day(pts[i][0])] = run / n
        vals = [by.get(_utc_day(t)) for t in ts]
        if sum(v is not None for v in vals) > 1:
            out.append({"n": n, "name": name, "v": [None if v is None else round(v, 4) for v in vals]})
    return out


def history(rng, symbol=None, compare=False, avg=False):
    if rng == "all" and symbol:
        rng = "1y"
    if rng == "all":  # the portfolio's real recorded value
        rows = full_rows()
        idx = growth(rows)
        if rows and rows[-1]["t"] - rows[0]["t"] > 14 * 86400:  # a long stretch: the last value of each day, so time runs evenly
            by = {}  # each day's value at the regular close, or its last one where there is none
            for i, row in enumerate(rows):
                at = datetime.fromtimestamp(row["t"], NY)
                day, late = at.strftime("%Y-%m-%d"), (at.hour, at.minute) > (16, 5)
                if day not in by or not late or by[day][1]:
                    by[day] = (i, late)
            keep = sorted({i for i, _ in by.values()} | {len(rows) - 1})
            keep = keep[::max(1, len(keep) // 1500)] + ([keep[-1]] if (len(keep) - 1) % max(1, len(keep) // 1500) else [])
        else:
            step = max(1, len(rows) // 1500)
            keep = list(range(0, len(rows), step))
            if rows and keep[-1] != len(rows) - 1:
                keep.append(len(rows) - 1)
        ts = [rows[i]["t"] for i in keep]
        out = {"t": ts, "v": [rows[i]["v"] for i in keep], "pct": [round((idx[i] - 1) * 100, 3) for i in keep],
               "base": None, "regular": None, "missing": [], "flow": rows[-1]["d"] - rows[0]["d"] if rows else 0}
        if compare and len(ts) > 1:  # the benchmark over the same stretch, from the shortest range that covers it
            span = (ts[-1] - ts[0]) / 86400
            brng = "5d" if span < 3 else "1mo" if span < 25 else "3mo" if span < 80 else "1y" if span < 350 else "5y"
            try:
                vals = _ffill(ts, yahoo(BENCH[0], brng)["points"])
                base = next((v for v in vals if v is not None), None)
                out["versus"] = [{"symbol": BENCH[0], "name": BENCH[1], "pct": _pct(vals, base)}]
            except Bad:
                pass
        return out
    if rng not in RANGES:
        raise Bad("Unknown range")
    with _lock:
        doc = load()
    pos, cash, _, _, _ = replay(doc["transactions"])
    if symbol:  # one stock's own price
        qt = yahoo(symbol, rng)
        held = pos.get(symbol)
        ts, vals = [t for t, _ in qt["points"]], [c for _, c in qt["points"]]
        base = qt["prevClose"] if rng == "1d" else None
        candles, per = _candles(qt["bars"])
        marks = []
        if rng != "1d" and ts:  # your trades in this stock, each on the first bar of its day
            day = [datetime.fromtimestamp(t, NY).strftime("%Y-%m-%d") for t in ts]
            for tx in doc["transactions"]:
                if tx.get("symbol") == symbol and tx["type"] in ("buy", "sell") and day[0] <= tx["date"] <= day[-1]:
                    i = next(i for i, d in enumerate(day) if d >= tx["date"])
                    marks.append({"i": i, "type": tx["type"], "qty": tx["qty"], "price": tx["price"], "date": tx["date"]})
        out = {"t": ts, "v": vals, "pct": _pct(vals, base or (vals[0] if vals else None)),
               "base": base, "missing": [], "symbol": symbol, "name": qt["name"],
               "regular": qt["periods"].get("regular") if rng == "1d" else None,
               "avg": held["cost"] / held["qty"] if held else None,
               "candles": candles, "per": per, "marks": marks}
        if compare and symbol != BENCH[0]:
            out["versus"] = _versus(ts, rng, [BENCH[0]], dict([BENCH]))
        if avg:
            out["ma"] = averages(symbol, rng, ts)
        return out
    q = {s: v for s, v in quotes(list(pos), rng).items() if v and v["points"]}
    if not q:
        return {"t": [], "v": [], "base": None, "missing": list(pos), "regular": None}
    series = {s: dict(v["points"]) for s, v in q.items()}
    ts = sorted(set().union(*[set(d) for d in series.values()]))
    last, t_out, v_out = {}, [], []
    for t in ts:
        for s, d in series.items():
            if t in d:
                last[s] = d[t]
        if len(last) == len(series):  # only once every holding has a price
            t_out.append(t)
            v_out.append(cash + sum(pos[s]["qty"] * last[s] for s in series))
    base = None
    if rng == "1d":
        base = cash + sum(pos[s]["qty"] * (q[s]["prevClose"] or 0) for s in series)
    regular = next(iter(q.values()))["periods"].get("regular") if rng == "1d" else None
    out = {"t": t_out, "v": v_out, "pct": _pct(v_out, base or (v_out[0] if v_out else None)),
           "base": base, "regular": regular, "missing": [s for s in pos if s not in q]}
    if compare:
        out["versus"] = _versus(t_out, rng, [BENCH[0]] + list(q), dict([BENCH]))
    return out


def news():
    """Recent stories that are really about a holding, newest first."""
    with _lock:
        doc = load()
    pos = replay(doc["transactions"])[0]
    key, now = ("news", tuple(sorted(pos))), time.time()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < 300:
        return hit[1]

    def stories_for(sym):
        return fetch("https://query1.finance.yahoo.com/v1/finance/search?q="
                     f"{urllib.parse.quote(sym)}&newsCount=20&quotesCount=0").get("news") or []

    seen, items, failed = {}, [], 0
    for sym, fut in [(s, _pool.submit(stories_for, s)) for s in pos]:
        try:
            stories = fut.result()
        except Exception:
            failed += 1
            continue
        # the name people actually use in headlines, where it differs from the registered one
        word = {"SPCX": "spacex"}.get(sym, re.sub(r"\W", "", pos[sym]["name"].split()[0].lower()))
        named = re.compile(rf"\b({re.escape(word)}|{re.escape(sym.lower())})\b")
        kept = 0
        for st in sorted(stories, key=lambda x: -x.get("providerPublishTime", 0)):
            title = st.get("title") or ""
            link = st.get("link") or ""
            low = title.lower()
            if not title or not link.startswith("https://") or not named.search(low):
                continue  # skip round-ups that only mention the stock in passing
            if st["uuid"] in seen:
                if sym not in seen[st["uuid"]]["symbols"]:
                    seen[st["uuid"]]["symbols"].append(sym)
                continue
            if kept >= 8:
                continue
            kept += 1
            thumbs = (st.get("thumbnail") or {}).get("resolutions") or []
            thumb = min(thumbs, key=lambda t: t.get("width", 1e9)).get("url") if thumbs else None
            seen[st["uuid"]] = {"title": title, "publisher": st.get("publisher") or "", "link": link,
                                "time": st.get("providerPublishTime", 0), "symbols": [sym],
                                "thumb": thumb if thumb and thumb.startswith("https://") else None}
            items.append(seen[st["uuid"]])
    items.sort(key=lambda x: -x["time"])
    out = {"items": items[:24], "failed": failed == len(pos) and bool(pos)}
    if hit and out["failed"]:
        return hit[1]
    if not out["failed"]:
        with _cache_lock:
            _cache[key] = (now, out)
    return out


def calendar():
    """Upcoming earnings reports and dividend payments for what you hold and watch, soonest first."""
    with _lock:
        doc = load()
    pos = replay(doc["transactions"])[0]
    syms = list(pos) + [s for s in doc["watchlist"] if s not in pos]

    def make():
        now, out = time.time(), []
        for s, r in quotes_full(syms).items():
            e = next((t for t in (r.get("earningsTimestampStart"), r.get("earningsTimestamp")) if t and t > now - 43200), None)
            if e:
                out.append({"symbol": s, "kind": "earnings", "t": e, "estimate": bool(r.get("isEarningsDateEstimate")), "held": s in pos})
            d = r.get("dividendDate")
            if d and d > now - 86400:
                out.append({"symbol": s, "kind": "dividend", "t": d, "estimate": False, "held": s in pos})
        return sorted(out, key=lambda x: x["t"])
    return cached(("calendar", tuple(syms)), 1800, make) if syms else []


# ---------- analysis ----------

def _mean(x):
    return sum(x) / len(x)


def _cov(x, y):
    mx, my = _mean(x), _mean(y)
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (len(x) - 1)


def _corr(x, y):
    sx, sy = _cov(x, x), _cov(y, y)
    return _cov(x, y) / math.sqrt(sx * sy) if sx > 0 and sy > 0 else None


def _drawdown(vals):
    """How far each value sits below the highest one before it, in percent."""
    out, top = [], vals[0]
    for v in vals:
        top = max(top, v)
        out.append((v / top - 1) * 100)
    return out


def _r(x, d=2):
    return None if x is None else round(x, d)


def analysis():
    """Risk, diversification and benchmark figures for today's holdings, from a year of daily prices."""
    with _lock:
        doc = load()
    pos, cash = replay(doc["transactions"])[:2]
    flows = tuple((t["date"], t["amount"] if t["type"] == "deposit" else -t["amount"])
                  for t in doc["transactions"] if t["type"] in ("deposit", "withdraw"))
    key = ("analysis", tuple(sorted((s, round(p["qty"], 6)) for s, p in pos.items())), round(cash, 2), flows)
    return cached(key, 900, lambda: _analyse(pos, cash, flows))


def _analyse(pos, cash, flows):
    q = quotes(list(pos) + [BENCH[0]], "1y")
    bench = q.pop(BENCH[0], None)
    if not bench or len(bench["points"]) < 30:
        raise Bad("Couldn't load a year of prices for the S&P 500.")
    days, ts, bpx = [], [], []  # one row per day the benchmark traded
    for t, c in bench["points"]:
        d = _utc_day(t)
        if days and days[-1] == d:
            bpx[-1] = c
        else:
            days.append(d), ts.append(t), bpx.append(c)
    px = {}
    for s in pos:  # each holding's close on those days, carrying the last one over gaps
        items = sorted({_utc_day(t): c for t, c in (q.get(s) or {"points": []})["points"]}.items())
        col, j, last = [], 0, None
        for d in days:
            while j < len(items) and items[j][0] <= d:
                last = items[j][1]
                j += 1
            col.append(last)
        if sum(c is not None for c in col) >= 21:
            px[s] = col
    rets = lambda col: [None if a is None or b is None or not a else b / a - 1 for a, b in zip(col, col[1:])]
    rb, rs = rets(bpx), {s: rets(col) for s, col in px.items()}
    ann = math.sqrt(252)

    def pair(x, y):  # the days both series have a return for
        both = [(a, b) for a, b in zip(x, y) if a is not None and b is not None]
        return [a for a, _ in both], [b for _, b in both]

    full = quotes_full(list(pos))
    value = {s: p["qty"] * ((q.get(s) or {}).get("price") or p["cost"] / p["qty"]) for s, p in pos.items()}
    total = sum(value.values()) + cash
    hold = {}
    for s, p in pos.items():
        kind = (full.get(s) or {}).get("quoteType") or ((q.get(s) or {}).get("type"))
        h = {"symbol": s, "kind": kind, "weight": _r(value[s] / total * 100 if total else 0),
             "sector": (profile(s).get("sector") if kind == "EQUITY" else None)
             or {"ETF": "Funds", "MUTUALFUND": "Funds", "CRYPTOCURRENCY": "Crypto"}.get(kind, "Other"),
             "divRate": (full.get(s) or {}).get("trailingAnnualDividendRate") or (full.get(s) or {}).get("dividendRate") or 0}
        if s in px:
            col = [c for c in px[s] if c is not None]
            a, b = pair(rs[s], rb)
            h.update(days=len(a), vol=_r(math.sqrt(_cov(a, a)) * ann * 100), beta=_r(_cov(a, b) / _cov(b, b)),
                     corr=_r(_corr(a, b)), ret=_r((col[-1] / col[0] - 1) * 100), dd=_r(min(_drawdown(col))))
        hold[s] = h
    out = {"asOf": int(time.time()), "holdings": list(hold.values()), "missing": [s for s in pos if s not in px],
           "bench": {"symbol": BENCH[0], "name": BENCH[1]}}

    names = list(px) + [BENCH[0]]
    series = {**rs, BENCH[0]: rb}
    matrix = []
    for a in names:
        row = []
        for b in names:
            x, y = pair(series[a], series[b])
            row.append(1.0 if a == b else _r(_corr(x, y)) if len(x) >= 20 else None)
        matrix.append(row)
    out["matrix"] = {"names": names, "v": matrix}

    # today's holdings and cash, held unchanged over the days every one of them has a price for
    start = max((next(i for i, c in enumerate(col) if c is not None) for col in px.values()), default=0)
    if px and len(days) - start >= 21:
        v = [cash + sum(pos[s]["qty"] * px[s][i] for s in px) for i in range(start, len(days))]
        rp = [b / a - 1 for a, b in zip(v, v[1:])]
        rbw = rb[start:]
        var = _cov(rp, rp)
        for s in px:  # each holding's share of the swings
            c = [pos[s]["qty"] * (px[s][start + i + 1] - px[s][start + i]) / v[i] for i in range(len(rp))]
            hold[s]["risk"] = _r(_cov(c, rp) / var * 100) if var > 0 else None
        try:
            rf = yahoo("^IRX", "1d")["price"] / 100  # the 13-week Treasury bill yield
        except Bad:
            rf = None
        sd = math.sqrt(var)
        dd, ddb = _drawdown(v), _drawdown(bpx[start:])
        low = dd.index(min(dd))
        short = [s for s, col in px.items() if col[0] is None]
        out["portfolio"] = {
            "days": len(rp), "from": ts[start], "limit": max(short, key=lambda s: px[s].index(next(c for c in px[s] if c is not None))) if short else None,
            "vol": _r(sd * ann * 100), "beta": _r(_cov(rp, rbw) / _cov(rbw, rbw)), "corr": _r(_corr(rp, rbw)),
            "ret": _r((v[-1] / v[0] - 1) * 100), "dd": _r(min(dd)), "ddAt": ts[start + low],
            "sharpe": _r((_mean(rp) - rf / 252) / sd * ann) if rf is not None and sd > 0 else None, "rf": _r(rf * 100) if rf is not None else None,
        }
        out["bench"].update(vol=_r(math.sqrt(_cov(rbw, rbw)) * ann * 100), ret=_r((bpx[-1] / bpx[start] - 1) * 100), dd=_r(min(ddb)))
        out["under"] = {"t": ts[start:], "pf": [_r(x) for x in dd], "bench": [_r(x) for x in ddb]}

    if flows:  # the same deposits and withdrawals, put into the benchmark on the same days
        src = bench["points"]
        if min(d for d, _ in flows) < days[0]:
            try:
                src = yahoo(BENCH[0], "10y")["points"]
            except Bad:
                pass
        dated = [(_utc_day(t), c) for t, c in src]
        units = sum(amt / next((c for d, c in reversed(dated) if d <= when), dated[0][1]) for when, amt in flows)
        out["whatif"] = {"value": _r(units * bench["price"]), "approx": min(d for d, _ in flows) < dated[0][0]}
    return out


SECTORS = {"XLK": ("Technology", "Technology"), "XLC": ("Communication", "Communication Services"), "XLY": ("Consumer discretionary", "Consumer Cyclical"),
           "XLF": ("Financials", "Financial Services"), "XLV": ("Health care", "Healthcare"), "XLI": ("Industrials", "Industrials"),
           "XLP": ("Consumer staples", "Consumer Defensive"), "XLE": ("Energy", "Energy"), "XLU": ("Utilities", "Utilities"),
           "XLB": ("Materials", "Basic Materials"), "XLRE": ("Real estate", "Real Estate")}  # fund -> (name, Yahoo's name for the sector)


def sectors():
    """Today's change in each part of the US market, from the sector funds that track them."""
    def make():
        out = []
        for s, qt in quotes(list(SECTORS), "1d").items():
            if qt and qt["prevClose"]:
                out.append({"symbol": s, "name": SECTORS[s][0], "sector": SECTORS[s][1], "pct": round((qt["regular"] / qt["prevClose"] - 1) * 100, 2)})
        if not out:
            raise Bad("Couldn't load the sector funds.")
        return out
    return cached("sectors", 60, make)


def set_plan(body):
    """Save the planner's settings: the goal, what you add, the assumptions and the target mix."""
    limits = {"goal": (0, 1e12), "monthly": (0, 1e9), "years": (1, 50), "growth": (-10, 30), "vol": (0, 100), "inflation": (0, 15)}
    with _lock:
        doc = load()
        plan = dict(doc["plan"])
        for k, (lo, hi) in limits.items():
            if k in body:
                try:
                    v = float(body[k])
                except (TypeError, ValueError):
                    raise Bad("That isn't a number.")
                if not lo <= v <= hi:  # also turns away NaN
                    raise Bad(f"{k.capitalize()} must be between {lo:g} and {hi:g}.")
                plan[k] = v
        if "targets" in body:
            t = body["targets"]
            if not isinstance(t, dict) or len(t) > 60:
                raise Bad("Bad target mix.")
            try:
                plan["targets"] = {str(k)[:16].upper(): min(100.0, max(0.0, float(v))) for k, v in t.items() if float(v) == float(v)}
            except (TypeError, ValueError):
                raise Bad("Targets must be numbers.")
        doc["plan"] = plan
        save(doc)


# ---------- long history: the lab and the stress test ----------

def daily(symbol):
    """Up to ten years of daily prices: rows of (day, close, close with dividends put back in). Kept for twelve hours."""
    def make():
        res = fetch(f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}?range=10y&interval=1d", timeout=20)["chart"]["result"][0]
        meta, ind = res["meta"], res.get("indicators") or {}
        close = (ind.get("quote") or [{}])[0].get("close") or []
        adj = (ind.get("adjclose") or [{}])[0].get("adjclose") or close
        off, rows = meta.get("gmtoffset") or 0, []
        for t, c, a in zip(res.get("timestamp") or [], close, adj):
            if c is None or c <= 0:
                continue
            row = (_utc_day(t + off), c, a if a and a > 0 else c)
            if rows and rows[-1][0] == row[0]:
                rows[-1] = row
            else:
                rows.append(row)
        if len(rows) < 2:
            raise Bad(f"No price history for {symbol}.")
        return {"symbol": meta["symbol"], "name": meta.get("longName") or meta.get("shortName") or meta["symbol"], "rows": rows}
    try:
        return cached(("daily", symbol), 43200, make)
    except Bad:
        raise
    except Exception:
        raise Bad(f"Couldn't load the price history of {symbol}.")


def _dailies(symbols):
    """daily() for several symbols at once; the ones that fail are left out."""
    out = {}
    for s, f in [(s, _pool.submit(daily, s)) for s in symbols]:
        try:
            out[s] = f.result()
        except Bad:
            pass
    return out


def _latest(symbol, rows):
    """Daily rows with the newest one brought up to the latest regular-hours price."""
    try:
        q = yahoo(symbol, "1d")
    except Bad:
        return rows
    d, px, last = ny_day(q["time"] or 0), q["regular"], rows[-1]
    if not px or d < last[0]:
        return rows
    return (rows[:-1] if d == last[0] else rows) + [(d, px, px * last[2] / last[1])]


def _ledger_on(txs, days, px):
    """Your real entries replayed over those days: the portfolio's value at each close, and the money that went in or out."""
    qty, last, cash, j, v, flows = {}, {}, 0.0, 0, [], []
    for i, d in enumerate(days):
        flow = 0.0
        while j < len(txs) and (txs[j]["date"] <= d or i == len(days) - 1):  # an entry dated after the last trading day still counts
            t, kind = txs[j], txs[j]["type"]
            j += 1
            fee = float(t.get("fee") or 0)
            if kind in ("deposit", "withdraw"):
                amount = t["amount"] if kind == "deposit" else -t["amount"]
                cash += amount
                flow += amount
            elif kind == "dividend":
                cash += t["amount"]
            else:
                sign = 1 if kind == "buy" else -1
                qty[t["symbol"]] = qty.get(t["symbol"], 0.0) + sign * t["qty"]
                cash -= sign * t["qty"] * t["price"] + fee
                last[t["symbol"]] = t["price"]
        if flow:
            flows.append([i, round(flow, 2)])
        held = sum(q * ((px[s][i] if s in px else None) or last.get(s, 0)) for s, q in qty.items() if q > 1e-9)
        v.append(round(cash + held, 2))
    return {"v": v, "flows": flows}


def lab_data(symbols, mine=False):
    """Prices for a mix to be tested, dividends included, on the benchmark's days.

    From your purchases: every trading day since your first entry, with your real portfolio replayed alongside.
    Otherwise: ten years of weeks."""
    syms = []
    for s in symbols:
        s = s.strip().upper()[:16]
        if s and s != "CASH" and s not in syms:
            syms.append(s)
    if len(syms) > 12:
        raise Bad("A mix can hold up to 12 tickers.")
    txs, own = [], []
    if mine:
        with _lock:
            doc = load()
        txs = sorted(doc["transactions"], key=lambda t: t["date"])
        if not txs:
            raise Bad("Record a deposit or a buy first, and the test can start from it.")
        own = list(dict.fromkeys(t["symbol"] for t in txs if t.get("symbol")))
    got = _dailies(list(dict.fromkeys(syms + own + [BENCH[0]])))
    if BENCH[0] not in got:
        raise Bad("Couldn't load the S&P 500's history.")
    rows = {s: _latest(s, g["rows"]) if mine else g["rows"] for s, g in got.items()}
    week = lambda d: datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
    ends = [d for d, _, _ in rows[BENCH[0]] if not mine or d >= txs[0]["date"]] or [rows[BENCH[0]][-1][0]]
    if not mine or len(ends) > 780:  # weeks: the last trading day of each
        ends = [d for d, nxt in zip(ends, ends[1:] + [None]) if nxt is None or week(nxt) != week(d)]

    def on_days(r, k=2):
        col, j, last = [], 0, None
        for d in ends:
            while j < len(r) and r[j][0] <= d:
                last = r[j][k]
                j += 1
            col.append(None if last is None else float(f"{last:.6g}"))
        return col
    stamp = lambda d: int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 72000
    out = {"t": [stamp(d) for d in ends],
           "bench": {"symbol": BENCH[0], "name": BENCH[1], "v": on_days(rows[BENCH[0]])},
           "series": [{"symbol": s, "name": got[s]["name"], "from": stamp(rows[s][0][0]), "v": on_days(rows[s])} for s in syms if s in got],
           "missing": [s for s in syms if s not in got]}
    if mine:
        out["mine"] = _ledger_on(txs, ends, {s: on_days(rows[s], 1) for s in own if s in rows})
    return out


def set_lab(body):
    """Save the lab's settings: the mix being tested and how it is tested."""
    lab = {}
    mix = body.get("mix")
    if not isinstance(mix, dict) or len(mix) > 13:
        raise Bad("Bad mix.")
    try:
        lab["mix"] = {str(k).strip().upper()[:16]: min(100.0, max(0.0, float(v))) for k, v in mix.items() if str(k).strip() and float(v) == float(v)}
        lab["start"] = min(1e12, max(0.0, float(body.get("start", 10000))))
        lab["monthly"] = min(1e9, max(0.0, float(body.get("monthly", 0))))
    except (TypeError, ValueError):
        raise Bad("Those must be numbers.")
    if lab["start"] != lab["start"] or lab["monthly"] != lab["monthly"]:
        raise Bad("Those must be numbers.")
    lab["years"] = body.get("years") if body.get("years") in (1, 3, 5, 10) else 5
    lab["rebalance"] = body.get("rebalance") if body.get("rebalance") in ("never", "yearly", "quarterly") else "yearly"
    lab["mode"] = "past" if body.get("mode") == "past" else "mine"
    with _lock:
        doc = load()
        doc["lab"] = lab
        save(doc)


EPISODES = [  # stretches the S&P 500 fell hard in, from its closing high to its closing low
    ("Late 2018 sell-off", "2018-09-20", "2018-12-24"),
    ("2020 Covid crash", "2020-02-19", "2020-03-23"),
    ("2022 bear market", "2022-01-03", "2022-10-12"),
    ("2025 tariff shock", "2025-02-19", "2025-04-08"),
]


def stress():
    """What past market falls would do to today's holdings."""
    with _lock:
        doc = load()
    pos, cash = replay(doc["transactions"])[:2]
    if not pos:
        return {"episodes": []}
    key = ("stress", tuple(sorted((s, round(p["qty"], 6)) for s, p in pos.items())), round(cash, 2))
    return cached(key, 21600, lambda: _stress(pos, cash))


def _stress(pos, cash):
    got = _dailies(list(pos) + [BENCH[0]])
    if BENCH[0] not in got:
        raise Bad("Couldn't load the S&P 500's history.")
    q = quotes(list(pos), "1d")
    value = {s: p["qty"] * ((q.get(s) or {}).get("price") or p["cost"] / p["qty"]) for s, p in pos.items()}
    total = sum(value.values()) + cash
    try:
        beta = {h["symbol"]: h.get("beta") for h in analysis()["holdings"]}
    except Exception:
        beta = {}

    def at(rows, day):  # the close on that day, or the last one before it
        lo, hi = 0, len(rows)
        while lo < hi:
            mid = (lo + hi) // 2
            if rows[mid][0] <= day:
                lo = mid + 1
            else:
                hi = mid
        return rows[lo - 1][1] if lo else None

    brows = got[BENCH[0]]["rows"]
    episodes = list(EPISODES)
    year = brows[-252:]  # the worst stretch of the past year, whenever it was
    dd = _drawdown([c for _, c, _ in year])
    low = dd.index(min(dd))
    if dd[low] <= -5:
        high = max(range(low + 1), key=lambda i: year[i][1])
        if all(not (a <= year[low][0] and year[high][0] <= b) for _, a, b in episodes):
            episodes.append(("The past year’s deepest fall", year[high][0], year[low][0]))
    out = []
    for name, a, b in episodes:
        b0, b1 = at(brows, a), at(brows, b)
        if not b0 or not b1 or brows[0][0] > a:
            continue
        fall, parts, pf = b1 / b0 - 1, [], 0.0
        for s in pos:
            rows = (got.get(s) or {}).get("rows") or []
            if rows and rows[0][0] <= a:
                move, est = at(rows, b) / at(rows, a) - 1, False
            else:  # not listed yet: what its usual link to the market suggests
                move, est = max(-0.95, (beta.get(s) if beta.get(s) is not None else 1.0) * fall), True
            parts.append({"symbol": s, "pct": _r(move * 100, 1), "est": est})
            pf += value[s] / total * move if total else 0
        out.append({"name": name, "from": a, "to": b, "bench": _r(fall * 100, 1), "pf": _r(pf * 100, 1), "parts": parts})
    return {"episodes": out, "cash": _r(cash / total * 100, 1) if total else 0}


# ---------- markets around the world ----------

WORLD = [
    ("United States", [("^GSPC", "S&P 500"), ("^IXIC", "Nasdaq"), ("^DJI", "Dow Jones"), ("^RUT", "Russell 2000"), ("^VIX", "VIX (volatility)")]),
    ("Europe", [("^STOXX50E", "Euro Stoxx 50"), ("^GDAXI", "DAX · Germany"), ("^FTSE", "FTSE 100 · UK"), ("^FCHI", "CAC 40 · France"), ("GD.AT", "Athens General")]),
    ("Asia and Pacific", [("^N225", "Nikkei 225 · Japan"), ("^HSI", "Hang Seng · Hong Kong"), ("000001.SS", "Shanghai Composite"), ("^NSEI", "Nifty 50 · India"), ("^AXJO", "ASX 200 · Australia")]),
    ("Interest rates", [("^IRX", "US 3-month"), ("^FVX", "US 5-year"), ("^TNX", "US 10-year"), ("^TYX", "US 30-year")]),
    ("Commodities", [("GC=F", "Gold"), ("SI=F", "Silver"), ("CL=F", "Oil · WTI"), ("BZ=F", "Oil · Brent"), ("NG=F", "Natural gas"), ("HG=F", "Copper")]),
    ("Currencies", [("EURUSD=X", "EUR / USD"), ("GBPUSD=X", "GBP / USD"), ("JPY=X", "USD / JPY"), ("EURGBP=X", "EUR / GBP"), ("DX-Y.NYB", "Dollar index")]),
    ("Crypto", [("BTC-USD", "Bitcoin"), ("ETH-USD", "Ethereum"), ("SOL-USD", "Solana")]),
]
RANKED = [("^GSPC", "S&P 500"), ("^IXIC", "Nasdaq"), ("^RUT", "Russell 2000"), ("^STOXX50E", "Euro Stoxx 50"), ("^GDAXI", "DAX"), ("^FTSE", "FTSE 100"),
          ("GD.AT", "Athens General"), ("^N225", "Nikkei 225"), ("^HSI", "Hang Seng"), ("GC=F", "Gold"), ("SI=F", "Silver"), ("CL=F", "Oil"),
          ("BTC-USD", "Bitcoin"), ("ETH-USD", "Ethereum"), ("DX-Y.NYB", "US dollar")]  # what the longer-run ranking compares
EXCHANGES = [  # usual trading hours, local time
    ("Sydney", "Australia/Sydney", (10, 0), (16, 0)), ("Tokyo", "Asia/Tokyo", (9, 0), (15, 30)), ("Hong Kong", "Asia/Hong_Kong", (9, 30), (16, 0)),
    ("Mumbai", "Asia/Kolkata", (9, 15), (15, 30)), ("Athens", "Europe/Athens", (10, 15), (17, 20)), ("Frankfurt", "Europe/Berlin", (9, 0), (17, 30)),
    ("London", "Europe/London", (8, 0), (16, 30)), ("New York", "America/New_York", (9, 30), (16, 0)),
]


def _sparks(symbols):
    """Today's prices for many symbols in one request each twenty: {symbol: (price, previous close, closes)}."""
    out = {}
    for i in range(0, len(symbols), 18):
        part = symbols[i:i + 18]
        try:
            res = fetch("https://query1.finance.yahoo.com/v8/finance/spark?symbols=" + urllib.parse.quote(",".join(part)) + "&range=1d&interval=5m")
        except Exception as e:
            print("sparks:", repr(e), file=sys.stderr)
            continue
        for s, r in res.items():
            closes = [c for c in r.get("close") or [] if c is not None]
            prev = r.get("previousClose") or r.get("chartPreviousClose")
            if closes and prev:
                out[s] = (closes[-1], prev, closes[::max(1, len(closes) // 48)])
    return out


def sessions(now):
    """Each exchange's trading sessions around now, as [open, close] times."""
    out = []
    for city, zone, o, c in EXCHANGES:
        try:
            tz = ZoneInfo(zone)
        except Exception:
            continue
        today, spans = datetime.fromtimestamp(now, tz).date(), []
        for k in range(-1, 4):
            d = today + timedelta(days=k)
            if d.weekday() < 5:
                at = lambda hm: int(datetime(d.year, d.month, d.day, hm[0], hm[1], tzinfo=tz).timestamp())
                spans.append([at(o), at(c)])
        out.append({"city": city, "spans": spans})
    return out


def _movers():
    row = lambda q: {"symbol": q["symbol"], "name": q.get("shortName") or q.get("longName") or q["symbol"], "price": q.get("regularMarketPrice"),
                     "pct": _r(q.get("regularMarketChangePercent") or 0), "volume": q.get("regularMarketVolume"), "cap": q.get("marketCap")}
    out = {}
    jobs = {k: _pool.submit(_screen, scr, 10) for k, scr in (("gainers", "day_gainers"), ("losers", "day_losers"), ("active", "most_actives"))}
    for k, f in jobs.items():
        try:
            out[k] = [row(q) for q in f.result() if q.get("regularMarketPrice")]
        except Exception:
            out[k] = []
    try:
        syms = [q["symbol"] for q in fetch("https://query1.finance.yahoo.com/v1/finance/trending/US?count=14")["finance"]["result"][0]["quotes"]]
        full = quotes_full(syms)
        out["trending"] = [row(full[s]) for s in syms if (full.get(s) or {}).get("regularMarketPrice") and not s.startswith("^")][:10]
    except Exception:
        out["trending"] = []
    if not any(out.values()):
        raise Bad("Couldn't load the movers.")
    return out


def _ranked(held):
    """How far each market, and each thing you hold, has come over a month, this year and a year."""
    names = dict(RANKED)
    names.update(held)
    now, out = time.time(), []
    year = time.gmtime(now).tm_year
    for s, qt in quotes(list(names), "1y").items():
        pts = (qt or {}).get("points") or []
        if len(pts) < 30:
            continue
        last = pts[-1][1]
        back = lambda keep: next((c for t, c in reversed(pts) if keep(t)), None)
        chg = lambda base: _r((last / base - 1) * 100) if base else None
        span = (pts[-1][0] - pts[0][0]) / 86400
        out.append({"symbol": s, "name": names[s], "held": s in held, "m1": chg(back(lambda t: t <= now - 30 * 86400)),
                    "ytd": chg(back(lambda t: time.gmtime(t).tm_year < year)), "y1": chg(pts[0][1]) if span > 330 else None})
    return out


def world():
    """The markets page: boards of indices, rates, commodities, currencies and coins; who is open; today's movers."""
    with _lock:
        doc = load()
    pos = replay(doc["transactions"])[0]

    def board():
        got = _sparks([s for _, items in WORLD for s, _ in items])
        if not got:
            raise Bad("Couldn't load the markets.")
        return [{"name": g, "items": [{"symbol": s, "name": n, "price": got[s][0], "prevClose": got[s][1], "spark": got[s][2]} for s, n in items if s in got]}
                for g, items in WORLD]
    held = {s: s for s in pos}
    out = {"groups": cached("world", 45, board), "sessions": sessions(time.time())}
    for key, ttl, make in (("movers", 180, _movers), (("ranked", tuple(sorted(held))), 1800, lambda: _ranked(held))):
        try:
            out[key if isinstance(key, str) else key[0]] = cached(key, ttl, make)
        except Exception:
            out[key if isinstance(key, str) else key[0]] = {} if key == "movers" else []
    return out


# ---------- the inbox: things Folio noticed while you were away ----------

INBOX = DATA.with_name("inbox.json")
_inbox_lock = threading.Lock()
NOTICE = {"moves": True, "highs": True, "earnings": True, "summary": True, "desktop": False, "movePct": 5.0}


def inbox_load():
    try:
        box = json.loads(INBOX.read_text())
    except (OSError, ValueError):
        box = {}
    box.setdefault("events", [])
    box.setdefault("marks", {})
    box.setdefault("seen", 0)
    box["settings"] = {**NOTICE, **(box.get("settings") or {})}
    return box


def inbox_save(box):
    INBOX.parent.mkdir(parents=True, exist_ok=True)
    tmp = INBOX.with_suffix(".tmp")
    tmp.write_text(json.dumps(box, indent=1))
    tmp.replace(INBOX)


def inbox_view():
    box = inbox_load()
    return {"events": box["events"][-40:][::-1], "seen": box["seen"], "settings": box["settings"]}


def emit(kind, key, title, body="", symbol=None, tone=None, quiet=False):
    """Note something worth knowing, once: key names the occurrence, so seeing it again adds nothing."""
    now = int(time.time())
    with _inbox_lock:
        box = inbox_load()
        if key in box["marks"]:
            return False
        box["marks"] = {k: t for k, t in box["marks"].items() if now - t < 60 * 86400}
        box["marks"][key] = now
        box["events"] = (box["events"] + [{"id": uuid.uuid4().hex[:10], "t": now, "kind": kind, "title": title, "body": body, "symbol": symbol, "tone": tone}])[-80:]
        inbox_save(box)
        desktop = box["settings"]["desktop"]
    if desktop and not quiet:
        try:
            subprocess.run(["notify-send", "-a", "Folio", "-i", str(ROOT / "icon.svg"), title, body], timeout=5, check=False)
        except Exception:
            pass
    return True


def set_inbox(body):
    with _inbox_lock:
        box = inbox_load()
        if body.get("seen"):
            box["seen"] = int(time.time())
        if body.get("clear"):
            box["events"] = []
        st = body.get("settings")
        if isinstance(st, dict):
            for k in ("moves", "highs", "earnings", "summary", "desktop"):
                if k in st:
                    box["settings"][k] = bool(st[k])
            if "movePct" in st:
                try:
                    v = float(st["movePct"])
                except (TypeError, ValueError):
                    raise Bad("The size of move must be a number.")
                if not 1 <= v <= 50:
                    raise Bad("The size of move must be between 1 and 50 percent.")
                box["settings"]["movePct"] = v
        inbox_save(box)


def notice(beat):
    """Look over the holdings for things worth a line in the inbox."""
    st = inbox_load()["settings"]
    if not any(st[k] for k in ("moves", "highs", "earnings", "summary")):
        return
    now = time.time()
    s = state()
    day, ny = ny_day(now), datetime.fromtimestamp(now, NY)
    fresh = [p for p in s["positions"] if p["time"] and ny_day(p["time"]) == day]  # traded today
    for p in fresh:
        sym, move = p["symbol"], p["dayPct"]
        if st["moves"] and abs(move) >= st["movePct"]:
            emit("move", f"move:{sym}:{day}", f"{sym} is {'up' if move > 0 else 'down'} {abs(move):.1f}% today", f"Now ${p['price']:,.2f}.", sym, "up" if move > 0 else "down")
        if st["highs"] and p["dayHigh"] and p["wkHigh"] and p["dayHigh"] >= p["wkHigh"] * 0.99995:
            emit("high", f"high:{sym}:{day}", f"{sym} reached its highest price in a year", f"${p['dayHigh']:,.2f} today.", sym, "up")
        if st["highs"] and p["dayLow"] and p["wkLow"] and p["dayLow"] <= p["wkLow"] * 1.00005:
            emit("low", f"low:{sym}:{day}", f"{sym} fell to its lowest price in a year", f"${p['dayLow']:,.2f} today.", sym, "down")
    if st["summary"] and fresh and len(fresh) == len(s["positions"]) and ny.weekday() < 5 and ny.hour >= 16 and not s["stale"]:
        ranked = sorted(fresh, key=lambda p: p["dayPct"])
        pct = lambda v: f"{'+' if v > 0 else '−' if v < 0 else ''}{abs(v):.1f}%"
        text = f"Best: {ranked[-1]['symbol']} {pct(ranked[-1]['dayPct'])}. Worst: {ranked[0]['symbol']} {pct(ranked[0]['dayPct'])}." if len(ranked) > 1 else ""
        try:
            b = yahoo(BENCH[0], "1d")
            text += f" The S&P 500 {'rose' if b['regular'] >= b['prevClose'] else 'fell'} {abs(b['regular'] / b['prevClose'] - 1) * 100:.1f}%."
        except Exception:
            pass
        move = s["dayPct"]
        emit("summary", f"summary:{day}", f"The day closed {'up' if move > 0.004 else 'down' if move < -0.004 else 'flat'}"
             + (f" {abs(move):.1f}%" if abs(move) > 0.004 else ""), text.strip(), None, "up" if move > 0 else "down" if move < 0 else None)
    if st["earnings"] and beat % 20 == 0:
        for ev in calendar():
            ahead = ev["t"] - now
            if ev["kind"] == "earnings" and -3600 < ahead < 30 * 3600:
                when = "today" if ny_day(ev["t"]) == day else "tomorrow"
                emit("earnings", f"earn:{ev['symbol']}:{ny_day(ev['t'])}", f"{ev['symbol']} reports earnings {when}",
                     ("You hold it." if ev["held"] else "On your watchlist.") + (" The date is not confirmed." if ev["estimate"] else ""), ev["symbol"], None)


def num(body, key, positive=True):
    try:
        v = float(body.get(key))
    except (TypeError, ValueError):
        v = float("nan")
    key = {"qty": "shares", "pct": "percentage"}.get(key, key)
    if v != v:
        raise Bad(f"{key.capitalize()} must be a number.")
    if v != v or v in (float("inf"), float("-inf")) or (positive and v <= 0) or v < 0:
        raise Bad(f"{key.capitalize()} must be greater than zero." if positive else f"{key.capitalize()} can't be negative.")
    return v


def build_tx(body, doc):
    """A validated ledger entry from a submitted form."""
    kind = body.get("type")
    date = str(body.get("date") or time.strftime("%Y-%m-%d"))
    try:
        time.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise Bad("Date must look like 2026-01-31.")
    tx = {"id": uuid.uuid4().hex[:10], "type": kind, "date": date, "note": " ".join(str(body.get("note") or "").split())[:200]}
    sym = str(body.get("symbol") or "").strip().upper()
    if kind in ("deposit", "withdraw"):
        tx["amount"] = num(body, "amount")
    elif kind == "dividend":
        if not sym:
            raise Bad("Enter the ticker that paid the dividend.")
        if not any(t.get("symbol") == sym and t["type"] == "buy" for t in doc["transactions"]):
            raise Bad(f"There is no recorded buy of {sym}, so it can't have paid you a dividend.")
        tx.update(symbol=sym, amount=num(body, "amount"))
    elif kind in ("buy", "sell"):
        if not sym:
            raise Bad("Enter a ticker symbol.")
        tx.update(symbol=sym, qty=num(body, "qty"), price=num(body, "price"),
                  fee=num(body, "fee", positive=False) if body.get("fee") not in (None, "") else 0.0)
        if kind == "buy":
            tx["name"] = yahoo(sym, "1d")["name"]  # also rejects unknown tickers
    else:
        raise Bad("Unknown transaction type.")
    return tx


def add_tx(body):
    with _lock:
        doc = load()
    tx = build_tx(body, doc)  # may look the ticker up, so not under the lock
    new, kind = [tx], tx["type"]
    with _lock:
        doc = load()
        cash = replay(doc["transactions"])[1]
        if kind == "buy":
            short = tx["qty"] * tx["price"] + tx["fee"] - cash
            if short > 0.005:
                if not body.get("fund"):
                    raise Bad(f"Cash balance is ${short:,.2f} short for this buy.")
                new.insert(0, {"id": uuid.uuid4().hex[:10], "type": "deposit", "date": tx["date"],
                               "amount": round(short, 2), "note": f"Added to buy {tx['symbol']}"})
        if kind == "withdraw" and tx["amount"] > cash + 0.005:
            raise Bad(f"Only ${cash:,.2f} cash available to withdraw.")
        doc["transactions"].extend(new)
        replay(doc["transactions"])  # validates sells
        save(doc, backup=True)


def edit_tx(tid, body):
    with _lock:
        doc = load()
    if not any(t["id"] == tid for t in doc["transactions"]):
        raise Bad("That entry is already gone.")
    tx = build_tx(body, doc)
    tx["id"] = tid
    with _lock:
        doc = load()
        i = next((i for i, t in enumerate(doc["transactions"]) if t["id"] == tid), None)
        if i is None:
            raise Bad("That entry is already gone.")
        doc["transactions"][i] = tx
        cash = replay(doc["transactions"])[1]  # validates sells
        if cash < -0.005:
            raise Bad(f"That change would leave the cash balance ${-cash:,.2f} below zero. Record a deposit first.")
        save(doc, backup=True)


def del_tx(tid):
    with _lock:
        doc = load()
        keep = [t for t in doc["transactions"] if t["id"] != tid]
        if len(keep) == len(doc["transactions"]):
            raise Bad("That entry is already gone.")
        try:
            replay(keep)
        except Bad:
            raise Bad("Can't remove this entry: a later sell depends on it. Remove the sell first.")
        doc["transactions"] = keep
        save(doc, backup=True)


def export_csv():
    with _lock:
        doc = load()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["date", "type", "symbol", "shares", "price", "fee", "amount", "note"])
    for t in sorted(doc["transactions"], key=lambda t: t["date"]):
        stock = t["type"] in ("buy", "sell")
        amount = t["qty"] * t["price"] + (1 if t["type"] == "buy" else -1) * (t.get("fee") or 0) if stock else t["amount"]
        w.writerow([t["date"], t["type"], t.get("symbol", ""), t.get("qty", ""), t.get("price", ""),
                    t.get("fee", "") if stock else "", round(amount, 2), t.get("note", "")])
    return buf.getvalue().encode()


def add_watch(body):
    sym = str(body.get("symbol") or "").strip().upper()
    if not sym:
        raise Bad("Enter a ticker symbol.")
    yahoo(sym, "1d")  # rejects unknown tickers
    with _lock:
        doc = load()
        if sym not in doc["watchlist"]:
            if len(doc["watchlist"]) >= 20:
                raise Bad("The watchlist is full (20). Remove one first.")
            doc["watchlist"].append(sym)
            save(doc)


def del_watch(sym):
    with _lock:
        doc = load()
        doc["watchlist"] = [s for s in doc["watchlist"] if s != sym]
        save(doc)


def add_alert(body):
    sym = str(body.get("symbol") or "").strip().upper()
    op = body.get("op")
    if not sym or op not in ("above", "below", "move"):
        raise Bad("Pick a stock and what to alert on.")
    alert = {"id": uuid.uuid4().hex[:10], "symbol": sym, "op": op, "fired": None}
    qt = yahoo(sym, "1d")
    if op == "move":  # a day's move of this many percent, either way; watches again each new day
        alert["pct"] = num(body, "pct")
        if alert["pct"] > 100:
            raise Bad("Percentage can't be more than 100.")
    else:
        alert["price"] = price = num(body, "price")
        now = qt["price"]
        if (op == "above" and now >= price) or (op == "below" and now <= price):
            raise Bad(f"{sym} is already {op} ${price:,.2f} (now ${now:,.2f}).")
    with _lock:
        doc = load()
        if len(doc["alerts"]) >= 30:
            raise Bad("Too many alerts (30). Remove one first.")
        doc["alerts"].append(alert)
        save(doc)


def del_alert(aid):
    with _lock:
        doc = load()
        doc["alerts"] = [a for a in doc["alerts"] if a["id"] != aid]
        save(doc)


def rearm_alert(aid):
    with _lock:
        doc = load()
        for a in doc["alerts"]:
            if a["id"] == aid:
                a["fired"] = None
                a.pop("firedPrice", None)
                a.pop("firedPct", None)
        save(doc)


def ny_day(t):
    return datetime.fromtimestamp(t, NY).strftime("%Y-%m-%d")


def check_alerts():
    now = time.time()
    with _lock:
        doc = load()
        stale = [a for a in doc["alerts"] if a["op"] == "move" and a.get("fired") and ny_day(a["fired"]) != ny_day(now)]
        for a in stale:  # a day-move alert watches again on each new day
            a["fired"] = None
        if stale:
            save(doc)
        pending = [a for a in doc["alerts"] if not a.get("fired")]
    if not pending:
        return
    q = quotes({a["symbol"] for a in pending}, "1d")
    hit = {}
    for a in pending:
        qt = q.get(a["symbol"])
        if not qt:
            continue
        move = (qt["price"] / qt["prevClose"] - 1) * 100 if qt["prevClose"] else 0
        if a["op"] == "move":
            fresh = ny_day(qt["time"] or 0) == ny_day(now)  # not yesterday's move, seen again before today's first trade
            ok = fresh and abs(move) >= a["pct"]
        else:
            ok = qt["price"] >= a["price"] if a["op"] == "above" else qt["price"] <= a["price"]
        if ok:
            hit[a["id"]] = (qt["price"], move)
    if not hit:
        return
    fired = []
    with _lock:
        doc = load()
        for a in doc["alerts"]:
            if a["id"] in hit and not a.get("fired"):
                a["fired"], a["firedPrice"], a["firedPct"] = int(now), hit[a["id"]][0], round(hit[a["id"]][1], 2)
                fired.append(a)
        save(doc)
    for a in fired:  # desktop notification, where the system has one
        title = (f"{a['symbol']} is {'up' if a['firedPct'] > 0 else 'down'} {abs(a['firedPct']):.1f}% today" if a["op"] == "move"
                 else f"{a['symbol']} is {a['op']} ${a['price']:,.2f}")
        emit("alert", f"alert:{a['id']}:{a['fired']}", title, f"Now ${a['firedPrice']:,.2f}.", a["symbol"],
             ("up" if a["firedPct"] > 0 else "down") if a["op"] == "move" else None, quiet=True)
        try:
            subprocess.run(["notify-send", "-a", "Folio", "-i", str(ROOT / "icon.svg"), title, f"Now ${a['firedPrice']:,.2f}"],
                           timeout=5, check=False)
        except Exception:
            pass


def snapshot(was_active):
    """Append the portfolio's value to the history file. Returns whether a market session is active."""
    s = state()
    if s["stale"] or (not s["positions"] and abs(s["cash"]) < 0.005):
        return was_active
    active = s["market"] != "closed"
    if active or was_active is not False:  # every 5 min while trading, then once at the close
        HIST.parent.mkdir(parents=True, exist_ok=True)
        with HIST.open("a") as f:
            f.write(json.dumps({"t": int(time.time()), "v": round(s["total"], 2), "d": round(s["deposited"], 2)}) + "\n")
    return active


def worker():
    active, beat = None, 0
    while True:
        try:
            check_alerts()
            if beat % 10 == 0:
                active = snapshot(active)
            if beat % 2 == 0:
                notice(beat)
        except Exception as e:
            print("worker:", repr(e), file=sys.stderr)
        beat += 1
        time.sleep(30)


MANIFEST = {
    "name": "Folio", "short_name": "Folio", "start_url": "/", "display": "standalone",
    "background_color": "#0c0f14", "theme_color": "#0c0f14",
    "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"}],
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json", download=None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        if download:
            self.send_header("Content-Disposition", f'attachment; filename="{download}"')
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        doc = json.loads(self.rfile.read(n) or b"{}")
        if not isinstance(doc, dict):
            raise Bad("Bad request.")
        return doc

    def route(self, method):
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        arg = lambda k, d="": qs.get(k, [d])[0]
        tail = urllib.parse.unquote(u.path.rsplit("/", 1)[1])
        try:
            if method == "GET":
                if u.path in ("/", "/index.html"):
                    return self.send(200, (ROOT / "index.html").read_bytes().replace(b"__UI__", ui_version().encode(), 1), "text/html; charset=utf-8")
                if u.path == "/icon.svg":
                    return self.send(200, (ROOT / "icon.svg").read_bytes(), "image/svg+xml")
                if u.path == "/manifest.webmanifest":
                    return self.send(200, MANIFEST, "application/manifest+json")
                if u.path == "/api/state":
                    return self.send(200, state())
                if u.path == "/api/history":
                    return self.send(200, history(arg("range", "1d"), arg("symbol").strip().upper() or None, arg("compare") == "1", arg("avg") == "1"))
                if u.path == "/api/market":
                    return self.send(200, market())
                if u.path == "/api/news":
                    return self.send(200, news())
                if u.path == "/api/insights":
                    return self.send(200, insights())
                if u.path == "/api/analysis":
                    return self.send(200, analysis())
                if u.path == "/api/sectors":
                    return self.send(200, sectors())
                if u.path == "/api/world":
                    return self.send(200, world())
                if u.path == "/api/stress":
                    return self.send(200, stress())
                if u.path == "/api/lab":
                    return self.send(200, lab_data(arg("symbols").split(","), arg("mine") == "1"))
                if u.path == "/api/calendar":
                    return self.send(200, calendar())
                if u.path == "/api/detail":
                    return self.send(200, detail(arg("symbol").strip().upper()))
                if u.path == "/api/search":
                    return self.send(200, search(arg("q")))
                if u.path == "/api/lookup":
                    q = yahoo(arg("symbol").strip().upper(), "1d")
                    return self.send(200, {k: q[k] for k in ("symbol", "name", "price", "currency")})
                if u.path == "/api/export.csv":
                    return self.send(200, export_csv(), "text/csv; charset=utf-8", f"folio-{time.strftime('%Y-%m-%d')}.csv")
                if u.path == "/api/export.json":
                    with _lock:
                        doc = load()
                    return self.send(200, json.dumps(doc, indent=2).encode(), "application/json", f"folio-{time.strftime('%Y-%m-%d')}.json")
            elif method == "POST":
                if u.path == "/api/watch":
                    add_watch(self.body())
                elif u.path == "/api/alerts":
                    add_alert(self.body())
                elif re.fullmatch(r"/api/alerts/\w+/rearm", u.path):
                    rearm_alert(u.path.split("/")[3])
                elif u.path == "/api/tx":
                    add_tx(self.body())
                elif u.path == "/api/inbox":
                    set_inbox(self.body())
                else:
                    return self.send(404, {"error": "Not found"})
                return self.send(200, state())
            elif method == "PUT" and u.path.startswith("/api/tx/"):
                edit_tx(tail, self.body())
                return self.send(200, state())
            elif method == "PUT" and u.path == "/api/plan":
                set_plan(self.body())
                return self.send(200, state())
            elif method == "PUT" and u.path == "/api/lab":
                set_lab(self.body())
                return self.send(200, state())
            elif method == "DELETE":
                if u.path.startswith("/api/watch/"):
                    del_watch(tail)
                elif u.path.startswith("/api/alerts/"):
                    del_alert(tail)
                elif u.path.startswith("/api/tx/"):
                    del_tx(tail)
                else:
                    return self.send(404, {"error": "Not found"})
                return self.send(200, state())
            self.send(404, {"error": "Not found"})
        except Bad as e:
            self.send(400, {"error": str(e)})
        except (BrokenPipeError, ConnectionResetError):
            pass  # the browser went away mid-answer
        except Exception as e:
            print("error:", repr(e), file=sys.stderr)
            self.send(500, {"error": "Something went wrong on the server."})

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_PUT(self):
        self.route("PUT")

    def do_DELETE(self):
        self.route("DELETE")


if __name__ == "__main__":
    print(f"Folio {VERSION} on http://{HOST}:{PORT}  (data: {DATA})")
    threading.Thread(target=worker, daemon=True).start()
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
