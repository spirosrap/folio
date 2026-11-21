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

VERSION = "3.0"
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
        "alerts": doc["alerts"], "plan": doc["plan"],
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


def growth(rows):
    """The portfolio's growth at each snapshot, starting from 1, with deposits and withdrawals taken out."""
    idx = [1.0] * len(rows)
    for k in range(1, len(rows)):
        prev, flow = rows[k - 1]["v"], rows[k]["d"] - rows[k - 1]["d"]
        idx[k] = idx[k - 1] * ((rows[k]["v"] - flow) / prev if prev > 0 else 1)
    return idx


def insights():
    """Day-by-day results and the high-water mark, from the recorded history."""
    rows = hist_rows()
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
        if own and (dt.hour, dt.minute) <= (16, 5):  # a day's result is its value at the regular close
            d["reg"] = (r, ix)
    out, prev = [], None
    for date, d in days.items():
        if d["n"] < 3:
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
        rows = hist_rows()
        idx = growth(rows)
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
                else:
                    return self.send(404, {"error": "Not found"})
                return self.send(200, state())
            elif method == "PUT" and u.path.startswith("/api/tx/"):
                edit_tx(tail, self.body())
                return self.send(200, state())
            elif method == "PUT" and u.path == "/api/plan":
                set_plan(self.body())
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
