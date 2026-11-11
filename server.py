#!/usr/bin/env python3
"""Folio: a small self-hosted portfolio dashboard.

Holdings come from a transaction ledger (data/portfolio.json); prices come
from Yahoo Finance's public chart endpoint. Standard library only.
"""
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = Path(os.environ.get("FOLIO_DATA", ROOT / "data" / "portfolio.json"))
PORT = int(os.environ.get("FOLIO_PORT", "8791"))
HOST = os.environ.get("FOLIO_HOST", "0.0.0.0")

RANGES = {  # range -> (yahoo interval, cache seconds)
    "1d": ("1m", 5),  # with pre-market and after-hours trades
    "5d": ("30m", 120),
    "1mo": ("60m", 600),
    "3mo": ("1d", 1800),
    "1y": ("1d", 1800),
}

HIST = DATA.with_name("history.jsonl")  # recorded portfolio value over time
_lock = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=8)


class Bad(Exception):
    pass


# ---------- ledger ----------

def load():
    doc = json.loads(DATA.read_text()) if DATA.exists() else {}
    for key in ("transactions", "watchlist", "alerts"):
        doc.setdefault(key, [])
    return doc


def save(doc):
    DATA.parent.mkdir(parents=True, exist_ok=True)
    tmp = DATA.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2))
    tmp.replace(DATA)


def replay(txs):
    """Average-cost replay. Returns (positions, cash, realized, deposited)."""
    pos, cash, realized, deposited = {}, 0.0, 0.0, 0.0
    for t in sorted(txs, key=lambda t: t["date"]):  # stable: same-day keeps entry order
        kind, fee = t["type"], float(t.get("fee") or 0)
        if kind == "deposit":
            cash += t["amount"]
            deposited += t["amount"]
        elif kind == "withdraw":
            cash -= t["amount"]
            deposited -= t["amount"]
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
    return pos, cash, realized, deposited


# ---------- quotes ----------

def yahoo(symbol, rng):
    interval, ttl = RANGES[rng]
    key = (symbol, rng)
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
           f"?range={rng}&interval={interval}" + ("&includePrePost=true" if rng == "1d" else ""))
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            res = json.load(r)["chart"]["result"][0]
        meta = res["meta"]
        closes = (res.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
        pts = [(t, c) for t, c in zip(res.get("timestamp") or [], closes) if c is not None]
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
            "periods": {k: [v.get("start", 0), v.get("end", 0)] for k, v in tp.items()},
            "symbol": meta["symbol"],
            "name": meta.get("longName") or meta.get("shortName") or meta["symbol"],
            "price": price,
            "prevClose": prev,
            "currency": meta.get("currency"),
            "time": last_t,
            "state": state,
            "points": pts,
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


# ---------- api ----------

def ui_version():
    return str(int((ROOT / "index.html").stat().st_mtime))


def state():
    with _lock:
        doc = load()
    pos, cash, realized, deposited = replay(doc["transactions"])
    q = quotes(list(pos), "1d")
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
        "realized": realized, "deposited": deposited, "market": market, "stale": stale,
        "asOf": max([r["time"] or 0 for r in rows], default=0),
        "transactions": sorted(doc["transactions"], key=lambda t: t["date"])[::-1],  # newest first
        "watch": [{"symbol": s, "name": w["name"], "price": w["price"], "prevClose": w["prevClose"]}
                  for s, w in quotes(doc["watchlist"], "1d").items() if w],
        "alerts": doc["alerts"],
        "ui": ui_version(),
    }


MARKET = {"^GSPC": "S&P 500", "^IXIC": "Nasdaq", "^DJI": "Dow Jones", "^VIX": "VIX (volatility)", "EURUSD=X": "EUR / USD"}
POPULAR = {"BTC-USD": ("Bitcoin", "crypto")}  # last good ranking; Bitcoin is always shown


def _screen(scr_id, count):
    url = f"https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds={scr_id}&count={count}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)["finance"]["result"][0]["quotes"]


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


def history(rng, symbol=None):
    if rng == "all" and symbol:
        rng = "1y"
    if rng == "all":  # the portfolio's real recorded value
        rows = [json.loads(l) for l in HIST.read_text().splitlines() if l.strip()] if HIST.exists() else []
        step = max(1, len(rows) // 1500)
        rows = rows[::step] + (rows[-1:] if rows and (len(rows) - 1) % step else [])
        return {"t": [r["t"] for r in rows], "v": [r["v"] for r in rows], "base": None, "regular": None,
                "missing": [], "flow": rows[-1]["d"] - rows[0]["d"] if rows else 0}
    if rng not in RANGES:
        raise Bad("Unknown range")
    with _lock:
        doc = load()
    pos, cash, _, _ = replay(doc["transactions"])
    if symbol:  # one stock's own price
        qt = yahoo(symbol, rng)
        held = pos.get(symbol)
        return {"t": [t for t, _ in qt["points"]], "v": [c for _, c in qt["points"]],
                "base": qt["prevClose"] if rng == "1d" else None, "missing": [], "symbol": symbol,
                "regular": qt["periods"].get("regular") if rng == "1d" else None,
                "avg": held["cost"] / held["qty"] if held else None}
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
    return {"t": t_out, "v": v_out, "base": base, "regular": regular, "missing": [s for s in pos if s not in q]}


def news():
    """Recent stories that are really about a holding, newest first."""
    with _lock:
        doc = load()
    pos, _, _, _ = replay(doc["transactions"])
    key, now = ("news", tuple(sorted(pos))), time.time()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < 300:
        return hit[1]

    def fetch(sym):
        url = ("https://query1.finance.yahoo.com/v1/finance/search?q="
               f"{urllib.parse.quote(sym)}&newsCount=20&quotesCount=0")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.load(r).get("news") or []

    seen, items, failed = {}, [], 0
    for sym, fut in [(s, _pool.submit(fetch, s)) for s in pos]:
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
    out = {"items": items[:20], "failed": failed == len(pos) and bool(pos)}
    if hit and out["failed"]:
        return hit[1]
    if not out["failed"]:
        with _cache_lock:
            _cache[key] = (now, out)
    return out


def num(body, key, positive=True):
    try:
        v = float(body.get(key))
    except (TypeError, ValueError):
        v = float("nan")
    key = {"qty": "shares"}.get(key, key)
    if v != v:
        raise Bad(f"{key.capitalize()} must be a number.")
    if v != v or v in (float("inf"), float("-inf")) or (positive and v <= 0) or v < 0:
        raise Bad(f"{key.capitalize()} must be greater than zero." if positive else f"{key.capitalize()} can't be negative.")
    return v


def add_tx(body):
    kind = body.get("type")
    date = str(body.get("date") or time.strftime("%Y-%m-%d"))
    try:
        time.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise Bad("Date must look like 2026-01-31.")
    new = [{"id": uuid.uuid4().hex[:10], "type": kind, "date": date, "note": str(body.get("note") or "")[:200]}]
    tx = new[0]
    if kind in ("deposit", "withdraw"):
        tx["amount"] = num(body, "amount")
    elif kind in ("buy", "sell"):
        sym = str(body.get("symbol") or "").strip().upper()
        if not sym:
            raise Bad("Enter a ticker symbol.")
        tx.update(symbol=sym, qty=num(body, "qty"), price=num(body, "price"),
                  fee=num(body, "fee", positive=False) if body.get("fee") not in (None, "") else 0.0)
        if kind == "buy":
            tx["name"] = yahoo(sym, "1d")["name"]  # also rejects unknown tickers
    else:
        raise Bad("Unknown transaction type.")
    with _lock:
        doc = load()
        _, cash, _, _ = replay(doc["transactions"])
        if kind == "buy":
            short = tx["qty"] * tx["price"] + tx["fee"] - cash
            if short > 0.005:
                if not body.get("fund"):
                    raise Bad(f"Cash balance is ${short:,.2f} short for this buy.")
                new.insert(0, {"id": uuid.uuid4().hex[:10], "type": "deposit", "date": date,
                               "amount": round(short, 2), "note": f"Added to buy {tx['symbol']}"})
        if kind == "withdraw" and tx["amount"] > cash + 0.005:
            raise Bad(f"Only ${cash:,.2f} cash available to withdraw.")
        doc["transactions"].extend(new)
        replay(doc["transactions"])  # validates sells
        save(doc)


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
        save(doc)


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
    if not sym or op not in ("above", "below"):
        raise Bad("Pick a stock and whether to alert above or below a price.")
    price = num(body, "price")
    now = yahoo(sym, "1d")["price"]
    if (op == "above" and now >= price) or (op == "below" and now <= price):
        raise Bad(f"{sym} is already {op} ${price:,.2f} (now ${now:,.2f}).")
    with _lock:
        doc = load()
        if len(doc["alerts"]) >= 30:
            raise Bad("Too many alerts (30). Remove one first.")
        doc["alerts"].append({"id": uuid.uuid4().hex[:10], "symbol": sym, "op": op, "price": price, "fired": None})
        save(doc)


def del_alert(aid):
    with _lock:
        doc = load()
        doc["alerts"] = [a for a in doc["alerts"] if a["id"] != aid]
        save(doc)


def check_alerts():
    with _lock:
        pending = [a for a in load()["alerts"] if not a.get("fired")]
    if not pending:
        return
    q = quotes({a["symbol"] for a in pending}, "1d")
    hit = {}
    for a in pending:
        qt = q.get(a["symbol"])
        if qt and (qt["price"] >= a["price"] if a["op"] == "above" else qt["price"] <= a["price"]):
            hit[a["id"]] = qt["price"]
    if not hit:
        return
    fired = []
    with _lock:
        doc = load()
        for a in doc["alerts"]:
            if a["id"] in hit and not a.get("fired"):
                a["fired"], a["firedPrice"] = int(time.time()), hit[a["id"]]
                fired.append(a)
        save(doc)
    for a in fired:  # desktop notification, where the system has one
        try:
            subprocess.run(["notify-send", "-a", "Folio", "-i", str(ROOT / "icon.svg"),
                            f"{a['symbol']} is {a['op']} ${a['price']:,.2f}", f"Now ${a['firedPrice']:,.2f}"],
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


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def route(self, method):
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        try:
            if method == "GET" and u.path in ("/", "/index.html"):
                return self.send(200, (ROOT / "index.html").read_bytes().replace(b"__UI__", ui_version().encode(), 1), "text/html; charset=utf-8")
            if method == "GET" and u.path == "/icon.svg":
                return self.send(200, (ROOT / "icon.svg").read_bytes(), "image/svg+xml")
            if method == "GET" and u.path == "/api/state":
                return self.send(200, state())
            if method == "GET" and u.path == "/api/history":
                return self.send(200, history(qs.get("range", ["1d"])[0], qs.get("symbol", [""])[0].strip().upper() or None))
            if method == "GET" and u.path == "/api/market":
                return self.send(200, market())
            if method == "GET" and u.path == "/api/news":
                return self.send(200, news())
            if method == "GET" and u.path == "/api/lookup":
                q = yahoo(qs.get("symbol", [""])[0].strip().upper(), "1d")
                return self.send(200, {k: q[k] for k in ("symbol", "name", "price", "currency")})
            if method == "POST" and u.path in ("/api/watch", "/api/alerts"):
                n = int(self.headers.get("Content-Length") or 0)
                (add_watch if u.path == "/api/watch" else add_alert)(json.loads(self.rfile.read(n) or b"{}"))
                return self.send(200, state())
            if method == "DELETE" and u.path.startswith("/api/watch/"):
                del_watch(urllib.parse.unquote(u.path.rsplit("/", 1)[1]))
                return self.send(200, state())
            if method == "DELETE" and u.path.startswith("/api/alerts/"):
                del_alert(u.path.rsplit("/", 1)[1])
                return self.send(200, state())
            if method == "POST" and u.path == "/api/tx":
                n = int(self.headers.get("Content-Length") or 0)
                add_tx(json.loads(self.rfile.read(n) or b"{}"))
                return self.send(200, state())
            if method == "DELETE" and u.path.startswith("/api/tx/"):
                del_tx(u.path.rsplit("/", 1)[1])
                return self.send(200, state())
            self.send(404, {"error": "Not found"})
        except Bad as e:
            self.send(400, {"error": str(e)})
        except Exception as e:
            print("error:", repr(e), file=sys.stderr)
            self.send(500, {"error": "Something went wrong on the server."})

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_DELETE(self):
        self.route("DELETE")


if __name__ == "__main__":
    print(f"Folio on http://{HOST}:{PORT}  (data: {DATA})")
    threading.Thread(target=worker, daemon=True).start()
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
