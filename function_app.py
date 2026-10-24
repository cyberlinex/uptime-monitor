"""Website uptime & latency monitor: the whole backend in one file.

check_sites  (timer, every 5 min): ping sites -> save to Table Storage
                                   -> detect outages/slowdowns -> log incidents + Telegram alerts
status       (HTTP GET /api/status): returns sites, latency series and incidents as JSON
"""
import json
import logging
import os
import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import azure.functions as func
import requests
from azure.data.tables import TableServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ---------- settings ----------
SITES = [
    {"name": "IRCTC",           "url": "https://www.irctc.co.in"},
    {"name": "Flipkart",        "url": "https://www.flipkart.com"},
    {"name": "Zerodha",         "url": "https://zerodha.com"},
    {"name": "SBI",             "url": "https://sbi.co.in"},
    {"name": "Times of India",  "url": "https://timesofindia.indiatimes.com"},
    {"name": "NDTV",            "url": "https://www.ndtv.com"},
    {"name": "Paytm",           "url": "https://paytm.com"},
    {"name": "Swiggy",          "url": "https://www.swiggy.com"},
    {"name": "Zomato",          "url": "https://www.zomato.com"},
    {"name": "HDFC Bank",       "url": "https://www.hdfcbank.com"},
]
TIMEOUT_S = 10      # a request slower than this counts as a timeout
REPEATS = 3         # checks in a row before we call something an incident
Z_LIMIT = 3         # slowdown = response time more than 3 std devs above normal
MIN_STD_MS = 20     # floor on std dev so very steady sites don't false-alarm
MIN_HISTORY = 30    # good checks needed (~2.5 h) before judging speed


# ---------- small helpers ----------
def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat(timespec="milliseconds")  # sorts correctly as text


def slug(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


_tables = {}


def table(name):
    """Table Storage client (the table is created on first use)."""
    if name not in _tables:
        conn = os.environ["AzureWebJobsStorage"]
        _tables[name] = TableServiceClient.from_connection_string(conn).create_table_if_not_exists(name)
    return _tables[name]


def notify(text):
    """Send a Telegram message. Token and chat id come from app settings, not code."""
    token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (token and chat):
        logging.warning("Telegram not configured. Message: %s", text)
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat, "text": text}, timeout=10)
    except requests.RequestException:
        logging.exception("Telegram alert failed")


# ---------- 1. checking ----------
def ping(site):
    status, error = 0, ""
    start = time.perf_counter()
    try:
        # stream=True: we only need the response headers, not the whole page
        with requests.get(site["url"], timeout=TIMEOUT_S, stream=True,
                          headers={"User-Agent": "uptime-monitor/1.0 (personal project)"}) as r:
            status = r.status_code
    except requests.Timeout:
        error = "timeout"
    except requests.RequestException as e:
        error = type(e).__name__
    ok = 200 <= status < 300
    if status and not ok:
        error = f"HTTP {status}"
    return {"status_code": status, "response_ms": round((time.perf_counter() - start) * 1000),
            "ok": ok, "error": error}


def get_checks(site_slug, since):
    rows = table("checks").query_entities(
        "PartitionKey eq @p and RowKey ge @s", parameters={"p": site_slug, "s": iso(since)})
    return [{"t": r["RowKey"], "ok": r["ok"], "status_code": r["status_code"],
             "ms": r["response_ms"]} for r in rows]  # oldest -> newest


# ---------- 2. detection ----------
def is_outage(checks):
    """Last 3 checks all failed (non-2xx or timeout)."""
    last = checks[-REPEATS:]
    return len(last) == REPEATS and not any(c["ok"] for c in last)


def is_slowdown(checks):
    """Rolling z-score: are the last 3 checks all far slower than the previous 24 h?"""
    recent, history = checks[-REPEATS:], checks[:-REPEATS]
    times = [c["ms"] for c in history if c["ok"]]
    if len(recent) < REPEATS or len(times) < MIN_HISTORY:
        return False
    mean = statistics.mean(times)
    std = max(statistics.pstdev(times), MIN_STD_MS)
    return all(c["ok"] and (c["ms"] - mean) / std > Z_LIMIT for c in recent)


# ---------- 3. incidents + alerts ----------
def update_incident(site, kind, active, open_incidents, start_msg, end_msg):
    """Open an incident (and alert) when a problem starts; close it when it ends.
    Alerting only on changes means a long outage sends 2 messages, not hundreds."""
    existing = open_incidents.get((site["name"], kind))
    if active and not existing:
        started = iso(now())
        table("incidents").create_entity({
            "PartitionKey": "incident", "RowKey": f"{started}|{slug(site['name'])}|{kind}",
            "site": site["name"], "kind": kind, "started": started, "ended": ""})
        notify(start_msg)
    elif not active and existing:
        existing["ended"] = iso(now())
        table("incidents").update_entity(existing)
        notify(end_msg)


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False)
def check_sites(timer: func.TimerRequest) -> None:
    # 1. ping all sites in parallel and store the results
    with ThreadPoolExecutor(max_workers=len(SITES)) as pool:
        results = list(pool.map(ping, SITES))
    stamp = iso(now())
    for site, result in zip(SITES, results):
        table("checks").upsert_entity({"PartitionKey": slug(site["name"]), "RowKey": stamp, **result})

    # 2. look at each site's last 24 h and open/close incidents
    open_incidents = {(r["site"], r["kind"]): r for r in table("incidents").query_entities("ended eq ''")}
    since = now() - timedelta(hours=24)
    for site in SITES:
        checks = get_checks(slug(site["name"]), since)
        outage = is_outage(checks)
        update_incident(site, "outage", outage, open_incidents,
                        f"🔴 {site['name']} is DOWN ({REPEATS} failed checks in a row)\n{site['url']}",
                        f"🟢 {site['name']} is back up")
        if not outage:
            update_incident(site, "slowdown", is_slowdown(checks), open_incidents,
                            f"🟡 {site['name']} is unusually slow right now",
                            f"🟢 {site['name']} speed is back to normal")


# ---------- 4. API for the status page ----------
@app.route(route="status", methods=["GET"])
def status(req: func.HttpRequest) -> func.HttpResponse:
    since = now() - timedelta(hours=24)
    sites = []
    for s in SITES:
        checks = get_checks(slug(s["name"]), since)
        good = [c["ms"] for c in checks if c["ok"]]
        sites.append({
            "name": s["name"], "url": s["url"],
            "current": checks[-1] if checks else None,
            "uptime_24h": round(100 * sum(c["ok"] for c in checks) / len(checks), 2) if checks else None,
            "avg_ms": round(statistics.mean(good)) if good else None,
            # failed checks become null so the chart shows a gap, not a 10-second spike
            "series": [{"t": c["t"], "ms": c["ms"] if c["ok"] else None} for c in checks],
        })

    week_ago = iso(now() - timedelta(days=7))
    rows = table("incidents").query_entities("RowKey ge @s", parameters={"s": week_ago})
    incidents = sorted(({k: r[k] for k in ("site", "kind", "started", "ended")} for r in rows),
                       key=lambda r: r["started"], reverse=True)[:30]

    body = {"generated": iso(now()), "sites": sites, "incidents": incidents}
    return func.HttpResponse(json.dumps(body), mimetype="application/json",
                             headers={"Cache-Control": "public, max-age=60"})
