#!/usr/bin/env python3
"""
Polymarket consistent-trader tracker.

Two jobs:
  1. RANK  - find wallets that show up near the top across MULTIPLE time
             windows (that's the "consistency" filter, not raw headline PnL).
  2. WATCH - poll those wallets' recent trades and alert on new activity
             and on consensus (several tracked wallets on the same side).

Usage:
    python tracker.py rank      # rebuild watchlist.json (run weekly)
    python tracker.py watch     # check for new trades (run every 15 min)
    python tracker.py probe     # diagnose which leaderboard endpoint works
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

# ----------------------------------------------------------------------------
# Config (override any of these with env vars / repo secrets)
# ----------------------------------------------------------------------------
UA = {"User-Agent": "polytracker/1.0 (personal research script)"}
DATA_API = "https://data-api.polymarket.com"

# Confirmed against docs.polymarket.com (Data API OpenAPI spec, /v1/leaderboard)
WINDOWS = ["DAY", "WEEK", "MONTH", "ALL"]
WINDOW_WEIGHT = {"DAY": 0.5, "WEEK": 2.0, "MONTH": 3.0, "ALL": 2.5}
PAGE_SIZE = 50          # hard max the API allows per request
TOP_N_PER_WINDOW = int(os.getenv("TOP_N_PER_WINDOW", "100"))
MIN_WINDOWS = int(os.getenv("MIN_WINDOWS", "3"))          # must appear in >= this many windows
WATCHLIST_SIZE = int(os.getenv("WATCHLIST_SIZE", "25"))

MIN_TRADE_USD = float(os.getenv("MIN_TRADE_USD", "1000"))  # ignore dust

# Freshness: only alert on trades placed within this many minutes.
# LOOKBACK_HOURS is how far back we FETCH (safety margin for delayed runs);
# MAX_AGE_MIN is the hard filter on what actually reaches you.
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "6"))
MAX_AGE_MIN = float(os.getenv("MAX_AGE_MIN", "60"))

# Route each category to its own Discord channel (optional).
# Falls back to DISCORD_WEBHOOK if a specific one isn't set.
SPORTS_WEBHOOK = os.getenv("SPORTS_WEBHOOK", "").strip()
CRYPTO_WEBHOOK = os.getenv("CRYPTO_WEBHOOK", "").strip()
OTHER_WEBHOOK = os.getenv("OTHER_WEBHOOK", "").strip()

GAMMA = "https://gamma-api.polymarket.com"
CONSENSUS_MIN = int(os.getenv("CONSENSUS_MIN", "3"))       # N wallets, same market+side
CONSENSUS_HOURS = int(os.getenv("CONSENSUS_HOURS", "24"))

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WATCHLIST_FILE = "watchlist.json"
STATE_FILE = "state.json"

# Official endpoint per the Data API spec. Params are case-sensitive enums:
#   category=OVERALL|POLITICS|SPORTS|ESPORTS|CRYPTO|CULTURE|MENTIONS|WEATHER|
#            ECONOMICS|TECH|FINANCE
#   timePeriod=DAY|WEEK|MONTH|ALL      orderBy=PNL|VOL
#   limit<=50, offset<=1000
LEADERBOARD_URL = f"{DATA_API}/v1/leaderboard"
CATEGORY = os.getenv("CATEGORY", "OVERALL")


# ----------------------------------------------------------------------------
# HTTP helper with backoff
# ----------------------------------------------------------------------------
def get(url, params=None, tries=4):
    for attempt in range(tries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=25)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)
    return None


def normalize_rows(payload):
    """Leaderboard responses come back as a bare list or wrapped in a key."""
    if payload is None:
        return []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for key in ("data", "traders", "results", "leaderboard", "items"):
            if isinstance(payload.get(key), list):
                rows = payload[key]
                break
        else:
            return []
    else:
        return []

    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = (row.get("proxyWallet") or row.get("wallet")
                  or row.get("address") or row.get("user") or "")
        if not wallet:
            continue
        out.append({
            "wallet": wallet.lower(),
            "name": row.get("userName") or row.get("name") or row.get("pseudonym") or "",
            "pnl": float(row.get("pnl") or row.get("profit") or row.get("amount") or 0),
            "vol": float(row.get("vol") or row.get("volume") or 0),
        })
    return out


def fetch_leaderboard(window):
    """Page through the leaderboard until we have TOP_N_PER_WINDOW traders.

    The API caps limit at 50, so anything above that needs offset pagination.
    """
    collected = []
    offset = 0
    while len(collected) < TOP_N_PER_WINDOW and offset <= 1000:
        params = {
            "category": CATEGORY,
            "timePeriod": window,
            "orderBy": "PNL",
            "limit": min(PAGE_SIZE, TOP_N_PER_WINDOW - len(collected)),
            "offset": offset,
        }
        try:
            rows = normalize_rows(get(LEADERBOARD_URL, params))
        except Exception as e:
            print(f"    ! {window} offset {offset}: {e}")
            break
        if not rows:
            break
        collected.extend(rows)
        offset += len(rows)
        time.sleep(0.3)

    # Dedupe while preserving rank order
    seen, out = set(), []
    for r in collected:
        if r["wallet"] not in seen:
            seen.add(r["wallet"])
            out.append(r)
    return out


# ----------------------------------------------------------------------------
# CATEGORY CLASSIFICATION
# ----------------------------------------------------------------------------
# Polymarket tags events, but the /activity feed doesn't include tags. So we
# classify from the market title/slug, then (optionally) confirm via Gamma.
# Results are cached in state.json so we don't re-query the same market.

SPORT_WORDS = [
    "nfl", "nba", "mlb", "nhl", "ncaa", "college football", "college basketball",
    "premier league", "la liga", "serie a", "bundesliga", "ligue 1", "epl",
    "champions league", "uefa", "fifa", "world cup", "mls", "soccer",
    "super bowl", "world series", "stanley cup", "finals", "playoff",
    "ufc", "mma", "boxing", "wwe", "tennis", "atp", "wta", "us open",
    "wimbledon", "french open", "australian open", "golf", "pga", "masters",
    "f1", "formula 1", "grand prix", "nascar", "cricket", "ipl", "rugby",
    "olympics", "heisman", "mvp", "vs.", " vs ", "beat the", "score",
]
ESPORT_WORDS = [
    "esports", "league of legends", "lol worlds", "dota", "csgo", "cs2",
    "counter-strike", "valorant", "overwatch", "rocket league", "starcraft",
    "call of duty", "cdl", "lck", "lpl", "lec", "fortnite", "apex legends",
]
CRYPTO_WORDS = [
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol ", "xrp", "ripple",
    "dogecoin", "doge", "crypto", "altcoin", "stablecoin", "usdc", "usdt",
    "binance", "coinbase", "etf approval", "halving", "memecoin", "token",
    "cardano", "ada", "avalanche", "chainlink", "polygon", "matic",
]
POLITICS_WORDS = [
    "election", "president", "senate", "house of representatives", "congress",
    "governor", "primary", "nominee", "parliament", "prime minister",
    "supreme court", "impeach", "cabinet", "secretary of", "vote", "ballot",
    "democrat", "republican", "gop", "poll", "approval rating", "referendum",
]
ECON_WORDS = [
    "fed ", "federal reserve", "interest rate", "rate cut", "rate hike",
    "inflation", "cpi", "gdp", "recession", "unemployment", "jobs report",
    "s&p", "nasdaq", "dow jones", "treasury", "tariff", "earnings",
]
TECH_WORDS = [
    "openai", "gpt", "anthropic", "claude", "gemini", "llm", "agi",
    "apple", "tesla", "spacex", "nvidia", "ipo", "acquisition", "chatgpt",
]

BUCKETS = [
    ("ESPORTS", ESPORT_WORDS),     # before SPORTS — "league" overlaps
    ("SPORTS", SPORT_WORDS),
    ("CRYPTO", CRYPTO_WORDS),
    ("POLITICS", POLITICS_WORDS),
    ("ECONOMICS", ECON_WORDS),
    ("TECH", TECH_WORDS),
]

# Word-boundary matching. Substring matching breaks badly here: "LEC" (an
# esports league) is inside "election", "SOL" is inside "solar", etc.
_PATTERNS = {
    name: re.compile("|".join(r"\b" + re.escape(w.strip()) + r"\b" for w in words))
    for name, words in BUCKETS
}

# Where each bucket's alerts go
ROUTES = {
    "SPORTS": lambda: SPORTS_WEBHOOK or DISCORD_WEBHOOK,
    "ESPORTS": lambda: SPORTS_WEBHOOK or DISCORD_WEBHOOK,
    "CRYPTO": lambda: CRYPTO_WEBHOOK or OTHER_WEBHOOK or DISCORD_WEBHOOK,
}


def classify(title, slug, cache):
    """Return a category string. Cached per market slug."""
    key = slug or title
    if key in cache:
        return cache[key]

    blob = f"{title} {slug}".lower().replace("-", " ")
    result = None
    for name, words in BUCKETS:
        if any(_PATTERNS[name].search(blob) for w in [1]):
            result = name
            break

    # Ambiguous? Ask Gamma for the event's real tags.
    if result is None and slug:
        try:
            ev = get(f"{GAMMA}/events", {"slug": slug}, tries=1)
            if isinstance(ev, list) and ev:
                tags = [t.get("label", "").upper() for t in (ev[0].get("tags") or [])]
                for cand in ["SPORTS", "ESPORTS", "CRYPTO", "POLITICS",
                             "ECONOMICS", "TECH", "CULTURE"]:
                    if any(cand in t for t in tags):
                        result = cand
                        break
        except Exception:
            pass

    result = result or "OTHER"
    cache[key] = result
    return result


# ----------------------------------------------------------------------------
# RANK
# ----------------------------------------------------------------------------
def rank():
    scores = defaultdict(float)
    appearances = defaultdict(list)
    meta = {}

    for w in WINDOWS:
        rows = fetch_leaderboard(w)
        print(f"  {w:>4}: {len(rows)} rows")
        for rank_i, row in enumerate(rows):
            wal = row["wallet"]
            # Rank decay: #1 is worth 1.0, #100 is worth ~0.1
            rank_factor = 1.0 - (rank_i / max(len(rows), 1)) * 0.9
            scores[wal] += WINDOW_WEIGHT[w] * rank_factor
            appearances[wal].append(w)
            if wal not in meta or row["pnl"] > meta[wal]["pnl"]:
                meta[wal] = row
        time.sleep(0.5)

    if not scores:
        sys.exit('No leaderboard data returned. Run the "0 - Test connection" workflow.')

    # THE CONSISTENCY FILTER: must show up in several windows, and specifically
    # must be present in a medium-term window. One lucky month doesn't count.
    candidates = []
    for wal, sc in scores.items():
        wins = appearances[wal]
        if len(wins) < MIN_WINDOWS:
            continue
        if "MONTH" not in wins and "ALL" not in wins:
            continue
        candidates.append((sc, wal, wins))

    candidates.sort(reverse=True)
    watchlist = []
    for sc, wal, wins in candidates[:WATCHLIST_SIZE]:
        m = meta[wal]
        stats = wallet_stats(wal)
        watchlist.append({
            "wallet": wal,
            "name": m["name"],
            "consistency_score": round(sc, 3),
            "windows": wins,
            "pnl": m["pnl"],
            "vol": m["vol"],
            **stats,
        })
        time.sleep(0.3)

    with open(WATCHLIST_FILE, "w") as f:
        json.dump({"generated": now_iso(), "traders": watchlist}, f, indent=2)

    print(f"\nWatchlist ({len(watchlist)}):")
    for t in watchlist:
        print(f"  {t['consistency_score']:>6}  {t['name'] or t['wallet'][:10]:<20} "
              f"windows={','.join(t['windows']):<16} trades={t.get('trade_count','?')} "
              f"open=${t.get('open_value',0):,.0f}")


def wallet_stats(wallet):
    """Sample size matters more than headline PnL. 4 huge bets != skill."""
    out = {}
    try:
        pos = get(f"{DATA_API}/positions", {"user": wallet, "limit": 500}) or []
        if isinstance(pos, dict):
            pos = pos.get("data", [])
        out["open_positions"] = len(pos)
        out["open_value"] = sum(float(p.get("currentValue") or 0) for p in pos)
    except Exception:
        pass
    try:
        act = get(f"{DATA_API}/activity",
                  {"user": wallet, "limit": 500, "type": "TRADE"}) or []
        if isinstance(act, dict):
            act = act.get("data", [])
        out["trade_count"] = len(act)
        markets = {a.get("conditionId") for a in act if a.get("conditionId")}
        out["distinct_markets"] = len(markets)
    except Exception:
        pass
    return out


# ----------------------------------------------------------------------------
# WATCH
# ----------------------------------------------------------------------------
def watch():
    if not os.path.exists(WATCHLIST_FILE):
        sys.exit("No watchlist.json. Run the 'Rebuild watchlist' workflow first.")

    traders = json.load(open(WATCHLIST_FILE))["traders"]
    state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    seen = set(state.get("seen_tx", []))
    recent = state.get("recent", [])
    cat_cache = state.get("categories", {})

    new_events = []
    now = time.time()
    fetch_cutoff = now - LOOKBACK_HOURS * 3600     # how far back we look
    age_cutoff = now - MAX_AGE_MIN * 60            # how fresh it must be to alert
    stale_skipped = 0

    for t in traders:
        try:
            act = get(f"{DATA_API}/activity",
                      {"user": t["wallet"], "limit": 40, "type": "TRADE"}) or []
        except Exception as e:
            print(f"  ! {t['wallet'][:10]}: {e}")
            continue
        if isinstance(act, dict):
            act = act.get("data", [])

        for a in act:
            ts = int(a.get("timestamp") or 0)
            if ts < fetch_cutoff:
                continue
            key = a.get("transactionHash") or f"{t['wallet']}-{ts}-{a.get('asset')}"
            if key in seen:
                continue
            usd = float(a.get("usdcSize") or a.get("size") or 0)
            if usd < MIN_TRADE_USD:
                seen.add(key)
                continue

            # Freshness gate — mark seen either way so it never resurfaces
            if ts < age_cutoff:
                seen.add(key)
                stale_skipped += 1
                continue

            title = a.get("title") or a.get("slug") or "(unknown market)"
            slug = a.get("eventSlug") or a.get("slug") or ""
            cat = classify(title, slug, cat_cache)

            ev = {
                "key": key, "ts": ts, "cat": cat,
                "trader": t["name"] or t["wallet"][:10],
                "wallet": t["wallet"], "score": t["consistency_score"],
                "title": title, "outcome": a.get("outcome") or "",
                "side": a.get("side") or "", "price": float(a.get("price") or 0),
                "usd": usd, "market": a.get("conditionId") or slug,
            }
            seen.add(key)
            new_events.append(ev)
            recent.append(ev)
        time.sleep(0.25)

    # Trim rolling consensus window
    rc = now - CONSENSUS_HOURS * 3600
    recent = [e for e in recent if e["ts"] >= rc]

    clusters, detail = defaultdict(set), defaultdict(list)
    for e in recent:
        k = (e["market"], e["outcome"], e["side"])
        clusters[k].add(e["wallet"])
        detail[k].append(e)

    fired = set(state.get("fired_consensus", []))
    fresh_consensus = [(k, v) for k, v in clusters.items()
                       if len(v) >= CONSENSUS_MIN and str(k) not in fired]

    # ---- group into buckets and send separately ----
    groups = defaultdict(list)
    for e in new_events:
        groups[route_bucket(e["cat"])].append(e)
    for k, wallets in fresh_consensus:
        d = detail[k][0]
        groups[route_bucket(d["cat"])].append({"consensus": True, "k": k,
                                               "wallets": wallets,
                                               "detail": detail[k], "d": d})
        fired.add(str(k))

    if not groups:
        msg = "No new qualifying activity"
        if stale_skipped:
            msg += f" ({stale_skipped} trade(s) skipped as older than {MAX_AGE_MIN:.0f} min)"
        print(msg)
    else:
        for bucket, items in groups.items():
            body = render(bucket, items)
            print(f"\n===== {bucket} =====\n{body}")
            send(body, webhook_for(bucket))
        if stale_skipped:
            print(f"\n({stale_skipped} trade(s) skipped as older than {MAX_AGE_MIN:.0f} min)")

    json.dump({
        "updated": now_iso(),
        "seen_tx": list(seen)[-6000:],
        "recent": recent[-2000:],
        "fired_consensus": list(fired)[-500:],
        "categories": dict(list(cat_cache.items())[-4000:]),
    }, open(STATE_FILE, "w"), indent=2)


def route_bucket(cat):
    """Collapse categories into the channels we actually send to."""
    if cat in ("SPORTS", "ESPORTS"):
        return "SPORTS"
    if cat == "CRYPTO":
        return "CRYPTO"
    return "OTHER"


ICON = {"SPORTS": "\U0001F3C8", "CRYPTO": "\u20BF", "OTHER": "\U0001F4CA"}


def render(bucket, items):
    trades = [i for i in items if not i.get("consensus")]
    cons = [i for i in items if i.get("consensus")]
    lines = [f"{ICON.get(bucket,'')} **{bucket}** — {len(trades)} trade(s)"]

    for e in sorted(trades, key=lambda x: -x["usd"])[:12]:
        age = int((time.time() - e["ts"]) / 60)
        lines.append(
            f"\u2022 `{e['trader']}` (score {e['score']}) {e['side']} "
            f"**{e['outcome']}** @ {e['price']:.2f} \u2014 ${e['usd']:,.0f} "
            f"\u2014 {age}m ago\n   _{e['title'][:90]}_"
        )
    for c in cons:
        d, total = c["d"], sum(x["usd"] for x in c["detail"])
        lines.append(
            f"\n\U0001F53A **CONSENSUS** \u2014 {len(c['wallets'])} wallets {d['side']} "
            f"**{d['outcome']}** on _{d['title'][:80]}_ "
            f"(${total:,.0f} combined, last {CONSENSUS_HOURS}h)"
        )
    return "\n".join(lines)


# ----------------------------------------------------------------------------
def send(msg, webhook=None):
    hook = webhook or DISCORD_WEBHOOK
    if hook:
        for chunk in [msg[i:i + 1900] for i in range(0, len(msg), 1900)]:
            try:
                requests.post(hook, json={"content": chunk}, timeout=15)
            except Exception as e:
                print(f"discord failed: {e}")
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                      "parse_mode": "Markdown"}, timeout=15)
        except Exception as e:
            print(f"telegram failed: {e}")
    if not hook and not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        print("(no webhook configured \u2014 printing only)")


def webhook_for(bucket):
    return {"SPORTS": SPORTS_WEBHOOK,
            "CRYPTO": CRYPTO_WEBHOOK,
            "OTHER": OTHER_WEBHOOK}.get(bucket) or DISCORD_WEBHOOK


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def probe():
    print("Leaderboard endpoint check:\n")
    for w in WINDOWS:
        params = {"category": CATEGORY, "timePeriod": w,
                  "orderBy": "PNL", "limit": 5, "offset": 0}
        try:
            r = requests.get(LEADERBOARD_URL, params=params, headers=UA, timeout=20)
            rows = normalize_rows(r.json()) if r.ok else []
            print(f"{r.status_code}  timePeriod={w:<6} -> {len(rows)} parsed rows")
            if rows:
                print(f"      sample: {rows[0]}")
        except Exception as e:
            print(f"ERR  timePeriod={w} -> {e}")

    print("\nData API check:")
    for ep in ["/positions", "/activity", "/value"]:
        try:
            r = requests.get(f"{DATA_API}{ep}",
                             params={"user": "0x6af75d4e4aaf700450efbac3708cce1665810ff1",
                                     "limit": 3},
                             headers=UA, timeout=20)
            print(f"{r.status_code}  {ep}  {str(r.text)[:120]}")
        except Exception as e:
            print(f"ERR  {ep} -> {e}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "watch"
    {"rank": rank, "watch": watch, "probe": probe}.get(cmd, watch)()
