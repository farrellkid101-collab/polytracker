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

WINDOWS = ["1d", "7d", "30d", "all"]
WINDOW_WEIGHT = {"1d": 0.5, "7d": 2.0, "30d": 3.0, "all": 2.5}
TOP_N_PER_WINDOW = int(os.getenv("TOP_N_PER_WINDOW", "100"))
MIN_WINDOWS = int(os.getenv("MIN_WINDOWS", "3"))          # must appear in >= this many windows
WATCHLIST_SIZE = int(os.getenv("WATCHLIST_SIZE", "25"))

MIN_TRADE_USD = float(os.getenv("MIN_TRADE_USD", "1000"))  # ignore dust
CONSENSUS_MIN = int(os.getenv("CONSENSUS_MIN", "3"))       # N wallets, same market+side
CONSENSUS_HOURS = int(os.getenv("CONSENSUS_HOURS", "24"))

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WATCHLIST_FILE = "watchlist.json"
STATE_FILE = "state.json"

# Polymarket has moved the leaderboard host around. Rather than hardcode one
# and silently break, we try each shape and cache whichever answers.
LEADERBOARD_CANDIDATES = [
    ("https://lb-api.polymarket.com/leaderboard",
     lambda w: {"window": w, "limit": TOP_N_PER_WINDOW, "rankType": "pnl"}),
    (f"{DATA_API}/leaderboard",
     lambda w: {"window": w, "limit": TOP_N_PER_WINDOW, "rankType": "pnl"}),
    (f"{DATA_API}/ranking",
     lambda w: {"window": w, "limit": TOP_N_PER_WINDOW, "rankType": "pnl"}),
    (f"{DATA_API}/leaderboard/profit",
     lambda w: {"window": w, "limit": TOP_N_PER_WINDOW}),
]


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


def fetch_leaderboard(window, cached=None):
    """Returns (rows, working_url_index)."""
    order = ([cached] if cached is not None else []) + \
            [i for i in range(len(LEADERBOARD_CANDIDATES)) if i != cached]
    for i in order:
        url, mk = LEADERBOARD_CANDIDATES[i]
        try:
            rows = normalize_rows(get(url, mk(window), tries=2))
            if rows:
                return rows, i
        except Exception:
            continue
    return [], cached


# ----------------------------------------------------------------------------
# RANK
# ----------------------------------------------------------------------------
def rank():
    scores = defaultdict(float)
    appearances = defaultdict(list)
    meta = {}
    working = None

    for w in WINDOWS:
        rows, working = fetch_leaderboard(w, working)
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
        sys.exit("No leaderboard data returned. Run `python tracker.py probe`.")

    # THE CONSISTENCY FILTER: must show up in several windows, and specifically
    # must be present in a medium-term window. One lucky month doesn't count.
    candidates = []
    for wal, sc in scores.items():
        wins = appearances[wal]
        if len(wins) < MIN_WINDOWS:
            continue
        if "30d" not in wins and "all" not in wins:
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
        sys.exit("No watchlist.json. Run `python tracker.py rank` first.")

    traders = json.load(open(WATCHLIST_FILE))["traders"]
    state = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    seen = set(state.get("seen_tx", []))
    recent = state.get("recent", [])   # rolling window for consensus detection

    new_events = []
    cutoff = time.time() - 60 * 60 * 8   # look back 8h (covers a 2h cadence w/ margin)

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
            if ts < cutoff:
                continue
            key = a.get("transactionHash") or f"{t['wallet']}-{ts}-{a.get('asset')}"
            if key in seen:
                continue
            usd = float(a.get("usdcSize") or a.get("size") or 0)
            if usd < MIN_TRADE_USD:
                seen.add(key)
                continue

            ev = {
                "key": key,
                "ts": ts,
                "trader": t["name"] or t["wallet"][:10],
                "wallet": t["wallet"],
                "score": t["consistency_score"],
                "title": a.get("title") or a.get("slug") or "(unknown market)",
                "outcome": a.get("outcome") or "",
                "side": a.get("side") or "",
                "price": float(a.get("price") or 0),
                "usd": usd,
                "market": a.get("conditionId") or a.get("slug") or "",
            }
            seen.add(key)
            new_events.append(ev)
            recent.append(ev)
        time.sleep(0.25)

    # Trim rolling window
    rc = time.time() - CONSENSUS_HOURS * 3600
    recent = [e for e in recent if e["ts"] >= rc]

    # Consensus: distinct tracked wallets, same market + outcome + side
    clusters = defaultdict(set)
    detail = defaultdict(list)
    for e in recent:
        k = (e["market"], e["outcome"], e["side"])
        clusters[k].add(e["wallet"])
        detail[k].append(e)

    consensus = [(k, v) for k, v in clusters.items() if len(v) >= CONSENSUS_MIN]
    fired = set(state.get("fired_consensus", []))
    fresh_consensus = [(k, v) for k, v in consensus if str(k) not in fired]

    # ---- report ----
    lines = []
    if new_events:
        lines.append(f"**{len(new_events)} new trade(s) from tracked wallets**")
        for e in sorted(new_events, key=lambda x: -x["usd"])[:12]:
            lines.append(
                f"• `{e['trader']}` (score {e['score']}) {e['side']} "
                f"**{e['outcome']}** @ {e['price']:.2f} — ${e['usd']:,.0f}\n"
                f"   _{e['title'][:90]}_"
            )
    for k, wallets in fresh_consensus:
        d = detail[k][0]
        total = sum(x["usd"] for x in detail[k])
        lines.append(
            f"\n🔺 **CONSENSUS** — {len(wallets)} tracked wallets {d['side']} "
            f"**{d['outcome']}** on _{d['title'][:80]}_ "
            f"(${total:,.0f} combined, last {CONSENSUS_HOURS}h)"
        )
        fired.add(str(k))

    if lines:
        send("\n".join(lines))
        print("\n".join(lines))
    else:
        print("No new qualifying activity.")

    json.dump({
        "updated": now_iso(),
        "seen_tx": list(seen)[-6000:],
        "recent": recent[-2000:],
        "fired_consensus": list(fired)[-500:],
    }, open(STATE_FILE, "w"), indent=2)


# ----------------------------------------------------------------------------
def send(msg):
    if DISCORD_WEBHOOK:
        for chunk in [msg[i:i + 1900] for i in range(0, len(msg), 1900)]:
            try:
                requests.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=15)
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
    if not DISCORD_WEBHOOK and not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        print("(no webhook configured — printing only)")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def probe():
    print("Testing leaderboard endpoints...\n")
    for url, mk in LEADERBOARD_CANDIDATES:
        p = mk("7d")
        try:
            r = requests.get(url, params=p, headers=UA, timeout=20)
            rows = normalize_rows(r.json()) if r.ok else []
            print(f"{r.status_code}  {url}  -> {len(rows)} parsed rows")
            if rows:
                print(f"      sample: {rows[0]}")
        except Exception as e:
            print(f"ERR  {url}  -> {e}")
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
