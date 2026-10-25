"""Candidate pool and daily follower snapshots for X accounts (phase 0 of the KOL-growth idea).

Records only. Nothing here ranks accounts for trading, reads their posts, or issues orders:
the selection rule that would turn "fast-growing" into a signal must first be frozen in a
protocol (see docs/X_KOL_GROWTH_TRACKER.md), and X keeps no follower history, so growth can
only be measured forward from the first snapshot.

SQLite file under the gitignored data/research/x_kol_growth/. One snapshot per account per
UTC day; the first one of a day wins unless it is replaced on purpose.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "research" / "x_kol_growth" / "growth.db"
MISS_LIMIT = 3  # an account missing from this many successful lookups in a row is treated as gone
PENDING_TRIES = 3

SCHEMA = """
create table if not exists candidates(
    user_id text primary key,
    username text not null,
    name text,
    description text,
    account_created_at text,
    kind_guess text,
    origin text,
    mention_count integer default 0,
    protected integer default 0,
    first_seen_at text not null,
    miss_count integer default 0,
    active integer default 1
);
create unique index if not exists candidates_username on candidates(lower(username));
create table if not exists snapshots(
    user_id text not null,
    day text not null,
    taken_at text not null,
    followers integer,
    following integer,
    statuses integer,
    listed integer,
    verified integer,
    blue_verified integer,
    primary key(user_id, day)
);
create table if not exists meta(
    key text primary key,
    value text
);
create table if not exists pending(
    username text primary key,
    mention_count integer default 0,
    state text default 'new',
    tries integer default 0,
    last_try text,
    note text
);
"""

_PROJECT = re.compile(
    r"\b(official|protocol|exchange|dex|network|foundation|labs?|dao|wallet|ecosystem|launchpad|marketplace|"
    r"infrastructure|platform|the home of|powered by|inc\.?|ltd|ventures|fund|capital)\b", re.I)
_FEED = re.compile(r"\b(news|alerts?|bot|signals?|tracker|updates|feed|daily|breaking)\b|快讯|资讯", re.I)
_PERSON = re.compile(
    r"\b(trader|analyst|researcher|investor|founder|ceo|angel|degen|my views|dm|nfa|not financial advice|"
    r"opinions? (are )?my own|i\b|my\b)\b|交易员|分析师|研究员|投资人|个人观点|我", re.I)


def guess_kind(name: Any, description: Any, username: Any = "") -> str:
    """Rough project / feed / individual / unknown label from the profile text. A hint for later, not a filter."""
    head = f"{name or ''} {str(description or '')[:200]}"
    if _PROJECT.search(head):
        return "project"
    if _FEED.search(head):
        return "feed"
    if _PERSON.search(head):
        return "individual"
    return "unknown"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def day_of(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d")


def connect(path: Optional[Path] = None) -> sqlite3.Connection:
    target = path or DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma journal_mode=wal")
    conn.executescript(SCHEMA)
    return conn


def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("select value from meta where key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("insert or replace into meta(key, value) values(?, ?)", (key, value))


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def upsert_candidate(conn: sqlite3.Connection, record: Dict[str, Any], origin: str, now: datetime,
                     mention_count: Optional[int] = None) -> bool:
    """Add or refresh one account from a Scweet profile dict. True when it was new."""
    user_id = str(record.get("user_id") or "").strip()
    username = str(record.get("username") or "").strip()
    if not user_id.isdigit() or not username:
        return False
    existing = conn.execute("select 1 from candidates where user_id = ?", (user_id,)).fetchone()
    kind = guess_kind(record.get("name"), record.get("description"), username)
    if existing:
        conn.execute(
            "update candidates set username=?, name=?, description=?, kind_guess=?, protected=?, miss_count=0, active=1 "
            "where user_id=?",
            (username, record.get("name"), record.get("description"), kind, int(bool(record.get("protected"))), user_id))
        return False
    conn.execute(
        "insert into candidates(user_id, username, name, description, account_created_at, kind_guess, origin, "
        "mention_count, protected, first_seen_at) values(?,?,?,?,?,?,?,?,?,?)",
        (user_id, username, record.get("name"), record.get("description"), record.get("created_at"), kind, origin,
         mention_count or 0, int(bool(record.get("protected"))), now.isoformat()))
    return True


def add_pending(conn: sqlite3.Connection, counts: Dict[str, int]) -> int:
    """Queue usernames (with how often KOLs mentioned them) that are not candidates or queued yet."""
    added = 0
    for username, count in counts.items():
        name = str(username).strip().lower()
        if not name:
            continue
        if conn.execute("select 1 from candidates where lower(username) = ?", (name,)).fetchone():
            continue
        if conn.execute("select 1 from pending where username = ?", (name,)).fetchone():
            conn.execute("update pending set mention_count = max(mention_count, ?) where username = ?", (count, name))
            continue
        conn.execute("insert into pending(username, mention_count) values(?, ?)", (name, count))
        added += 1
    return added


def pending_batch(conn: sqlite3.Connection, limit: int) -> List[Tuple[str, int]]:
    rows = conn.execute(
        "select username, mention_count from pending where state in ('new','retry') "
        "order by mention_count desc, tries asc, username limit ?", (limit,)).fetchall()
    return [(r["username"], r["mention_count"]) for r in rows]


def mark_pending(conn: sqlite3.Connection, username: str, state: str, now: datetime, note: str = "") -> None:
    """done / missing end the queue entry; retry counts a try and gives up as 'failed' after PENDING_TRIES."""
    row = conn.execute("select tries from pending where username = ?", (username,)).fetchone()
    tries = (row["tries"] if row else 0) + (1 if state == "retry" else 0)
    if state == "retry" and tries >= PENDING_TRIES:
        state = "failed"
    conn.execute("update pending set state=?, tries=?, last_try=?, note=? where username=?",
                 (state, tries, now.isoformat(), note[:200], username))


def save_snapshot(conn: sqlite3.Connection, record: Dict[str, Any], taken_at: datetime, replace: bool = False) -> bool:
    user_id = str(record.get("user_id") or "").strip()
    followers = _int(record.get("followers_count"))
    if not user_id.isdigit() or followers is None:
        return False
    verb = "insert or replace" if replace else "insert or ignore"
    cur = conn.execute(
        f"{verb} into snapshots(user_id, day, taken_at, followers, following, statuses, listed, verified, blue_verified) "
        "values(?,?,?,?,?,?,?,?,?)",
        (user_id, day_of(taken_at), taken_at.isoformat(), followers, _int(record.get("following_count")),
         _int(record.get("statuses_count")), _int(record.get("listed_count")), int(bool(record.get("verified"))),
         int(bool(record.get("blue_verified")))))
    return cur.rowcount > 0


def due_ids(conn: sqlite3.Connection, day: str) -> List[str]:
    """Active accounts with no snapshot for this UTC day, oldest candidates first."""
    rows = conn.execute(
        "select user_id from candidates c where active = 1 and not exists "
        "(select 1 from snapshots s where s.user_id = c.user_id and s.day = ?) order by first_seen_at, user_id",
        (day,)).fetchall()
    return [r["user_id"] for r in rows]


def stale_accounts(conn: sqlite3.Connection, day: str, min_age_days: int, limit: int) -> List[Tuple[str, str]]:
    """(user_id, username) of active accounts whose newest snapshot is at least `min_age_days` old, never-seen first."""
    cutoff = day_of(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=min_age_days))
    rows = conn.execute(
        "select c.user_id, c.username, max(s.day) as last_day from candidates c "
        "left join snapshots s on s.user_id = c.user_id where c.active = 1 group by c.user_id "
        "having last_day is null or last_day <= ? order by last_day is not null, last_day, c.first_seen_at limit ?",
        (cutoff, limit)).fetchall()
    return [(r["user_id"], r["username"]) for r in rows]


def note_lookup(conn: sqlite3.Connection, requested: Iterable[str], returned: Iterable[str]) -> int:
    """After a lookup that succeeded: ids that did not come back (deleted, suspended) accumulate misses."""
    got = {str(i) for i in returned}
    gone = 0
    for user_id in requested:
        if str(user_id) in got:
            continue
        conn.execute("update candidates set miss_count = miss_count + 1, "
                     "active = case when miss_count + 1 >= ? then 0 else active end where user_id = ?",
                     (MISS_LIMIT, str(user_id)))
        gone += 1
    return gone


def growth_rows(conn: sqlite3.Connection, days: int, min_followers: int = 0,
                today: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Follower change between each account's latest snapshot and its newest snapshot at least `days` older."""
    latest_day = day_of(today or now_utc())
    cutoff = day_of((today or now_utc()) - timedelta(days=days))
    out: List[Dict[str, Any]] = []
    for row in conn.execute("select * from candidates where active = 1"):
        last = conn.execute("select * from snapshots where user_id = ? and day <= ? order by day desc limit 1",
                            (row["user_id"], latest_day)).fetchone()
        first = conn.execute("select * from snapshots where user_id = ? and day <= ? order by day desc limit 1",
                             (row["user_id"], cutoff)).fetchone()
        if not last or not first or first["day"] >= last["day"] or (first["followers"] or 0) < max(1, min_followers):
            continue
        change = last["followers"] - first["followers"]
        out.append({
            "username": row["username"], "name": row["name"], "kind_guess": row["kind_guess"],
            "followers": last["followers"], "from_day": first["day"], "to_day": last["day"],
            "change": change, "pct": 100.0 * change / first["followers"],
        })
    out.sort(key=lambda r: r["pct"], reverse=True)
    return out


def status(conn: sqlite3.Connection) -> Dict[str, Any]:
    one = lambda sql, *a: conn.execute(sql, a).fetchone()[0]  # noqa: E731
    return {
        "candidates": one("select count(*) from candidates"),
        "active": one("select count(*) from candidates where active = 1"),
        "by_kind": {r["k"]: r["n"] for r in conn.execute(
            "select coalesce(kind_guess,'?') k, count(*) n from candidates group by 1 order by 2 desc")},
        "pending": {r["state"]: r["n"] for r in conn.execute("select state, count(*) n from pending group by 1")},
        "last_search_seed": get_meta(conn, "last_search_seed"),
        "snapshot_days": {r["day"]: r["n"] for r in conn.execute(
            "select day, count(*) n from snapshots group by 1 order by 1 desc limit 7")},
    }
