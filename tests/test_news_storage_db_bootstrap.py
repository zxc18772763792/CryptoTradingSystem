from __future__ import annotations

import sqlite3
from pathlib import Path

from core.news.storage import db as news_db


def _create_news_schema(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.executescript(
            """
            CREATE TABLE news_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                title TEXT,
                url TEXT,
                content TEXT,
                published_at TEXT,
                fetched_at TEXT,
                lang TEXT,
                content_hash TEXT,
                symbols TEXT,
                payload TEXT
            );
            CREATE TABLE news_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                ts TEXT,
                symbol TEXT,
                event_type TEXT,
                sentiment INTEGER,
                impact_score REAL,
                half_life_min INTEGER,
                evidence TEXT,
                model_source TEXT,
                raw_news_id INTEGER,
                payload TEXT,
                created_at TEXT
            );
            CREATE TABLE news_source_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                cursor_type TEXT,
                cursor_value TEXT,
                updated_at TEXT,
                last_success_at TEXT,
                paused_until TEXT,
                last_error TEXT,
                error_count INTEGER,
                success_count INTEGER,
                failure_count INTEGER
            );
            CREATE TABLE news_llm_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_news_id INTEGER,
                source TEXT,
                status TEXT,
                priority INTEGER,
                attempt_count INTEGER,
                last_error TEXT,
                created_at TEXT,
                updated_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                next_retry_at TEXT,
                last_rate_limit_at TEXT
            );
            """
        )


def test_bootstrap_sqlite_news_history_copies_legacy_rows(tmp_path: Path):
    legacy_db = tmp_path / "crypto_trading.db"
    target_db = tmp_path / "news.db"
    _create_news_schema(legacy_db)
    _create_news_schema(target_db)

    with sqlite3.connect(str(legacy_db)) as conn:
        conn.execute(
            """
            INSERT INTO news_raw
                (id, source, title, url, content, published_at, fetched_at, lang, content_hash, symbols, payload)
            VALUES
                (1, 'rss', 'ETF inflow', 'https://example.com/etf', 'content', '2026-04-06T00:00:00+00:00',
                 '2026-04-06T00:01:00+00:00', 'en', 'hash-1', '{}', '{"provider":"rss"}')
            """
        )
        conn.execute(
            """
            INSERT INTO news_events
                (id, event_id, ts, symbol, event_type, sentiment, impact_score, half_life_min, evidence, model_source, raw_news_id, payload, created_at)
            VALUES
                (1, 'evt-1', '2026-04-06T00:00:00+00:00', 'BTCUSDT', 'etf', 1, 0.9, 180, '{"title":"ETF inflow"}',
                 'llm', 1, '{}', '2026-04-06T00:02:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO news_source_state
                (id, source, cursor_type, cursor_value, updated_at, last_success_at, paused_until, last_error, error_count, success_count, failure_count)
            VALUES
                (1, 'rss', 'ts', '2026-04-06T00:00:00+00:00', '2026-04-06T00:03:00+00:00',
                 '2026-04-06T00:03:00+00:00', NULL, NULL, 0, 1, 0)
            """
        )
        conn.execute(
            """
            INSERT INTO news_llm_tasks
                (id, raw_news_id, source, status, priority, attempt_count, last_error, created_at, updated_at, started_at, finished_at, next_retry_at, last_rate_limit_at)
            VALUES
                (1, 1, 'news', 'done', 10, 1, NULL, '2026-04-06T00:01:00+00:00', '2026-04-06T00:02:00+00:00',
                 '2026-04-06T00:01:10+00:00', '2026-04-06T00:02:00+00:00', NULL, NULL)
            """
        )
        conn.commit()

    result = news_db._bootstrap_sqlite_news_history(target_path=target_db, legacy_path=legacy_db)

    assert result["skipped"] is None
    assert result["copied_total"] == 4
    assert result["copied_rows"]["news_raw"] == 1
    assert result["copied_rows"]["news_events"] == 1
    assert result["copied_rows"]["news_source_state"] == 1
    assert result["copied_rows"]["news_llm_tasks"] == 1

    with sqlite3.connect(str(target_db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM news_raw").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM news_events").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM news_source_state").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM news_llm_tasks").fetchone()[0] == 1
