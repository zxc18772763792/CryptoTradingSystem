from core.news.service import worker as worker_module


def test_coinglass_low_budget_mode_slows_news_sources(monkeypatch):
    monkeypatch.setenv("COINGLASS_RATE_LIMIT_PER_MIN", "10")
    monkeypatch.delenv("NEWS_COINGLASS_LOW_BUDGET_MODE", raising=False)
    monkeypatch.delenv("NEWS_INTERVAL_COINGLASS_ARTICLES", raising=False)
    monkeypatch.delenv("NEWS_INTERVAL_COINGLASS_NEWSFLASH", raising=False)

    assert worker_module._source_interval("coinglass_articles") == 300
    assert worker_module._source_interval("coinglass_newsflash") == 600


def test_explicit_interval_override_wins_under_low_budget(monkeypatch):
    monkeypatch.setenv("COINGLASS_RATE_LIMIT_PER_MIN", "10")
    monkeypatch.setenv("NEWS_INTERVAL_COINGLASS_ARTICLES", "120")

    assert worker_module._source_interval("coinglass_articles") == 120
