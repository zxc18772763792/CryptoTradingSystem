"""Static asset version registry for cache-busting."""
from __future__ import annotations

from typing import Final


ASSET_VERSIONS: Final[dict[str, int]] = {
    "css/style.css": 119,
    "js/app.js": 143,
    "js/altcoin_radar.js": 26,
    "js/research_workbench.js": 12,
    "js/ai_research.js": 51,
    "js/ai_research_diagnostics.js": 6,
    "js/ai_research_runtime.js": 8,
    "js/ai_research_agent.js": 14,
    "js/news_tab_runtime.js": 20,
    "js/dashboard_unstructured_news.js": 17,
}


def static_asset_url(asset_path: str) -> str:
    normalized = str(asset_path or "").strip().lstrip("/")
    if not normalized:
        raise ValueError("asset_path must not be empty")
    base_url = f"/static/{normalized}"
    version = ASSET_VERSIONS.get(normalized)
    if version is None:
        return base_url
    return f"{base_url}?v={version}"
