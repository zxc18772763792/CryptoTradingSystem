import base64
import hashlib
from html.parser import HTMLParser
from pathlib import Path

import pytest
from jinja2 import Environment

from web.asset_versions import static_asset_url


ROOT = Path(__file__).resolve().parents[1]


class ScriptSources(HTMLParser):
    def __init__(self):
        super().__init__()
        self.scripts = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "script" and attributes.get("src"):
            self.scripts.append(attributes)


@pytest.mark.parametrize("page", ["index.html", "news.html"])
def test_chart_library_loads_locally_with_valid_integrity_before_consumers(page):
    source = (ROOT / "web/templates" / page).read_text(encoding="utf-8-sig")
    rendered = Environment(autoescape=True).from_string(source).render(
        static_asset_url=static_asset_url,
    )
    parser = ScriptSources()
    parser.feed(rendered)
    chart_scripts = [s for s in parser.scripts if "plotly" in s["src"]]
    assert len(chart_scripts) == 1
    library = chart_scripts[0]
    assert library["src"].startswith("/static/vendor/")
    path = ROOT / "web" / library["src"].split("?", 1)[0].lstrip("/")
    digest = base64.b64encode(hashlib.sha384(path.read_bytes()).digest()).decode()
    assert library["integrity"] == f"sha384-{digest}"
    assert "async" not in library
    for consumer in parser.scripts[parser.scripts.index(library) + 1:]:
        # A deferred library must finish before deferred (or module) consumers.
        if "defer" in library:
            assert "defer" in consumer or consumer.get("type") == "module"
    assert parser.scripts.index(library) == 0
