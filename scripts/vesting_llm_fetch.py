"""Round 6 stage 1 (see docs/LLM_TRADING_RESEARCH_ROUND6_2026-09-27.md): test set = DefiLlama-scheduled tokens on Binance spot whose adapter lists an HTML doc source.

Usage: vesting_llm_fetch.py <adapters tree json>, where the tree comes from
GET api.github.com/repos/Omni-Chain-Protocols/emissions-adapters/git/trees/FelixBruguera-patch-1?recursive=1
(the DefiLlama upstream repo is no longer public). PDFs are never fetched.
"""
import asyncio, json, re, sys
from pathlib import Path
import httpx
sys.path.insert(0, ".")
from core.research import unlock_short_tracker as ut
from core.research.unlock_events import entry_ticker, entry_price

OUT = Path("data/research/vesting_llm"); OUT.mkdir(parents=True, exist_ok=True)
FORK = "https://raw.githubusercontent.com/Omni-Chain-Protocols/emissions-adapters/FelixBruguera-patch-1"
from config.settings import settings
PX = settings.HTTPS_PROXY or None
SKIP_EXT = (".pdf", ".png", ".jpg", ".jpeg", ".docx")
SKIP_HOST = ("etherscan", "arbiscan", "bscscan", "twitter.com", "x.com", "dune.com", "github.com/", "solscan", "explorer", "cryptorank", "tokenunlocks", "token.unlocks", "defillama", "coingecko", "coinmarketcap", "messari", "tokenomist", "binance.com/en/research")

def html_to_text(html):
    from bs4 import BeautifulSoup
    s = BeautifulSoup(html, "html.parser")
    for t in s(["script", "style", "nav", "footer", "svg", "noscript"]):
        t.decompose()
    text = s.get_text("\n")
    return re.sub(r"\n\s*\n+", "\n", text).strip()

async def get_retry(c, url, tries=4):
    for i in range(tries):
        try:
            return await c.get(url)
        except Exception:  # noqa: BLE001
            if i == tries - 1:
                raise
            await asyncio.sleep(2 * (i + 1))

async def adapter_text(c, path):
    cache = OUT / "adapters" / Path(path).name
    if cache.exists():
        return cache.read_text(encoding="utf-8")
    text = (await get_retry(c, f"{FORK}/{path}")).text
    cache.parent.mkdir(exist_ok=True); cache.write_text(text, encoding="utf-8")
    return text

async def main():
    idx = json.loads((ut.CACHE_DIR / "emissions_index.json").read_text(encoding="utf-8"))
    idx = idx["data"] if isinstance(idx, dict) else idx
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as bn:
        prices = {p["symbol"]: float(p["price"]) for p in (await bn.get("https://api.binance.com/api/v3/ticker/price")).json()}
    uni = {}
    for e in idx:
        t, px, slug = entry_ticker(e), entry_price(e), e.get("protocolSlug")
        if t and px > 0 and f"{t}USDT" in prices and abs(prices[f"{t}USDT"] / px - 1) <= ut.PRICE_MATCH_TOLERANCE and (ut.CACHE_DIR / "emissions" / f"{slug}.json").exists():
            uni.setdefault(t, slug)
    print("binance-matched scheduled tokens:", len(uni))
    tree = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    files = {Path(x["path"]).stem.lower(): x["path"] for x in tree["tree"] if x["path"].startswith("protocols/")}
    rows, stats = [], {"no_adapter": 0, "no_html_source": 0, "fetch_fail": 0, "too_short": 0}
    ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36"}
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, proxy=PX, headers=ua) as c:
        for ticker, slug in sorted(uni.items()):
            path = files.get(slug.lower()) or files.get(slug.lower().replace("-", ""))
            if not path:
                stats["no_adapter"] += 1; continue
            try:
                ts = await adapter_text(c, path)
            except Exception:  # noqa: BLE001
                stats["adapter_fail"] = stats.get("adapter_fail", 0) + 1; continue
            m = re.search(r"sources\s*:\s*\[(.*?)\]", ts, re.S)
            srcs = re.findall(r"[\"'`](https?://[^\"'`]+)[\"'`]", m.group(1)) if m else []
            srcs = [s for s in srcs if not s.lower().split("#")[0].split("?")[0].endswith(SKIP_EXT) and not any(h in s.lower() for h in SKIP_HOST)]
            if not srcs:
                stats["no_html_source"] += 1; continue
            texts = []
            for s in srcs[:3]:
                key = OUT / "pages" / (re.sub(r"[^A-Za-z0-9]+", "_", s.split("#")[0])[:150] + ".txt")
                if key.exists():
                    texts.append({"url": s, "text": key.read_text(encoding="utf-8")}); continue
                try:
                    r = await get_retry(c, s.split("#")[0], tries=2)
                    if r.status_code == 200 and "html" in r.headers.get("content-type", ""):
                        texts.append({"url": s, "text": html_to_text(r.text)[:60000]})
                        key.parent.mkdir(exist_ok=True); key.write_text(texts[-1]["text"], encoding="utf-8")
                except Exception as exc:  # noqa: BLE001
                    print("  fetch fail", ticker, s[:80], type(exc).__name__)
            if not texts:
                stats["fetch_fail"] += 1; continue
            if sum(len(t["text"]) for t in texts) < 800:
                stats["too_short"] += 1; continue
            rows.append({"ticker": ticker, "slug": slug, "adapter": path, "sources": texts})
            print(ticker, slug, [len(t["text"]) for t in texts])
    (OUT / "docs.json").write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    print("test set:", len(rows), stats)

asyncio.run(main())
