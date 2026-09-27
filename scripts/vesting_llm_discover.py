"""Round 6 step 2a: find each Binance token's tokenomics pages without DefiLlama's help.

The step-1 test (scripts/vesting_llm_fetch.py) handed the model the exact doc
URL a DefiLlama analyst had used. Tokens DefiLlama does not cover have no
such URL, so this builds the pipeline that would run for them: CoinGecko id
(matched on Binance price) -> homepage / whitepaper links -> docs root ->
the linked pages whose URL or anchor text looks like tokenomics. It runs on
ALL Binance USDT spot tokens, covered ones included, so the same pipeline can
be scored end-to-end against DefiLlama before its output is trusted on the
uncovered ones. Tokenized stocks and stablecoins are skipped; PDFs are never
fetched. Output: data/research/vesting_llm/discovered.json (docs.json format).
"""
import asyncio
import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx

sys.path.insert(0, ".")
from config.settings import settings
from core.research import unlock_short_tracker as ut
from core.research.delist_risk import STABLE_OR_LEVERAGED
from core.research.unlock_events import entry_ticker

OUT = Path("data/research/vesting_llm")
CG = "https://api.coingecko.com/api/v3"
CG_PACE = 6.0  # keyless CoinGecko: a steady pace beats 429 back-offs
COVERED_SAMPLE = 60  # covered tokens also run, to score the pipeline end-to-end
STOCK_NAME = re.compile(r"xstock|tokenized|\(ondo|backed|stock token", re.I)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36"}
# Exact CoinGecko categories of assets that have no vesting schedule of their own. Not "Stablecoin
# Issuer" (AAVE, ENA) or "Liquid Staking" (LDO): those are protocols with their own tokens.
SKIP_CATEGORIES = re.compile(r"^(stablecoins|.*stablecoin|tokenized stocks?( \(by name\))?|tokenized gold|wrapped-tokens|bridged.*tokens?|liquid staking tokens)$", re.I)
DOC_ROOT = re.compile(r"(^|\.)docs?\.|gitbook|/docs?(/|$)|whitepaper|litepaper|/learn(/|$)|/wiki", re.I)
PAGE_SCORE = [(re.compile(p, re.I), w) for p, w in (
    (r"tokenomic", 6), (r"vesting", 6), (r"unlock", 5), (r"allocation", 5), (r"distribution", 4),
    (r"token[-_ ]?(economics|supply|model)", 5), (r"\$?[A-Z]{2,10} token", 2), (r"\btoken\b", 2), (r"supply", 2), (r"emission", 3), (r"airdrop", 1),
)]
BAD_EXT = (".pdf", ".png", ".jpg", ".jpeg", ".svg", ".zip", ".docx", ".mp4")


def html_to_text(html: str) -> str:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "svg", "noscript"]):
        tag.decompose()
    return re.sub(r"\n\s*\n+", "\n", soup.get_text("\n")).strip()


def anchors(html: str, base: str):
    from bs4 import BeautifulSoup

    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        href = urljoin(base, a["href"].strip()).split("#")[0]
        if href.startswith("http") and not href.lower().split("?")[0].endswith(BAD_EXT):
            yield href, " ".join(a.get_text(" ").split())[:80]


def page_score(url: str, text: str) -> int:
    hay = f"{urlparse(url).path} {text}"
    return sum(w for rx, w in PAGE_SCORE if rx.search(hay))


async def get(client, url, tries=2):
    for i in range(tries):
        try:
            return await client.get(url)
        except Exception:  # noqa: BLE001
            if i == tries - 1:
                return None
            await asyncio.sleep(2)


async def cg_json(client, path, params=None):
    for attempt in range(5):
        resp = await get(client, httpx.URL(f"{CG}{path}", params=params or {}))
        if resp is not None and resp.status_code == 200:
            await asyncio.sleep(CG_PACE)
            return resp.json()
        await asyncio.sleep(20 * (attempt + 1) if resp is not None and resp.status_code == 429 else 3)
    return None


async def html_page(client, url, cache_dir, tries=2):
    key = cache_dir / (re.sub(r"[^A-Za-z0-9]+", "_", url)[:150] + ".html")
    miss = key.with_suffix(".miss")
    if key.exists():
        return key.read_text(encoding="utf-8")
    if miss.exists():
        return None
    try:
        resp = await asyncio.wait_for(get(client, url, tries), timeout=45)
    except asyncio.TimeoutError:
        resp = None
    if resp is None or resp.status_code >= 500 or resp.status_code == 429:
        return None  # transient (the proxy drops connections under load): retry on the next run
    if resp.status_code != 200 or "html" not in resp.headers.get("content-type", ""):
        miss.write_text(str(resp.status_code), encoding="utf-8")  # a real answer: do not ask again
        return None
    key.write_text(resp.text, encoding="utf-8")
    return resp.text


async def crawl(client, ticker, links, sem, cache_dir):
    """homepage + whitepaper -> docs roots -> best-scoring tokenomics pages (max 3)."""
    async with sem:
        seeds = [u for u in links if u]
        roots, candidates = [], {}
        for seed in seeds:
            html = await html_page(client, seed, cache_dir)
            if not html:
                continue
            if DOC_ROOT.search(seed):
                roots.append((seed, html))
            for href, text in anchors(html, seed):
                if DOC_ROOT.search(href) and len(roots) < 3 and all(href != r for r, _ in roots):
                    sub = await html_page(client, href, cache_dir)
                    if sub:
                        roots.append((href, sub))
                s = page_score(href, text)
                if s >= 5:
                    candidates[href] = max(candidates.get(href, 0), s)
        if seeds:  # common locations that homepages often fail to link (JS menus)
            host = urlparse(seeds[0]).netloc.removeprefix("www.")
            for guess in (f"https://docs.{host}/", f"https://{host}/tokenomics", f"https://docs.{host}/tokenomics"):
                if guess in candidates or any(guess == r for r, _ in roots):
                    continue
                html = await html_page(client, guess, cache_dir, tries=1)
                if html and len(html_to_text(html)) > 400:
                    if guess.endswith("tokenomics"):
                        candidates[guess] = 8
                    elif len(roots) < 4:
                        roots.append((guess, html))
        for root, html in roots:
            candidates.setdefault(root, 1)
            for href, text in anchors(html, root):
                s = page_score(href, text)
                if s >= 5:
                    candidates[href] = max(candidates.get(href, 0), s + 1)
        best = sorted(candidates.items(), key=lambda kv: -kv[1])[:3]
        sources = []
        for url, score in best:
            html = await html_page(client, url, cache_dir)
            if html:
                text = html_to_text(html)[:60000]
                if len(text) > 400:
                    sources.append({"url": url, "score": score, "text": text})
        return sources


async def main():
    cache_dir = OUT / "discover_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    meta_path = OUT / "cg_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    proxy = settings.HTTPS_PROXY or None
    async with httpx.AsyncClient(timeout=30) as bn:
        info = (await bn.get("https://api.binance.com/api/v3/exchangeInfo")).json()
        prices = {p["symbol"]: float(p["price"]) for p in (await bn.get("https://api.binance.com/api/v3/ticker/price")).json()}
    skip = re.compile(STABLE_OR_LEVERAGED)
    bases = sorted({s["baseAsset"] for s in info["symbols"] if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and not skip.search(s["baseAsset"])})
    idx = json.loads((ut.CACHE_DIR / "emissions_index.json").read_text(encoding="utf-8"))
    idx = idx["data"] if isinstance(idx, dict) else idx
    covered = {entry_ticker(e) for e in idx}

    async with httpx.AsyncClient(timeout=15, follow_redirects=True, proxy=proxy, headers=UA) as c:
        import random

        uncovered = [b for b in bases if b not in covered]
        sample = sorted(random.Random(0).sample(sorted(b for b in bases if b in covered), min(COVERED_SAMPLE, len(bases) - len(uncovered))))
        bases = uncovered + sample
        todo = [b for b in bases if b not in meta]
        if todo:
            list_cache = OUT / "cg_coins_list.json"
            coin_list = json.loads(list_cache.read_text(encoding="utf-8")) if list_cache.exists() else await cg_json(c, "/coins/list")
            if coin_list and not list_cache.exists():
                list_cache.write_text(json.dumps(coin_list), encoding="utf-8")
            by_sym = {}
            for coin in coin_list or []:
                by_sym.setdefault(str(coin.get("symbol") or "").upper(), []).append(coin["id"])
            ids = sorted({i for b in todo for i in by_sym.get(re.sub(r"^1000+", "", b), [])})
            market = {}
            for i in range(0, len(ids), 200):
                for m in await cg_json(c, "/coins/markets", {"vs_currency": "usd", "ids": ",".join(ids[i:i + 200]), "per_page": 250}) or []:
                    market[m["id"]] = m
            for b in todo:
                px = prices.get(f"{b}USDT") or 0
                scale = 1000 if b.startswith("1000") else 1
                cands = [market[i] for i in by_sym.get(re.sub(r"^1000+", "", b), []) if i in market and market[i].get("current_price")]
                cands = [m for m in cands if px and abs(m["current_price"] * scale / px - 1) < 0.1]
                if not cands:
                    meta[b] = {"cg_id": None}
                    continue
                best = max(cands, key=lambda m: m.get("market_cap") or 0)
                if STOCK_NAME.search(best.get("name") or ""):
                    meta[b] = {"cg_id": best["id"], "categories": ["Tokenized Stock (by name)"], "homepage": [], "whitepaper": None}
                    continue
                detail = await cg_json(c, f"/coins/{best['id']}", {"localization": "false", "tickers": "false", "market_data": "false",
                                                                    "community_data": "false", "developer_data": "false"}) or {}
                links = detail.get("links") or {}
                meta[b] = {"cg_id": best["id"], "categories": detail.get("categories") or [],
                           "homepage": [u for u in links.get("homepage") or [] if u][:2],
                           "whitepaper": links.get("whitepaper") or None}
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
                if len(meta) % 25 == 0:
                    print(f"  coingecko details: {len(meta)} tokens", flush=True)
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")

        stats = {"no_cg_match": 0, "skipped_category": 0, "no_pages": 0}
        targets = []
        for b in bases:
            m = meta.get(b) or {}
            if not m.get("cg_id"):
                stats["no_cg_match"] += 1
            elif any(SKIP_CATEGORIES.search(cat or "") for cat in m.get("categories") or []):
                stats["skipped_category"] += 1
            else:
                targets.append(b)
        sem = asyncio.Semaphore(4)
        done_count = [0]

        async def bounded(b):
            async with sem:  # the time limit starts once this token's turn comes
                try:
                    return await asyncio.wait_for(crawl(c, b, meta[b]["homepage"] + [meta[b]["whitepaper"]], asyncio.Semaphore(1), cache_dir), timeout=300)
                except Exception:  # noqa: BLE001 - includes the per-token time limit
                    return []
                finally:
                    done_count[0] += 1
                    if done_count[0] % 25 == 0:
                        print(f"  crawled {done_count[0]}/{len(targets)}", flush=True)

        results = await asyncio.gather(*(bounded(b) for b in targets))
    rows = []
    for b, sources in zip(targets, results):
        if not sources:
            stats["no_pages"] += 1
            continue
        rows.append({"ticker": b, "slug": meta[b]["cg_id"], "covered": b in covered, "sources": sources})
    (OUT / "discovered.json").write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    print(f"bases {len(bases)} | crawled {len(targets)} | with candidate pages {len(rows)} "
          f"(covered {sum(r['covered'] for r in rows)}, uncovered {sum(not r['covered'] for r in rows)}) | {stats}")


asyncio.run(main())
