"""Does the frozen 7-day Coinglass composite work on coins it never saw? (2026-10-12)

WHY THIS EXISTS:
The 10/12 audit found the 131-coin study universe missing 37 of the 100 most-traded Binance USDT
perps (HYPE among them); it was frozen on 10/01 from the coins with 120 days of local 1h bars, not
by size. Coinglass has history for all of them. The user asked for a wider version of the 7-day
composite (docs/COINGLASS_COMBINATION_STUDY_2026-10-11.md). The weights were fitted on the 131
coins only, so coins outside them are an out-of-sample test the earlier work never had.

PRE-REGISTERED before any data of the new coins was downloaded (do not tune after seeing results):
  Wide universe (built once by --universe, frozen in data/research/coinglass_wide/universe.json):
    Binance USDT-margined perpetuals with status TRADING and underlyingType COIN on 2026-10-12,
    listed (onboardDate) before 2026-07-14 (>= 90 days), not pegged (highest / lowest daily close
    over the 30 days > 1.02, which drops stablecoins), ranked by the mean daily quote volume of the
    30 UTC days 2026-09-12 .. 2026-10-11: the top 200.
    Coin name = the perp's base asset without a contract multiplier (1000PEPE -> PEPE, 1MBABYDOGE ->
    BABYDOGE), the convention of the 131 study coins, and Coinglass is asked with that name.
    "known" = the 131 coins of the 10/11 study (data/research/xs_reversal/universe.json);
    "held-out" = the top-200 coins outside them. Neither the weights nor any earlier test saw them.
    The wide universe is known + held-out.
  Data, timing, signals: exactly the 10/11 study (scripts/coinglass_positioning_study.py): the same
    window 2024-01-01 .. 2026-10-10, the same six Coinglass series and Binance 4h klines, entry at
    the D+1 00:00 open; the held-out coins are downloaded the same way into
    data/research/coinglass_wide/raw.
  Score: the frozen 7-day composite of core/research/coinglass_composite_tracker.py (WEIGHTS),
    computed within the cross-section being tested, as a tracker trading it would.
  Tests, 7-day horizon, test half only (days >= 2025-05-22):
    A1 (primary) held-out coins:
      mean daily Spearman IC of the score against the 7-day log return, 90% CI by week-block
      bootstrap; whole-coin permutation p (1000 reps); the 7-day quintile book (rebalanced every
      7 days, averaged over the 7 start days) net of 0.12% per round trip on the replaced names and
      of Binance funding (longs pay, shorts earn).
      Pass: IC > 0 with the CI excluding 0, permutation p < 0.05, and the book net > 0.
    A2 the wide universe: the same statistics and pass rule (not clean: the 131 known coins' test
      half was seen on 10/11; it gives the numbers a wide tracker would be judged against).
  Reported, never the verdict: A1 and A2 on the first half; the sign-equal composite (the 10/11
    secondary variant); entries 4h later.
  Decision, fixed now: a forward paper tracker on the wide universe starts only if A1 passes (the
    rule carries over to coins it never saw). Otherwise the 131-coin tracker carries on alone and
    the wide data stays for research.

    python scripts/coinglass_wide_study.py --universe   # build and freeze the wide universe (Binance only)
    python scripts/coinglass_wide_study.py --fetch      # held-out history + klines + funding (~2h at 5 req/min)
    python scripts/coinglass_wide_study.py              # evaluate
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.research import coinglass_composite_tracker as ct  # noqa: E402
from scripts import coinglass_combination_study as combo  # noqa: E402
from scripts import coinglass_positioning_study as single  # noqa: E402

WIDE_DIR = PROJECT_ROOT / "data" / "research" / "coinglass_wide"
UNIVERSE_PATH = WIDE_DIR / "universe.json"
HELD_OUT_PERPS_PATH = WIDE_DIR / "perp_symbols.json"  # held-out coin -> Binance perp
FUNDING_PATH = WIDE_DIR / "funding_binance.json"
RESULTS_PATH = WIDE_DIR / "results.json"
KNOWN_UNIVERSE = PROJECT_ROOT / "data" / "research" / "xs_reversal" / "universe.json"
KNOWN_PERPS = PROJECT_ROOT / "data" / "research" / "lsr_positioning" / "perp_symbols.json"

TOP_N = 200
LISTED_BEFORE = datetime(2026, 7, 14, tzinfo=timezone.utc)
VOLUME_FIRST_DAY = datetime(2026, 9, 12, tzinfo=timezone.utc)
VOLUME_DAYS = 30
PEG_RATIO = 1.02
HORIZON = 7
ALPHA = 0.05
MULTIPLIER = re.compile(r"^(?:1000000|100000|10000|1000|1M)(?=[A-Z])")
DAY_MS = 86_400_000


def coin_of(base_asset: str) -> str:
    """The coin behind a perp's base asset: 1000PEPE -> PEPE, 1MBABYDOGE -> BABYDOGE, 1INCH stays."""
    return MULTIPLIER.sub("", str(base_asset))


# -------------------------------------------------------------------------------------- universe
def build_universe() -> Dict[str, Any]:
    """The frozen wide universe; built from Binance on the first call and never rewritten."""
    if UNIVERSE_PATH.exists():
        return json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    import httpx

    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env

    ensure_proxy_env(settings)
    known_bases = json.loads(KNOWN_UNIVERSE.read_text(encoding="utf-8"))["bases"]
    known_perps = json.loads(KNOWN_PERPS.read_text(encoding="utf-8"))
    known = {base: known_perps[base] for base in known_bases if base in known_perps}
    start_ms = int(VOLUME_FIRST_DAY.timestamp() * 1000)
    end_ms = start_ms + VOLUME_DAYS * DAY_MS - 1
    candidates: Dict[str, Dict[str, Any]] = {}
    with httpx.Client(timeout=30, trust_env=True) as client:
        info = _get(client, f"{single.FAPI}/fapi/v1/exchangeInfo")
        for s in info["symbols"]:
            if (s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING"
                    or s.get("underlyingType") != "COIN"):
                continue
            if int(s.get("onboardDate") or 0) >= LISTED_BEFORE.timestamp() * 1000:
                continue
            bars = _get(client, f"{single.FAPI}/fapi/v1/klines", {"symbol": s["symbol"], "interval": "1d",
                                                                   "startTime": start_ms, "endTime": end_ms, "limit": VOLUME_DAYS})
            time.sleep(0.2)
            closes = [float(b[4]) for b in bars]
            if len(bars) < VOLUME_DAYS or min(closes) <= 0:
                continue
            candidates[s["symbol"]] = {"coin": coin_of(s["baseAsset"]), "volume": float(np.mean([float(b[7]) for b in bars])),
                                       "pegged": max(closes) / min(closes) <= PEG_RATIO}
    ranked = sorted((p for p, c in candidates.items() if not c["pegged"]), key=lambda p: -candidates[p]["volume"])
    top = ranked[:TOP_N]
    known_perp_set = set(known.values())
    held_out = {candidates[p]["coin"]: p for p in top if p not in known_perp_set and candidates[p]["coin"] not in known}
    universe = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "rule": f"Binance USDT perps TRADING, underlyingType COIN, listed before {LISTED_BEFORE:%Y-%m-%d}, not pegged "
                f"(max/min close > {PEG_RATIO}), top {TOP_N} by mean daily quote volume "
                f"{VOLUME_FIRST_DAY:%Y-%m-%d} + {VOLUME_DAYS} days",
        "known": known,
        "held_out": dict(sorted(held_out.items())),
        "top_rank": {p: i + 1 for i, p in enumerate(top)},
        "volume_usd": {p: round(candidates[p]["volume"]) for p in top},
        "pegged_dropped": sorted(p for p, c in candidates.items() if c["pegged"]),
    }
    WIDE_DIR.mkdir(parents=True, exist_ok=True)
    UNIVERSE_PATH.write_text(json.dumps(universe, ensure_ascii=False, indent=1), encoding="utf-8")
    HELD_OUT_PERPS_PATH.write_text(json.dumps(universe["held_out"], ensure_ascii=False, indent=1), encoding="utf-8")
    return universe


def _get(client, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(6):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
        except Exception:  # noqa: BLE001 - the proxy drops connections now and then
            pass
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Binance kept failing: {url} {params}")


# ----------------------------------------------------------------------------------------- fetch
def fetch(universe: Dict[str, Any]) -> None:
    """Held-out coins through the 10/11 study's own downloader (resumable), then their funding."""
    single.PERP_SYMBOLS = HELD_OUT_PERPS_PATH  # the downloader maps coin -> perp from this file
    asyncio.run(single.fetch_all(sorted(universe["held_out"]), out_dir=WIDE_DIR))
    combo.fetch_binance_funding(sorted(universe["held_out"].values()), single._ms(single.START) - 2 * DAY_MS, path=FUNDING_PATH)


# -------------------------------------------------------------------------------------- evaluate
def wide_frames() -> Dict[str, pd.DataFrame]:
    """Day frames of the known and the held-out coins together (columns = perps)."""
    return single.build_panel({**single.load_cached(), **single.load_cached(WIDE_DIR)})


def wide_panel(frames: Dict[str, pd.DataFrame], entry: str = "open_00") -> Dict[str, pd.DataFrame]:
    return combo.add_features(single.build_signals(frames, entry=entry), frames)


def funding_cache() -> Dict[str, List[List[float]]]:
    cache: Dict[str, List[List[float]]] = {}
    for path in (combo.FUNDING_PATH, FUNDING_PATH):
        if path.exists():
            cache.update(json.loads(path.read_text(encoding="utf-8")))
    return cache


def test_set(panel: Dict[str, pd.DataFrame], perps: List[str], weights: Dict[str, float], days: np.ndarray,
             funding: Dict[str, List[List[float]]], reps: int = single.N_PERM) -> Dict[str, Any]:
    """The 7-day statistics of the composite computed within ``perps`` on ``days``."""
    cols = [p for p in perps if p in panel["fwd_7d"].columns]
    sub = {name: panel[name].reindex(columns=cols) for name in (*ct.WEIGHTS, "fwd_1d", f"fwd_{HORIZON}d")}
    score = combo.composite(sub, weights)[days]
    fwd, fwd_1d = sub[f"fwd_{HORIZON}d"][days], sub["fwd_1d"][days]
    ic, ci, n_days = single._ic_ci(score, fwd)
    perm = single.coin_permutation(score, fwd, reps=reps) if reps else {"p": float("nan")}
    book = combo.funded_books(score, fwd, fwd_1d, combo.daily_funding(funding, fwd_1d.index, cols), HORIZON, combo.COST)
    plain = single.quintile_spread(score, fwd, 1.0, HORIZON)
    coins_per_day = score.notna().sum(axis=1)
    return {"coins": len(cols), "days": n_days, "coins_per_day_median": float(coins_per_day[coins_per_day >= combo.MIN_COINS].median()),
            "ic": ic, "ci": ci, "perm_p": perm["p"], "gross_per_day": plain["gross_per_day"], "net_ex_funding_per_day": plain["net_per_day"],
            "net_per_day": book["plain_net"], "net_per_book": book["plain_net"] * HORIZON, "funding_per_day": -book["plain_funding"],
            "long_leg_per_day": book["long_leg"], "short_leg_per_day": book["short_leg"], "by_quarter": book["plain_by_quarter"],
            "passes": bool(ic > 0 and ci[0] > 0 and perm["p"] < ALPHA and book["plain_net"] > 0)}


def evaluate(universe: Dict[str, Any]) -> Dict[str, Any]:
    frames = wide_frames()
    panel = wide_panel(frames)
    funding = funding_cache()
    held = sorted(universe["held_out"].values())
    wide = sorted(set(universe["known"].values()) | set(held))
    index = panel["fwd_1d"].index
    test, first = np.asarray(index >= single.SPLIT), np.asarray(index < single.SPLIT)
    signs = {k: float(np.sign(v)) for k, v in ct.WEIGHTS.items()}
    out: Dict[str, Any] = {
        "held_out_coins_with_data": sum(p in panel["fwd_7d"].columns and panel["fwd_7d"][p].notna().any() for p in held),
        "A1": test_set(panel, held, ct.WEIGHTS, test, funding),
        "A2": test_set(panel, wide, ct.WEIGHTS, test, funding),
        "secondary": {
            "A1_first_half": test_set(panel, held, ct.WEIGHTS, first, funding, reps=0),
            "A2_first_half": test_set(panel, wide, ct.WEIGHTS, first, funding, reps=0),
            "A1_sign_equal": test_set(panel, held, signs, test, funding, reps=0),
            "A2_sign_equal": test_set(panel, wide, signs, test, funding, reps=0),
        },
    }
    late = wide_panel(frames, entry="open_04")
    out["secondary"]["A1_entry_4h_later"] = test_set(late, held, ct.WEIGHTS, test, funding, reps=0)
    out["secondary"]["A2_entry_4h_later"] = test_set(late, wide, ct.WEIGHTS, test, funding, reps=0)
    out["decision"] = "start the wide forward tracker" if out["A1"]["passes"] else "no wide tracker: A1 did not pass"
    return out


def _line(name: str, r: Dict[str, Any]) -> str:
    return (f"  {name:20s} coins {r['coins']:3d} (median {r['coins_per_day_median']:.0f}/day) days {r['days']:3d} "
            f"IC {r['ic']:+.4f} [{r['ci'][0]:+.4f}, {r['ci'][1]:+.4f}] p {r['perm_p']:.3f} | "
            f"gross {r['gross_per_day'] * 100:+.3f}%/d net {r['net_per_day'] * 100:+.3f}%/d "
            f"({r['net_per_book'] * 100:+.2f}%/book, funding {r['funding_per_day'] * 100:+.3f}%/d) "
            f"legs {r['long_leg_per_day'] * 100:+.3f}/{r['short_leg_per_day'] * 100:+.3f} | {'PASS' if r['passes'] else 'fail'}")


def report(out: Dict[str, Any]) -> str:
    lines = [f"held-out coins with data: {out['held_out_coins_with_data']}",
             "pre-registered (test half, 7-day, frozen weights):", _line("A1 held-out", out["A1"]), _line("A2 wide", out["A2"]),
             "secondary (reported, not the verdict):"]
    lines += [_line(k, v) for k, v in out["secondary"].items()]
    lines.append("quarters A1: " + ", ".join(f"{q} {v * 100:+.3f}%" for q, v in out["A1"]["by_quarter"].items()))
    lines.append("quarters A2: " + ", ".join(f"{q} {v * 100:+.3f}%" for q, v in out["A2"]["by_quarter"].items()))
    lines.append(f"decision: {out['decision']}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--universe", action="store_true", help="build and freeze the wide universe")
    parser.add_argument("--fetch", action="store_true", help="download the held-out coins (resumable)")
    args = parser.parse_args(argv)
    universe = build_universe()
    if args.universe:
        print(f"known {len(universe['known'])}, held-out {len(universe['held_out'])}: {', '.join(universe['held_out'])}")
        return 0
    if args.fetch:
        fetch(universe)
        return 0
    out = evaluate(universe)
    RESULTS_PATH.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    print(report(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
