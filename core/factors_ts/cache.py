"""Backtest-scoped factor cache.

Why this exists
---------------
The backtest loop hands each strategy a trailing window and asks for signals, so
a strategy recomputes its factors from scratch on every bar. Profiling a
MultiFactorHF month (8,636 bars) showed 51,522 ``compute_factor`` calls costing
84s -- 81% of the whole backtest. The cost is NOT proportional to window length
(measured: a 4x larger window costs the same); it is a fixed ~1.6ms of pandas
overhead per call -- object construction, ``to_numeric`` coercion, two full-column
``replace`` passes, ``reindex``, Series construction. So the win comes from making
51,522 calls into a handful, not from touching less data.

Inside a scope the factor is computed once over the *full* frame and each window
is served as a slice of that result.

Safety
------
Slicing a full-series result is only valid for factors whose value at row i
depends on a bounded window ending at i. That holds for ``rolling(n)`` factors but
NOT for path-dependent ones: ``ema_slope`` is an EWM recursion (infinite impulse
response) and OBV / williams_ad / hurst use ``cumsum`` from the series start.
Their windowed and full-series values genuinely differ.

Rather than maintain a denylist across 70 factors in 6 modules -- which would
silently rot as factors are added -- this cache **verifies itself**. On a factor's
first use, and periodically after that, it computes the value both ways and
compares. A factor is only ever served from cache while it keeps matching to
floating-point tolerance; on any mismatch it is rejected for the rest of the scope
and every later call falls through to the original code path. The authoritative
(windowed) value is what gets returned on verification bars, so a rejected factor
never returns a cached number to a caller.

Measured on the six factors MultiFactorHF uses: atr_pct and spread_proxy are
bit-identical, realized_vol / volume_z / zscore_price agree to <=5e-11 relative,
and ema_slope differs by 2.3e-05 -- so ema_slope is rejected automatically by the
check below rather than by being named here.
"""
from __future__ import annotations

import os
import threading
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

# How closely a cached value must match the uncached one for a factor to stay
# cached. Default 0.0 means bit-exact.
#
# This is deliberately strict. Computing a rolling factor over the full series
# rather than a 200-bar window is mathematically the same but not bit-identical:
# pandas accumulates over a different-length array, leaving ~1e-11 of float noise
# in realized_vol / volume_z / zscore_price / ema_slope (atr_pct and spread_proxy
# do come out bit-identical). That noise is harmless right up until a strategy
# compares against a threshold -- `score > enter_th` turns 1e-11 into a different
# trade. Measured on an 8-trial MultiFactorHF sweep over 3,000 bars: a 1e-9
# tolerance gives 2.64x but changes the position on 8 bars; bit-exact gives 1.40x
# and changes nothing.
#
# So exactness is the default and the faster mode is opt-in, for parameter sweeps
# and research runs where the tiny drift is acceptable:
#     FACTOR_CACHE_REL_TOL=1e-9
_REL_TOL = float(os.getenv("FACTOR_CACHE_REL_TOL", "0") or 0.0)

# Re-verify every Nth cached call, so a factor that only diverges later (e.g. once
# its warmup completes) is still caught rather than trusted forever.
_RECHECK_EVERY = 500

_local = threading.local()

# Full-series results survive scope exit, keyed by a fingerprint of the frame.
# An optimize run replays the SAME price frame once per parameter trial, and the
# gate factors (realized_vol, atr_pct, spread_proxy, volume_z) do not depend on
# the strategy parameters being swept -- without this they would be recomputed
# from scratch for every trial. Signal factors whose parameters do vary simply
# land under different keys, so this stays correct without special-casing.
#
# Bounded so a long-lived web process cannot accumulate frames indefinitely.
_STORE_MAX = 512
_store: "OrderedDict[Tuple, pd.Series]" = OrderedDict()
_store_status: "OrderedDict[Tuple, str]" = OrderedDict()
_store_lock = threading.Lock()


def _params_key(params: Optional[Dict[str, Any]]) -> Tuple:
    if not params:
        return ()
    return tuple(sorted((str(k), repr(v)) for k, v in params.items()))


def _fingerprint(df: pd.DataFrame) -> Tuple:
    """Cheap content identity for a price frame.

    Index plus the close column is enough: two frames sharing both are the same
    replay input. Deliberately not id()-based -- the optimize pool ships a fresh
    unpickled copy of the frame to every trial, so object identity would never
    match and the cross-trial reuse this exists for would never happen.
    """
    idx = df.index
    try:
        close_hash = int(pd.util.hash_pandas_object(df["close"], index=False).sum())
    except Exception:
        close_hash = 0
    return (len(df), str(idx[0]), str(idx[-1]), close_hash)


def _store_get(key: Tuple) -> Optional[pd.Series]:
    with _store_lock:
        series = _store.get(key)
        if series is not None:
            _store.move_to_end(key)
        return series


def _store_put(key: Tuple, series: pd.Series) -> None:
    with _store_lock:
        _store[key] = series
        _store.move_to_end(key)
        while len(_store) > _STORE_MAX:
            _store.popitem(last=False)


def clear_factor_cache() -> None:
    """Drop all retained full-series results (used by tests)."""
    with _store_lock:
        _store.clear()
        _store_status.clear()


class _Scope:
    __slots__ = ("full", "fp", "_index", "_pos", "calls",
                 "served", "computed", "reused", "rejected_names")

    def __init__(self, full: pd.DataFrame) -> None:
        self.full = full
        self.fp = _fingerprint(full)
        self._index = full.index
        # Position lookup so we can cheaply confirm a window is a contiguous
        # slice of the full frame rather than some unrelated frame.
        self._pos = {ts: i for i, ts in enumerate(full.index)}
        self.calls: Dict[Tuple, int] = {}
        self.served = 0
        self.computed = 0   # full-series passes done by THIS scope
        self.reused = 0     # full-series results inherited from an earlier scope
        self.rejected_names: set[str] = set()

    def is_slice_of_full(self, window: pd.DataFrame) -> bool:
        idx = window.index
        n = len(idx)
        if n == 0 or n > len(self._index):
            return False
        start = self._pos.get(idx[0])
        if start is None:
            return False
        # Contiguity: first and last must be `n-1` apart in the full frame, and
        # the endpoints must match. Cheap, and enough because the engine always
        # passes contiguous windows.
        end = self._pos.get(idx[-1])
        if end is None or end - start != n - 1:
            return False
        return True


def _values_match(a: pd.Series, b: pd.Series) -> bool:
    """Compare the last value -- the only one a per-bar strategy consumes."""
    if len(a) == 0 or len(b) == 0:
        return len(a) == len(b)
    try:
        x = float(a.iloc[-1])
        y = float(b.iloc[-1])
    except Exception:
        return False
    if np.isnan(x) and np.isnan(y):
        return True
    if np.isnan(x) or np.isnan(y):
        return False
    scale = max(abs(x), abs(y), 1.0)
    return abs(x - y) <= _REL_TOL * scale


@contextmanager
def factor_cache_scope(full: pd.DataFrame) -> Iterator[None]:
    """Enable factor caching for windows taken from ``full``.

    Nested scopes are ignored (the outermost wins) so a nested backtest cannot
    install a scope whose frame does not match the windows being passed.
    """
    if getattr(_local, "scope", None) is not None or full is None or len(full) == 0:
        yield
        return
    scope = _Scope(full)
    _local.scope = scope
    try:
        yield
    finally:
        _local.scope = None
        if scope.served:
            msg = (f"factor cache: served {scope.served} calls from "
                   f"{scope.computed} full-series computations "
                   f"({scope.reused} reused from an earlier run)")
            if scope.rejected_names:
                msg += f"; not cacheable: {sorted(scope.rejected_names)}"
            logger.debug(msg)


def try_cached(
    name: str,
    df: pd.DataFrame,
    params: Optional[Dict[str, Any]],
    compute: Callable[[str, pd.DataFrame, Optional[Dict[str, Any]]], pd.Series],
) -> Optional[pd.Series]:
    """Return a cached factor slice, or None to fall through to normal compute.

    ``compute`` is the uncached implementation; it is called here for the
    full-series pass and for verification.
    """
    scope: Optional[_Scope] = getattr(_local, "scope", None)
    if scope is None:
        return None
    if not scope.is_slice_of_full(df):
        return None

    # Keyed by frame fingerprint so a later replay of the same frame -- the next
    # trial of an optimize sweep -- reuses this work instead of redoing it.
    key = (scope.fp, str(name), _params_key(params))
    state = _store_status.get(key)
    if state == "rejected":
        scope.rejected_names.add(str(name))
        return None

    full_series = _store_get(key)
    if full_series is None:
        try:
            full_series = compute(name, scope.full, params)
        except Exception as exc:
            # A factor that cannot run on the full frame (e.g. needs a column the
            # window has but the frame lacks) is simply not cacheable.
            _store_status[key] = "rejected"
            scope.rejected_names.add(str(name))
            logger.debug(f"factor cache: {name} not cacheable on full frame: {exc}")
            return None
        if not isinstance(full_series, pd.Series):
            _store_status[key] = "rejected"
            scope.rejected_names.add(str(name))
            return None
        _store_put(key, full_series)
        scope.computed += 1
    elif key not in scope.calls:
        scope.reused += 1

    n = scope.calls.get(key, 0)
    scope.calls[key] = n + 1
    must_verify = state is None or (n % _RECHECK_EVERY == 0)

    candidate = full_series.reindex(df.index)

    if must_verify:
        actual = compute(name, df, params)
        if _values_match(candidate, actual):
            _store_status[key] = "verified"
        else:
            _store_status[key] = "rejected"
            scope.rejected_names.add(str(name))
            logger.debug(
                f"factor cache: {name} rejected -- windowed and full-series values "
                f"differ beyond {_REL_TOL:g} relative tolerance (expected for "
                f"EWM/cumsum-style path-dependent factors)"
            )
        # Always hand back the authoritative windowed value on verification bars.
        return actual

    scope.served += 1
    candidate.name = full_series.name
    return candidate


def scope_stats() -> Optional[Dict[str, Any]]:
    scope: Optional[_Scope] = getattr(_local, "scope", None)
    if scope is None:
        return None
    verified = sorted({
        key[1] for key, state in _store_status.items()
        if state == "verified" and key[0] == scope.fp
    })
    return {
        "served": scope.served,
        "full_computations": scope.computed,
        "reused_from_earlier_run": scope.reused,
        "verified": verified,
        "rejected": sorted(scope.rejected_names),
    }
