from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def _iter_python_files(*rel_roots: str) -> list[Path]:
    files: list[Path] = []
    for rel_root in rel_roots:
        root = REPO_ROOT / rel_root
        if root.is_file() and root.suffix == ".py":
            files.append(root)
        elif root.is_dir():
            files.extend(path for path in root.rglob("*.py") if path.is_file())
    return sorted(files)


def test_order_book_ws_skeleton_is_not_used_by_runtime_authority_paths():
    """Depth/book-ticker WS skeletons must not enter execution authority paths."""
    forbidden = (
        "BinancePerpWSClient",
        "core.marketdata.ws_client",
        "core.marketdata.binance_perp_ws_client",
        "subscribe_depth",
        "subscribe_book_ticker",
        "subscribe_agg_trade",
        "subscribe_mark_price",
    )
    checked_files = _iter_python_files(
        "core/trading",
        "core/execution",
        "core/exchange_adapters",
        "strategies",
        "web/main.py",
    )

    offenders: list[str] = []
    for path in checked_files:
        text = path.read_text(encoding="utf-8-sig")
        rel = path.relative_to(REPO_ROOT).as_posix()
        for marker in forbidden:
            if marker in text:
                offenders.append(f"{rel}: {marker}")

    assert offenders == []


def test_user_data_ws_is_not_present_in_account_order_authority_paths():
    """Private/user-data WS must remain out of account, order, and position authority."""
    forbidden = (
        "listenKey",
        "listen_key",
        "userData",
        "user_data",
        "watch_orders",
        "watchOrders",
        "watch_balance",
        "watchBalance",
        "watch_positions",
        "watchPositions",
        "watch_my_trades",
        "watchMyTrades",
        "private_ws",
        "user stream",
        "user-stream",
    )
    checked_files = _iter_python_files(
        "core/trading",
        "core/execution",
        "core/exchange_adapters",
        "core/exchanges",
        "web/api/trading.py",
        "web/api/strategies.py",
    )

    offenders: list[str] = []
    for path in checked_files:
        text = path.read_text(encoding="utf-8-sig")
        rel = path.relative_to(REPO_ROOT).as_posix()
        for marker in forbidden:
            if marker in text:
                offenders.append(f"{rel}: {marker}")

    assert offenders == []

