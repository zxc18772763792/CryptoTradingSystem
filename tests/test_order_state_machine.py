from core.execution.order_state_machine import (
    OrderLifecycleState,
    OrderStateMachine,
)


def test_submit_ack_partial_fill_final_and_no_regression():
    osm = OrderStateMachine()
    osm.on_submit(
        order_id="local-1",
        client_order_id="cli-1",
        symbol="BTC/USDT",
        side="buy",
        order_type="limit",
        qty=1.0,
    )

    snap = osm.on_exchange_ack(order_id="local-1", exchange_order_id="ex-1", status="new")
    assert snap is not None
    assert snap.state == OrderLifecycleState.ACKED
    assert snap.exchange_order_id == "ex-1"

    snap = osm.apply_update(order_id=None, client_order_id="cli-1", status="partially_filled", filled_qty=0.4, avg_price=100.0)
    assert snap is not None
    assert snap.state == OrderLifecycleState.PARTIALLY_FILLED
    assert snap.filled_qty == 0.4
    assert snap.remaining_qty == 0.6

    # Out-of-order smaller fill should not regress filled quantity.
    snap = osm.apply_update(order_id="local-1", status="partially_filled", filled_qty=0.2, avg_price=99.0)
    assert snap.filled_qty == 0.4
    assert snap.state == OrderLifecycleState.PARTIALLY_FILLED

    snap = osm.apply_update(order_id=None, exchange_order_id="ex-1", status="filled", filled_qty=1.0, avg_price=101.0)
    assert snap.state == OrderLifecycleState.FILLED
    assert snap.filled_qty == 1.0
    assert snap.remaining_qty == 0.0

    # Duplicate / regressive event should not downgrade terminal state.
    snap = osm.apply_update(order_id="local-1", status="open", filled_qty=1.0)
    assert snap.state == OrderLifecycleState.FILLED


def test_lookup_by_aliases_and_all_open():
    osm = OrderStateMachine()
    osm.on_submit("o1", "ETH/USDT", "sell", "market", 2, client_order_id="c1")
    osm.on_submit("o2", "SOL/USDT", "buy", "limit", 3, client_order_id="c2")
    osm.on_exchange_ack(order_id="o1", exchange_order_id="e1", status="open")
    osm.apply_update(order_id="o1", status="canceled")

    s1 = osm.snapshot(exchange_order_id="e1")
    assert s1 is not None and s1.state == OrderLifecycleState.CANCELED

    open_ids = {s.order_id for s in osm.all_open()}
    assert "o1" not in open_ids
    assert "o2" in open_ids


def test_export_restore_roundtrip():
    osm = OrderStateMachine()
    osm.on_submit("o1", "BTC/USDT", "buy", "limit", 1, client_order_id="c1", reduce_only=False)
    osm.on_exchange_ack(order_id="o1", exchange_order_id="e1", status="new")
    osm.apply_update(order_id="o1", status="partially_filled", filled_qty=0.5, avg_price=123.4)

    payload = osm.export_state()
    restored = OrderStateMachine.from_export(payload)

    snap = restored.snapshot(order_id="o1")
    assert snap is not None
    assert snap.exchange_order_id == "e1"
    assert snap.filled_qty == 0.5
    assert snap.state == OrderLifecycleState.PARTIALLY_FILLED
    assert len(snap.history) >= 2


def test_drop_terminal_orders():
    osm = OrderStateMachine()
    osm.on_submit("o1", "BTC/USDT", "buy", "limit", 1)
    osm.on_submit("o2", "BTC/USDT", "buy", "limit", 1)
    osm.apply_update(order_id="o1", status="rejected")
    assert osm.drop_terminal() == 1
    assert osm.snapshot(order_id="o1") is None
    assert osm.snapshot(order_id="o2") is not None

