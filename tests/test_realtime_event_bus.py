import asyncio

from core.realtime.event_bus import RealtimeEventBus


def test_publish_keeps_full_subscriber_and_drops_oldest():
    async def run():
        bus = RealtimeEventBus()
        queue = await bus.subscribe(maxsize=1)

        await bus.publish("first", {"seq": 1})
        await bus.publish("second", {"seq": 2})

        assert bus.subscriber_count() == 1
        event = queue.get_nowait()
        assert event["event"] == "second"
        assert event["payload"] == {"seq": 2}

    asyncio.run(run())


def test_publish_nowait_safe_swallows_publish_errors(monkeypatch):
    bus = RealtimeEventBus()

    async def boom(*args, **kwargs):
        raise RuntimeError("publish failed")

    monkeypatch.setattr(bus, "publish", boom)
    asyncio.run(bus.publish_nowait_safe("event", {}))
