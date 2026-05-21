from __future__ import annotations

import asyncio

from core.news.service import worker as worker_module


def test_ensure_event_processor_reuses_running_task(monkeypatch):
    async def _run():
        original_task = asyncio.current_task()

        async def fake_loop():
            await asyncio.sleep(10)

        monkeypatch.setattr(worker_module, "_event_processor_task", None)
        monkeypatch.setattr(worker_module, "_event_processor_loop", fake_loop)

        await worker_module._ensure_event_processor()
        first_task = worker_module._event_processor_task
        assert first_task is not None
        assert first_task is not original_task

        await worker_module._ensure_event_processor()
        assert worker_module._event_processor_task is first_task

        first_task.cancel()
        try:
            await first_task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_event_processor_restarts_after_task_done(monkeypatch):
    async def _run():
        created = []

        async def fake_loop():
            return None

        monkeypatch.setattr(worker_module, "_event_processor_task", None)
        monkeypatch.setattr(worker_module, "_event_processor_loop", fake_loop)

        await worker_module._ensure_event_processor()
        first_task = worker_module._event_processor_task
        created.append(first_task)
        await first_task
        assert first_task.done()

        await worker_module._ensure_event_processor()
        second_task = worker_module._event_processor_task
        created.append(second_task)
        await second_task

        assert len(created) == 2
        assert second_task is not first_task

    asyncio.run(_run())


def test_process_event_batch_reloads_config_and_uses_task_queue(monkeypatch):
    async def _run():
        cfgs = [{"llm": {"timeout_sec": 11}}, {"llm": {"timeout_sec": 90}}]
        seen = []
        load_count = 0

        def fake_load_service_config():
            nonlocal load_count
            cfg = cfgs[load_count]
            load_count += 1
            seen.append(("load", cfg["llm"]["timeout_sec"]))
            return cfg

        async def fake_enqueue(news_items, min_importance=35):
            seen.append(("enqueue", [item["id"] for item in news_items], min_importance))
            return {"queued_count": len(news_items), "requeued_count": 0, "skipped_count": 0}

        async def fake_process_llm_batch(cfg, limit=8):
            seen.append(("process", cfg["llm"]["timeout_sec"], limit))
            return {"claimed": limit, "events_count": 0, "llm_used": False, "errors": []}

        async def unexpected_task_batches(*_args, **_kwargs):
            raise AssertionError("event path must not bypass task queue")

        monkeypatch.setattr(worker_module, "load_service_config", fake_load_service_config)
        monkeypatch.setattr(worker_module.news_db, "enqueue_llm_tasks", fake_enqueue)
        monkeypatch.setattr(worker_module, "process_llm_batch", fake_process_llm_batch)
        monkeypatch.setattr(worker_module, "_process_llm_task_batches", unexpected_task_batches)
        monkeypatch.setenv("NEWS_LLM_MIN_IMPORTANCE", "41")

        batch = [{"id": 1, "source": "rss", "payload": {"importance_score": 80}}]
        await worker_module._process_event_batch(batch)
        await worker_module._process_event_batch(batch)

        assert seen == [
            ("load", 11),
            ("enqueue", [1], 41),
            ("process", 11, 1),
            ("load", 90),
            ("enqueue", [1], 41),
            ("process", 90, 1),
        ]

    asyncio.run(_run())
