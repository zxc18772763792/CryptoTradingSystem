from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_data_download_batch_controls_and_hooks_exist():
    template = _read("web/templates/index.html")
    app_js = _read("web/static/js/app.js")

    assert 'id="download-start-date"' in template
    assert 'id="download-end-date"' in template
    assert 'id="download-symbols-batch"' in template
    assert 'id="btn-download-fill-research"' in template
    assert 'id="btn-download-refresh-research"' in template
    assert 'id="btn-download-clear-batch"' in template
    assert 'id="download-output"' in template
    assert 'id="download-research-refresh-status"' in template
    assert "默认追平 1m / 5m / 15m" in template
    assert "批量币种（可选）" in template
    assert "填入研究币池" in template

    assert "async function pollBatchDownloadTasks" in app_js
    assert "async function loadResearchUniverseRefreshStatus" in app_js
    assert "async function triggerResearchUniverseRefresh" in app_js
    assert "function getDownloadDateRange()" in app_js
    assert "function getDownloadRequestedDays(" in app_js
    assert "download-symbols-batch" in app_js
    assert "btn-download-fill-research" in app_js
    assert "btn-download-refresh-research" in app_js
    assert "btn-download-clear-batch" in app_js
    assert "download-days" in app_js
    assert "/data/download/batch" in app_js
    assert "/data/research/refresh/start" in app_js
    assert "/data/research/refresh/status" in app_js
    assert "/data/download/tasks?task_ids=" in app_js
    assert "按钮说明: 刷新体检=重扫并重新生成问题清单" in app_js
    assert "后台会按低并发顺序执行" in template
