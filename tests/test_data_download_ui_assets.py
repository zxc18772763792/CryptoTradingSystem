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
    assert 'id="btn-download-clear-batch"' in template
    assert 'id="download-output"' in template
    assert "批量币种（可选）" in template
    assert "填入研究币池" in template

    assert "async function pollBatchDownloadTasks" in app_js
    assert "function getDownloadDateRange()" in app_js
    assert "function getDownloadRequestedDays(" in app_js
    assert "download-symbols-batch" in app_js
    assert "btn-download-fill-research" in app_js
    assert "btn-download-clear-batch" in app_js
    assert "download-days" in app_js
    assert "/data/download/batch" in app_js
