"""The AI research page: rejected candidates stay rejected, and the desk reads ledgers first."""
import asyncio
import json
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from core.research import orchestrator
from web.api import ai_research as ai_module

ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8-sig")


def _candidate(promotion="reject", validation="reject", status="new", stamp=None):
    return SimpleNamespace(
        candidate_id="cand-1",
        proposal_id="proposal-1",
        status=status,
        metadata={},
        created_at=stamp,
        promotion=SimpleNamespace(decision=promotion, constraints={}),
        validation_summary=SimpleNamespace(decision=validation),
    )


def _patch_promotion(monkeypatch, candidate):
    promote = AsyncMock(return_value={"candidate": candidate, "proposal": SimpleNamespace(), "promotion": candidate.promotion})
    monkeypatch.setattr(orchestrator, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(orchestrator.settings, "GOVERNANCE_ENABLED", False, raising=False)
    monkeypatch.setattr(orchestrator, "get_candidate", lambda app, cid: candidate)
    monkeypatch.setattr(orchestrator, "get_proposal", lambda app, pid: SimpleNamespace())
    monkeypatch.setattr(orchestrator, "promote_candidate", promote)
    monkeypatch.setattr(orchestrator, "save_proposal", lambda app, proposal: None)
    monkeypatch.setattr(orchestrator, "_refresh_runtime_eligibility_snapshot_safe_compat", lambda **kwargs: None)
    return promote


@pytest.mark.parametrize("target", ["paper", "shadow", "live_candidate"])
def test_manual_target_cannot_overwrite_a_rejected_verdict(monkeypatch, target):
    candidate = _candidate()
    promote = _patch_promotion(monkeypatch, candidate)
    app = SimpleNamespace(state=SimpleNamespace(ai_candidate_registry=SimpleNamespace(save=lambda row: None)))

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(orchestrator.promote_existing_candidate(app, candidate_id="cand-1", actor="test", target=target))

    assert exc_info.value.status_code == 400
    assert candidate.promotion.decision == "reject"
    assert promote.await_count == 0


def test_validated_candidate_can_still_go_to_paper(monkeypatch):
    candidate = _candidate(promotion="paper", validation="paper")
    promote = _patch_promotion(monkeypatch, candidate)
    app = SimpleNamespace(state=SimpleNamespace(ai_candidate_registry=SimpleNamespace(save=lambda row: None)))

    asyncio.run(orchestrator.promote_existing_candidate(app, candidate_id="cand-1", actor="test", target="paper"))

    assert promote.await_count == 1
    assert candidate.promotion.decision == "paper"


def test_validation_verdict_alone_marks_a_candidate_rejected():
    assert orchestrator.candidate_failed_validation(_candidate(promotion="paper", validation="reject"))
    assert not orchestrator.candidate_failed_validation(_candidate(promotion="paper", validation="paper"))
    assert not orchestrator.candidate_failed_validation(SimpleNamespace(promotion=None, validation_summary=None))


@pytest.mark.parametrize(
    "marks",
    [
        {"metadata": {"reserve_eligible": True}},
        {"metadata": {"outcome_type": "borderline"}},
        {"validation_summary": SimpleNamespace(decision="reject", reserve_eligible=True)},
        {"validation_summary": SimpleNamespace(decision="reject", outcome_type="redundant_correlated")},
    ],
)
def test_reserve_candidates_stay_open_to_a_manual_decision(marks):
    candidate = _candidate()
    for field, value in marks.items():
        setattr(candidate, field, value)
    assert not orchestrator.candidate_failed_validation(candidate)


def test_archive_summary_counts_without_comparing_naive_and_aware_times(monkeypatch):
    aware = datetime(2026, 9, 26, 5, tzinfo=timezone.utc)
    naive = datetime(2026, 9, 27, 5)
    candidates = [
        _candidate(stamp=aware),
        _candidate(promotion="paper", validation="paper", stamp=naive),
        _candidate(promotion="paper", validation="paper", status="paper_running", stamp=aware),
        _candidate(promotion=None, validation=None, stamp=aware),
    ]
    proposals = [SimpleNamespace(updated_at=aware, created_at=aware)]
    state = SimpleNamespace(
        ai_proposal_registry=SimpleNamespace(list=lambda limit=None: proposals),
        ai_candidate_registry=SimpleNamespace(list=lambda limit=None: candidates),
    )
    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)

    summary = asyncio.run(ai_module.get_proposal_archive_summary(SimpleNamespace(app=SimpleNamespace(state=state))))

    assert summary["proposals"] == 1
    assert summary["candidates"] == 4
    assert summary["passed"] == 2
    assert summary["awaiting_decision"] == 1
    assert summary["last_updated_at"].startswith("2026-09-27T05:00:00")


def test_page_leads_with_the_desk_and_keeps_the_archive_closed():
    html = _read("web/templates/index.html")
    research_tab = html.split('<div id="ai-research" class="tab-content">', 1)[1].split('<div id="ai-agent"', 1)[0]

    positions = [research_tab.index(marker) for marker in (
        'id="research-desk-status"',
        'id="research-trackers-tbody"',
        'id="ai-loop-primary"',
        'id="ai-other-loops"',
        'id="ui-paper-ledgers"',
        'id="ai-proposal-archive"',
        'id="ai-flow-console"',
    )]
    assert positions == sorted(positions)
    assert '<details id="ai-proposal-archive" class="ui-disclosure ui-ledger-group" data-ui-disclosure="true">' in research_tab
    # The collapsed archive keeps an entry in the status bar.
    assert 'id="rd-archive-tile" data-desk-action="archive"' in research_tab
    assert "action==='archive'" in _read("web/static/js/research_desk.js")
    # The primary loop action must not live in the paused-loop drawer's headline.
    assert 'id="ai-iteration-start-btn"' not in html
    assert 'id="ai-oneclick-allocation"' not in html
    assert 'one-click' not in research_tab
    for numbered in ("0) AI", "1) 生成", "2) 生成", "3) 运行", "Step 1-2", "第 4 步"):
        assert numbered not in research_tab


def test_trading_side_panels_moved_off_the_research_tab():
    html = _read("web/templates/index.html")
    research_tab = html.split('<div id="ai-research" class="tab-content">', 1)[1].split('<div id="ai-agent"', 1)[0]
    strategies_tab = html.split('<div id="strategies" class="tab-content">', 1)[1].split('<div id="ai-research"', 1)[0]
    agent_tab = html.split('<div id="ai-agent"', 1)[1]

    assert 'id="ai-review-drawer"' not in research_tab and 'id="ai-review-drawer"' in strategies_tab
    assert 'id="ai-signal-mini"' not in research_tab and 'id="ai-signal-mini"' in agent_tab
    assert 'id="research-trackers-tbody"' not in strategies_tab
    assert 'id="btn-open-research-ledgers"' in strategies_tab


def test_one_click_never_deploys_from_the_page():
    ai_js = _read("web/static/js/ai_research.js")
    assert "/oneclick/research-deploy" in ai_js
    assert "/oneclick/deploy-candidate" not in ai_js
    assert "runOneClickResearchDeploy" not in ai_js


def test_closed_drawers_do_not_poll():
    ai_js = _read("web/static/js/ai_research.js")
    workbench = ai_js.split("async function refreshWorkbench(", 1)[1].split("state.refreshWorkbenchPendingRequest", 1)[0]
    assert "if (!isProposalArchiveOpen()) return null;" in workbench

    cockpit = ai_js.split("async function refreshAutonomousResearchCockpit()", 1)[1].split("async function toggleAutonomousResearchLoop", 1)[0]
    assert "refreshListingShortTracker" in cockpit.split("function refreshPaperLedgerPanels()", 1)[1]
    assert "refreshListingShortTracker" not in cockpit.split("function refreshPaperLedgerPanels()", 1)[0]

    diagnostics = _read("web/static/js/ai_research_diagnostics.js")
    assert "document.getElementById('ai-proposal-archive')?.open" in diagnostics
    assert "isAiResearchActive" not in diagnostics


def test_each_ledger_panel_gets_its_own_collapsed_wrapper():
    ui_js = _read("web/static/js/ui_presentation.js")
    desk_js = _read("web/static/js/research_desk.js")
    assert "item.dataset.ledgerPanel = panel.id" in ui_js
    # requestAnimationFrame pauses in background tabs and used to leave panels unwrapped.
    prepare_scheduler = ui_js.split("const observer = new MutationObserver", 1)[1].split("observer.observe", 1)[0]
    assert "requestAnimationFrame(" not in prepare_scheduler
    assert 'details.ui-ledger-item[data-ledger-panel="${panelId}"]' in desk_js


def test_every_record_link_points_at_a_panel_the_page_can_render():
    trackers_js = _read("web/static/js/research_trackers.js")
    ai_js = _read("web/static/js/ai_research.js")
    mapping = trackers_js.split("const recordPanels={", 1)[1].split("};", 1)[0]
    panel_ids = {item.split(":", 1)[1].strip("'") for item in mapping.split(",")}
    assert len(panel_ids) >= 14
    for panel_id in panel_ids:
        assert panel_id in ai_js or panel_id.replace("-tracker", "") in ai_js, panel_id


def _run_desk_logic(script: str):
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")
    prelude = (
        "const fs=require('fs'),vm=require('vm');"
        "globalThis.window=globalThis;"
        f"vm.runInThisContext(fs.readFileSync({json.dumps(str(ROOT / 'web/static/js/research_trackers.js'))},'utf8'));"
        f"vm.runInThisContext(fs.readFileSync({json.dumps(str(ROOT / 'web/static/js/research_desk.js'))},'utf8'));"
    )
    done = subprocess.run(
        [node, "-e", prelude + script], capture_output=True, text=True, encoding="utf-8", timeout=30, check=True
    )
    return json.loads(done.stdout)


def _days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def test_judgment_estimate_extrapolates_the_sample_pace():
    rows = {
        "pace": {"n": 7, "min_n": 20, "started_at": _days_ago(33)},
        "empty": {"n": 0, "min_n": 20, "started_at": _days_ago(33)},
        "full": {"n": 20, "min_n": 20, "started_at": _days_ago(33)},
        "undated": {"n": 3, "min_n": 20},
    }
    result = _run_desk_logic(
        f"const rows={json.dumps(rows)};"
        "const out={};for(const k in rows)out[k]=ResearchTrackers.judgmentEta(rows[k]);"
        "console.log(JSON.stringify(out));"
    )
    assert result["pace"] == {"days": 61, "reached": False}
    assert result["empty"] is None
    assert result["full"] == {"days": 0, "reached": True}
    assert result["undated"] is None


def test_attention_ignores_late_events_and_stopped_ledgers():
    rows = {
        # "late" counts events found after the window. Protocols record and exclude them, so it is not a fault.
        "late_only": {"n": 3, "min_n": 20, "late": 3, "tracker_status_label": "运行中"},
        "stopped": {"n": 3, "min_n": 20, "verdict": "retire", "tracker_status_label": "已停收 · 结算完毕"},
        "retire": {"n": 20, "min_n": 20, "verdict": "retire"},
        "confirmed": {"n": 3, "min_n": 20, "verdict": "confirmed"},
        "full": {"n": 20, "min_n": 20, "verdict": "collecting"},
        "stale": {"n": 3, "min_n": 20, "runner_active": False},
        "broken": {"error": "boom"},
    }
    result = _run_desk_logic(
        f"const rows={json.dumps(rows, ensure_ascii=True)};"
        "const out={};for(const k in rows)out[k]=ResearchTrackers.attention(rows[k]);"
        "console.log(JSON.stringify(out));"
    )
    assert result == {
        "late_only": None,
        "stopped": None,
        "retire": "decide",
        "confirmed": "decide",
        "full": "decide",
        "stale": "check",
        "broken": "check",
    }


def test_desk_summary_counts_ledgers_factors_and_the_archive():
    trackers = [
        {"id": "a", "name": "A", "n": 7, "min_n": 20, "started_at": _days_ago(33)},
        {"id": "b", "name": "B", "n": 5, "min_n": 30, "started_at": _days_ago(40)},
        {"id": "c", "name": "C", "n": 1, "min_n": 30, "tracker_status_label": "已停用", "started_at": _days_ago(40)},
        {"id": "d", "name": "D", "n": 1, "min_n": 30, "runner_active": False},
    ]
    loops = {"v3": {"config": {"enabled": True}, "frozen": [
        {"name": "f1", "status": "forward_observing"},
        {"name": "f2", "status": "forward_passed"},
    ]}}
    result = _run_desk_logic(
        f"console.log(JSON.stringify(ResearchDesk.summarize({json.dumps(trackers)},{json.dumps(loops)},{{awaiting_decision:2}})));"
    )
    assert result["total"] == 4
    assert result["running"] == 3
    assert result["stopped"] == 1
    assert result["frozen"] == 2
    assert result["decide"] == ["因子 f2", "2 个提案候选"]
    assert result["check"] == ["D"]
    assert result["nearest"] == {"name": "A", "days": 61}
