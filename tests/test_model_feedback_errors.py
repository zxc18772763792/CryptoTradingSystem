from __future__ import annotations

from core.ai.model_feedback_errors import describe_model_feedback_issue


def test_describe_model_feedback_issue_distinguishes_auth_error():
    issue = describe_model_feedback_issue(
        'model_error:codex_http_401:{"code":"INVALID_API_KEY","message":"Invalid API key"}'
    )

    assert issue["kind"] == "auth_error"
    assert issue["code"] == "model_auth_failed"
    assert issue["http_status"] == 401


def test_describe_model_feedback_issue_distinguishes_unsupported_model():
    issue = describe_model_feedback_issue(
        'model_error:codex_chat_http_400:{"error":{"message":"Param Incorrect","param":"Not supported model gpt-5.4"}}'
    )

    assert issue["kind"] == "unsupported_model"
    assert issue["code"] == "model_unsupported"
    assert issue["http_status"] == 400


def test_describe_model_feedback_issue_distinguishes_live_policy_restriction():
    issue = describe_model_feedback_issue(
        "model_error:codex_live_trading_not_permitted:live trading is not permitted"
    )

    assert issue["kind"] == "policy_restricted"
    assert issue["code"] == "model_policy_restricted"
    assert "实盘交易" in issue["label"]
