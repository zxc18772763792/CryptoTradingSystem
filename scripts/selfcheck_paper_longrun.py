from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import requests


DEFAULT_BASE_URL = os.getenv("PAPER_LONGRUN_BASE_URL") or os.getenv("WEB_BASE_URL") or os.getenv("OPS_BASE_URL") or "http://127.0.0.1:8000"
DEFAULT_RUN_ONCE_PATH = os.getenv("PAPER_LONGRUN_RUN_ONCE_PATH") or "/ops/news/worker_run_once"


def _join_url(base_url: str, path: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    suffix = "/" + str(path or "").strip().lstrip("/")
    return f"{base}{suffix}"


def _safe_json(response: requests.Response) -> Dict[str, Any]:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {"value": payload}
    except Exception:
        return {"raw": response.text}


def _safe_get(data: Dict[str, Any] | None, *path: str, default: Any = None) -> Any:
    current: Any = data or {}
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _request_json(
    base_url: str,
    token: str,
    method: str,
    path: str,
    timeout: float,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    url = _join_url(base_url, path)
    headers = {
        "X-OPS-CALLER": "selfcheck_paper_longrun",
    }
    if token:
        headers["X-OPS-TOKEN"] = token

    response = requests.request(method.upper(), url, headers=headers, json=payload, timeout=timeout)
    body = _safe_json(response)
    return {
        "method": method.upper(),
        "path": path,
        "url": url,
        "status_code": int(response.status_code),
        "body": body,
    }


def _make_check_result(
    name: str,
    response: Dict[str, Any],
    ok: bool,
    summary: Dict[str, Any] | None = None,
    error: str | None = None,
) -> Dict[str, Any]:
    result = {
        "name": name,
        "method": response["method"],
        "path": response["path"],
        "url": response["url"],
        "status_code": response["status_code"],
        "ok": bool(ok),
        "summary": summary or {},
    }
    if error:
        result["error"] = error
    return result


def _evaluate_process_health(response: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str | None]:
    body = response["body"]
    status_text = str(body.get("status") or "").strip().lower()
    ok = response["status_code"] == 200 and status_text in {"healthy", "running"}
    summary = {
        "status": body.get("status"),
        "timestamp": body.get("timestamp"),
    }
    error = None if ok else f"unexpected health response: status_code={response['status_code']} status={body.get('status')!r}"
    return ok, summary, error


def _evaluate_web_status(response: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str | None]:
    body = response["body"]
    status_text = str(body.get("status") or "").strip().lower()
    paper_trading = body.get("paper_trading")
    risk_halted = _safe_get(body, "risk", "trading_halted")
    ok = response["status_code"] == 200 and status_text == "running" and paper_trading is not None
    summary = {
        "status": body.get("status"),
        "paper_trading": paper_trading,
        "risk_trading_halted": risk_halted,
        "engine_running": body.get("engine_running"),
    }
    safe_ok = bool(paper_trading) and not _truthy(risk_halted)
    error = None if ok else f"unexpected api status: status_code={response['status_code']} status={body.get('status')!r}"
    if ok and not safe_ok:
        error = "unsafe runtime state: paper_trading is false or risk is halted"
    return ok and safe_ok, summary, error


def _evaluate_ops_health(response: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str | None]:
    body = response["body"]
    ok_flag = _truthy(body.get("ok"))
    summary = {
        "service": body.get("service"),
        "engine_running": body.get("engine_running"),
        "trading_mode": body.get("trading_mode"),
        "risk_halted": body.get("risk_halted"),
        "news_llm_queue_pending": body.get("news_llm_queue_pending"),
    }
    ok = response["status_code"] == 200 and ok_flag
    error = None if ok else f"unexpected ops health response: status_code={response['status_code']} ok={body.get('ok')!r}"
    return ok, summary, error


def _evaluate_ops_status(response: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str | None]:
    body = response["body"]
    ok_flag = _truthy(body.get("ok"))
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    execution = data.get("execution_engine") if isinstance(data, dict) else {}
    risk = data.get("risk_manager") if isinstance(data, dict) else {}
    execution_mode = _safe_get(execution, "mode")
    risk_halted = _safe_get(risk, "trading_halted")
    ok = response["status_code"] == 200 and ok_flag
    summary = {
        "execution_mode": execution_mode,
        "queue_worker_alive": _safe_get(execution, "queue_worker_alive"),
        "risk_trading_halted": risk_halted,
        "conditional_orders_count": _safe_get(execution, "conditional_orders_count"),
    }
    safe_ok = str(execution_mode or "").strip().lower() == "paper" and not _truthy(risk_halted)
    error = None if ok else f"unexpected ops status response: status_code={response['status_code']} ok={body.get('ok')!r}"
    if ok and not safe_ok:
        error = "unsafe runtime state: execution mode is not paper or risk manager is halted"
    return ok and safe_ok, summary, error


def _evaluate_run_once(response: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str | None]:
    body = response["body"]
    ok_flag = _truthy(body.get("ok"))
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    summary = {
        "keys": sorted(list(data.keys())) if isinstance(data, dict) else [],
        "queued_count": _safe_get(data, "queued_count"),
        "events_count": _safe_get(data, "events_count"),
    }
    ok = response["status_code"] == 200 and ok_flag
    error = None if ok else f"run-once endpoint failed: status_code={response['status_code']} ok={body.get('ok')!r}"
    return ok, summary, error


def _status_line(item: Dict[str, Any]) -> str:
    state = "PASS" if item.get("ok") else "FAIL"
    extra = ""
    summary = item.get("summary") or {}
    if summary:
        fragments = []
        for key in ("status", "paper_trading", "risk_trading_halted", "execution_mode", "engine_running", "queue_worker_alive"):
            if key in summary and summary[key] is not None:
                fragments.append(f"{key}={summary[key]}")
        if fragments:
            extra = " (" + ", ".join(fragments) + ")"
    return f"[{state}] {item.get('name')}: {item.get('method')} {item.get('path')}{extra}"


def _print_human_summary(report: Dict[str, Any]) -> None:
    lines = [
        f"paper-longrun selfcheck: {'PASS' if report.get('overall_ok') else 'FAIL'}",
        f"base_url: {report.get('base_url')}",
        f"checked_at: {report.get('checked_at')}",
    ]
    lines.extend(_status_line(item) for item in report.get("checks", []))
    safe = report.get("safe_state") or {}
    lines.append(
        "safe_state: "
        + (
            "PASS"
            if safe.get("ok")
            else "FAIL"
        )
        + f" (paper_trading={safe.get('paper_trading')}, risk_trading_halted={safe.get('risk_trading_halted')}, execution_mode={safe.get('execution_mode')})"
    )
    for line in lines:
        print(line, file=sys.stderr)
    errors = [item.get("error") for item in report.get("checks", []) if item.get("error")]
    if errors:
        print("errors:", file=sys.stderr)
        for err in errors:
            print(f"- {err}", file=sys.stderr)


def run_selfcheck(base_url: str, token: str, timeout: float, run_once_path: str) -> Dict[str, Any]:
    checked_at = datetime.now(timezone.utc).isoformat()
    checks: List[Dict[str, Any]] = []

    process_health = _request_json(base_url, token, "GET", "/health", timeout)
    ok, summary, error = _evaluate_process_health(process_health)
    checks.append(_make_check_result("process_health", process_health, ok, summary, error))

    web_status = _request_json(base_url, token, "GET", "/api/status", timeout)
    ok, summary, error = _evaluate_web_status(web_status)
    checks.append(_make_check_result("web_status", web_status, ok, summary, error))

    ops_health = _request_json(base_url, token, "GET", "/ops/health", timeout)
    ok, summary, error = _evaluate_ops_health(ops_health)
    checks.append(_make_check_result("ops_health", ops_health, ok, summary, error))

    ops_status = _request_json(base_url, token, "GET", "/ops/status", timeout)
    ok, summary, error = _evaluate_ops_status(ops_status)
    checks.append(_make_check_result("ops_status", ops_status, ok, summary, error))

    run_once_payload = {
        "sources": [],
        "llm_limit": 1,
        "pull_only": True,
        "llm_only": True,
    }
    run_once = _request_json(base_url, token, "POST", run_once_path, timeout, payload=run_once_payload)
    ok, summary, error = _evaluate_run_once(run_once)
    checks.append(_make_check_result("run_once_probe", run_once, ok, summary, error))

    safe_state = {
        "paper_trading": _safe_get(web_status["body"], "paper_trading"),
        "risk_trading_halted": _safe_get(ops_status["body"], "data", "risk_manager", "trading_halted"),
        "execution_mode": _safe_get(ops_status["body"], "data", "execution_engine", "mode"),
    }
    safe_state["ok"] = bool(safe_state["paper_trading"]) and not _truthy(safe_state["risk_trading_halted"]) and str(safe_state["execution_mode"] or "").strip().lower() == "paper"

    overall_ok = all(item["ok"] for item in checks) and bool(safe_state["ok"])
    report = {
        "base_url": base_url,
        "checked_at": checked_at,
        "overall_ok": overall_ok,
        "safe_state": safe_state,
        "checks": checks,
        "summary": {
            "pass_count": sum(1 for item in checks if item["ok"]),
            "fail_count": sum(1 for item in checks if not item["ok"]),
            "check_count": len(checks),
        },
    }
    return report


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Paper longrun self-check for the trading web stack")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Root web URL, e.g. http://127.0.0.1:8000")
    parser.add_argument("--token", default=os.getenv("OPS_TOKEN", ""), help="Ops auth token (required for /ops checks)")
    parser.add_argument("--timeout", type=float, default=float(os.getenv("PAPER_LONGRUN_TIMEOUT", "12")), help="Per-request timeout in seconds")
    parser.add_argument("--run-once-path", default=DEFAULT_RUN_ONCE_PATH, help="Run-once endpoint path relative to base URL")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if not str(args.token or "").strip():
        print("OPS_TOKEN is required via --token or environment for the paper longrun selfcheck", file=sys.stderr)
        return 2

    try:
        report = run_selfcheck(
            base_url=str(args.base_url).strip(),
            token=str(args.token).strip(),
            timeout=float(args.timeout),
            run_once_path=str(args.run_once_path).strip() or DEFAULT_RUN_ONCE_PATH,
        )
    except Exception as exc:
        report = {
            "base_url": str(args.base_url).strip(),
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "overall_ok": False,
            "safe_state": {"ok": False},
            "checks": [
                {
                    "name": "selfcheck",
                    "method": "-",
                    "path": "-",
                    "url": str(args.base_url).strip(),
                    "status_code": 0,
                    "ok": False,
                    "summary": {},
                    "error": str(exc),
                }
            ],
            "summary": {"pass_count": 0, "fail_count": 1, "check_count": 1},
        }

    _print_human_summary(report)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report.get("overall_ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
