from __future__ import annotations

import inspect
import re
import uuid
from datetime import datetime, timezone
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from rca.models import RCAReport
from rca.providers import (
    AgentIntelligenceProvider,
    GatewayGitHubContextProvider,
    SQLiteLogProvider,
    WebSearchProvider,
)

LOG_ID_PATTERN = re.compile(r"^LOG-\d{4}$")
STACK_FRAME_PATTERN = re.compile(r'File "([^"]+)", line (\d+), in ([^\s]+)')

CATEGORY_PATTERNS: dict[str, list[str]] = {
    "timeout": ["timeout", "timed out", "readtimeout", "deadline exceeded"],
    "null reference": ["nonetype", "attributeerror", "null reference"],
    "db error": ["sqlalchemy", "database", "queuepool", "operationalerror", "connection timed out"],
    "auth error": ["jwt", "unauthorized", "expiredsignatureerror", "401"],
    "dependency failure": ["503", "service unavailable", "httperror", "upstream"],
    "config issue": ["nosuchkey", "missing env", "configuration", "invalid config", "bucket"],
}

LOG_PROVIDER = SQLiteLogProvider()
GITHUB_PROVIDER = GatewayGitHubContextProvider()
WEB_PROVIDER = WebSearchProvider()
AGENT_PROVIDER = AgentIntelligenceProvider()


class RCAState(TypedDict, total=False):
    log_id: str
    status: str
    errors: list[str]
    log_record: dict[str, Any]
    analysis: dict[str, Any]
    source_location: dict[str, Any]
    github_context: dict[str, Any]
    web_findings: list[dict[str, Any]]
    web_probable_solution: str
    agent_intelligence: dict[str, Any]
    report: dict[str, Any]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _duration_ms(start: datetime, end: datetime) -> float:
    return round((end - start).total_seconds() * 1000.0, 2)


def _apply_update(state: RCAState, update: dict[str, Any] | None) -> None:
    if update:
        state.update(update)


async def _run_step_function(step_fn, state: RCAState) -> dict[str, Any]:
    result = step_fn(state)
    if inspect.isawaitable(result):
        result = await result
    return result if isinstance(result, dict) else {}


def _has_errors(state: RCAState) -> bool:
    return bool(state.get("errors"))


def _classify_issue(text: str) -> str:
    lowered = text.lower()
    for category, patterns in CATEGORY_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return category
    return "unknown"


def _extract_key_error(stack_trace: list[str], log_lines: list[str]) -> str:
    for line in reversed(stack_trace):
        if "error" in line.lower() or "exception" in line.lower():
            return line.strip()
    for line in reversed(log_lines):
        if "error" in line.lower() or "exception" in line.lower() or "failed" in line.lower():
            return line.strip()
    return "No explicit error found in log lines."


def _extract_primary_source_location(stack_trace: list[str]) -> dict[str, Any]:
    for line in stack_trace:
        match = STACK_FRAME_PATTERN.search(line)
        if not match:
            continue
        file_path = match.group(1)
        line_number = int(match.group(2))
        function_name = match.group(3)
        directory = "/".join(file_path.split("/")[:-1]) if "/" in file_path else ""
        file_name = file_path.split("/")[-1]
        return {
            "file_path": file_path,
            "directory": directory,
            "file_name": file_name,
            "function_or_class": function_name,
            "line_number": line_number,
        }
    return {
        "file_path": "",
        "directory": "",
        "file_name": "",
        "function_or_class": "",
        "line_number": None,
    }


def validate_input(state: RCAState) -> RCAState:
    raw_log_id = state.get("log_id", "")
    if not isinstance(raw_log_id, str):
        return {"status": "failed", "errors": ["`log_id` must be a string in format LOG-####."]}

    log_id = raw_log_id.strip()
    if not LOG_ID_PATTERN.fullmatch(log_id):
        return {
            "status": "failed",
            "errors": [f"Invalid log_id '{raw_log_id}'. Expected format LOG-####."],
        }
    return {"log_id": log_id, "status": "in_progress", "errors": []}


def fetch_logs_from_db(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}
    log_record = LOG_PROVIDER.get_log(state["log_id"])
    if not log_record:
        available = ", ".join(LOG_PROVIDER.available_log_ids())
        return {
            "status": "failed",
            "errors": [
                f"log_id '{state['log_id']}' not found in DB. Available log_ids: {available}."
            ],
        }
    return {"log_record": log_record.to_dict()}


def analyze_logs(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}

    record = state["log_record"]
    stack_trace = record.get("stack_trace", [])
    log_lines = record.get("log_lines", [])
    issue_category = _classify_issue(" ".join([*log_lines, *stack_trace]))
    key_error_message = _extract_key_error(stack_trace=stack_trace, log_lines=log_lines)
    source_location = _extract_primary_source_location(stack_trace)

    exception_type = "Unknown"
    if ":" in key_error_message:
        exception_type = key_error_message.split(":", 1)[0].strip()

    analysis = {
        "service_name": record.get("service", ""),
        "timestamp": record.get("timestamp", ""),
        "endpoint_or_job": record.get("endpoint_or_job", ""),
        "correlation_id": record.get("correlation_id", ""),
        "stack_trace": stack_trace,
        "exception_category": issue_category,
        "exception_type": exception_type,
        "key_error_message": key_error_message,
        "issue_summary": (
            f"{issue_category} detected in {record.get('service')} at {record.get('timestamp')} "
            f"for {record.get('endpoint_or_job')}."
        ),
    }
    return {"analysis": analysis, "source_location": source_location}


async def fetch_github_context(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}

    location = state.get("source_location", {})
    analysis = state.get("analysis", {})
    try:
        context = await GITHUB_PROVIDER.get_context(
            stack_file_path=str(location.get("file_path", "")),
            line_number=location.get("line_number"),
            function_name=location.get("function_or_class"),
            service_name=str(analysis.get("service_name", "")),
            endpoint_or_job=str(analysis.get("endpoint_or_job", "")),
            key_error_message=str(analysis.get("key_error_message", "")),
        )
    except Exception as exc:
        return {
            "status": "failed",
            "errors": [f"GitHub Gateway context retrieval failed: {exc}"],
        }

    if str(context.get("status", "")).lower() != "resolved":
        return {
            "status": "failed",
            "errors": [
                f"GitHub Gateway source resolution failed: {context.get('reason', 'unresolved')}"
            ],
        }

    updated_location = dict(location)
    selected_path = str(context.get("file_path", "")).strip()
    selected_fn = str(context.get("function_or_class", "")).strip()
    selected_line = context.get("line_number")
    if selected_path:
        updated_location["file_path"] = selected_path
        updated_location["directory"] = "/".join(selected_path.split("/")[:-1])
        updated_location["file_name"] = selected_path.split("/")[-1]
    if selected_fn:
        updated_location["function_or_class"] = selected_fn
    if isinstance(selected_line, int):
        updated_location["line_number"] = selected_line

    return {"github_context": context, "source_location": updated_location}


async def do_web_search(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}

    analysis = state["analysis"]
    query = (
        f"{analysis['exception_type']} {analysis['key_error_message']} "
        f"{analysis['service_name']} {analysis['endpoint_or_job']} python"
    )
    try:
        findings = await WEB_PROVIDER.search_probable_fixes(query=query, limit=3)
    except Exception as exc:
        return {
            "status": "failed",
            "errors": [f"Tavily Gateway research failed: {exc}"],
        }

    web_fix_lines = []
    for item in findings:
        probable_fix = str(item.get("probable_fix", "")).strip()
        summary = str(item.get("summary", "")).strip()
        if probable_fix:
            web_fix_lines.append(probable_fix)
        elif summary:
            web_fix_lines.append(summary)

    if not web_fix_lines:
        return {
            "status": "failed",
            "errors": [
                "Tavily Gateway returned findings but no usable solution text for RCA synthesis."
            ],
        }

    web_probable_solution = " | ".join(web_fix_lines[:2])
    return {"web_findings": findings, "web_probable_solution": web_probable_solution}


async def generate_agent_solution(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}

    intelligence_input = {
        "analysis": state.get("analysis", {}),
        "source_location": state.get("source_location", {}),
        "github_context": state.get("github_context", {}),
        "web_findings": state.get("web_findings", []),
    }
    try:
        result = await AGENT_PROVIDER.generate(intelligence_input)
        return {"agent_intelligence": result}
    except Exception as exc:
        return {
            "status": "failed",
            "errors": [f"Agent intelligence generation failed using Bedrock model: {exc}"],
        }


def combine_solutions_and_report(state: RCAState) -> RCAState:
    generated_at = datetime.now(timezone.utc).isoformat()
    failed = _has_errors(state)
    report_id = f"RCA-{uuid.uuid4().hex[:12].upper()}"

    if failed:
        error_message = state.get("errors", ["Unknown RCA failure."])[0]
        lower_error = error_message.lower()
        if "github gateway" in lower_error:
            research_summary = "GitHub Gateway context resolution failed before Tavily research."
        elif "tavily gateway" in lower_error:
            research_summary = "Tavily Gateway research failed."
        elif "invalid log_id" in lower_error or "not found in db" in lower_error:
            research_summary = "Research not started due to input validation or DB lookup failure."
        else:
            research_summary = "RCA pipeline failed before final synthesis."
        report = RCAReport(
            report_id=report_id,
            log_id=state.get("log_id", ""),
            status="failed",
            issue_summary=error_message,
            impacted_service="unknown",
            probable_root_cause=error_message,
            root_cause_confidence=0.0,
            evidence={"errors": state.get("errors", [])},
            suspected_files=[],
            github_context={},
            research_summary=research_summary,
            web_probable_solution="",
            agent_intelligence_solution="",
            probable_solution="Unable to produce a final solution because RCA evidence collection failed.",
            next_actions=[
                "Retry with a valid log_id in format LOG-####.",
                "Verify Gateway target/tool availability for GitHub and Tavily.",
            ],
            preventive_actions=[
                "Add upstream validation before invoking RCA workflow.",
                "Add health checks for Gateway tool readiness before running RCA.",
            ],
            generated_at=generated_at,
            markdown_report=_failure_markdown(
                report_id=report_id,
                log_id=state.get("log_id", ""),
                error_message=error_message,
                generated_at=generated_at,
            ),
        )
        return {"report": report.to_dict()}

    analysis = state["analysis"]
    location = state.get("source_location", {})
    github_context = state.get("github_context", {})
    web_findings = state.get("web_findings", [])
    web_solution = state.get("web_probable_solution", "")
    agent = state.get("agent_intelligence", {})

    agent_solution = str(agent.get("agent_solution", "")).strip()
    probable_root_cause = str(agent.get("probable_root_cause", "")).strip()
    confidence = float(agent.get("confidence", 0.65))

    combined_solution = (
        f"Agent intelligence solution: {agent_solution}\n\n"
        f"Web corroborated fix signals: {web_solution}"
    )

    suspected_files = [
        {
            "repo": github_context.get("repo", "unknown"),
            "directory": location.get("directory", ""),
            "file_path": github_context.get("file_path") or location.get("file_path", ""),
            "function_or_class": location.get("function_or_class", ""),
            "line_number": location.get("line_number"),
        }
    ]

    report = RCAReport(
        report_id=report_id,
        log_id=state["log_id"],
        status="success",
        issue_summary=analysis["issue_summary"],
        impacted_service=analysis["service_name"],
        probable_root_cause=probable_root_cause or "Root cause inferred from logs and source context.",
        root_cause_confidence=min(max(confidence, 0.0), 0.98),
        evidence={
            "log_analysis": analysis,
            "source_location": location,
            "github_context": github_context,
            "web_findings": web_findings,
            "agent_intelligence": agent,
        },
        suspected_files=suspected_files,
        github_context=github_context,
        research_summary=_research_summary(web_findings),
        web_probable_solution=web_solution,
        agent_intelligence_solution=agent_solution,
        probable_solution=combined_solution,
        next_actions=agent.get("next_actions", []),
        preventive_actions=agent.get("preventive_actions", []),
        generated_at=generated_at,
        markdown_report=_success_markdown(
            report_id=report_id,
            log_id=state["log_id"],
            generated_at=generated_at,
            analysis=analysis,
            location=location,
            probable_root_cause=probable_root_cause,
            confidence=min(max(confidence, 0.0), 0.98),
            web_solution=web_solution,
            agent_solution=agent_solution,
            combined_solution=combined_solution,
            next_actions=agent.get("next_actions", []),
            preventive_actions=agent.get("preventive_actions", []),
            github_context=github_context,
            web_findings=web_findings,
        ),
    )
    return {"report": report.to_dict()}


def _research_summary(web_findings: list[dict[str, Any]]) -> str:
    if not web_findings:
        return "No web findings returned."
    lines = []
    for finding in web_findings[:3]:
        title = str(finding.get("title", "Untitled"))
        url = str(finding.get("url", ""))
        lines.append(f"{title} ({url})")
    return " | ".join(lines)


def _failure_markdown(report_id: str, log_id: str, error_message: str, generated_at: str) -> str:
    return (
        "# RCA Report\n\n"
        f"- Report ID: {report_id}\n"
        f"- Log ID: {log_id}\n"
        "- Status: failed\n"
        f"- Generated At: {generated_at}\n\n"
        "## Failure\n"
        f"{error_message}\n"
    )


def _success_markdown(
    report_id: str,
    log_id: str,
    generated_at: str,
    analysis: dict[str, Any],
    location: dict[str, Any],
    probable_root_cause: str,
    confidence: float,
    web_solution: str,
    agent_solution: str,
    combined_solution: str,
    next_actions: list[str],
    preventive_actions: list[str],
    github_context: dict[str, Any],
    web_findings: list[dict[str, Any]],
) -> str:
    next_lines = "\n".join(f"- {item}" for item in next_actions) or "- None"
    preventive_lines = "\n".join(f"- {item}" for item in preventive_actions) or "- None"
    web_lines = "\n".join(
        f"- {item.get('title', 'Untitled')}: {item.get('url', '')}" for item in web_findings[:3]
    ) or "- None"

    return (
        "# RCA Report\n\n"
        f"- Report ID: {report_id}\n"
        f"- Log ID: {log_id}\n"
        "- Status: success\n"
        f"- Generated At: {generated_at}\n"
        f"- Confidence: {confidence}\n\n"
        "## Issue Summary\n"
        f"{analysis.get('issue_summary', '')}\n\n"
        "## Error Location\n"
        f"- Directory: {location.get('directory', '')}\n"
        f"- File: {location.get('file_path', '')}\n"
        f"- Function: {location.get('function_or_class', '')}\n"
        f"- Line: {location.get('line_number', '')}\n\n"
        "## GitHub Context\n"
        f"- Status: {github_context.get('status', 'unavailable')}\n"
        f"- Repo: {github_context.get('repo', '')}\n"
        f"- Source URL: {github_context.get('source_url', '')}\n\n"
        "## Probable Root Cause\n"
        f"{probable_root_cause}\n\n"
        "## Web-Based Probable Solution\n"
        f"{web_solution}\n\n"
        "## Agent-Intelligence Solution\n"
        f"{agent_solution}\n\n"
        "## Final Combined Solution\n"
        f"{combined_solution}\n\n"
        "## Next Actions\n"
        f"{next_lines}\n\n"
        "## Preventive Actions\n"
        f"{preventive_lines}\n\n"
        "## Web References\n"
        f"{web_lines}\n"
    )


def _build_graph():
    graph = StateGraph(RCAState)
    graph.add_node("validate_input", validate_input)
    graph.add_node("fetch_logs_from_db", fetch_logs_from_db)
    graph.add_node("analyze_logs", analyze_logs)
    graph.add_node("fetch_github_context", fetch_github_context)
    graph.add_node("do_web_search", do_web_search)
    graph.add_node("generate_agent_solution", generate_agent_solution)
    graph.add_node("combine_solutions_and_report", combine_solutions_and_report)

    graph.set_entry_point("validate_input")
    graph.add_edge("validate_input", "fetch_logs_from_db")
    graph.add_edge("fetch_logs_from_db", "analyze_logs")
    graph.add_edge("analyze_logs", "fetch_github_context")
    graph.add_edge("fetch_github_context", "do_web_search")
    graph.add_edge("do_web_search", "generate_agent_solution")
    graph.add_edge("generate_agent_solution", "combine_solutions_and_report")
    graph.add_edge("combine_solutions_and_report", END)
    return graph.compile()


RCA_GRAPH = _build_graph()


async def run_rca_workflow(log_id: str | None) -> dict[str, Any]:
    initial_state: RCAState = {"log_id": log_id or ""}
    result = await RCA_GRAPH.ainvoke(initial_state)
    return result["report"]


async def run_rca_workflow_stream(log_id: str | None):
    state: RCAState = {"log_id": log_id or ""}

    # Step 1: Validating + DB fetch
    step_key = "validate_fetch"
    step_title = "Validating Log ID & Fetching Logs From Database"
    started = datetime.now(timezone.utc)
    yield {
        "type": "step_started",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": _utc_now_iso(),
    }
    yield {
        "type": "step_stream",
        "step_key": step_key,
        "text": "Checking log_id format and request payload.",
        "timestamp": _utc_now_iso(),
    }
    _apply_update(state, await _run_step_function(validate_input, state))
    if not _has_errors(state):
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": f"Fetching logs from database for {state.get('log_id', '')}.",
            "timestamp": _utc_now_iso(),
        }
        _apply_update(state, await _run_step_function(fetch_logs_from_db, state))
        if not _has_errors(state):
            log_record = state.get("log_record", {})
            log_lines = log_record.get("log_lines", []) if isinstance(log_record, dict) else []
            stack_trace = log_record.get("stack_trace", []) if isinstance(log_record, dict) else []
            yield {
                "type": "step_stream",
                "step_key": step_key,
                "text": f"Fetched {len(log_lines)} log lines and {len(stack_trace)} stack trace lines.",
                "timestamp": _utc_now_iso(),
            }
    ended = datetime.now(timezone.utc)
    step1_failed = _has_errors(state)
    yield {
        "type": "step_failed" if step1_failed else "step_completed",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_ms": _duration_ms(started, ended),
        "message": state.get("errors", [""])[0] if step1_failed else "Completed.",
    }

    # Step 2: Log analysis
    step_key = "analyze_logs"
    step_title = "Analyzing Logs and Detecting Error"
    started = datetime.now(timezone.utc)
    yield {
        "type": "step_started",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": _utc_now_iso(),
    }
    if not _has_errors(state):
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": "Extracting key exception and classifying error category.",
            "timestamp": _utc_now_iso(),
        }
        _apply_update(state, await _run_step_function(analyze_logs, state))
        if not _has_errors(state):
            analysis = state.get("analysis", {})
            yield {
                "type": "step_stream",
                "step_key": step_key,
                "text": (
                    f"Detected category '{analysis.get('exception_category', 'unknown')}' with key error: "
                    f"{analysis.get('key_error_message', '')}"
                ),
                "timestamp": _utc_now_iso(),
            }
    ended = datetime.now(timezone.utc)
    step2_failed = _has_errors(state)
    yield {
        "type": "step_failed" if step2_failed else "step_completed",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_ms": _duration_ms(started, ended),
        "message": state.get("errors", [""])[0] if step2_failed else "Completed.",
    }

    # Step 3: Source location
    step_key = "locate_source"
    step_title = "Locating Source File and Function"
    started = datetime.now(timezone.utc)
    yield {
        "type": "step_started",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": _utc_now_iso(),
    }
    if not _has_errors(state):
        location = state.get("source_location", {})
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": (
                f"Mapped likely source to {location.get('file_path', 'unknown')}::"
                f"{location.get('function_or_class', 'unknown')} (line {location.get('line_number', 'n/a')})."
            ),
            "timestamp": _utc_now_iso(),
        }
    ended = datetime.now(timezone.utc)
    step3_failed = _has_errors(state)
    yield {
        "type": "step_failed" if step3_failed else "step_completed",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_ms": _duration_ms(started, ended),
        "message": state.get("errors", [""])[0] if step3_failed else "Completed.",
    }

    # Step 4: GitHub + Tavily
    step_key = "github_tavily"
    step_title = "Fetching GitHub Context & Researching Fixes on Tavily"
    started = datetime.now(timezone.utc)
    yield {
        "type": "step_started",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": _utc_now_iso(),
    }
    if not _has_errors(state):
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": "Fetching GitHub file context for the suspected source location.",
            "timestamp": _utc_now_iso(),
        }
        _apply_update(state, await _run_step_function(fetch_github_context, state))
        github_context = state.get("github_context", {})
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": f"GitHub context status: {github_context.get('status', 'unknown')}.",
            "timestamp": _utc_now_iso(),
        }
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": "Searching Tavily for external fix patterns and references.",
            "timestamp": _utc_now_iso(),
        }
        _apply_update(state, await _run_step_function(do_web_search, state))
        if not _has_errors(state):
            web_findings = state.get("web_findings", [])
            yield {
                "type": "step_stream",
                "step_key": step_key,
                "text": (
                    f"Tavily returned {len(web_findings) if isinstance(web_findings, list) else 0} findings."
                ),
                "timestamp": _utc_now_iso(),
            }
    ended = datetime.now(timezone.utc)
    step4_failed = _has_errors(state)
    yield {
        "type": "step_failed" if step4_failed else "step_completed",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_ms": _duration_ms(started, ended),
        "message": state.get("errors", [""])[0] if step4_failed else "Completed.",
    }

    # Step 5: Bedrock reasoning + RCA draft
    step_key = "bedrock_draft"
    step_title = "Bedrock Reasoning & Drafting Final RCA"
    started = datetime.now(timezone.utc)
    yield {
        "type": "step_started",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": _utc_now_iso(),
    }
    if not _has_errors(state):
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": "Sending consolidated evidence to Bedrock for root-cause reasoning.",
            "timestamp": _utc_now_iso(),
        }
        _apply_update(state, await _run_step_function(generate_agent_solution, state))
        if not _has_errors(state):
            agent = state.get("agent_intelligence", {})
            yield {
                "type": "step_stream",
                "step_key": step_key,
                "text": f"Bedrock generated RCA hypothesis with confidence {agent.get('confidence', 'n/a')}.",
                "timestamp": _utc_now_iso(),
            }
        else:
            yield {
                "type": "step_stream",
                "step_key": step_key,
                "text": "Bedrock reasoning failed while generating agent intelligence output.",
                "timestamp": _utc_now_iso(),
            }
    if not _has_errors(state):
        yield {
            "type": "step_stream",
            "step_key": step_key,
            "text": "Combining model reasoning and web evidence into final RCA report.",
            "timestamp": _utc_now_iso(),
        }
    _apply_update(state, await _run_step_function(combine_solutions_and_report, state))
    ended = datetime.now(timezone.utc)
    step5_failed = _has_errors(state) or state.get("report", {}).get("status") == "failed"
    yield {
        "type": "step_failed" if step5_failed else "step_completed",
        "step_key": step_key,
        "step_title": step_title,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_ms": _duration_ms(started, ended),
        "message": (
            state.get("errors", [""])[0]
            if step5_failed
            else "Completed."
        ),
    }

    report = state.get("report", {})
    yield {
        "type": "report_generated",
        "report": report,
    }
