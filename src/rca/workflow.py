from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from rca.models import RCAReport
from rca.providers import (
    AgentIntelligenceProvider,
    DummyGitHubContextProvider,
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
GITHUB_PROVIDER = DummyGitHubContextProvider()
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


def fetch_github_context(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}

    location = state.get("source_location", {})
    context = GITHUB_PROVIDER.get_context(
        stack_file_path=str(location.get("file_path", "")),
        line_number=location.get("line_number"),
        function_name=location.get("function_or_class"),
    )
    return {"github_context": context}


def do_web_search(state: RCAState) -> RCAState:
    if _has_errors(state):
        return {}

    analysis = state["analysis"]
    query = (
        f"{analysis['exception_type']} {analysis['key_error_message']} "
        f"{analysis['service_name']} {analysis['endpoint_or_job']} python"
    )
    findings = WEB_PROVIDER.search_probable_fixes(query=query, limit=3)
    web_fix_lines = [item.get("probable_fix", "").strip() for item in findings if item.get("probable_fix")]
    web_probable_solution = (
        " | ".join(web_fix_lines[:2])
        if web_fix_lines
        else "No actionable web fix could be extracted from live search results."
    )
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
            research_summary="Web search not executed due to input/DB failure.",
            web_probable_solution="",
            agent_intelligence_solution="",
            probable_solution="Unable to produce solution because validation or DB fetch failed.",
            next_actions=[
                "Retry with a valid log_id in format LOG-####.",
                "Ensure log_id exists in the logs DB table.",
            ],
            preventive_actions=[
                "Add upstream validation before invoking RCA workflow.",
                "Expose available log_ids endpoint for operators.",
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
