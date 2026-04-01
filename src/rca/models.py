from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class LogRecord:
    log_id: str
    service: str
    timestamp: str
    endpoint_or_job: str
    correlation_id: str
    log_lines: list[str]
    stack_trace: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RCAReport:
    report_id: str
    log_id: str
    status: str
    issue_summary: str
    impacted_service: str
    probable_root_cause: str
    root_cause_confidence: float
    evidence: dict[str, Any]
    suspected_files: list[dict[str, Any]]
    github_context: dict[str, Any]
    research_summary: str
    web_probable_solution: str
    agent_intelligence_solution: str
    probable_solution: str
    next_actions: list[str]
    preventive_actions: list[str]
    generated_at: str
    markdown_report: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

