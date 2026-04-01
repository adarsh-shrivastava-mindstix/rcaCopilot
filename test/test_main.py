import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from main import invoke


def test_invoke_with_valid_log_id_returns_success_report() -> None:
    mocked_agent_output = {
        "probable_root_cause": "DB timeout due to pool saturation in order repository path.",
        "agent_solution": "Tune pool and optimize slow query in order_repository.py.",
        "next_actions": ["Patch and deploy query optimization."],
        "preventive_actions": ["Add pool saturation alerting."],
        "confidence": 0.81,
    }
    mocked_tavily_findings = [
        {
            "source": "Tavily",
            "title": "SQLAlchemy timeout mitigation",
            "url": "https://example.com/sqlalchemy-timeout",
            "summary": "Tune pool and retry strategy.",
            "probable_fix": "Tune pool sizing and add bounded retries.",
        }
    ]
    with patch(
        "rca.workflow.AGENT_PROVIDER.generate",
        new=AsyncMock(return_value=mocked_agent_output),
    ), patch(
        "rca.workflow.WEB_PROVIDER.search_probable_fixes",
        return_value=mocked_tavily_findings,
    ):
        result = asyncio.run(invoke({"log_id": "LOG-1002"}))
    report = result["report"]

    assert report["status"] == "success"
    assert report["log_id"] == "LOG-1002"
    assert report["impacted_service"] == "orders-service"
    assert report["root_cause_confidence"] > 0.0
    assert len(report["suspected_files"]) > 0


def test_invoke_with_invalid_log_id_format_returns_failed_report() -> None:
    result = asyncio.run(invoke({"log_id": "bad-id"}))
    report = result["report"]

    assert report["status"] == "failed"
    assert "Invalid log_id" in report["issue_summary"]
    assert report["root_cause_confidence"] == 0.0


def test_invoke_with_unknown_log_id_returns_failed_report() -> None:
    result = asyncio.run(invoke({"log_id": "LOG-9999"}))
    report = result["report"]

    assert report["status"] == "failed"
    assert "not found in DB" in report["issue_summary"]
    assert report["root_cause_confidence"] == 0.0


def test_invoke_with_json_string_payload_parses_log_id() -> None:
    mocked_agent_output = {
        "probable_root_cause": "Sample root cause from model output.",
        "agent_solution": "Sample agent solution from model output.",
        "next_actions": ["Do action A"],
        "preventive_actions": ["Do preventive action A"],
        "confidence": 0.73,
    }
    mocked_tavily_findings = [
        {
            "source": "Tavily",
            "title": "Sample Tavily result",
            "url": "https://example.com",
            "summary": "Sample summary",
            "probable_fix": "Sample probable fix",
        }
    ]
    with patch(
        "rca.workflow.AGENT_PROVIDER.generate",
        new=AsyncMock(return_value=mocked_agent_output),
    ), patch(
        "rca.workflow.WEB_PROVIDER.search_probable_fixes",
        return_value=mocked_tavily_findings,
    ):
        result = asyncio.run(invoke('{"log_id":"LOG-1001"}'))
    report = result["report"]

    assert report["status"] == "success"
    assert report["log_id"] == "LOG-1001"


def test_invoke_with_nested_message_payload_parses_log_id() -> None:
    mocked_agent_output = {
        "probable_root_cause": "Sample nested root cause.",
        "agent_solution": "Sample nested agent solution.",
        "next_actions": ["Nested action"],
        "preventive_actions": ["Nested preventive action"],
        "confidence": 0.74,
    }
    mocked_tavily_findings = [
        {
            "source": "Tavily",
            "title": "Nested Tavily result",
            "url": "https://example.com/nested",
            "summary": "Nested summary",
            "probable_fix": "Nested probable fix",
        }
    ]
    payload = {
        "messages": [
            {
                "role": "user",
                "content": "{\"log_id\":\"LOG-1003\"}",
            }
        ]
    }
    with patch(
        "rca.workflow.AGENT_PROVIDER.generate",
        new=AsyncMock(return_value=mocked_agent_output),
    ), patch(
        "rca.workflow.WEB_PROVIDER.search_probable_fixes",
        return_value=mocked_tavily_findings,
    ):
        result = asyncio.run(invoke(payload))
    report = result["report"]

    assert report["status"] == "success"
    assert report["log_id"] == "LOG-1003"
