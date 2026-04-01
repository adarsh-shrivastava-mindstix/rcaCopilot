from bedrock_agentcore import BedrockAgentCoreApp
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()


def _configure_logging() -> None:
    level_name = os.getenv("RCA_LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


_configure_logging()

from rca.storage import persist_report_to_s3
from rca.workflow import run_rca_workflow, run_rca_workflow_stream

# Integrate with Bedrock AgentCore
app = BedrockAgentCoreApp(
    middleware=[
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
    ]
)

LOG_ID_PATTERN = re.compile(r"LOG-\d{4}")


def _extract_log_id(payload: object) -> str:
    keyed_matches: list[str] = []
    pattern_matches: list[str] = []
    visited_ids: set[int] = set()

    def visit(node: Any) -> None:
        obj_id = id(node)
        if obj_id in visited_ids:
            return
        visited_ids.add(obj_id)

        if isinstance(node, dict):
            raw_log_id = node.get("log_id")
            if isinstance(raw_log_id, str):
                direct = LOG_ID_PATTERN.search(raw_log_id)
                if direct:
                    keyed_matches.append(direct.group(0))
            for value in node.values():
                visit(value)
            return

        if isinstance(node, (list, tuple)):
            for item in node:
                visit(item)
            return

        if isinstance(node, bytes):
            try:
                visit(node.decode("utf-8", errors="replace"))
            except Exception:
                return
            return

        if isinstance(node, str):
            direct = LOG_ID_PATTERN.search(node)
            if direct:
                pattern_matches.append(direct.group(0))

            try:
                parsed = json.loads(node)
            except json.JSONDecodeError:
                return
            visit(parsed)

    visit(payload)

    if keyed_matches:
        return keyed_matches[0]
    if pattern_matches:
        return pattern_matches[0]
    return ""


def _extract_stream_flag(payload: object) -> bool:
    visited_ids: set[int] = set()

    def visit(node: Any) -> bool:
        obj_id = id(node)
        if obj_id in visited_ids:
            return False
        visited_ids.add(obj_id)

        if isinstance(node, dict):
            if "stream" in node:
                value = node.get("stream")
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.strip().lower() in {"1", "true", "yes", "on"}
            for value in node.values():
                if visit(value):
                    return True
            return False

        if isinstance(node, (list, tuple)):
            for item in node:
                if visit(item):
                    return True
            return False

        if isinstance(node, str):
            try:
                parsed = json.loads(node)
            except json.JSONDecodeError:
                return False
            return visit(parsed)

        return False

    return visit(payload)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _stream_invoke(log_id: str):
    final_report: dict[str, Any] | None = None

    try:
        async for event in run_rca_workflow_stream(log_id):
            if event.get("type") == "report_generated":
                maybe_report = event.get("report")
                if isinstance(maybe_report, dict):
                    final_report = maybe_report
            yield event

        if final_report is None:
            final_report = {
                "status": "failed",
                "issue_summary": "RCA workflow did not return a report.",
                "log_id": log_id,
            }

        # Step 6: S3 storage
        step6_key = "store_s3"
        step6_title = "Storing Report to S3"
        step6_start = datetime.now(timezone.utc)
        yield {
            "type": "step_started",
            "step_key": step6_key,
            "step_title": step6_title,
            "started_at": step6_start.isoformat(),
        }
        yield {
            "type": "step_stream",
            "step_key": step6_key,
            "text": "Preparing report payload for object storage.",
            "timestamp": _iso_now(),
        }

        storage = persist_report_to_s3(final_report)
        step6_end = datetime.now(timezone.utc)
        step6_payload = {
            "type": "step_completed" if storage.get("stored") else "step_failed",
            "step_key": step6_key,
            "step_title": step6_title,
            "started_at": step6_start.isoformat(),
            "ended_at": step6_end.isoformat(),
            "duration_ms": round((step6_end - step6_start).total_seconds() * 1000.0, 2),
            "message": (
                f"Stored at {storage.get('s3_uri')}"
                if storage.get("stored")
                else storage.get("error", "Failed to store to S3.")
            ),
            "storage": storage,
        }
        if storage.get("stored"):
            yield {
                "type": "step_stream",
                "step_key": step6_key,
                "text": f"Report stored at {storage.get('s3_uri')}.",
                "timestamp": _iso_now(),
            }
        else:
            yield {
                "type": "step_stream",
                "step_key": step6_key,
                "text": f"S3 storage failed: {storage.get('error', 'Unknown error')}",
                "timestamp": _iso_now(),
            }
        yield step6_payload

        # Step 7: Ready
        step7_key = "report_ready"
        step7_title = "RCA Report Ready"
        step7_start = datetime.now(timezone.utc)
        yield {
            "type": "step_started",
            "step_key": step7_key,
            "step_title": step7_title,
            "started_at": step7_start.isoformat(),
        }
        yield {
            "type": "step_stream",
            "step_key": step7_key,
            "text": "Finalizing response payload for UI rendering.",
            "timestamp": _iso_now(),
        }
        step7_end = datetime.now(timezone.utc)
        yield {
            "type": "step_completed",
            "step_key": step7_key,
            "step_title": step7_title,
            "started_at": step7_start.isoformat(),
            "ended_at": step7_end.isoformat(),
            "duration_ms": round((step7_end - step7_start).total_seconds() * 1000.0, 2),
            "message": "Report prepared and ready.",
        }
        yield {
            "type": "report_ready",
            "generated_at": _iso_now(),
            "report": final_report,
            "storage": {"s3": storage},
        }
    except Exception as exc:
        yield {
            "type": "stream_error",
            "generated_at": _iso_now(),
            "error": str(exc),
        }

@app.entrypoint
async def invoke(payload: dict) -> dict:
    log_id = _extract_log_id(payload)
    if _extract_stream_flag(payload):
        return _stream_invoke(log_id)
    report = await run_rca_workflow(log_id)
    storage_info = persist_report_to_s3(report)
    return {"report": report, "storage": {"s3": storage_info}}

if __name__ == "__main__":
    app.run()
