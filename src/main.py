from bedrock_agentcore import BedrockAgentCoreApp
import json
import re
from typing import Any
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from rca.workflow import run_rca_workflow

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

@app.entrypoint
async def invoke(payload: dict) -> dict:
    log_id = _extract_log_id(payload)
    report = await run_rca_workflow(log_id)
    return {"report": report}

if __name__ == "__main__":
    app.run()
