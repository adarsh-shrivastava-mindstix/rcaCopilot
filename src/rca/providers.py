from __future__ import annotations

import html
import json
import os
import re
import sqlite3
import textwrap
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage

from rca.dummy_data import DUMMY_GITHUB_CONTEXT, SEED_LOGS
from rca.models import LogRecord

HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sanitize_text(raw: str) -> str:
    without_tags = HTML_TAG_PATTERN.sub(" ", raw)
    unescaped = html.unescape(without_tags)
    return WHITESPACE_PATTERN.sub(" ", unescaped).strip()


def _http_get_json(url: str, timeout: float = 8.0) -> dict[str, Any]:
    request = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _http_post_json(url: str, payload: dict[str, Any], timeout: float = 12.0) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


class SQLiteLogProvider:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or (_repo_root() / "data" / "rca_logs.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_db()

    def _initialize_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    log_id TEXT PRIMARY KEY,
                    service TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    endpoint_or_job TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    log_lines_json TEXT NOT NULL,
                    stack_trace_json TEXT NOT NULL
                )
                """
            )

            count = conn.execute("SELECT COUNT(1) FROM logs").fetchone()
            total_rows = int(count[0]) if count else 0
            if total_rows > 0:
                return

            for record in SEED_LOGS.values():
                conn.execute(
                    """
                    INSERT INTO logs (
                        log_id, service, timestamp, endpoint_or_job, correlation_id,
                        log_lines_json, stack_trace_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.log_id,
                        record.service,
                        record.timestamp,
                        record.endpoint_or_job,
                        record.correlation_id,
                        json.dumps(record.log_lines),
                        json.dumps(record.stack_trace),
                    ),
                )

    def get_log(self, log_id: str) -> LogRecord | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT log_id, service, timestamp, endpoint_or_job, correlation_id, log_lines_json, stack_trace_json
                FROM logs
                WHERE log_id = ?
                """,
                (log_id,),
            ).fetchone()

        if not row:
            return None

        return LogRecord(
            log_id=str(row[0]),
            service=str(row[1]),
            timestamp=str(row[2]),
            endpoint_or_job=str(row[3]),
            correlation_id=str(row[4]),
            log_lines=json.loads(str(row[5])),
            stack_trace=json.loads(str(row[6])),
        )

    def available_log_ids(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT log_id FROM logs ORDER BY log_id").fetchall()
        return [str(item[0]) for item in rows]


class DummyGitHubContextProvider:
    def get_context(
        self, stack_file_path: str, line_number: int | None, function_name: str | None
    ) -> dict[str, Any]:
        record = DUMMY_GITHUB_CONTEXT.get(stack_file_path)
        if not record:
            return {
                "status": "unresolved",
                "reason": f"No dummy GitHub context configured for '{stack_file_path}'.",
            }
        return {
            "status": "resolved",
            "repo": record["repo"],
            "branch": record["branch"],
            "file_path": record["file_path"],
            "directory": record["directory"],
            "function_or_class": function_name or "",
            "line_number": line_number,
            "snippet": record["snippet"],
            "source_url": record["source_url"],
        }


class WebSearchProvider:
    DEFAULT_TAVILY_API_KEY = "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"

    def __init__(self) -> None:
        self.api_key = os.getenv("TAVILY_API_KEY", self.DEFAULT_TAVILY_API_KEY).strip()
        self.search_url = os.getenv("TAVILY_SEARCH_URL", "https://api.tavily.com/search").strip()

    def search_probable_fixes(self, query: str, limit: int = 3) -> list[dict[str, Any]]:
        if not self.api_key:
            return [
                {
                    "source": "Tavily",
                    "title": "Tavily API key missing",
                    "url": "",
                    "summary": "Set TAVILY_API_KEY to enable web search.",
                    "probable_fix": "",
                }
            ]

        payload = {
            "api_key": self.api_key,
            "query": query,
            "search_depth": "advanced",
            "max_results": limit,
            "include_answer": True,
            "include_raw_content": False,
        }

        try:
            response = _http_post_json(self.search_url, payload=payload, timeout=12.0)
        except Exception as exc:  # pragma: no cover - network condition dependent
            return [
                {
                    "source": "Tavily",
                    "title": "Tavily search unavailable",
                    "url": "",
                    "summary": f"Tavily lookup failed: {exc}",
                    "probable_fix": "",
                }
            ]

        answer = _sanitize_text(str(response.get("answer", "")))
        findings: list[dict[str, Any]] = []
        results = response.get("results", [])
        for result in results[:limit]:
            title = _sanitize_text(str(result.get("title", "")))
            link = str(result.get("url", ""))
            content = _sanitize_text(str(result.get("content", "")))
            probable_fix = textwrap.shorten(content, width=320, placeholder="...") if content else ""
            summary = probable_fix or answer or "Relevant Tavily result found; inspect source."
            findings.append(
                {
                    "source": "Tavily",
                    "title": title,
                    "url": link,
                    "summary": summary[:400],
                    "probable_fix": probable_fix[:400],
                }
            )

        if not findings and answer:
            findings.append(
                {
                    "source": "Tavily",
                    "title": "Tavily direct answer",
                    "url": "",
                    "summary": answer[:400],
                    "probable_fix": answer[:400],
                }
            )

        return findings


class AgentIntelligenceProvider:
    def __init__(self) -> None:
        self._llm = None

    def _get_llm(self):
        if self._llm is None:
            from model.load import load_model

            self._llm = load_model()
        return self._llm

    async def generate(self, inputs: dict[str, Any]) -> dict[str, Any]:
        try:
            return await self._generate_with_llm(inputs)
        except Exception as exc:
            raise RuntimeError(
                "Bedrock agent-intelligence generation failed; no heuristic fallback is enabled."
            ) from exc

    async def _generate_with_llm(self, inputs: dict[str, Any]) -> dict[str, Any]:
        llm = self._get_llm()
        prompt = (
            "You are an RCA expert. Produce strict JSON with keys: probable_root_cause,"
            " agent_solution, next_actions, preventive_actions, confidence.\n"
            "Use only provided context. Do not hallucinate missing facts.\n\n"
            f"Context JSON:\n{json.dumps(inputs, ensure_ascii=True)}"
        )
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        text = str(getattr(response, "content", "")).strip()
        parsed = self._extract_json(text)

        probable_root_cause = str(parsed.get("probable_root_cause", "")).strip()
        agent_solution = str(parsed.get("agent_solution", "")).strip()
        next_actions = parsed.get("next_actions", [])
        preventive_actions = parsed.get("preventive_actions", [])
        confidence = parsed.get("confidence", 0.6)

        if not probable_root_cause or not agent_solution:
            raise ValueError("Model response missing probable_root_cause or agent_solution.")

        return {
            "probable_root_cause": probable_root_cause,
            "agent_solution": agent_solution,
            "next_actions": next_actions if isinstance(next_actions, list) else [],
            "preventive_actions": preventive_actions
            if isinstance(preventive_actions, list)
            else [],
            "confidence": float(confidence) if isinstance(confidence, (int, float)) else 0.6,
        }

    def _extract_json(self, text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise ValueError("Model output did not contain valid JSON.")
        return json.loads(match.group(0))
