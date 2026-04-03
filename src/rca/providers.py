from __future__ import annotations

import html
import json
import logging
import os
import re
import sqlite3
import textwrap
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage

from mcp_client.client import get_streamable_http_mcp_client
from rca.dummy_data import SEED_LOGS
from rca.models import LogRecord

HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")
STRUCTURED_LIST_KEYS = ("results", "items", "documents", "data", "hits", "sources")
logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sanitize_text(raw: str) -> str:
    without_tags = HTML_TAG_PATTERN.sub(" ", raw)
    unescaped = html.unescape(without_tags)
    return WHITESPACE_PATTERN.sub(" ", unescaped).strip()


def _shorten(text: str, width: int = 420) -> str:
    cleaned = _sanitize_text(text)
    return textwrap.shorten(cleaned, width=width, placeholder="...") if cleaned else ""


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


class GatewayGitHubContextProvider:
    REQUIRED_SUFFIXES = (
        "list_files",
        "get_file_content",
        "list_commits",
        "list_branches",
        "get_repo_info",
    )

    def __init__(self) -> None:
        self._client = get_streamable_http_mcp_client()
        self._llm = None
        self._tool_prefix = os.getenv("RCA_GITHUB_TOOL_PREFIX", "github-mcp-lambda___").strip()

    def _get_llm(self):
        if self._llm is None:
            from model.load import load_model

            self._llm = load_model()
        return self._llm

    async def get_context(
        self,
        stack_file_path: str,
        line_number: int | None,
        function_name: str | None,
        service_name: str,
        endpoint_or_job: str,
        key_error_message: str,
    ) -> dict[str, Any]:
        owner, repo = self._resolve_owner_repo()
        logger.info(
            "GitHub Worker: starting Gateway GitHub lookup (owner=%s, repo=%s, stack_path=%s).",
            owner,
            repo,
            stack_file_path,
        )

        tools = await self._client.get_tools()
        tool_map = self._resolve_tool_map(tools)
        logger.info(
            "GitHub Worker: resolved required Gateway tools=%s",
            list(tool_map.keys()),
        )

        repo_info = await self._call_tool(tool_map["get_repo_info"], {"owner": owner, "repo": repo})
        branches_data = await self._call_tool(
            tool_map["list_branches"],
            {"owner": owner, "repo": repo, "per_page": 50, "page": 1},
        )
        branch = self._select_branch(branches_data=branches_data, repo_info=repo_info)
        logger.info("GitHub Worker: selected branch '%s'.", branch)

        root_entries = await self._list_entries(
            list_tool=tool_map["list_files"],
            owner=owner,
            repo=repo,
            branch=branch,
            path="",
        )
        root_dirs = [entry.get("path", "") for entry in root_entries if entry.get("type") == "dir"]
        logger.info(
            "GitHub Worker: root inspection returned %s entries (%s dirs).",
            len(root_entries),
            len(root_dirs),
        )

        candidate_paths = self._build_candidate_paths(
            stack_file_path=stack_file_path,
            service_name=service_name,
            root_entries=root_entries,
        )
        logger.info(
            "GitHub Worker: generated %s direct candidate path(s).",
            len(candidate_paths),
        )

        candidate_files = await self._fetch_candidate_files(
            get_file_tool=tool_map["get_file_content"],
            owner=owner,
            repo=repo,
            branch=branch,
            candidate_paths=candidate_paths,
        )

        if not candidate_files:
            basename = self._basename(stack_file_path)
            if basename:
                logger.info(
                    "GitHub Worker: no direct file hit; scanning repo tree for basename '%s'.",
                    basename,
                )
                scanned_paths = await self._scan_for_basename(
                    list_tool=tool_map["list_files"],
                    owner=owner,
                    repo=repo,
                    branch=branch,
                    root_entries=root_entries,
                    basename=basename,
                )
                if scanned_paths:
                    logger.info(
                        "GitHub Worker: basename scan found %s candidate path(s).",
                        len(scanned_paths),
                    )
                candidate_files = await self._fetch_candidate_files(
                    get_file_tool=tool_map["get_file_content"],
                    owner=owner,
                    repo=repo,
                    branch=branch,
                    candidate_paths=scanned_paths,
                )

        if not candidate_files:
            raise RuntimeError(
                "GitHub Worker could not resolve any candidate file using Gateway tools."
            )

        ranking = await self._rank_candidates_with_model(
            stack_file_path=stack_file_path,
            function_name=function_name or "",
            line_number=line_number,
            service_name=service_name,
            endpoint_or_job=endpoint_or_job,
            key_error_message=key_error_message,
            candidates=candidate_files,
        )
        ranked_candidates = ranking.get("ranked_candidates", [])
        if not isinstance(ranked_candidates, list) or not ranked_candidates:
            raise RuntimeError(
                "GitHub Worker model ranking did not return ranked_candidates."
            )

        selected = ranked_candidates[0] if isinstance(ranked_candidates[0], dict) else {}
        selected_path = str(
            selected.get("file_path") or ranking.get("selected_file_path") or ""
        ).strip()
        if not selected_path:
            raise RuntimeError("GitHub Worker model ranking did not select a file_path.")

        selected_meta = next(
            (item for item in candidate_files if str(item.get("file_path")) == selected_path),
            candidate_files[0],
        )

        commits = await self._safe_list_commits(
            list_commits_tool=tool_map["list_commits"],
            owner=owner,
            repo=repo,
            branch=branch,
            path=selected_path,
        )
        logger.info(
            "GitHub Worker: selected file '%s' with %s ranked candidate(s).",
            selected_path,
            len(ranked_candidates),
        )

        return {
            "status": "resolved",
            "repo": f"{owner}/{repo}",
            "branch": branch,
            "file_path": selected_path,
            "directory": "/".join(selected_path.split("/")[:-1]),
            "function_or_class": str(
                selected.get("function_or_class")
                or ranking.get("selected_function_or_class")
                or function_name
                or ""
            ),
            "line_number": self._to_int(
                selected.get("line_hint") or ranking.get("selected_line_hint") or line_number
            ),
            "snippet": str(selected_meta.get("snippet", "")),
            "source_url": f"https://github.com/{owner}/{repo}/blob/{branch}/{selected_path}",
            "ranked_candidates": ranked_candidates[:5],
            "repo_structure_sample": [entry.get("path", "") for entry in root_entries[:20]],
            "commits_sample": commits[:5],
            "tools_used": [f"{self._tool_prefix}{suffix}" for suffix in self.REQUIRED_SUFFIXES],
        }

    def _resolve_owner_repo(self) -> tuple[str, str]:
        raw = os.getenv("RCA_GITHUB_REPO", "").strip()
        if not raw or "/" not in raw:
            raise ValueError(
                "RCA_GITHUB_REPO must be configured as 'owner/repo' for GitHub Gateway worker."
            )
        owner, repo = raw.split("/", 1)
        owner = owner.strip()
        repo = repo.strip()
        if not owner or not repo:
            raise ValueError("RCA_GITHUB_REPO is invalid. Expected 'owner/repo'.")
        return owner, repo

    def _resolve_tool_map(self, tools: Sequence[Any]) -> dict[str, Any]:
        by_name = {str(getattr(tool, "name", "")): tool for tool in tools}
        result: dict[str, Any] = {}
        missing: list[str] = []
        for suffix in self.REQUIRED_SUFFIXES:
            exact = f"{self._tool_prefix}{suffix}"
            tool = by_name.get(exact)
            if not tool:
                missing.append(exact)
                continue
            result[suffix] = tool
        if missing:
            available = sorted(name for name in by_name.keys() if name)
            raise RuntimeError(
                "GitHub Gateway tools missing. Expected tools: "
                f"{missing}. Available tools: {available}"
            )
        return result

    async def _call_tool(self, tool: Any, args: dict[str, Any]) -> Any:
        tool_name = str(getattr(tool, "name", "unknown"))
        logger.debug("GitHub Worker: invoking '%s' with args keys=%s", tool_name, sorted(args.keys()))
        raw = await tool.ainvoke(args)
        payload = self._extract_payload(raw)
        if isinstance(payload, dict) and payload.get("ok") is False:
            raise RuntimeError(
                f"GitHub Gateway tool '{tool_name}' returned error: {payload.get('error')}"
            )
        if isinstance(payload, dict) and "data" in payload:
            return payload.get("data")
        return payload

    def _extract_payload(self, raw: Any) -> Any:
        content = raw
        artifact_structured: dict[str, Any] = {}

        if isinstance(raw, tuple) and len(raw) == 2:
            content = raw[0]
            artifact = raw[1]
            if isinstance(artifact, dict):
                structured = artifact.get("structured_content")
                if isinstance(structured, dict):
                    artifact_structured = structured
        elif isinstance(raw, dict):
            structured = raw.get("structured_content")
            if isinstance(structured, dict):
                artifact_structured = structured

        if artifact_structured:
            return artifact_structured

        if isinstance(content, dict):
            return content

        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    if item.get("type") == "text" and isinstance(item.get("text"), str):
                        text = item.get("text", "").strip()
                        if not text:
                            continue
                        try:
                            return json.loads(text)
                        except Exception:
                            continue
                if isinstance(item, str):
                    text = item.strip()
                    if not text:
                        continue
                    try:
                        return json.loads(text)
                    except Exception:
                        continue
        return content

    def _select_branch(self, branches_data: Any, repo_info: Any) -> str:
        configured = os.getenv("RCA_GITHUB_BRANCH", "").strip()
        branch_names = self._extract_branch_names(branches_data)
        default_branch = self._extract_default_branch(repo_info)
        if configured and configured in branch_names:
            return configured
        if configured and not branch_names:
            return configured
        if default_branch:
            return default_branch
        if branch_names:
            return branch_names[0]
        return configured or "main"

    def _extract_default_branch(self, repo_info: Any) -> str:
        if isinstance(repo_info, dict):
            for key in ("default_branch", "defaultBranch", "default"):
                value = repo_info.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            nested = repo_info.get("repository")
            if isinstance(nested, dict):
                value = nested.get("default_branch")
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    def _extract_branch_names(self, branches_data: Any) -> list[str]:
        result: list[str] = []
        if isinstance(branches_data, list):
            for item in branches_data:
                if isinstance(item, dict):
                    name = item.get("name")
                    if isinstance(name, str) and name.strip():
                        result.append(name.strip())
        elif isinstance(branches_data, dict):
            for key in ("branches", "items", "data", "results"):
                value = branches_data.get(key)
                if isinstance(value, list):
                    result.extend(self._extract_branch_names(value))
        seen: set[str] = set()
        deduped: list[str] = []
        for name in result:
            if name in seen:
                continue
            seen.add(name)
            deduped.append(name)
        return deduped

    async def _list_entries(
        self,
        list_tool: Any,
        owner: str,
        repo: str,
        branch: str,
        path: str,
    ) -> list[dict[str, Any]]:
        args: dict[str, Any] = {"owner": owner, "repo": repo, "path": path, "ref": branch}
        data = await self._call_tool(list_tool, args)
        return self._extract_entries(data, parent_path=path)

    def _extract_entries(self, payload: Any, parent_path: str) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []

        if isinstance(payload, list):
            source = payload
        elif isinstance(payload, dict):
            source = None
            for key in ("entries", "items", "data", "results", "files", "tree"):
                value = payload.get(key)
                if isinstance(value, list):
                    source = value
                    break
            if source is None:
                return entries
        else:
            return entries

        for item in source:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            path = str(item.get("path", "")).strip()
            item_type = str(item.get("type", "")).strip().lower()
            if not path and name:
                if parent_path:
                    path = f"{parent_path.rstrip('/')}/{name}"
                else:
                    path = name
            if not path:
                continue
            if item_type not in {"file", "dir"}:
                if name and "." in name:
                    item_type = "file"
                else:
                    item_type = "dir"
            entries.append({"path": path, "name": name or path.split("/")[-1], "type": item_type})
        return entries

    def _build_candidate_paths(
        self,
        stack_file_path: str,
        service_name: str,
        root_entries: list[dict[str, Any]],
    ) -> list[str]:
        service_hint = service_name.replace("-service", "").replace("_service", "").strip()
        normalized = stack_file_path.strip().replace("\\", "/").lstrip("/")
        base = normalized.split("/")[-1] if normalized else ""

        candidates: list[str] = []

        def add(path: str) -> None:
            clean = path.strip().replace("\\", "/").lstrip("/")
            if not clean:
                return
            if clean not in candidates:
                candidates.append(clean)

        add(normalized)

        if normalized.startswith("srv/"):
            after_srv = normalized[len("srv/") :]
            add(after_srv)
            add(f"services/{after_srv}")
            add(f"src/{after_srv}")
            add(f"app/{after_srv}")

        if base:
            add(base)

        root_dirs = [item.get("path", "") for item in root_entries if item.get("type") == "dir"]
        if service_hint:
            for root_dir in root_dirs:
                add(f"{root_dir.rstrip('/')}/{service_hint}/{base}")
                add(f"{root_dir.rstrip('/')}/{service_hint}/handlers/{base}")
                add(f"{root_dir.rstrip('/')}/{service_hint}/repository/{base}")
                add(f"{root_dir.rstrip('/')}/{service_hint}/integrations/{base}")

        return candidates

    async def _fetch_candidate_files(
        self,
        get_file_tool: Any,
        owner: str,
        repo: str,
        branch: str,
        candidate_paths: list[str],
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for path in candidate_paths[:40]:
            try:
                data = await self._call_tool(
                    get_file_tool,
                    {"owner": owner, "repo": repo, "path": path, "ref": branch},
                )
            except Exception:
                continue

            text = self._extract_file_text(data)
            if not text:
                continue

            results.append(
                {
                    "file_path": path,
                    "snippet": text[:1400],
                    "raw_length": len(text),
                }
            )
        return results

    def _extract_file_text(self, data: Any) -> str:
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            for key in ("content", "decoded_content", "text", "body", "file_content"):
                value = data.get(key)
                if isinstance(value, str) and value.strip():
                    return value
        return ""

    async def _scan_for_basename(
        self,
        list_tool: Any,
        owner: str,
        repo: str,
        branch: str,
        root_entries: list[dict[str, Any]],
        basename: str,
    ) -> list[str]:
        queue: list[tuple[str, int]] = []
        matches: list[str] = []
        for entry in root_entries:
            if entry.get("type") == "dir":
                queue.append((str(entry.get("path", "")), 1))

        visited: set[str] = set()
        max_nodes = 80
        nodes = 0
        while queue and len(matches) < 12 and nodes < max_nodes:
            current, depth = queue.pop(0)
            current = current.strip("/")
            if not current or current in visited:
                continue
            visited.add(current)
            nodes += 1

            try:
                entries = await self._list_entries(
                    list_tool=list_tool,
                    owner=owner,
                    repo=repo,
                    branch=branch,
                    path=current,
                )
            except Exception:
                continue

            for entry in entries:
                path = str(entry.get("path", ""))
                name = str(entry.get("name", ""))
                item_type = str(entry.get("type", ""))
                if item_type == "file" and name == basename:
                    if path not in matches:
                        matches.append(path)
                elif item_type == "dir" and depth < 3:
                    queue.append((path, depth + 1))
        return matches

    async def _rank_candidates_with_model(
        self,
        stack_file_path: str,
        function_name: str,
        line_number: int | None,
        service_name: str,
        endpoint_or_job: str,
        key_error_message: str,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        llm = self._get_llm()
        payload = {
            "stack_file_path": stack_file_path,
            "function_name": function_name,
            "line_number": line_number,
            "service_name": service_name,
            "endpoint_or_job": endpoint_or_job,
            "key_error_message": key_error_message,
            "candidates": [
                {
                    "file_path": str(item.get("file_path", "")),
                    "snippet": str(item.get("snippet", ""))[:900],
                }
                for item in candidates[:12]
            ],
        }
        prompt = (
            "You are a code locator for incident RCA.\n"
            "Given stack trace hints and candidate GitHub files, return strict JSON with keys:\n"
            "selected_file_path, selected_function_or_class, selected_line_hint, ranked_candidates, summary.\n"
            "ranked_candidates must be a list of objects with keys: file_path, confidence, reason, "
            "function_or_class, line_hint.\n"
            "Use only candidate file paths provided.\n\n"
            f"Input JSON:\n{json.dumps(payload, ensure_ascii=True)}"
        )
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        text = str(getattr(response, "content", "")).strip()
        parsed = self._extract_json(text)
        return parsed

    def _extract_json(self, text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise ValueError("Model output did not contain valid JSON.")
        return json.loads(match.group(0))

    async def _safe_list_commits(
        self,
        list_commits_tool: Any,
        owner: str,
        repo: str,
        branch: str,
        path: str,
    ) -> list[dict[str, Any]]:
        try:
            data = await self._call_tool(
                list_commits_tool,
                {
                    "owner": owner,
                    "repo": repo,
                    "path": path,
                    "sha": branch,
                    "per_page": 5,
                    "page": 1,
                },
            )
        except Exception:
            return []

        commits: list[dict[str, Any]] = []
        source = data
        if isinstance(data, dict):
            for key in ("items", "commits", "data", "results"):
                value = data.get(key)
                if isinstance(value, list):
                    source = value
                    break

        if isinstance(source, list):
            for item in source[:5]:
                if not isinstance(item, dict):
                    continue
                commits.append(
                    {
                        "sha": str(item.get("sha", "")),
                        "message": str(
                            item.get("message")
                            or item.get("commit", {}).get("message", "")
                            if isinstance(item.get("commit"), dict)
                            else item.get("message", "")
                        ),
                    }
                )
        return commits

    def _basename(self, path: str) -> str:
        cleaned = path.strip().replace("\\", "/")
        return cleaned.split("/")[-1] if cleaned else ""

    def _to_int(self, value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            if value.isdigit():
                return int(value)
        return None


class WebSearchProvider:
    def __init__(self) -> None:
        self._client = get_streamable_http_mcp_client()

    async def search_probable_fixes(self, query: str, limit: int = 3) -> list[dict[str, Any]]:
        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("Tavily Gateway search query was empty.")
        logger.info(
            "Research Worker: starting Tavily Gateway search (query_len=%s, limit=%s).",
            len(normalized_query),
            limit,
        )

        try:
            tools = await self._client.get_tools()
        except Exception as exc:
            logger.exception("Research Worker: Gateway tools/list failed.")
            raise RuntimeError(
                f"Unable to load tools from AgentCore Gateway via tools/list: {exc}"
            ) from exc

        logger.info("Research Worker: Gateway tools/list returned %s tool(s).", len(tools))
        tavily_tools = self._discover_tavily_tools(tools)
        if not tavily_tools:
            available = [str(getattr(tool, "name", "")) for tool in tools]
            logger.error(
                "Research Worker: no Tavily-prefixed tools found. Available tools=%s",
                available,
            )
            raise RuntimeError(
                "No Tavily-prefixed tool was exposed by AgentCore Gateway. "
                f"Available tools: {available}"
            )

        logger.info(
            "Research Worker: Tavily candidate tools in ranked order=%s",
            [str(getattr(tool, "name", "")) for tool in tavily_tools],
        )
        failure_messages: list[str] = []
        for tool in tavily_tools:
            tool_name = str(getattr(tool, "name", "unknown"))
            arg_attempts = self._build_argument_attempts(
                tool=tool,
                query=normalized_query,
                limit=limit,
            )
            logger.info(
                "Research Worker: trying tool '%s' with %s argument attempt(s).",
                tool_name,
                len(arg_attempts),
            )
            raw_result, used_args, error = await self._invoke_with_attempts(tool, arg_attempts)
            if raw_result is None:
                logger.warning(
                    "Research Worker: tool '%s' invocation failed across all attempts.",
                    tool_name,
                )
                failure_messages.append(
                    f"{tool_name}: invoke failed ({error}) with attempts {arg_attempts}"
                )
                continue

            findings = self._normalize_findings(raw_result, tool_name=tool_name)
            if not findings:
                logger.warning(
                    "Research Worker: tool '%s' returned no parsable findings.",
                    tool_name,
                )
                failure_messages.append(
                    f"{tool_name}: returned no parsable findings (args {used_args})"
                )
                continue

            logger.info(
                "Research Worker: tool '%s' succeeded with %s finding(s).",
                tool_name,
                len(findings),
            )
            return findings[: max(1, limit)]

        logger.error(
            "Research Worker: all Tavily Gateway tools failed. Failures=%s",
            failure_messages,
        )
        raise RuntimeError(
            "All Tavily Gateway tools failed for query-based research. "
            f"Failures: {failure_messages}"
        )

    def _discover_tavily_tools(self, tools: Sequence[Any]) -> list[Any]:
        def score(tool: Any) -> tuple[int, int, int, int, str]:
            name = str(getattr(tool, "name", "")).strip()
            lowered = name.lower()
            schema = self._extract_schema(tool)
            props = schema.get("properties", {}) if isinstance(schema, dict) else {}
            required = schema.get("required", []) if isinstance(schema, dict) else []
            required_set = {str(item).lower() for item in required} if isinstance(required, list) else set()
            prop_keys = {str(key).lower() for key in props.keys()} if isinstance(props, dict) else set()

            has_query_param = self._pick_key(
                props if isinstance(props, dict) else {},
                candidates=["query", "q", "search_query", "keywords", "input", "text", "question"],
            ) is not None
            has_urls_required = "urls" in required_set
            has_urls_param = "urls" in prop_keys

            is_search_named = 1 if "search" in lowered else 0
            # Prefer query-compatible and avoid extract-style urls-only tools.
            query_penalty = 0 if has_query_param else 1
            urls_penalty = 1 if (has_urls_required and not has_query_param) else 0
            starts_penalty = 0 if lowered.startswith("tavily___") else 1
            # Lower tuple sorts first.
            return (
                query_penalty,
                urls_penalty,
                -is_search_named,
                starts_penalty,
                lowered,
            )

        matches = []
        for tool in tools:
            name = str(getattr(tool, "name", "")).lower()
            if "tavily" in name:
                matches.append(tool)
        return sorted(matches, key=score)

    async def _invoke_with_attempts(
        self, tool: Any, attempts: list[dict[str, Any]]
    ) -> tuple[Any | None, dict[str, Any], Exception | None]:
        last_error: Exception | None = None
        tool_name = str(getattr(tool, "name", "unknown"))
        for index, args in enumerate(attempts, start=1):
            logger.debug(
                "Research Worker: invoking '%s' attempt %s/%s with args keys=%s",
                tool_name,
                index,
                len(attempts),
                sorted(list(args.keys())),
            )
            try:
                return await tool.ainvoke(args), args, None
            except Exception as exc:
                last_error = exc
                logger.debug(
                    "Research Worker: '%s' attempt %s failed: %s",
                    tool_name,
                    index,
                    exc,
                )
        return None, {}, last_error

    def _build_argument_attempts(
        self, tool: Any, query: str, limit: int
    ) -> list[dict[str, Any]]:
        schema = self._extract_schema(tool)
        properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
        required = schema.get("required", []) if isinstance(schema, dict) else []
        required_set = {str(item) for item in required} if isinstance(required, list) else set()

        schema_args: dict[str, Any] = {}
        query_key = self._pick_key(
            properties,
            candidates=["query", "q", "search_query", "keywords", "input", "text", "question"],
        )
        if query_key:
            schema_args[query_key] = query

        limit_key = self._pick_key(
            properties,
            candidates=[
                "max_results",
                "limit",
                "count",
                "top_k",
                "k",
                "num_results",
                "max_items",
            ],
        )
        if limit_key:
            schema_args[limit_key] = max(1, limit)

        for candidate_key in ["search_depth", "include_answer", "include_raw_content"]:
            if candidate_key in properties and candidate_key not in schema_args:
                schema_args[candidate_key] = self._default_property_value(
                    prop_schema=properties.get(candidate_key, {}),
                    query=query,
                    limit=limit,
                )

        for key in required_set:
            if key in schema_args:
                continue
            schema_args[key] = self._default_property_value(
                prop_schema=properties.get(key, {}),
                query=query,
                limit=limit,
            )

        attempts: list[dict[str, Any]] = []
        if schema_args:
            attempts.append({k: v for k, v in schema_args.items() if v is not None})

        attempts.extend(
            [
                {"query": query, "max_results": max(1, limit)},
                {"query": query},
                {"q": query, "max_results": max(1, limit)},
                {"q": query},
                {"search_query": query, "max_results": max(1, limit)},
                {"search_query": query},
            ]
        )

        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for args in attempts:
            try:
                signature = json.dumps(args, sort_keys=True, default=str)
            except Exception:
                signature = str(args)
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(args)
        return deduped

    def _extract_schema(self, tool: Any) -> dict[str, Any]:
        args_schema = getattr(tool, "args_schema", None)
        if args_schema is not None and hasattr(args_schema, "model_json_schema"):
            try:
                schema = args_schema.model_json_schema()
                if isinstance(schema, dict):
                    return schema
            except Exception:
                pass

        raw_args = getattr(tool, "args", None)
        if isinstance(raw_args, dict):
            return {"type": "object", "properties": raw_args}
        return {"type": "object", "properties": {}}

    def _pick_key(self, properties: dict[str, Any], candidates: list[str]) -> str | None:
        if not properties:
            return None

        exact_index = {key.lower(): key for key in properties.keys()}
        for candidate in candidates:
            if candidate in exact_index:
                return exact_index[candidate]

        for key in properties.keys():
            lowered = key.lower()
            if any(candidate in lowered for candidate in candidates):
                return key
        return None

    def _default_property_value(
        self, prop_schema: dict[str, Any], query: str, limit: int
    ) -> Any:
        if not isinstance(prop_schema, dict):
            return None

        enum_values = prop_schema.get("enum")
        if isinstance(enum_values, list) and enum_values:
            lowered = {str(item).lower(): item for item in enum_values}
            if "advanced" in lowered:
                return lowered["advanced"]
            if "basic" in lowered:
                return lowered["basic"]
            return enum_values[0]

        prop_type = str(prop_schema.get("type", "")).lower()
        if prop_type in {"integer", "number"}:
            return max(1, limit)
        if prop_type == "boolean":
            return False
        if prop_type == "array":
            return [query]
        if prop_type == "string" or not prop_type:
            return query
        return None

    def _normalize_findings(self, raw_result: Any, tool_name: str) -> list[dict[str, Any]]:
        content, artifact = self._split_tool_result(raw_result)
        records = self._extract_structured_records(artifact)
        findings: list[dict[str, Any]] = []

        for record in records:
            title = self._pick_text(
                record,
                keys=("title", "name", "headline", "topic"),
                fallback="Tavily result",
            )
            url = self._pick_text(
                record,
                keys=("url", "link", "source_url", "source"),
                fallback="",
            )
            summary = self._pick_text(
                record,
                keys=("summary", "snippet", "content", "text", "description"),
                fallback="Relevant web finding returned by Tavily through Gateway.",
            )
            probable_fix = self._pick_text(
                record,
                keys=("probable_fix", "fix", "solution", "recommendation", "summary", "snippet"),
                fallback=summary,
            )
            findings.append(
                {
                    "source": "Tavily (Gateway)",
                    "title": _shorten(title, width=180),
                    "url": url,
                    "summary": _shorten(summary),
                    "probable_fix": _shorten(probable_fix),
                    "tool": tool_name,
                }
            )

        if findings:
            return findings

        text_chunks = self._extract_text_blocks(content)
        if not text_chunks:
            text_chunks = self._extract_text_blocks(artifact)

        if text_chunks:
            combined = _shorten(" ".join(text_chunks))
            return [
                {
                    "source": "Tavily (Gateway)",
                    "title": f"{tool_name} response",
                    "url": "",
                    "summary": combined,
                    "probable_fix": combined,
                    "tool": tool_name,
                }
            ]
        return []

    def _split_tool_result(self, raw_result: Any) -> tuple[Any, dict[str, Any]]:
        content = raw_result
        artifact_structured: dict[str, Any] = {}

        if isinstance(raw_result, tuple) and len(raw_result) == 2:
            content = raw_result[0]
            artifact_candidate = raw_result[1]
            if isinstance(artifact_candidate, dict):
                structured = artifact_candidate.get("structured_content")
                if isinstance(structured, dict):
                    artifact_structured = structured
        elif isinstance(raw_result, dict):
            structured = raw_result.get("structured_content")
            if isinstance(structured, dict):
                artifact_structured = structured
            elif isinstance(raw_result.get("artifact"), dict):
                nested = raw_result["artifact"].get("structured_content")
                if isinstance(nested, dict):
                    artifact_structured = nested

        return content, artifact_structured

    def _extract_structured_records(self, payload: Any) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                for key in STRUCTURED_LIST_KEYS:
                    maybe_list = node.get(key)
                    if isinstance(maybe_list, list):
                        for item in maybe_list:
                            if isinstance(item, dict):
                                records.append(item)
                for value in node.values():
                    visit(value)
                return

            if isinstance(node, list):
                for item in node:
                    visit(item)

        visit(payload)
        return records

    def _extract_text_blocks(self, payload: Any) -> list[str]:
        chunks: list[str] = []

        def visit(node: Any) -> None:
            if isinstance(node, str):
                cleaned = _sanitize_text(node)
                if cleaned:
                    chunks.append(cleaned)
                return

            if isinstance(node, dict):
                node_type = str(node.get("type", "")).lower()
                if node_type == "text" and isinstance(node.get("text"), str):
                    cleaned = _sanitize_text(node["text"])
                    if cleaned:
                        chunks.append(cleaned)
                for value in node.values():
                    visit(value)
                return

            if isinstance(node, (list, tuple)):
                for item in node:
                    visit(item)

        visit(payload)
        return chunks

    def _pick_text(self, record: dict[str, Any], keys: tuple[str, ...], fallback: str) -> str:
        for key in keys:
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return fallback

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
