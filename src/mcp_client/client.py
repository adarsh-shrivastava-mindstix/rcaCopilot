from __future__ import annotations

import logging
import os
from typing import Any

import boto3
import httpx
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from langchain_mcp_adapters.client import MultiServerMCPClient

DEFAULT_GATEWAY_URL = (
    "https://agent-gateway-developer-h2kdcyycva.gateway."
    "bedrock-agentcore.us-west-2.amazonaws.com/mcp"
)
DEFAULT_SERVER_NAME = "agentcore_gateway"
DEFAULT_SIGV4_SERVICE = "bedrock-agentcore"
DEFAULT_REGION = "us-west-2"
logger = logging.getLogger(__name__)


class AgentCoreGatewaySigV4Auth(httpx.Auth):
    requires_request_body = True

    def __init__(self, *, service: str, region: str) -> None:
        self._service = service
        self._region = region
        self._session = boto3.Session(region_name=region)

    def _resolve_frozen_credentials(self):
        credentials = self._session.get_credentials()
        if credentials is None:
            raise RuntimeError(
                "AWS credentials were not found for AgentCore Gateway IAM authentication."
            )
        return credentials.get_frozen_credentials()

    def auth_flow(self, request: httpx.Request):
        frozen = self._resolve_frozen_credentials()
        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers=dict(request.headers),
        )
        SigV4Auth(frozen, self._service, self._region).add_auth(aws_request)
        for key, value in aws_request.headers.items():
            request.headers[str(key)] = str(value)
        yield request


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _resolve_region() -> str:
    return (
        _env("RCA_GATEWAY_REGION", "")
        or _env("AWS_REGION", "")
        or _env("AWS_DEFAULT_REGION", "")
        or DEFAULT_REGION
    )


def _resolve_auth_type() -> str:
    return _env("RCA_GATEWAY_AUTH_TYPE", "iam").lower()


def get_streamable_http_mcp_client() -> MultiServerMCPClient:
    """
    Returns an MCP client configured to call Bedrock AgentCore Gateway.
    """
    gateway_url = _env("RCA_GATEWAY_URL", DEFAULT_GATEWAY_URL)
    server_name = _env("RCA_GATEWAY_SERVER_NAME", DEFAULT_SERVER_NAME)
    auth_type = _resolve_auth_type()
    region = _resolve_region()
    service = _env("RCA_GATEWAY_SIGV4_SERVICE", DEFAULT_SIGV4_SERVICE)

    connection: dict[str, Any] = {
        "transport": "streamable_http",
        "url": gateway_url,
    }

    if auth_type == "iam":
        connection["auth"] = AgentCoreGatewaySigV4Auth(service=service, region=region)
    elif auth_type == "bearer":
        token = _env("RCA_GATEWAY_BEARER_TOKEN", "")
        if token:
            connection["headers"] = {"Authorization": f"Bearer {token}"}
    elif auth_type not in {"none", ""}:
        raise ValueError(
            f"Unsupported RCA_GATEWAY_AUTH_TYPE '{auth_type}'. Use iam, bearer, or none."
        )

    logger.info(
        "Initialized AgentCore Gateway MCP client (server=%s, url=%s, auth_type=%s, region=%s, service=%s).",
        server_name,
        gateway_url,
        auth_type,
        region,
        service,
    )
    return MultiServerMCPClient({server_name: connection}, tool_name_prefix=False)
