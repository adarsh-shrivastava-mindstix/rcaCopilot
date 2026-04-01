from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3


def _is_enabled() -> bool:
    raw = os.getenv("RCA_S3_PERSIST_ENABLED", "true").strip().lower()
    if "PYTEST_CURRENT_TEST" in os.environ:
        return False
    return raw not in {"0", "false", "no", "off"}


def _bucket() -> str:
    return os.getenv("RCA_S3_BUCKET", "agentcore-developers-bucket").strip()


def _prefix() -> str:
    value = os.getenv("RCA_S3_PREFIX", "rcaCopilot/").strip()
    if not value:
        return "rcaCopilot/"
    if not value.endswith("/"):
        return value + "/"
    return value


def _region() -> str:
    return (
        os.getenv("RCA_S3_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-west-2"
    )


def _build_key(report: dict[str, Any]) -> str:
    report_id = str(report.get("report_id", "unknown-report"))
    log_id = str(report.get("log_id", "unknown-log")) or "unknown-log"
    status = str(report.get("status", "unknown"))
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    filename = f"{report_id}_{log_id}_{status}.json".replace(" ", "_")
    return f"{_prefix()}{date_path}/{filename}"


def persist_report_to_s3(report: dict[str, Any]) -> dict[str, Any]:
    if not _is_enabled():
        return {
            "enabled": False,
            "stored": False,
            "reason": "RCA_S3_PERSIST_ENABLED is disabled.",
        }

    bucket = _bucket()
    key = _build_key(report)
    region = _region()

    if not bucket:
        return {"enabled": True, "stored": False, "error": "S3 bucket is empty or not configured."}

    body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    client = boto3.client("s3", region_name=region)

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return {
            "enabled": True,
            "stored": True,
            "bucket": bucket,
            "key": key,
            "region": region,
            "s3_uri": f"s3://{bucket}/{key}",
        }
    except Exception as exc:
        return {
            "enabled": True,
            "stored": False,
            "bucket": bucket,
            "key": key,
            "region": region,
            "error": str(exc),
        }

