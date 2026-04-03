from __future__ import annotations

from rca.models import LogRecord


SEED_LOGS: dict[str, LogRecord] = {
    "LOG-1001": LogRecord(
        log_id="LOG-1001",
        service="payments-service",
        timestamp="2026-03-30T08:14:22Z",
        endpoint_or_job="POST /payments/charge",
        correlation_id="corr-pay-8f4c92a1",
        log_lines=[
            "INFO request_id=9d13 start charge request for user_id=U-5531",
            "DEBUG profile_cache lookup completed in 2ms",
            "ERROR charge request failed while reading customer tier from profile",
        ],
        stack_trace=[
            "Traceback (most recent call last):",
            '  File "/srv/payments/handlers/charge_handler.py", line 142, in create_charge',
            '    customer_tier = user_profile.get("tier")',
            "AttributeError: 'NoneType' object has no attribute 'get'",
        ],
    ),
    "LOG-1002": LogRecord(
        log_id="LOG-1002",
        service="orders-service",
        timestamp="2026-03-30T09:48:10Z",
        endpoint_or_job="job: order-reconciliation-worker",
        correlation_id="corr-ord-41cd117e",
        log_lines=[
            "INFO reconciliation batch_id=B-291 started",
            "WARN database call latency exceeded 28s threshold",
            "ERROR reconciliation batch aborted due to DB timeout",
        ],
        stack_trace=[
            "Traceback (most recent call last):",
            '  File "/srv/orders/repository/order_repository.py", line 77, in fetch_pending_orders',
            "    rows = session.execute(query).fetchall()",
            "sqlalchemy.exc.TimeoutError: QueuePool limit reached, connection timed out, timeout 30.00",
        ],
    ),
    "LOG-1003": LogRecord(
        log_id="LOG-1003",
        service="gateway-service",
        timestamp="2026-03-30T11:02:03Z",
        endpoint_or_job="GET /api/v1/claims",
        correlation_id="corr-gw-7a6df4ea",
        log_lines=[
            "INFO incoming request from client=mobile-app",
            "WARN token validation failed for authorization header",
            "ERROR request rejected with 401 Unauthorized",
        ],
        stack_trace=[
            "Traceback (most recent call last):",
            '  File "/srv/gateway/middleware/token_verifier.py", line 55, in verify_jwt',
            "    decoded = jwt.decode(token, secret, algorithms=['HS256'])",
            "jwt.exceptions.ExpiredSignatureError: Signature has expired",
        ],
    ),
    "LOG-1004": LogRecord(
        log_id="LOG-1004",
        service="document-service",
        timestamp="2026-03-30T12:21:49Z",
        endpoint_or_job="job: ingest-contract-pdf",
        correlation_id="corr-doc-3be917ce",
        log_lines=[
            "INFO ingest pipeline started for contract_id=C-8843",
            "DEBUG attempting S3 read bucket=contracts-prod key=2026/03/C-8843.pdf",
            "ERROR file fetch from S3 failed",
        ],
        stack_trace=[
            "Traceback (most recent call last):",
            '  File "/srv/document/storage/s3_reader.py", line 33, in read_contract_pdf',
            "    body = s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read()",
            "botocore.exceptions.ClientError: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.",
        ],
    ),
    "LOG-1005": LogRecord(
        log_id="LOG-1005",
        service="notification-service",
        timestamp="2026-03-30T13:33:17Z",
        endpoint_or_job="POST /notify/sms",
        correlation_id="corr-notify-bf12211a",
        log_lines=[
            "INFO sms dispatch attempt message_id=M-7102",
            "WARN provider latency crossed 6.5s",
            "ERROR sms dispatch failed due to upstream API error",
        ],
        stack_trace=[
            "Traceback (most recent call last):",
            '  File "/srv/notification/integrations/sms_client.py", line 91, in send_sms',
            "    response.raise_for_status()",
            "requests.exceptions.HTTPError: 503 Server Error: Service Unavailable for url: https://api.sms-provider.example/v1/messages",
        ],
    ),
    "LOG-2101": LogRecord(
        log_id="LOG-2101",
        service="calculator-service",
        timestamp="2026-04-03T10:15:42Z",
        endpoint_or_job="job: calculator/mock_calculator.py",
        correlation_id="corr-calc-2101",
        log_lines=[
            "INFO starting calculator mock run",
            "DEBUG NUMERATOR=42.0 DENOMINATOR=0.0",
            "ERROR calculator mock run failed due to runtime exception",
        ],
        stack_trace=[
            "Traceback (most recent call last):",
            '  File "/home/rahul/projects/rca-repo/calculator/mock_calculator.py", line 18, in <module>',
            "    main()",
            '  File "/home/rahul/projects/rca-repo/calculator/mock_calculator.py", line 14, in main',
            "    _ = NUMERATOR / DENOMINATOR",
            "        ~~~~~~~~~~^~~~~~~~~~~~~",
            "ZeroDivisionError: float division by zero",
        ],
    ),
}


DUMMY_GITHUB_CONTEXT: dict[str, dict[str, str]] = {
    "/srv/payments/handlers/charge_handler.py": {
        "repo": "org/rca-microservices",
        "branch": "main",
        "file_path": "services/payments/handlers/charge_handler.py",
        "directory": "services/payments/handlers",
        "snippet": (
            "138: user_profile = profile_cache.get(user_id)\n"
            "139: if user_profile is None:\n"
            "140:     raise ValueError('user profile missing')\n"
            "141: # bug-prone line from incident history\n"
            "142: customer_tier = user_profile.get('tier')"
        ),
        "source_url": "https://github.com/org/rca-microservices/blob/main/services/payments/handlers/charge_handler.py#L142",
    },
    "/srv/orders/repository/order_repository.py": {
        "repo": "org/rca-microservices",
        "branch": "main",
        "file_path": "services/orders/repository/order_repository.py",
        "directory": "services/orders/repository",
        "snippet": (
            "71: query = select(Order).where(Order.status == 'PENDING')\n"
            "72: # long-running query under peak batches\n"
            "73: rows = session.execute(query).fetchall()"
        ),
        "source_url": "https://github.com/org/rca-microservices/blob/main/services/orders/repository/order_repository.py#L73",
    },
    "/srv/gateway/middleware/token_verifier.py": {
        "repo": "org/rca-microservices",
        "branch": "main",
        "file_path": "services/gateway/middleware/token_verifier.py",
        "directory": "services/gateway/middleware",
        "snippet": (
            "52: decoded = jwt.decode(token, secret, algorithms=['HS256'])\n"
            "53: if decoded['aud'] != expected_audience:\n"
            "54:     raise UnauthorizedError('Invalid audience')"
        ),
        "source_url": "https://github.com/org/rca-microservices/blob/main/services/gateway/middleware/token_verifier.py#L52",
    },
    "/srv/document/storage/s3_reader.py": {
        "repo": "org/rca-microservices",
        "branch": "main",
        "file_path": "services/document/storage/s3_reader.py",
        "directory": "services/document/storage",
        "snippet": (
            "30: response = s3_client.get_object(Bucket=bucket_name, Key=object_key)\n"
            "31: body = response['Body'].read()\n"
            "32: return body"
        ),
        "source_url": "https://github.com/org/rca-microservices/blob/main/services/document/storage/s3_reader.py#L30",
    },
    "/srv/notification/integrations/sms_client.py": {
        "repo": "org/rca-microservices",
        "branch": "main",
        "file_path": "services/notification/integrations/sms_client.py",
        "directory": "services/notification/integrations",
        "snippet": (
            "88: response = requests.post(url, json=payload, timeout=5)\n"
            "89: response.raise_for_status()\n"
            "90: return response.json()"
        ),
        "source_url": "https://github.com/org/rca-microservices/blob/main/services/notification/integrations/sms_client.py#L88",
    },
}
