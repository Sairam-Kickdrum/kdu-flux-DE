from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from urllib import error, request

import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

WINDOW_START = date(2026, 3, 15)
WINDOW_END = date(2026, 4, 15)
API_URL = os.environ["ADSCRIBE_API_URL"]
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "7"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

DYNAMODB_CLIENT = boto3.client("dynamodb")
STEP_FUNCTIONS_CLIENT = boto3.client("stepfunctions")


def resolve_current_date(event: dict | None) -> date:
    override_date = (event or {}).get("today")
    if override_date is None:
        return datetime.now(timezone.utc).date()

    try:
        return date.fromisoformat(override_date)
    except ValueError as exc:
        raise ValueError("event.today must be a valid ISO date in YYYY-MM-DD format") from exc


def build_request_payload(current_date: date) -> dict[str, str]:
    end_date = current_date
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    if not WINDOW_START <= start_date <= WINDOW_END:
        raise ValueError(
            f"Calculated start_date {start_date.isoformat()} is outside the allowed window "
            f"{WINDOW_START.isoformat()} to {WINDOW_END.isoformat()}."
        )

    if not WINDOW_START <= end_date <= WINDOW_END:
        raise ValueError(
            f"Calculated end_date {end_date.isoformat()} is outside the allowed window "
            f"{WINDOW_START.isoformat()} to {WINDOW_END.isoformat()}."
        )

    if (end_date - start_date).days > MAX_RANGE_DAYS:
        raise ValueError(
            f"Calculated date range exceeds the maximum of {MAX_RANGE_DAYS} days."
        )

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def request_presigned_url(payload: dict[str, str]) -> tuple[int, dict]:
    body = json.dumps(payload).encode("utf-8")
    http_request = request.Request(
        API_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status_code = response.getcode()
            response_body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        LOGGER.exception("Adscribe API returned an HTTP error.")
        return exc.code, {
            "message": "Adscribe API request failed.",
            "details": error_body,
        }
    except error.URLError as exc:
        LOGGER.exception("Unable to reach the Adscribe API.")
        return 502, {
            "message": "Unable to reach the Adscribe API.",
            "details": str(exc.reason),
        }

    if not 200 <= status_code < 300:
        LOGGER.error("Adscribe API returned unexpected status code %s.", status_code)
        return 502, {
            "message": "Adscribe API returned an unexpected response.",
            "details": response_body,
        }

    try:
        parsed_body = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise ValueError("Adscribe API returned invalid JSON.") from exc

    required_fields = {"download_url", "expires_in_seconds", "start_date", "end_date"}
    missing_fields = sorted(required_fields.difference(parsed_body))
    if missing_fields:
        raise ValueError(
            f"Adscribe API response is missing required fields: {', '.join(missing_fields)}."
        )

    return status_code, parsed_body


def build_run_id() -> str:
    return f"ads_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def reserve_batch(
    batch_id: str,
    start_date: str,
    end_date: str,
    run_id: str,
    timestamp: str,
) -> bool:
    try:
        DYNAMODB_CLIENT.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                "key": {"S": batch_id},
                "source_type": {"S": "adscribe"},
                "client_name": {"S": "adscribe"},
                "status": {"S": "RECEIVED"},
                "start_date": {"S": start_date},
                "end_date": {"S": end_date},
                "run_id": {"S": run_id},
                "created_at": {"S": timestamp},
                "updated_at": {"S": timestamp},
            },
            ConditionExpression="attribute_not_exists(#k)",
            ExpressionAttributeNames={"#k": "key"},
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def start_raw_landing_execution(
    batch_id: str,
    start_date: str,
    end_date: str,
    presigned_url: str,
    run_id: str,
) -> dict[str, str]:
    return STEP_FUNCTIONS_CLIENT.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=run_id.replace("_", "-"),
        input=json.dumps(
            {
                "batch_id": batch_id,
                "start_date": start_date,
                "end_date": end_date,
                "presigned_url": presigned_url,
                "run_id": run_id,
            }
        ),
    )


def lambda_handler(event: dict | None, _context: object) -> dict[str, object]:
    LOGGER.info("Received event: %s", json.dumps(event or {}))

    try:
        current_date = resolve_current_date(event)
        payload = build_request_payload(current_date)
        status_code, response_body = request_presigned_url(payload)
    except ValueError as exc:
        LOGGER.exception("Request validation failed.")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(exc)}),
        }
    except Exception as exc:
        LOGGER.exception("Unexpected Lambda failure.")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Unhandled error while requesting the Adscribe CSV URL.",
                    "details": str(exc),
                }
            ),
        }

    start_date = response_body["start_date"]
    end_date = response_body["end_date"]
    presigned_url = response_body["download_url"]
    batch_id = f"ADSCRIBE#{start_date}#{end_date}"
    run_id = build_run_id()
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        is_new_batch = reserve_batch(batch_id, start_date, end_date, run_id, timestamp)
        if not is_new_batch:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "duplicate batch skipped",
                        "batch_id": batch_id,
                    }
                ),
            }

        execution_response = start_raw_landing_execution(
            batch_id=batch_id,
            start_date=start_date,
            end_date=end_date,
            presigned_url=presigned_url,
            run_id=run_id,
        )
    except Exception as exc:
        LOGGER.exception("Failed to reserve batch or start Step Functions execution.")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Failed to reserve Adscribe batch or start raw landing workflow.",
                    "details": str(exc),
                }
            ),
        }

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Adscribe raw landing workflow started.",
                "batch_id": batch_id,
                "run_id": run_id,
                "executionArn": execution_response["executionArn"],
            }
        ),
    }
