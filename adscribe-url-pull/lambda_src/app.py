from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from urllib import error, request


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

WINDOW_START = date(2026, 3, 15)
WINDOW_END = date(2026, 4, 15)
API_URL = os.environ["ADSCRIBE_API_URL"]
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_RANGE_DAYS = int(os.getenv("MAX_RANGE_DAYS", "7"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))


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

    return {
        "statusCode": status_code,
        "body": json.dumps(response_body),
    }
