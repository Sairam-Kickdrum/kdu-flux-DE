import json
from typing import Any, Dict, Optional


def _cors_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
    }


def make_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps(body, default=str),
    }


def success(domain: str, filters: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    return make_response(200, {"domain": domain, "filters": filters, "data": data})


def bad_request(message: str, code: str = "BAD_REQUEST", details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"error": {"code": code, "message": message}}
    if details:
        payload["error"]["details"] = details
    return make_response(400, payload)


def internal_error(message: str = "Internal server error") -> Dict[str, Any]:
    return make_response(500, {"error": {"code": "INTERNAL_ERROR", "message": message}})
