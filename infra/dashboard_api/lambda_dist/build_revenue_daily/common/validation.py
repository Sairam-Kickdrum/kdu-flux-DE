from datetime import datetime
from typing import Any, Dict, Optional, Tuple


ALLOWED_DOMAINS = {"client_upload", "adscribe", "ascribe"}
ALLOWED_CLIENTS = {"all", "alpha", "beta", "gamma"}
ALLOWED_SORT_ORDER = {"asc", "desc"}

# UI-friendly aliases are mapped to real table columns.
BREAKDOWN_DIMENSIONS = {
    "client_upload": {
        "discount_code": "discount_code",
    },
    "adscribe": {
        "client_name": "client_name",
        "show_name": "show_name",
        "discount_code": "discount_code",
    },
}

DETAIL_SORT_FIELDS = {
    "client_upload": {
        "date": "order_date",
        "order_date": "order_date",
        "client": "client_name",
        "client_name": "client_name",
        "discount_code": "discount_code",
        "revenue": "revenue",
        "orders": "orders",
    },
    "adscribe": {
        "date": "date",
        "client": "client_name",
        "client_name": "client_name",
        "show": "show_name",
        "show_name": "show_name",
        "discount_code": "discount_code",
        "revenue": "revenue",
        "orders": "orders",
        "impressions": "impressions",
        "revenue_per_order": "revenue_per_order",
        "revenue_per_impression": "revenue_per_impression",
        "impressions_per_order": "impressions_per_order",
    },
}


class ValidationError(ValueError):
    pass


def _parse_date(date_str: str, field_name: str) -> str:
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be in YYYY-MM-DD format") from exc
    return parsed.strftime("%Y-%m-%d")


def get_query_params(event: Dict[str, Any]) -> Dict[str, str]:
    return event.get("queryStringParameters") or {}


def parse_domain(params: Dict[str, str]) -> str:
    domain = (params.get("domain") or "client_upload").strip().lower()
    if domain not in ALLOWED_DOMAINS:
        raise ValidationError("domain must be one of: client_upload, adscribe")
    return "adscribe" if domain == "ascribe" else domain


def parse_date_filters(params: Dict[str, str], domain: str) -> Tuple[Optional[str], Optional[str]]:
    start_date_raw = params.get("start_date")
    end_date_raw = params.get("end_date")

    if domain == "adscribe":
        if not start_date_raw or not end_date_raw:
            raise ValidationError("start_date and end_date are required for domain 'adscribe'")

    start_date = _parse_date(start_date_raw, "start_date") if start_date_raw else None
    end_date = _parse_date(end_date_raw, "end_date") if end_date_raw else None

    if start_date and end_date and start_date > end_date:
        raise ValidationError("start_date must be less than or equal to end_date")

    return start_date, end_date


def parse_client_upload_client(params: Dict[str, str], domain: str) -> str:
    client = (params.get("client") or "all").strip().lower()
    if domain != "client_upload":
        return client
    if client not in ALLOWED_CLIENTS:
        raise ValidationError("client must be one of: all, alpha, beta, gamma")
    return client


def parse_optional_ascribe_filters(params: Dict[str, str], domain: str) -> Dict[str, Optional[str]]:
    if domain != "adscribe":
        return {
            "client_name": None,
            "show_name": None,
            "discount_code": None,
        }
    return {
        "client_name": (params.get("client_name") or "").strip() or None,
        "show_name": (params.get("show_name") or "").strip() or None,
        "discount_code": (params.get("discount_code") or "").strip() or None,
    }


def parse_pagination(params: Dict[str, str]) -> Tuple[int, int]:
    limit = int(params.get("limit", "50"))
    offset = int(params.get("offset", "0"))
    if limit < 1 or limit > 500:
        raise ValidationError("limit must be between 1 and 500")
    if offset < 0:
        raise ValidationError("offset must be greater than or equal to 0")
    return limit, offset


def parse_sort_order(params: Dict[str, str]) -> str:
    sort_order = (params.get("sort_order") or "desc").strip().lower()
    if sort_order not in ALLOWED_SORT_ORDER:
        raise ValidationError("sort_order must be one of: asc, desc")
    return sort_order


def parse_details_sort_by(params: Dict[str, str], domain: str) -> str:
    raw_sort = (params.get("sort_by") or "date").strip().lower()
    field_map = DETAIL_SORT_FIELDS[domain]
    if raw_sort not in field_map:
        valid = ", ".join(sorted(field_map.keys()))
        raise ValidationError(f"sort_by '{raw_sort}' is not supported for domain '{domain}'. Supported: {valid}")
    return field_map[raw_sort]


def parse_breakdown_dimension(params: Dict[str, str], domain: str) -> str:
    raw_dimension = (params.get("dimension") or "discount_code").strip().lower()

    # dashboard alias support
    alias_map = {
        "show": "show_name",
        "product": "product",
    }
    normalized = alias_map.get(raw_dimension, raw_dimension)

    allowed = BREAKDOWN_DIMENSIONS[domain]
    if normalized not in allowed:
        valid = ", ".join(sorted(allowed.keys()))
        raise ValidationError(
            f"dimension '{raw_dimension}' is not supported for domain '{domain}'. Supported: {valid}"
        )
    return allowed[normalized]


def parse_top_n(params: Dict[str, str]) -> Optional[int]:
    top_n_raw = params.get("top_n")
    if not top_n_raw:
        return None
    top_n = int(top_n_raw)
    if top_n < 1 or top_n > 500:
        raise ValidationError("top_n must be between 1 and 500")
    return top_n


def build_filters_payload(
    domain: str,
    client: str,
    start_date: Optional[str],
    end_date: Optional[str],
    ascribe_filters: Dict[str, Optional[str]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "domain": domain,
        "start_date": start_date,
        "end_date": end_date,
    }
    if domain == "client_upload":
        payload["client"] = client
    else:
        payload.update(ascribe_filters)
    return payload
