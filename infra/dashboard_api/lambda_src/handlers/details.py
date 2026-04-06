from common import db
from common.logging_utils import log_error, log_info
from common.responses import bad_request, internal_error, success
from common.validation import parse_common_filters, parse_pagination


def lambda_handler(event, context):
    try:
        query_params = event.get("queryStringParameters") or {}
        filters = parse_common_filters(query_params)
        limit, offset = parse_pagination(query_params)
        where_sql, parameters = db.build_where_clause(filters)

        params_with_pagination = list(parameters)
        params_with_pagination.append({"name": "limit", "value": {"longValue": limit}})
        params_with_pagination.append({"name": "offset", "value": {"longValue": offset}})

        sql = f"""
SELECT
  order_date AS date,
  client_name AS client,
  discount_code,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders
FROM {db.fact_table()}
{where_sql}
ORDER BY order_date DESC
LIMIT :limit OFFSET :offset
"""
        rows = db.query(sql, params_with_pagination)
        log_info("Details query succeeded", row_count=len(rows), limit=limit, offset=offset, filters=filters)
        return success({"rows": rows}, meta={"limit": limit, "offset": offset, "returned": len(rows)})
    except ValueError as exc:
        return bad_request(str(exc))
    except Exception as exc:
        log_error("Details query failed", error=str(exc))
        return internal_error()
