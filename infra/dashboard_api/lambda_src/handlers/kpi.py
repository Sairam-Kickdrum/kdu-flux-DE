from common import db
from common.logging_utils import log_error, log_info
from common.responses import bad_request, internal_error, success
from common.validation import parse_common_filters


def lambda_handler(event, context):
    try:
        query_params = event.get("queryStringParameters") or {}
        filters = parse_common_filters(query_params)
        where_sql, parameters = db.build_where_clause(filters)

        sql = f"""
SELECT
  COALESCE(SUM(revenue), 0) AS total_revenue,
  COALESCE(SUM(orders), 0) AS total_orders
FROM {db.fact_table()}
{where_sql}
"""
        rows = db.query(sql, parameters)
        data = rows[0] if rows else {"total_revenue": 0, "total_orders": 0}
        log_info("KPI query succeeded", filters=filters)
        return success(data)
    except ValueError as exc:
        return bad_request(str(exc))
    except Exception as exc:
        log_error("KPI query failed", error=str(exc))
        return internal_error()
