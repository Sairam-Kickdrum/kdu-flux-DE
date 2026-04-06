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
  order_date AS date,
  COALESCE(SUM(revenue), 0) AS revenue
FROM {db.fact_table()}
{where_sql}
GROUP BY order_date
ORDER BY order_date
"""
        rows = db.query(sql, parameters)
        log_info("Daily revenue query succeeded", row_count=len(rows), filters=filters)
        return success({"series": rows})
    except ValueError as exc:
        return bad_request(str(exc))
    except Exception as exc:
        log_error("Daily revenue query failed", error=str(exc))
        return internal_error()
