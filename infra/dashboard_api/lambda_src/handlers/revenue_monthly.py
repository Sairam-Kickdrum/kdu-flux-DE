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
  TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM') AS month,
  COALESCE(SUM(revenue), 0) AS revenue
FROM {db.fact_table()}
{where_sql}
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date)
"""
        rows = db.query(sql, parameters)
        log_info("Monthly revenue query succeeded", row_count=len(rows), filters=filters)
        return success({"series": rows})
    except ValueError as exc:
        return bad_request(str(exc))
    except Exception as exc:
        log_error("Monthly revenue query failed", error=str(exc))
        return internal_error()
