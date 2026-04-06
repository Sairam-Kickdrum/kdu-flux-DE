from common import db
from common.logging_utils import log_error, log_info
from common.responses import bad_request, internal_error, success
from common.validation import parse_breakdown_group_by, parse_common_filters


def lambda_handler(event, context):
    try:
        query_params = event.get("queryStringParameters") or {}
        filters = parse_common_filters(query_params)
        group_by = parse_breakdown_group_by(query_params)
        where_sql, parameters = db.build_where_clause(filters)

        sql = f"""
SELECT
  {group_by} AS group_key,
  COALESCE(SUM(revenue), 0) AS revenue,
  COALESCE(SUM(orders), 0) AS orders
FROM {db.fact_table()}
{where_sql}
GROUP BY {group_by}
ORDER BY revenue DESC
"""
        rows = db.query(sql, parameters)
        log_info("Breakdown query succeeded", group_by=group_by, row_count=len(rows), filters=filters)
        return success({"group_by": group_by, "rows": rows})
    except ValueError as exc:
        return bad_request(str(exc))
    except Exception as exc:
        log_error("Breakdown query failed", error=str(exc))
        return internal_error()
