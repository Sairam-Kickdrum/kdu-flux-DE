from typing import Any, Dict, List, Optional, Tuple


def _append_condition(conditions: List[str], params: List[Any], clause: str, value: Any) -> None:
    conditions.append(clause)
    params.append(value)


def build_domain_where_clause(
    domain: str,
    date_column: str,
    client: str,
    start_date: Optional[str],
    end_date: Optional[str],
    ascribe_filters: Dict[str, Optional[str]],
) -> Tuple[str, List[Any]]:
    conditions: List[str] = []
    params: List[Any] = []

    if start_date:
        _append_condition(conditions, params, f"{date_column} >= %s", start_date)
    if end_date:
        _append_condition(conditions, params, f"{date_column} <= %s", end_date)

    if domain == "client_upload":
        if client != "all":
            _append_condition(conditions, params, "client_name = %s", client)
    else:
        if ascribe_filters.get("client_name"):
            _append_condition(conditions, params, "client_name = %s", ascribe_filters["client_name"])
        if ascribe_filters.get("show_name"):
            _append_condition(conditions, params, "show_name = %s", ascribe_filters["show_name"])
        if ascribe_filters.get("discount_code"):
            _append_condition(conditions, params, "discount_code = %s", ascribe_filters["discount_code"])

    if not conditions:
        return "", params
    return "WHERE " + " AND ".join(conditions), params


def kpi_sql(domain: str, table_name: str, where_clause: str) -> str:
    if domain == "client_upload":
        return f"""
SELECT
  COALESCE(SUM(revenue), 0) AS total_revenue,
  COALESCE(SUM(orders), 0) AS total_orders
FROM {table_name}
{where_clause}
"""
    return f"""
SELECT
  COALESCE(SUM(revenue), 0) AS total_revenue,
  COALESCE(SUM(orders), 0) AS total_orders,
  COALESCE(SUM(impressions), 0) AS total_impressions
FROM {table_name}
{where_clause}
"""


def revenue_daily_sql(table_name: str, date_column: str, where_clause: str) -> str:
    return f"""
SELECT
  {date_column} AS date,
  COALESCE(SUM(revenue), 0) AS revenue
FROM {table_name}
{where_clause}
GROUP BY {date_column}
ORDER BY {date_column}
"""


def revenue_monthly_sql(table_name: str, date_column: str, where_clause: str) -> str:
    return f"""
SELECT
  TO_CHAR(DATE_TRUNC('month', {date_column}), 'YYYY-MM') AS month,
  COALESCE(SUM(revenue), 0) AS revenue
FROM {table_name}
{where_clause}
GROUP BY 1
ORDER BY 1
"""


def breakdown_sql(domain: str, table_name: str, dimension_column: str, where_clause: str, top_n: Optional[int]) -> str:
    impressions_expr = "COALESCE(SUM(impressions), 0) AS impressions" if domain == "ascribe" else "NULL::double precision AS impressions"
    limit_clause = f"LIMIT {top_n}" if top_n is not None else ""

    return f"""
SELECT
  COALESCE(CAST({dimension_column} AS varchar), 'UNKNOWN') AS key,
  COALESCE(SUM(revenue), 0) AS revenue,
  COALESCE(SUM(orders), 0) AS orders,
  {impressions_expr}
FROM {table_name}
{where_clause}
GROUP BY 1
ORDER BY revenue DESC
{limit_clause}
"""


def details_sql(
    domain: str,
    table_name: str,
    where_clause: str,
    sort_column: str,
    sort_order: str,
) -> str:
    if domain == "client_upload":
        select_sql = """
SELECT
  order_date AS date,
  client_name,
  discount_code,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders
"""
    else:
        select_sql = """
SELECT
  date,
  client_name,
  show_name,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(orders, 0) AS orders,
  COALESCE(impressions, 0) AS impressions,
  COALESCE(revenue_per_order, 0) AS revenue_per_order,
  COALESCE(revenue_per_impression, 0) AS revenue_per_impression,
  COALESCE(impressions_per_order, 0) AS impressions_per_order
"""

    return f"""
{select_sql}
FROM {table_name}
{where_clause}
ORDER BY {sort_column} {sort_order.upper()}
LIMIT %s OFFSET %s
"""
