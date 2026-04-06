from common import db, sql_builders, validation
from common.logging_utils import log_error, log_info
from common.responses import bad_request, internal_error, success


def lambda_handler(event, context):
    try:
        params = validation.get_query_params(event)
        domain = validation.parse_domain(params)
        start_date, end_date = validation.parse_date_filters(params, domain)
        client = validation.parse_client_upload_client(params, domain)
        ascribe_filters = validation.parse_optional_ascribe_filters(params, domain)

        domain_cfg = db.get_domain_config(domain)
        where_clause, where_params = sql_builders.build_domain_where_clause(
            domain=domain,
            date_column=domain_cfg["date_column"],
            client=client,
            start_date=start_date,
            end_date=end_date,
            ascribe_filters=ascribe_filters,
        )

        sql = sql_builders.revenue_daily_sql(domain_cfg["table"], domain_cfg["date_column"], where_clause)
        rows = db.query(sql, where_params)

        filters = validation.build_filters_payload(domain, client, start_date, end_date, ascribe_filters)
        log_info("analytics_revenue_daily_success", domain=domain, row_count=len(rows), filters=filters)
        return success(domain, filters, {"items": rows})
    except validation.ValidationError as exc:
        return bad_request(str(exc))
    except Exception as exc:
        log_error("analytics_revenue_daily_failed", error=str(exc))
        return internal_error()
