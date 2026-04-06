import os
import time
from typing import Any, Dict

import boto3


client = boto3.client("redshift-data")


REDSHIFT_WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "public")
REDSHIFT_STAGING_TABLE = os.environ.get("REDSHIFT_STAGING_TABLE", "fact_client_uploads_staging")
REDSHIFT_FINAL_TABLE = os.environ.get("REDSHIFT_FINAL_TABLE", "fact_client_uploads")
CLEANUP_STAGING = os.environ.get("CLEANUP_STAGING", "true").lower() == "true"


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _qualified_table(schema_name: str, table_name: str) -> str:
    if "." in table_name:
        return table_name
    return f"{schema_name}.{table_name}"


def _execute_sql(sql: str) -> str:
    resp = client.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql,
    )
    return resp["Id"]


def _wait(statement_id: str, timeout_sec: int = 900) -> Dict[str, Any]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status in {"FINISHED", "FAILED", "ABORTED"}:
            return desc
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for Redshift Data API statement: {statement_id}")


def _run_sql_or_raise(sql: str) -> None:
    statement_id = _execute_sql(sql)
    desc = _wait(statement_id)
    if desc["Status"] != "FINISHED":
        raise RuntimeError(desc.get("Error", "Unknown Redshift Data API error"))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    load_id = event["load_id"]
    load_id_safe = _sql_escape(load_id)

    staging_table = _qualified_table(REDSHIFT_SCHEMA, REDSHIFT_STAGING_TABLE)
    final_table = _qualified_table(REDSHIFT_SCHEMA, REDSHIFT_FINAL_TABLE)

    delete_sql = f"""
DELETE FROM {final_table} f
USING (
  SELECT client_name, MIN(order_date) AS min_order_date, MAX(order_date) AS max_order_date
  FROM {staging_table}
  WHERE load_id = '{load_id_safe}'
  GROUP BY client_name
) s
WHERE f.client_name = s.client_name
  AND f.order_date BETWEEN s.min_order_date AND s.max_order_date;
"""

    insert_sql = f"""
INSERT INTO {final_table} (discount_code, orders, revenue, order_date, client_name, event_name, load_id, event_date, loaded_at)
SELECT discount_code, orders, revenue, order_date, client_name, event_name, load_id, event_date, loaded_at
FROM {staging_table}
WHERE load_id = '{load_id_safe}';
"""

    begin_sql = "BEGIN;"
    cleanup_sql = f"DELETE FROM {staging_table} WHERE load_id = '{load_id_safe}';"
    commit_sql = "COMMIT;"

    _run_sql_or_raise(begin_sql)
    try:
        _run_sql_or_raise(delete_sql)
        _run_sql_or_raise(insert_sql)
        if CLEANUP_STAGING:
            _run_sql_or_raise(cleanup_sql)
        _run_sql_or_raise(commit_sql)
    except Exception:
        _run_sql_or_raise("ROLLBACK;")
        raise

    return {
        "status": "SUCCESS",
        "load_id": load_id,
        "cleanup_staging": CLEANUP_STAGING,
    }
