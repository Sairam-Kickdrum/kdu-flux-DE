import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

import boto3


client = boto3.client("redshift-data")


REDSHIFT_WORKGROUP_NAME = os.environ["REDSHIFT_WORKGROUP_NAME"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "public")
REDSHIFT_STAGING_TABLE = os.environ.get("REDSHIFT_STAGING_TABLE", "fact_client_uploads_staging")
REDSHIFT_COPY_ROLE_ARN = os.environ["REDSHIFT_COPY_ROLE_ARN"]


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
    client_name = event["client_name"]
    event_date = event["event_date"]
    event_name = event.get("event_name", "ObjectCreated")

    bucket_name = event["bucket_name"]
    manifest_s3_uri = event.get("manifest_s3_uri") or f"s3://{bucket_name}/processed/client_uploads/{client_name}/_manifests/{load_id}.manifest.json"

    loaded_at = datetime.now(timezone.utc).isoformat()

    table = _qualified_table(REDSHIFT_SCHEMA, REDSHIFT_STAGING_TABLE)
    copy_sql = f"""
COPY {table} (discount_code, orders, revenue, order_date, client_name, event_name, load_id, event_date)
FROM '{_sql_escape(manifest_s3_uri)}'
IAM_ROLE '{_sql_escape(REDSHIFT_COPY_ROLE_ARN)}'
FORMAT AS PARQUET
MANIFEST;
"""

    update_sql = f"""
UPDATE {table}
SET loaded_at = GETDATE(),
    event_name = COALESCE(event_name, '{_sql_escape(event_name)}'),
    created_at = COALESCE(created_at, GETDATE())
WHERE load_id = '{_sql_escape(load_id)}' AND loaded_at IS NULL;
"""

    _run_sql_or_raise(copy_sql)
    _run_sql_or_raise(update_sql)

    return {
        "status": "SUCCESS",
        "client_name": client_name,
        "load_id": load_id,
        "event_date": event_date,
        "event_name": event_name,
        "loaded_at": loaded_at,
        "manifest_s3_uri": manifest_s3_uri,
    }
