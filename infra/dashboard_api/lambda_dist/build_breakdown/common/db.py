import json
import os
import re
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

import boto3


AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "").strip()
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "")
REDSHIFT_WORKGROUP_NAME = os.environ.get("REDSHIFT_WORKGROUP_NAME") or (REDSHIFT_HOST.split(".")[0] if REDSHIFT_HOST else "")

CLIENT_UPLOAD_TABLE = os.environ.get("CLIENT_UPLOAD_TABLE", "fact_client_uploads")
ADSCRIBE_TABLE = os.environ.get("ADSCRIBE_TABLE") or os.environ.get("ASCRIBE_TABLE", "flux_ascribe_performance")

_client = boto3.client("redshift-data", region_name=AWS_REGION)
_secrets = boto3.client("secretsmanager", region_name=AWS_REGION)


def get_domain_config(domain: str) -> Dict[str, str]:
    if domain == "client_upload":
        return {"table": CLIENT_UPLOAD_TABLE, "date_column": "order_date"}
    return {"table": ADSCRIBE_TABLE, "date_column": "date"}


def get_redshift_secret_metadata() -> Dict[str, Any]:
    if not REDSHIFT_SECRET_ARN:
        return {"secret_arn": "", "username": None}
    response = _secrets.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    payload = json.loads(response.get("SecretString", "{}"))
    return {
        "secret_arn": REDSHIFT_SECRET_ARN,
        "username": payload.get("username") or payload.get("user"),
    }


def _normalize_cell(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _field_to_python(field: Dict[str, Any]) -> Any:
    if field.get("isNull"):
        return None
    if "stringValue" in field:
        return field["stringValue"]
    if "longValue" in field:
        return int(field["longValue"])
    if "doubleValue" in field:
        return float(field["doubleValue"])
    if "booleanValue" in field:
        return bool(field["booleanValue"])
    return None


def _wait(statement_id: str, timeout_seconds: int = 90) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        desc = _client.describe_statement(Id=statement_id)
        status = desc.get("Status")
        if status == "FINISHED":
            return
        if status in {"FAILED", "ABORTED"}:
            raise RuntimeError(desc.get("Error", "Redshift query failed"))
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for statement {statement_id}")


def _convert_positional_params(sql: str, params: Sequence[Any]) -> Tuple[str, List[Dict[str, Any]]]:
    values = list(params or [])
    idx = {"i": 0}

    def _replace(_: re.Match[str]) -> str:
        idx["i"] += 1
        return f":p{idx['i']}"

    converted_sql = re.sub(r"%s", _replace, sql)

    if idx["i"] != len(values):
        raise ValueError("SQL placeholder count does not match number of parameters")

    converted_params: List[Dict[str, Any]] = []
    for i, value in enumerate(values, start=1):
        name = f"p{i}"
        if value is None:
            converted_params.append({"name": name, "value": ""})
        else:
            converted_params.append({"name": name, "value": str(value)})

    return converted_sql, converted_params


def query(sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    if not REDSHIFT_WORKGROUP_NAME:
        raise RuntimeError("Missing REDSHIFT_WORKGROUP_NAME (or REDSHIFT_HOST to derive it).")
    if not REDSHIFT_SECRET_ARN:
        raise RuntimeError("Missing REDSHIFT_SECRET_ARN. Serverless Redshift Data API requires SecretArn.")

    final_sql, final_params = _convert_positional_params(sql, params or [])

    execute_kwargs: Dict[str, Any] = {
        "WorkgroupName": REDSHIFT_WORKGROUP_NAME,
        "Database": REDSHIFT_DATABASE,
        "SecretArn": REDSHIFT_SECRET_ARN,
        "Sql": final_sql,
    }
    if final_params:
        execute_kwargs["Parameters"] = final_params

    response = _client.execute_statement(**execute_kwargs)
    statement_id = response["Id"]
    _wait(statement_id)

    rows: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        result_kwargs = {"Id": statement_id}
        if next_token:
            result_kwargs["NextToken"] = next_token

        result = _client.get_statement_result(**result_kwargs)
        columns = [c["name"] for c in result.get("ColumnMetadata", [])]
        for record in result.get("Records", []):
            item: Dict[str, Any] = {}
            for i, field in enumerate(record):
                item[columns[i]] = _normalize_cell(_field_to_python(field))
            rows.append(item)

        next_token = result.get("NextToken")
        if not next_token:
            break

    return rows
