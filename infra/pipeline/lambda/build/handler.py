import json
import logging
import os
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME") or os.getenv("IDEMPOTENCY_TABLE_NAME")
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
CLIENT_CONFIG_PATH = os.getenv("CLIENT_CONFIG_PATH") or os.getenv("PIPELINE_CONFIG_PATH", "config/clients.json")
CONFIG_S3_URI = os.getenv("CONFIG_S3_URI", "")

s3_client = boto3.client("s3")
dynamodb_table = boto3.resource("dynamodb").Table(DYNAMODB_TABLE_NAME) if DYNAMODB_TABLE_NAME else None
stepfunctions_client = boto3.client("stepfunctions")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_dynamodb() -> Any:
    if dynamodb_table is None:
        raise ValueError("DynamoDB table is not configured. Set DYNAMODB_TABLE_NAME or IDEMPOTENCY_TABLE_NAME.")
    return dynamodb_table


def _validate_config(cfg: Dict[str, Any], source: str) -> Dict[str, Any]:
    clients = cfg.get("clients")
    if not isinstance(clients, dict):
        raise ValueError(f"Invalid config format from {source}: expected top-level 'clients' object")
    return cfg


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    without_scheme = uri.replace("s3://", "", 1)
    return tuple(without_scheme.split("/", 1))  # type: ignore[return-value]


def _load_config() -> Dict[str, Any]:
    if CONFIG_S3_URI:
        bucket, key = _parse_s3_uri(CONFIG_S3_URI)
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        return _validate_config(json.loads(body), f"s3://{bucket}/{key}")

    path = Path(CLIENT_CONFIG_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Client config file not found: {CLIENT_CONFIG_PATH}")
    with path.open("r", encoding="utf-8") as fp:
        return _validate_config(json.load(fp), CLIENT_CONFIG_PATH)


def _parse_s3_event_records_from_sqs_record(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = record.get("body")
    if not body:
        return []
    parsed_body = json.loads(body)
    nested_records = parsed_body.get("Records", [])
    return nested_records if isinstance(nested_records, list) else []


def _derive_client_name(object_key: str) -> Optional[str]:
    parts = object_key.split("/")
    if len(parts) < 4 or parts[0] != "raw" or parts[1] != "client_uploads":
        return None
    return parts[2].strip().lower()


def _extract_event_date(event_timestamp: str) -> str:
    return event_timestamp[:10] if event_timestamp else datetime.now(timezone.utc).date().isoformat()


def _extract_s3_fields(s3_event_record: Dict[str, Any]) -> Optional[Dict[str, str]]:
    s3_node = s3_event_record.get("s3", {})
    bucket_node = s3_node.get("bucket", {})
    object_node = s3_node.get("object", {})

    object_key_raw = object_node.get("key")
    bucket_name = bucket_node.get("name")
    if not object_key_raw or not bucket_name:
        return None

    object_key = unquote_plus(object_key_raw)
    client_name = _derive_client_name(object_key)
    if not client_name:
        return None

    event_timestamp = s3_event_record.get("eventTime", "")
    return {
        "client_name": client_name,
        "object_key": object_key,
        "file_name": object_key.split("/")[-1],
        "bucket_name": bucket_name,
        "bucket_arn": bucket_node.get("arn", ""),
        "event_timestamp": event_timestamp,
        "event_date": _extract_event_date(event_timestamp),
        "etag": object_node.get("eTag", object_node.get("etag", "")),
    }


def _build_idempotency_key(client_name: str, object_key: str, event_date: str, etag: str) -> str:
    return f"{client_name}|{object_key}|{event_date}|{etag}"


def _build_execution_lock_key(client_name: str, event_date: str) -> str:
    return f"execution_lock|{client_name}|{event_date}"


def _put_idempotency_record(item: Dict[str, str]) -> bool:
    table = _require_dynamodb()
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(#k)",
            ExpressionAttributeNames={"#k": "key"},
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def _check_required_files_ready(client_name: str, event_date: str, required_files: Set[str]) -> Tuple[bool, List[Dict[str, Any]], List[str]]:
    table = _require_dynamodb()

    matching_items: List[Dict[str, Any]] = []
    scan_kwargs: Dict[str, Any] = {
        "FilterExpression": Attr("client_name").eq(client_name)
        & Attr("event_date").eq(event_date)
        & Attr("status").eq("RECEIVED"),
        "ProjectionExpression": "#k,file_name,object_key,etag,#ts,client_name,bucket_name,bucket_arn,event_date",
        "ExpressionAttributeNames": {"#k": "key", "#ts": "timestamp"},
    }

    while True:
        response = table.scan(**scan_kwargs)
        matching_items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    available_files = {item.get("file_name", "") for item in matching_items}
    missing_files = sorted(required_files - available_files)
    return len(missing_files) == 0, matching_items, missing_files


def _required_spec_label(required_spec: Any) -> str:
    if isinstance(required_spec, str):
        return required_spec
    if isinstance(required_spec, dict):
        file_type = required_spec.get("file_type")
        if file_type:
            return str(file_type)
        patterns = required_spec.get("patterns", [])
        if patterns:
            return str(patterns[0])
    return "unknown_required_file"


def _spec_is_satisfied(required_spec: Any, available_files: Set[str]) -> bool:
    if isinstance(required_spec, str):
        return required_spec in available_files

    if isinstance(required_spec, dict):
        patterns = required_spec.get("patterns", [])
        if not isinstance(patterns, list):
            return False
        for file_name in available_files:
            if any(fnmatch(file_name.lower(), str(pattern).lower()) for pattern in patterns):
                return True
    return False


def _evaluate_required_files(required_specs: List[Any], available_files: Set[str]) -> List[str]:
    missing: List[str] = []
    for spec in required_specs:
        if not _spec_is_satisfied(spec, available_files):
            missing.append(_required_spec_label(spec))
    return sorted(missing)


def _check_required_files_ready_with_config(
    client_name: str,
    event_date: str,
    required_specs: List[Any],
) -> Tuple[bool, List[Dict[str, Any]], List[str]]:
    table = _require_dynamodb()

    matching_items: List[Dict[str, Any]] = []
    scan_kwargs: Dict[str, Any] = {
        "FilterExpression": Attr("client_name").eq(client_name)
        & Attr("event_date").eq(event_date)
        & Attr("status").eq("RECEIVED"),
        "ProjectionExpression": "#k,file_name,object_key,etag,#ts,client_name,bucket_name,bucket_arn,event_date",
        "ExpressionAttributeNames": {"#k": "key", "#ts": "timestamp"},
    }

    while True:
        response = table.scan(**scan_kwargs)
        matching_items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    available_files = {item.get("file_name", "") for item in matching_items if item.get("file_name")}
    missing_files = _evaluate_required_files(required_specs, available_files)
    return len(missing_files) == 0, matching_items, missing_files


def _build_step_input(
    client_name: str,
    event_date: str,
    bucket_name: str,
    bucket_arn: str,
    ready_items: List[Dict[str, Any]],
    trigger_timestamp: str,
    lock_key: str,
) -> Dict[str, Any]:
    unique_by_file: Dict[str, Dict[str, Any]] = {}
    for item in ready_items:
        file_name = item.get("file_name", "")
        if file_name and file_name not in unique_by_file:
            unique_by_file[file_name] = item

    object_keys = [entry.get("object_key") for entry in unique_by_file.values()]
    file_names = list(unique_by_file.keys())
    etags = [entry.get("etag", "") for entry in unique_by_file.values()]

    return {
        "client_name": client_name,
        "bucket_name": bucket_name,
        "bucket_arn": bucket_arn,
        "object_keys": object_keys,
        "file_names": file_names,
        "event_date": event_date,
        "trigger_timestamp": trigger_timestamp,
        "etags": etags,
        "idempotency_group_key": lock_key,
    }


def _start_step_function_if_not_started(
    client_name: str,
    event_date: str,
    bucket_name: str,
    bucket_arn: str,
    ready_items: List[Dict[str, Any]],
    trigger_timestamp: str,
) -> Dict[str, Any]:
    lock_key = _build_execution_lock_key(client_name, event_date)
    lock_item = {
        "key": lock_key,
        "file_name": "",
        "timestamp": trigger_timestamp,
        "etag": "",
        "client_name": client_name,
        "object_key": "",
        "bucket_name": bucket_name,
        "bucket_arn": bucket_arn,
        "event_date": event_date,
        "status": "EXECUTION_STARTED",
        "created_at": _now_utc_iso(),
    }

    if not _put_idempotency_record(lock_item):
        logger.info("Step Function already started for client=%s date=%s", client_name, event_date)
        return {"started": False, "reason": "already_started"}

    step_input = _build_step_input(
        client_name=client_name,
        event_date=event_date,
        bucket_name=bucket_name,
        bucket_arn=bucket_arn,
        ready_items=ready_items,
        trigger_timestamp=trigger_timestamp,
        lock_key=lock_key,
    )

    execution_name = f"{client_name}-{event_date.replace('-', '')}-{int(datetime.now(timezone.utc).timestamp())}"
    response = stepfunctions_client.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=execution_name[:80],
        input=json.dumps(step_input),
    )

    logger.info("Started Step Function for client=%s date=%s executionArn=%s", client_name, event_date, response["executionArn"])
    return {"started": True, "execution_arn": response["executionArn"], "step_input": step_input}


def _build_received_item(extracted: Dict[str, str], idempotency_key: str) -> Dict[str, str]:
    return {
        "key": idempotency_key,
        "file_name": extracted["file_name"],
        "timestamp": extracted["event_timestamp"],
        "etag": extracted["etag"],
        "client_name": extracted["client_name"],
        "object_key": extracted["object_key"],
        "bucket_name": extracted["bucket_name"],
        "bucket_arn": extracted["bucket_arn"],
        "event_date": extracted["event_date"],
        "status": "RECEIVED",
        "created_at": _now_utc_iso(),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    if not DYNAMODB_TABLE_NAME:
        raise ValueError("Missing env var: DYNAMODB_TABLE_NAME or IDEMPOTENCY_TABLE_NAME")

    config = _load_config()

    processed_records: List[Dict[str, Any]] = []
    candidate_groups: Set[Tuple[str, str, str, str]] = set()

    for sqs_record in event.get("Records", []):
        message_id = sqs_record.get("messageId", "")
        try:
            s3_event_records = _parse_s3_event_records_from_sqs_record(sqs_record)
        except Exception as exc:
            logger.exception("Failed to parse SQS body messageId=%s error=%s", message_id, str(exc))
            processed_records.append({"message_id": message_id, "status": "parse_error", "error": str(exc)})
            continue

        if not s3_event_records:
            processed_records.append({"message_id": message_id, "status": "ignored_no_nested_records"})
            continue

        for s3_record in s3_event_records:
            extracted = _extract_s3_fields(s3_record)
            if not extracted:
                processed_records.append({"message_id": message_id, "status": "ignored_invalid_s3_record"})
                continue

            client_name = extracted["client_name"]
            if client_name not in config["clients"]:
                processed_records.append(
                    {
                        "message_id": message_id,
                        "status": "ignored_unknown_client",
                        "client_name": client_name,
                        "object_key": extracted["object_key"],
                    }
                )
                continue

            idempotency_key = _build_idempotency_key(
                client_name=client_name,
                object_key=extracted["object_key"],
                event_date=extracted["event_date"],
                etag=extracted["etag"],
            )

            if not _put_idempotency_record(_build_received_item(extracted, idempotency_key)):
                processed_records.append(
                    {
                        "message_id": message_id,
                        "status": "duplicate_event",
                        "client_name": client_name,
                        "idempotency_key": idempotency_key,
                    }
                )
                continue

            processed_records.append(
                {
                    "message_id": message_id,
                    "status": "stored",
                    "client_name": client_name,
                    "file_name": extracted["file_name"],
                    "event_date": extracted["event_date"],
                    "idempotency_key": idempotency_key,
                }
            )

            candidate_groups.add((client_name, extracted["event_date"], extracted["bucket_name"], extracted["bucket_arn"]))

    orchestration_results: List[Dict[str, Any]] = []

    for client_name, event_date, bucket_name, bucket_arn in sorted(candidate_groups):
        required_specs = config["clients"][client_name]["required_files"]
        is_ready, ready_items, missing_files = _check_required_files_ready_with_config(
            client_name=client_name,
            event_date=event_date,
            required_specs=required_specs,
        )

        if not is_ready:
            logger.info("Required files not ready for client=%s event_date=%s missing=%s", client_name, event_date, missing_files)
            orchestration_results.append(
                {
                    "client_name": client_name,
                    "event_date": event_date,
                    "ready": False,
                    "missing_files": missing_files,
                    "status": "waiting_for_files",
                }
            )
            continue

        try:
            orchestration_results.append(
                {
                    "client_name": client_name,
                    "event_date": event_date,
                    "ready": True,
                    **_start_step_function_if_not_started(
                        client_name=client_name,
                        event_date=event_date,
                        bucket_name=bucket_name,
                        bucket_arn=bucket_arn,
                        ready_items=ready_items,
                        trigger_timestamp=_now_utc_iso(),
                    ),
                }
            )
        except Exception as exc:
            logger.exception("Failed to start Step Function client=%s event_date=%s error=%s", client_name, event_date, str(exc))
            orchestration_results.append(
                {
                    "client_name": client_name,
                    "event_date": event_date,
                    "ready": True,
                    "started": False,
                    "status": "step_function_start_failed",
                    "error": str(exc),
                }
            )

    response = {
        "status": "ok",
        "processed_records": processed_records,
        "orchestration_results": orchestration_results,
    }
    logger.info("Lambda completed: %s", json.dumps(response))
    return response
