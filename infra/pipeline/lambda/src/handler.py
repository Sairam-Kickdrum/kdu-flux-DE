import hashlib
import json
import os
from typing import Any, Dict, List

from services.config_loader import load_pipeline_config
from services.file_discovery import detect_file_type, find_latest_required_files
from services.idempotency import IdempotencyStore
from services.stepfn import StepFunctionStarter
from utils.s3_event_parser import extract_s3_records_from_sqs_event


CONFIG_PATH = os.getenv("PIPELINE_CONFIG_PATH", "config/client_pipeline_config.json")
IDEMPOTENCY_TABLE_NAME = os.environ["IDEMPOTENCY_TABLE_NAME"]
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]
SOURCE_BUCKET_NAME = os.environ["SOURCE_BUCKET_NAME"]


def _derive_client_id(object_key: str) -> str:
    # Expected key shape: raw/client_uploads/{client_id}/file.csv
    parts = object_key.split("/")
    if len(parts) < 3 or parts[0] != "raw" or parts[1] != "client_uploads":
        raise ValueError(f"Unsupported S3 object key format: {object_key}")
    return parts[2].strip().lower()


def _build_event_idempotency_key(client_id: str, object_key: str, etag: str) -> str:
    payload = f"{client_id}|{etag or 'no-etag'}|{object_key}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_batch_idempotency_key(client_id: str, file_type_to_key: Dict[str, str]) -> str:
    canonical = "|".join([f"{k}:{v}" for k, v in sorted(file_type_to_key.items())])
    return f"batch#{hashlib.sha256(f'{client_id}|{canonical}'.encode('utf-8')).hexdigest()}"


def _safe_get_etag(record: Dict[str, Any]) -> str:
    return (
        record.get("s3", {})
        .get("object", {})
        .get("eTag")
        or record.get("s3", {}).get("object", {}).get("etag")
        or ""
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    pipeline_cfg = load_pipeline_config(CONFIG_PATH)
    idempotency = IdempotencyStore(IDEMPOTENCY_TABLE_NAME)
    step_fn = StepFunctionStarter(STEP_FUNCTION_ARN)

    s3_records = extract_s3_records_from_sqs_event(event)
    if not s3_records:
        return {"status": "ignored", "reason": "no_s3_records_found"}

    results: List[Dict[str, Any]] = []
    for s3_record in s3_records:
        bucket = s3_record["s3"]["bucket"]["name"]
        object_key = s3_record["s3"]["object"]["key"]
        etag = _safe_get_etag(s3_record)

        if bucket != SOURCE_BUCKET_NAME:
            results.append({"status": "ignored", "reason": "unexpected_bucket", "bucket": bucket})
            continue

        client_id = _derive_client_id(object_key)
        client_cfg = pipeline_cfg["clients"].get(client_id)
        if not client_cfg:
            results.append({"status": "ignored", "reason": "unknown_client", "client_id": client_id})
            continue

        file_type = detect_file_type(object_key, client_cfg["required_files"])
        if not file_type:
            results.append(
                {
                    "status": "ignored",
                    "reason": "file_type_not_tracked",
                    "client_id": client_id,
                    "object_key": object_key
                }
            )
            continue

        event_key = _build_event_idempotency_key(client_id, object_key, etag)
        if not idempotency.put_if_absent(
            key=event_key,
            payload={
                "record_type": "file_event",
                "client_id": client_id,
                "object_key": object_key,
                "etag": etag
            }
        ):
            results.append({"status": "duplicate_event", "client_id": client_id, "object_key": object_key})
            continue

        latest_files = find_latest_required_files(
            bucket=bucket,
            client_id=client_id,
            required_files=client_cfg["required_files"]
        )

        missing_file_types = [rf["file_type"] for rf in client_cfg["required_files"] if rf["file_type"] not in latest_files]
        if missing_file_types:
            results.append(
                {
                    "status": "waiting_for_more_files",
                    "client_id": client_id,
                    "missing_file_types": missing_file_types
                }
            )
            continue

        batch_keys = {file_type_name: file_info["key"] for file_type_name, file_info in latest_files.items()}
        batch_idempotency_key = _build_batch_idempotency_key(client_id, batch_keys)

        if not idempotency.put_if_absent(
            key=batch_idempotency_key,
            payload={
                "record_type": "ready_batch",
                "client_id": client_id,
                "file_keys": batch_keys
            }
        ):
            results.append({"status": "duplicate_batch", "client_id": client_id, "batch_key": batch_idempotency_key})
            continue

        step_input = {
            "client_id": client_id,
            "bucket": bucket,
            "files": latest_files,
            "config_s3_uri": os.environ["CONFIG_S3_URI"],
            "batch_idempotency_key": batch_idempotency_key
        }
        execution_arn = step_fn.start(client_id=client_id, input_payload=step_input)
        results.append({"status": "started", "client_id": client_id, "execution_arn": execution_arn})

    return {"status": "ok", "results": results}
