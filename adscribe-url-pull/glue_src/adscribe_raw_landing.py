from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from urllib import request

import boto3


def resolve_job_args() -> dict[str, str]:
    args: dict[str, str] = {}
    raw_args = sys.argv[1:]
    index = 0

    while index < len(raw_args):
        raw_arg = raw_args[index]
        if not raw_arg.startswith("--"):
            index += 1
            continue

        if "=" in raw_arg:
            key, value = raw_arg[2:].split("=", 1)
            args[key] = value
            index += 1
            continue

        next_index = index + 1
        if next_index < len(raw_args) and not raw_args[next_index].startswith("--"):
            args[raw_arg[2:]] = raw_args[next_index]
            index += 2
            continue

        index += 1

    required = ["batch_id", "start_date", "end_date", "presigned_url", "run_id"]
    missing = [key for key in required if not args.get(key)]
    if missing:
        raise ValueError(f"Missing required Glue arguments: {', '.join(missing)}")

    return args


def build_s3_prefix(start_date: str, end_date: str, batch_id: str) -> str:
    normalized_batch_id = batch_id.replace("#", "_")
    return (
        f"raw/adscribe/start_date={start_date}/"
        f"end_date={end_date}/"
        f"batch_id={normalized_batch_id}"
    )


def download_csv(presigned_url: str) -> bytes:
    with request.urlopen(presigned_url, timeout=60) as response:
        return response.read()


def update_batch_status(
    dynamodb_client: object,
    table_name: str,
    batch_id: str,
    *,
    status: str,
    updated_at: str,
    run_id: str | None = None,
    bucket_name: str | None = None,
    source_key: str | None = None,
    metadata_key: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    error_message: str | None = None,
) -> None:
    update_parts = ["#status = :status", "#updated_at = :updated_at"]
    expression_attribute_names = {
        "#status": "status",
        "#updated_at": "updated_at",
    }
    expression_attribute_values = {
        ":status": {"S": status},
        ":updated_at": {"S": updated_at},
    }

    optional_fields = [
        ("run_id", run_id),
        ("bucket", bucket_name),
        ("source_key", source_key),
        ("metadata_key", metadata_key),
        ("start_date", start_date),
        ("end_date", end_date),
        ("error_message", error_message),
    ]
    for field_name, field_value in optional_fields:
        if field_value is None:
            continue
        placeholder_name = f"#{field_name}"
        placeholder_value = f":{field_name}"
        update_parts.append(f"{placeholder_name} = {placeholder_value}")
        expression_attribute_names[placeholder_name] = field_name
        expression_attribute_values[placeholder_value] = {"S": field_value}

    dynamodb_client.update_item(
        TableName=table_name,
        Key={"key": {"S": batch_id}},
        UpdateExpression=f"SET {', '.join(update_parts)}",
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
    )


def main() -> None:
    args = resolve_job_args()

    batch_id = args["batch_id"]
    start_date = args["start_date"]
    end_date = args["end_date"]
    presigned_url = args["presigned_url"]
    run_id = args["run_id"]

    bucket_name = "kduflux-de-bucket"
    table_name = "kdu-flux-dynamodb-table-de"
    prefix = build_s3_prefix(start_date, end_date, batch_id)
    source_key = f"{prefix}/source.csv"
    metadata_key = f"{prefix}/metadata.json"
    ingested_at = datetime.now(timezone.utc).isoformat()

    s3_client = boto3.client("s3")
    dynamodb_client = boto3.client("dynamodb")

    try:
        csv_bytes = download_csv(presigned_url)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=source_key,
            Body=csv_bytes,
            ContentType="text/csv",
        )

        metadata = {
            "batch_id": batch_id,
            "run_id": run_id,
            "source": "adscribe",
            "start_date": start_date,
            "end_date": end_date,
            "bucket": bucket_name,
            "source_key": source_key,
            "ingested_at": ingested_at,
        }
        s3_client.put_object(
            Bucket=bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        update_batch_status(
            dynamodb_client,
            table_name,
            batch_id,
            status="RAW_LANDED",
            updated_at=ingested_at,
            run_id=run_id,
            bucket_name=bucket_name,
            source_key=source_key,
            metadata_key=metadata_key,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:
        failure_timestamp = datetime.now(timezone.utc).isoformat()
        try:
            update_batch_status(
                dynamodb_client,
                table_name,
                batch_id,
                status="FAILED",
                updated_at=failure_timestamp,
                error_message=str(exc)[:1000],
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
