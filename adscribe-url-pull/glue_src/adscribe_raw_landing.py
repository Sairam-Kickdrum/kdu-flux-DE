from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from urllib import request

import boto3


def resolve_job_args() -> dict[str, str]:
    args: dict[str, str] = {}
    for raw_arg in sys.argv[1:]:
        if not raw_arg.startswith("--") or "=" not in raw_arg:
            continue

        key, value = raw_arg[2:].split("=", 1)
        args[key] = value

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

    dynamodb_client.update_item(
        TableName=table_name,
        Key={"key": {"S": batch_id}},
        UpdateExpression=(
            "SET #status = :status, #run_id = :run_id, #bucket = :bucket, "
            "#source_key = :source_key, #metadata_key = :metadata_key, "
            "#start_date = :start_date, #end_date = :end_date, #updated_at = :updated_at"
        ),
        ExpressionAttributeNames={
            "#status": "status",
            "#run_id": "run_id",
            "#bucket": "bucket",
            "#source_key": "source_key",
            "#metadata_key": "metadata_key",
            "#start_date": "start_date",
            "#end_date": "end_date",
            "#updated_at": "updated_at",
        },
        ExpressionAttributeValues={
            ":status": {"S": "RAW_LANDED"},
            ":run_id": {"S": run_id},
            ":bucket": {"S": bucket_name},
            ":source_key": {"S": source_key},
            ":metadata_key": {"S": metadata_key},
            ":start_date": {"S": start_date},
            ":end_date": {"S": end_date},
            ":updated_at": {"S": ingested_at},
        },
    )


if __name__ == "__main__":
    main()
