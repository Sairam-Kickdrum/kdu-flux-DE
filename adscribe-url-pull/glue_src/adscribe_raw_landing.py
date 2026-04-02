from __future__ import annotations

import io
import json
import sys
from datetime import datetime, timezone
from urllib import request

import boto3
import pandas as pd


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


def load_config_from_s3(s3_client: object, bucket_name: str, config_key: str) -> dict:
    # Load the existing pipeline config from S3 without creating or overwriting it.
    response = s3_client.get_object(Bucket=bucket_name, Key=config_key)
    return json.loads(response["Body"].read().decode("utf-8"))


def get_config_value(config: dict, *paths: tuple[str, ...], default=None):
    for path in paths:
        current = config
        found = True
        for key in path:
            if not isinstance(current, dict) or key not in current:
                found = False
                break
            current = current[key]
        if found:
            return current
    return default


def get_null_tokens(config: dict) -> set[str]:
    tokens = get_config_value(
        config,
        ("null_tokens",),
        ("cleaning", "null_tokens"),
        ("defaults", "null_tokens"),
        default=[],
    )
    return {str(token).strip() for token in tokens if token is not None}


def parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def get_final_schema(config: dict) -> list[str]:
    schema = get_config_value(
        config,
        ("final_schema",),
        ("output", "final_schema"),
        ("schema", "final_columns"),
        default=[],
    )
    if isinstance(schema, dict):
        schema = schema.get("column_order", [])
    if isinstance(schema, list):
        columns: list[str] = []
        for item in schema:
            if isinstance(item, str):
                columns.append(item)
            elif isinstance(item, dict):
                columns.append(
                    item.get("name")
                    or item.get("target")
                    or item.get("column")
                    or item.get("field")
                )
        return [column for column in columns if column]
    return []


def get_rename_map(config: dict) -> dict[str, str]:
    rename_map = get_config_value(
        config,
        ("rename_columns",),
        ("transformations", "rename_columns"),
        ("schema", "rename_columns"),
        default={},
    )
    return rename_map if isinstance(rename_map, dict) else {}


def get_numeric_columns(config: dict, rename_map: dict[str, str]) -> list[str]:
    configured_columns = get_config_value(
        config,
        ("numeric_columns",),
        ("cleaning", "numeric_columns"),
        ("schema", "numeric_columns"),
        default=[],
    )
    columns = [rename_map.get(column, column) for column in configured_columns if column]
    for derived_column in ["revenue", "orders", "impressions"]:
        if derived_column not in columns:
            columns.append(derived_column)
    return columns


def get_text_columns(config: dict, rename_map: dict[str, str]) -> list[str]:
    configured_columns = get_config_value(
        config,
        ("text_columns",),
        ("cleaning", "text_columns"),
        ("schema", "text_columns"),
        default=[],
    )
    return [rename_map.get(column, column) for column in configured_columns if column]


def clean_text_value(value: object, *, trim: bool, empty_to_null: bool, null_tokens: set[str]):
    if value is None or pd.isna(value):
        return None
    text = str(value)
    if trim:
        text = text.strip()
    if text in null_tokens or (empty_to_null and text == ""):
        return None
    return text


def clean_numeric_series(series: pd.Series, null_tokens: set[str]) -> pd.Series:
    # Clean numeric columns using config-driven null token handling and coercion.
    cleaned = series.astype("string")
    for token in null_tokens:
        cleaned = cleaned.str.replace(token, "", regex=False)
    cleaned = (
        cleaned.str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    cleaned = cleaned.replace({"": pd.NA, "-": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def derive_canonical_date(
    dataframe: pd.DataFrame,
    config: dict,
    rename_map: dict[str, str],
) -> pd.DataFrame:
    # Derive the canonical date using the configured source priority.
    priority = get_config_value(
        config,
        ("canonical_date", "source_priority"),
        ("date_derivation", "source_priority"),
        ("date_source_priority",),
        default=[],
    )
    target_column = get_config_value(
        config,
        ("canonical_date", "target_column"),
        ("date_derivation", "target_column"),
        default="canonical_date",
    )
    resolved_priority = [rename_map.get(column, column) for column in priority]

    canonical_series = pd.Series(pd.NaT, index=dataframe.index, dtype="datetime64[ns]")
    for column in resolved_priority:
        if column not in dataframe.columns:
            continue
        parsed = pd.to_datetime(dataframe[column], errors="coerce", utc=False)
        canonical_series = canonical_series.fillna(parsed)

    dataframe[target_column] = canonical_series.dt.strftime("%Y-%m-%d")
    dataframe.loc[canonical_series.isna(), target_column] = None
    return dataframe


def apply_defaults(dataframe: pd.DataFrame, config: dict) -> pd.DataFrame:
    defaults = get_config_value(
        config,
        ("defaults", "column_defaults"),
        ("column_defaults",),
        ("default_values",),
        default={},
    )
    if not isinstance(defaults, dict):
        return dataframe

    for column, default_value in defaults.items():
        if column not in dataframe.columns:
            dataframe[column] = default_value
            continue
        dataframe[column] = dataframe[column].where(dataframe[column].notna(), default_value)
    return dataframe


def apply_derived_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Derive output metrics after core cleaning so Redshift-ready types stay stable.
    revenue = pd.to_numeric(dataframe.get("revenue"), errors="coerce")
    orders = pd.to_numeric(dataframe.get("orders"), errors="coerce")
    impressions = pd.to_numeric(dataframe.get("impressions"), errors="coerce")

    dataframe["revenue_per_order"] = revenue.divide(orders.where(orders.ne(0)))
    dataframe["revenue_per_impression"] = revenue.divide(impressions.where(impressions.ne(0)))
    dataframe["impressions_per_order"] = impressions.divide(orders.where(orders.ne(0)))
    return dataframe


def build_quarantine_mask(dataframe: pd.DataFrame, config: dict) -> pd.Series:
    required_columns = get_config_value(
        config,
        ("quarantine_rules", "required_columns"),
        ("validation", "required_columns"),
        default=[],
    )
    non_negative_columns = get_config_value(
        config,
        ("quarantine_rules", "non_negative_columns"),
        ("validation", "non_negative_columns"),
        default=[],
    )
    date_columns = get_config_value(
        config,
        ("quarantine_rules", "date_columns"),
        ("validation", "date_columns"),
        default=["canonical_date"],
    )

    mask = pd.Series(False, index=dataframe.index)

    for column in required_columns:
        if column not in dataframe.columns:
            mask = mask | True
            continue
        mask = mask | dataframe[column].isna()

    for column in non_negative_columns:
        if column not in dataframe.columns:
            continue
        numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
        mask = mask | numeric_series.lt(0).fillna(False)

    for column in date_columns:
        if column not in dataframe.columns:
            continue
        parsed_dates = pd.to_datetime(dataframe[column], errors="coerce")
        mask = mask | dataframe[column].notna() & parsed_dates.isna()

    return mask


def reorder_columns(dataframe: pd.DataFrame, final_columns: list[str]) -> pd.DataFrame:
    for column in final_columns:
        if column not in dataframe.columns:
            dataframe[column] = None
    remaining_columns = [column for column in dataframe.columns if column not in final_columns]
    ordered_columns = final_columns + remaining_columns
    return dataframe[ordered_columns]


def reorder_columns_strict(dataframe: pd.DataFrame, final_columns: list[str]) -> pd.DataFrame:
    # Enforce the final schema exactly so processed output excludes raw-only columns.
    for column in final_columns:
        if column not in dataframe.columns:
            dataframe[column] = None
    return dataframe[final_columns]


def build_processed_prefix(start_date: str, end_date: str, batch_id: str) -> str:
    normalized_batch_id = batch_id.replace("#", "_")
    return (
        f"processed/adscribe/start_date={start_date}/"
        f"end_date={end_date}/"
        f"batch_id={normalized_batch_id}"
    )


def build_quarantine_prefix(start_date: str, end_date: str, batch_id: str) -> str:
    normalized_batch_id = batch_id.replace("#", "_")
    return (
        f"quarantine/adscribe/start_date={start_date}/"
        f"end_date={end_date}/"
        f"batch_id={normalized_batch_id}"
    )


def dataframe_to_s3(
    dataframe: pd.DataFrame,
    *,
    s3_client: object,
    bucket_name: str,
    key_prefix: str,
    preferred_format: str,
) -> tuple[str, str]:
    # Write processed or quarantine output, preferring parquet and falling back to CSV when needed.
    normalized_format = str(preferred_format or "parquet").strip().lower()
    if normalized_format == "parquet":
        parquet_buffer = io.BytesIO()
        try:
            dataframe.to_parquet(parquet_buffer, index=False, compression="snappy")
            key = f"{key_prefix}/transformed.parquet"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            return key, "parquet"
        except Exception:
            pass

    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    key = f"{key_prefix}/transformed.csv"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    return key, "csv"


def write_partitioned_processed_output(
    dataframe: pd.DataFrame,
    *,
    s3_client: object,
    bucket_name: str,
    processed_prefix: str,
    batch_id: str,
    preferred_format: str,
    fallback_format: str,
) -> str:
    # Write processed output partitioned by canonical date under year/month/day paths.
    partitioned_dataframe = dataframe.copy()
    partition_dates = pd.to_datetime(partitioned_dataframe["date"], errors="coerce")
    valid_rows = partition_dates.notna()
    partitioned_dataframe = partitioned_dataframe.loc[valid_rows].copy()
    partition_dates = partition_dates.loc[valid_rows]

    partitioned_dataframe["_year"] = partition_dates.dt.strftime("%Y")
    partitioned_dataframe["_month"] = partition_dates.dt.strftime("%m")
    partitioned_dataframe["_day"] = partition_dates.dt.strftime("%d")

    normalized_batch_id = batch_id.replace("#", "_")
    normalized_processed_prefix = processed_prefix.rstrip("/")
    preferred = str(preferred_format or "parquet").strip().lower()
    fallback = str(fallback_format or "csv").strip().lower()

    for (year, month, day), group in partitioned_dataframe.groupby(["_year", "_month", "_day"], dropna=False):
        output_dataframe = group.drop(columns=["_year", "_month", "_day"])
        partition_prefix = f"{normalized_processed_prefix}/year={year}/month={month}/day={day}"

        if preferred == "parquet":
            parquet_buffer = io.BytesIO()
            try:
                output_dataframe.to_parquet(parquet_buffer, index=False, compression="snappy")
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{partition_prefix}/transformed_{normalized_batch_id}.parquet",
                    Body=parquet_buffer.getvalue(),
                    ContentType="application/octet-stream",
                )
                continue
            except Exception:
                pass

        output_format = fallback if fallback in {"csv", "parquet"} else "csv"
        if output_format == "parquet":
            parquet_buffer = io.BytesIO()
            output_dataframe.to_parquet(parquet_buffer, index=False, compression="snappy")
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{partition_prefix}/transformed_{normalized_batch_id}.parquet",
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            continue

        csv_buffer = io.StringIO()
        output_dataframe.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{partition_prefix}/transformed_{normalized_batch_id}.csv",
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

    return f"{normalized_processed_prefix}/"


def transform_adscribe_data(
    csv_bytes: bytes,
    *,
    config: dict,
    batch_id: str,
    run_id: str,
    source_key: str,
    processed_at: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dataframe = pd.read_csv(io.BytesIO(csv_bytes))
    rename_map = get_rename_map(config)
    final_schema = get_final_schema(config)
    null_tokens = get_null_tokens(config)

    if rename_map:
        dataframe = dataframe.rename(columns=rename_map)

    dataframe["source_type"] = "adscribe"
    dataframe["batch_id"] = batch_id
    dataframe["run_id"] = run_id
    dataframe["source_key"] = source_key
    dataframe["processed_at"] = processed_at

    dataframe = derive_canonical_date(dataframe, config, rename_map)

    # Drop redundant source date columns after canonical date derivation using config-driven keys.
    redundant_date_columns = get_config_value(
        config,
        ("drop_columns_after_derivation",),
        ("canonical_date", "drop_source_columns"),
        default=[],
    )
    resolved_redundant_dates = [
        rename_map.get(column, column)
        for column in redundant_date_columns
        if rename_map.get(column, column) in dataframe.columns
    ]
    if resolved_redundant_dates:
        dataframe = dataframe.drop(columns=resolved_redundant_dates)

    trim_text = parse_bool(
        get_config_value(config, ("text_cleaning", "trim"), ("cleaning", "trim_text"), default=True),
        default=True,
    )
    empty_to_null = parse_bool(
        get_config_value(
            config,
            ("text_cleaning", "empty_to_null"),
            ("cleaning", "empty_to_null"),
            default=True,
        ),
        default=True,
    )
    for column in get_text_columns(config, rename_map):
        if column not in dataframe.columns:
            continue
        dataframe[column] = dataframe[column].apply(
            lambda value: clean_text_value(
                value,
                trim=trim_text,
                empty_to_null=empty_to_null,
                null_tokens=null_tokens,
            )
        )

    for column in get_numeric_columns(config, rename_map):
        if column not in dataframe.columns:
            continue
        dataframe[column] = clean_numeric_series(dataframe[column], null_tokens)

    dataframe = apply_defaults(dataframe, config)
    dataframe = apply_derived_columns(dataframe)

    dedupe_enabled = parse_bool(
        get_config_value(
            config,
            ("deduplicate", "enabled"),
            ("drop_exact_duplicates",),
            default=False,
        ),
        default=False,
    )
    if dedupe_enabled:
        dataframe = dataframe.drop_duplicates()

    quarantine_mask = build_quarantine_mask(dataframe, config)
    quarantine_dataframe = dataframe.loc[quarantine_mask].copy()
    processed_dataframe = dataframe.loc[~quarantine_mask].copy()

    processed_dataframe = reorder_columns_strict(processed_dataframe, final_schema)
    if not quarantine_dataframe.empty:
        quarantine_dataframe = reorder_columns(quarantine_dataframe, final_schema)

    return processed_dataframe, quarantine_dataframe


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
    processed_key: str | None = None,
    quarantine_key: str | None = None,
    config_key: str | None = None,
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
        ("processed_key", processed_key),
        ("quarantine_key", quarantine_key),
        ("config_key", config_key),
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
    config_key = "pipeline/config/adscribe_pipeline_config.json"
    prefix = build_s3_prefix(start_date, end_date, batch_id)
    processed_prefix = build_processed_prefix(start_date, end_date, batch_id)
    quarantine_prefix = build_quarantine_prefix(start_date, end_date, batch_id)
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

        config = load_config_from_s3(s3_client, bucket_name, config_key)
        processed_at = datetime.now(timezone.utc).isoformat()
        processed_dataframe, quarantine_dataframe = transform_adscribe_data(
            csv_bytes,
            config=config,
            batch_id=batch_id,
            run_id=run_id,
            source_key=source_key,
            processed_at=processed_at,
        )

        preferred_format = get_config_value(
            config,
            ("output", "preferred_format"),
            ("output", "format"),
            ("format",),
            default="parquet",
        )
        fallback_format = get_config_value(
            config,
            ("output", "fallback_format"),
            default="csv",
        )
        processed_output_prefix = get_config_value(
            config,
            ("output", "processed_prefix"),
            default="processed/adscribe",
        )
        processed_key = write_partitioned_processed_output(
            processed_dataframe,
            s3_client=s3_client,
            bucket_name=bucket_name,
            processed_prefix=processed_output_prefix,
            batch_id=batch_id,
            preferred_format=preferred_format,
            fallback_format=fallback_format,
        )

        quarantine_key = None
        if not quarantine_dataframe.empty:
            quarantine_output_prefix = get_config_value(
                config,
                ("output", "quarantine_prefix"),
                default=quarantine_prefix,
            )
            quarantine_key, _ = dataframe_to_s3(
                quarantine_dataframe,
                s3_client=s3_client,
                bucket_name=bucket_name,
                key_prefix=quarantine_output_prefix,
                preferred_format=preferred_format,
            )

        update_batch_status(
            dynamodb_client,
            table_name,
            batch_id,
            status="PROCESSED",
            updated_at=processed_at,
            run_id=run_id,
            bucket_name=bucket_name,
            source_key=source_key,
            metadata_key=metadata_key,
            processed_key=processed_key,
            quarantine_key=quarantine_key,
            config_key=config_key,
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
