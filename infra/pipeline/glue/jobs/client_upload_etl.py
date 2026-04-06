import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import unquote_plus

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


FINAL_SCHEMA = [
    ("discount_code", "string"),
    ("orders", "bigint"),
    ("revenue", "double"),
    ("order_date", "date"),
    ("client_name", "string"),
    ("event_name", "string"),
    ("load_id", "string"),
    ("event_date", "date"),
]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_optional_args(argv: List[str]) -> Dict[str, str]:
    options: Dict[str, str] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        if token.startswith("--") and i + 1 < len(argv):
            options[token[2:]] = argv[i + 1]
            i += 2
        else:
            i += 1
    return options


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket_and_key = uri[5:]
    bucket, key = bucket_and_key.split("/", 1)
    return bucket, key


def _load_json_from_s3(s3_uri: str) -> Dict[str, Any]:
    bucket, key = _parse_s3_uri(s3_uri)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return json.loads(body)


def _require_non_empty(value: Any, field_name: str) -> str:
    if value is None:
        raise ValueError(f"Missing required field in EVENT_INPUT: {field_name}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Missing required field in EVENT_INPUT: {field_name}")
    return text


def _resolve_load_id(event_input: Dict[str, Any]) -> str:
    direct_load_id = event_input.get("load_id")
    if direct_load_id:
        return _require_non_empty(direct_load_id, "load_id")

    workflow_obj = event_input.get("workflow")
    if isinstance(workflow_obj, dict):
        nested_load_id = workflow_obj.get("load_id")
        if nested_load_id:
            return _require_non_empty(nested_load_id, "workflow.load_id")

    raise ValueError("Missing required field in EVENT_INPUT: load_id")


def _build_client_config_s3_uri(bucket_name: str, client_name: str, base_prefix: str, version_file: str) -> str:
    return f"s3://{bucket_name}/{base_prefix}/client={client_name}/{version_file}"


def _read_csv(glue_ctx: GlueContext, bucket_name: str, object_key: str) -> DataFrame:
    return (
        glue_ctx.spark_session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"s3://{bucket_name}/{object_key}")
    )


def _normalize_header_columns(df: DataFrame) -> DataFrame:
    out = df
    for col_name in df.columns:
        normalized = col_name.strip()
        if normalized != col_name:
            out = out.withColumnRenamed(col_name, normalized)
    return out


def _safe_rename(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    out = df
    for src, dst in rename_map.items():
        if src in out.columns and src != dst:
            out = out.withColumnRenamed(src, dst)
    return out


def _apply_output_from_source_mapping(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    out = df
    for output_col, source_col in mapping.items():
        if source_col in out.columns:
            out = out.withColumn(output_col, F.col(source_col))
    return out


def _normalize_columns(df: DataFrame, trim_columns: List[str], lower_columns: List[str]) -> DataFrame:
    out = df
    for col_name in trim_columns:
        if col_name in out.columns:
            out = out.withColumn(col_name, F.trim(F.col(col_name).cast("string")))
    for col_name in lower_columns:
        if col_name in out.columns:
            out = out.withColumn(col_name, F.lower(F.col(col_name).cast("string")))
    return out


def _apply_fill_nulls(df: DataFrame, fill_map: Dict[str, Any]) -> DataFrame:
    if not fill_map:
        return df
    valid = {k: v for k, v in fill_map.items() if k in df.columns}
    return df.fillna(valid) if valid else df


def _apply_derived_columns(df: DataFrame, derived_columns: List[Dict[str, str]]) -> DataFrame:
    out = df
    for item in derived_columns:
        name = item.get("name")
        expression = item.get("expression")
        if name and expression:
            out = out.withColumn(name, F.expr(expression))
    return out


def _apply_filters(df: DataFrame, filter_expressions: List[str]) -> DataFrame:
    out = df
    for expr in filter_expressions:
        out = out.filter(F.expr(expr))
    return out


def _build_agg_expr(metric: Dict[str, str]) -> F.Column:
    fn = metric["function"].lower()
    col_name = metric["column"]
    alias = metric["alias"]
    if fn == "sum":
        return F.sum(F.col(col_name)).alias(alias)
    if fn == "count":
        return F.count(F.col(col_name)).alias(alias)
    if fn == "count_distinct":
        return F.countDistinct(F.col(col_name)).alias(alias)
    raise ValueError(f"Unsupported aggregation function: {fn}")


def _aggregate(df: DataFrame, group_by_cols: List[str], metrics: List[Dict[str, str]]) -> DataFrame:
    return df.groupBy(*group_by_cols).agg(*[_build_agg_expr(m) for m in metrics])


def _pick_primary_and_lookup(client_name: str, file_map: Dict[str, str]) -> Tuple[str, str]:
    names = list(file_map.keys())
    if client_name == "alpha":
        return next(n for n in names if "orders" in n), next(n for n in names if "codes" in n)
    if client_name == "beta":
        return next(n for n in names if "sales" in n and "shows" not in n), next(n for n in names if "shows_and_codes" in n)
    if client_name == "gamma":
        return next(n for n in names if "creator_gamma_sales" in n), next(n for n in names if "salesforce_data" in n)
    raise ValueError(f"Unsupported client_name: {client_name}")


def _canonical_file_name(name: str) -> str:
    """
    Normalize filename for robust matching across client uploads/config:
    - decode URL-encoded names
    - keep only basename (strip any prefix/path)
    - trim and lowercase
    - treat `.csv` suffix as optional
    """
    raw = unquote_plus((name or "").strip())
    base = raw.split("/")[-1].strip().lower()
    if base.endswith(".csv"):
        base = base[:-4]
    return base


def _type_expr(type_name: str) -> T.DataType:
    mapping = {
        "string": T.StringType(),
        "double": T.DoubleType(),
        "date": T.DateType(),
        "bigint": T.LongType(),
    }
    return mapping[type_name]


def _write_schema_drift_quarantine(
    df: DataFrame,
    bucket_name: str,
    client_name: str,
    extra_columns: List[str],
    missing_columns: List[str],
    existing_columns: List[str],
    expected_columns: List[str],
) -> None:
    s3 = boto3.client("s3")
    drift_key = f"quarantine/client={client_name}/columns/{_now_utc().strftime('%Y%m%dT%H%M%SZ')}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=drift_key,
        Body=json.dumps(
            {
                "type": "schema_drift",
                "client_name": client_name,
                "timestamp_utc": _now_utc().isoformat(),
                "extra_columns": extra_columns,
                "missing_columns": missing_columns,
                "existing_columns": existing_columns,
                "expected_columns": expected_columns,
            },
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    if extra_columns:
        sample = df.withColumn("client_name", F.lit(client_name))
        if "order_date" not in sample.columns and "date" in sample.columns:
            sample = sample.withColumn("order_date", F.col("date").cast("date"))
        if "order_date" not in sample.columns:
            sample = sample.withColumn("order_date", F.lit(None).cast("date"))

        keep_cols = ["client_name", "order_date"] + [c for c in extra_columns if c in sample.columns]
        (
            sample.select(*keep_cols)
            .withColumn("year", F.date_format(F.col("order_date"), "yyyy"))
            .withColumn("month", F.date_format(F.col("order_date"), "MM"))
            .withColumn("day", F.date_format(F.col("order_date"), "dd"))
            .write.mode("append")
            .partitionBy("year", "month", "day")
            .parquet(f"s3://{bucket_name}/quarantine/client={client_name}/columns/")
        )


def _enforce_final_schema(
    df: DataFrame,
    client_name: str,
    bucket_name: str,
    load_id: str,
    event_date: str,
    event_name: str,
) -> DataFrame:
    expected_names = [name for name, _ in FINAL_SCHEMA]
    existing_names = df.columns

    extra_columns = sorted([c for c in existing_names if c not in expected_names])
    missing_columns = sorted([c for c in expected_names if c not in existing_names])
    if extra_columns or missing_columns:
        _write_schema_drift_quarantine(
            df=df,
            bucket_name=bucket_name,
            client_name=client_name,
            extra_columns=extra_columns,
            missing_columns=missing_columns,
            existing_columns=existing_names,
            expected_columns=expected_names,
        )

    out = df
    if "order_date" not in out.columns and "date" in out.columns:
        out = out.withColumn("order_date", F.col("date").cast("date"))

    out = out.withColumn("client_name", F.lit(client_name))
    out = out.withColumn("event_name", F.lit(event_name))
    out = out.withColumn("load_id", F.lit(load_id))
    out = out.withColumn("event_date", F.lit(event_date).cast("date"))

    for col_name, col_type in FINAL_SCHEMA:
        dtype = _type_expr(col_type)
        if col_name not in out.columns:
            out = out.withColumn(col_name, F.lit(None).cast(dtype))
        else:
            out = out.withColumn(col_name, F.col(col_name).cast(dtype))

    out = out.fillna({"orders": 0})
    return out.select(*[F.col(c) for c, _ in FINAL_SCHEMA])


def _run_client_transform(
    glue_ctx: GlueContext,
    client_name: str,
    bucket_name: str,
    event_input: Dict[str, Any],
    cfg: Dict[str, Any],
    load_id: str,
    event_date: str,
    event_name: str,
) -> DataFrame:
    file_names: List[str] = event_input.get("file_names", [])
    object_keys: List[str] = event_input.get("object_keys", [])
    if len(file_names) != len(object_keys):
        raise ValueError("file_names and object_keys length mismatch")

    # Build canonical map so matching is resilient to `.csv` suffix and case.
    file_map: Dict[str, str] = {}
    for i in range(len(file_names)):
        incoming_name = file_names[i] or object_keys[i].split("/")[-1]
        canonical_name = _canonical_file_name(incoming_name)
        if canonical_name:
            file_map[canonical_name] = object_keys[i]

    required_files_raw: List[str] = cfg.get("required_files", [])
    required_files_canonical = {_canonical_file_name(name): name for name in required_files_raw}
    missing_required = sorted(
        [
            required_files_canonical[key]
            for key in required_files_canonical
            if key not in file_map
        ]
    )
    if missing_required:
        raise ValueError(f"Missing required files for {client_name}: {missing_required}")

    primary_file, lookup_file = _pick_primary_and_lookup(client_name, file_map)
    primary_df = _normalize_header_columns(_read_csv(glue_ctx, bucket_name, file_map[primary_file]))
    lookup_df = _normalize_header_columns(_read_csv(glue_ctx, bucket_name, file_map[lookup_file]))

    primary_df = _safe_rename(primary_df, cfg.get("rename_columns", {}))
    join_cfg = (cfg.get("joins") or [None])[0]
    if not join_cfg:
        raise ValueError("Config must contain joins")
    lookup_df = _safe_rename(lookup_df, join_cfg.get("column_mapping", {}))

    trim_cols: List[str] = []
    lower_cols: List[str] = []
    if join_cfg.get("normalization", {}).get("trim"):
        trim_cols.append(join_cfg["keys"][0])
    if join_cfg.get("normalization", {}).get("lowercase"):
        lower_cols.append(join_cfg["keys"][0])
    for col_name, rule in (cfg.get("cleaning_rules") or {}).items():
        if "trim" in str(rule).lower() and col_name not in trim_cols:
            trim_cols.append(col_name)
        if "lower" in str(rule).lower() and col_name not in lower_cols:
            lower_cols.append(col_name)

    primary_df = _normalize_columns(primary_df, trim_cols, lower_cols)
    lookup_df = _normalize_columns(lookup_df, trim_cols, lower_cols)

    join_key = join_cfg["keys"][0]
    joined = primary_df.alias("left").join(
        lookup_df.alias("right"),
        F.col(f"left.{join_key}") == F.col(f"right.{join_key}"),
        join_cfg.get("how", "left"),
    )

    if join_cfg.get("quarantine_on_unmatched", False):
        joined = joined.filter(F.col(f"right.{join_key}").isNotNull())

    joined = joined.drop(F.col(f"right.{join_key}"))
    joined = _apply_fill_nulls(joined, cfg.get("transformations", {}).get("fill_nulls", {}))
    joined = _apply_derived_columns(joined, cfg.get("transformations", {}).get("derived_columns", []))
    joined = _apply_filters(joined, cfg.get("transformations", {}).get("filter_expressions", []))
    joined = _apply_output_from_source_mapping(joined, cfg.get("post_join_columns", {}))

    agg_cfg = cfg.get("gold_aggregations") or cfg.get("aggregation")
    if not agg_cfg:
        raise ValueError("Missing aggregation config")
    aggregated = _aggregate(joined, agg_cfg.get("group_by", []), agg_cfg.get("metrics", []))

    return _enforce_final_schema(aggregated, client_name, bucket_name, load_id, event_date, event_name)


def _write_processed_to_s3(df: DataFrame, bucket_name: str, client_name: str) -> str:
    out = (
        df.withColumn("order_date", F.to_date(F.col("order_date")))
        .filter(F.col("order_date").isNotNull())
        .withColumn("year", F.date_format(F.col("order_date"), "yyyy"))
        .withColumn("month", F.date_format(F.col("order_date"), "MM"))
        .withColumn("day", F.date_format(F.col("order_date"), "dd"))
    )
    uri = f"s3://{bucket_name}/processed/client_uploads/{client_name}/"
    out.write.mode("append").partitionBy("year", "month", "day").parquet(uri)
    return uri


def _build_manifest_for_load(bucket_name: str, client_name: str, load_id: str, not_before: datetime) -> str:
    s3 = boto3.client("s3")
    prefix = f"processed/client_uploads/{client_name}/"

    keys: List[str] = []
    continuation = None
    while True:
        kwargs: Dict[str, Any] = {"Bucket": bucket_name, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj.get("Key", "")
            if key.endswith(".parquet") and obj["LastModified"] >= not_before:
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        continuation = resp.get("NextContinuationToken")

    manifest = {"entries": [{"url": f"s3://{bucket_name}/{k}", "mandatory": True} for k in keys]}
    manifest_key = f"processed/client_uploads/{client_name}/_manifests/{load_id}.manifest.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=manifest_key,
        Body=json.dumps(manifest).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket_name}/{manifest_key}"


def main() -> None:
    required = getResolvedOptions(sys.argv, ["JOB_NAME", "EVENT_INPUT"])
    optional = _parse_optional_args(sys.argv)

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(required["JOB_NAME"], required)

    start_time = _now_utc()
    try:
        event_input = json.loads(required["EVENT_INPUT"])
        client_name = _require_non_empty(event_input.get("client_name"), "client_name").lower()
        bucket_name = _require_non_empty(event_input.get("bucket_name"), "bucket_name")
        load_id = _resolve_load_id(event_input)
        event_date = _require_non_empty(event_input.get("event_date"), "event_date")
        event_name = str(event_input.get("event_name", "ObjectCreated")).strip() or "ObjectCreated"

        config_uri = _build_client_config_s3_uri(
            bucket_name,
            client_name,
            optional.get("CONFIG_BASE_PREFIX", "config"),
            optional.get("CONFIG_VERSION_FILE", "v1.json"),
        )
        cfg = _load_json_from_s3(config_uri)

        transformed_df = _run_client_transform(
            glue_ctx=glue_ctx,
            client_name=client_name,
            bucket_name=bucket_name,
            event_input=event_input,
            cfg=cfg,
            load_id=load_id,
            event_date=event_date,
            event_name=event_name,
        )

        processed_uri = _write_processed_to_s3(transformed_df, bucket_name, client_name)
        manifest_uri = _build_manifest_for_load(bucket_name, client_name, load_id, start_time)

        print(
            json.dumps(
                {
                    "stage": "processed_write",
                    "status": "SUCCESS",
                    "client_name": client_name,
                    "load_id": load_id,
                    "event_date": event_date,
                    "processed_uri": processed_uri,
                    "manifest_s3_uri": manifest_uri,
                }
            )
        )
        job.commit()
    except Exception as exc:
        print(json.dumps({"stage": "failed", "error": str(exc)}))
        raise


if __name__ == "__main__":
    main()
