import json
import sys
from datetime import datetime
from typing import Any, Dict, List, Tuple

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
]


# ------------------------------
# Argument and config utilities
# ------------------------------

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


def _build_client_config_s3_uri(bucket_name: str, client_name: str, base_prefix: str, version_file: str) -> str:
    return f"s3://{bucket_name}/{base_prefix}/client={client_name}/{version_file}"


# ------------------------------
# DataFrame helpers
# ------------------------------

def _read_csv(glue_ctx: GlueContext, bucket_name: str, object_key: str) -> DataFrame:
    path = f"s3://{bucket_name}/{object_key}"
    return (
        glue_ctx.spark_session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def _normalize_header_columns(df: DataFrame) -> DataFrame:
    """
    Make raw CSV headers safer by trimming leading/trailing whitespace.
    Example: ' order_date\\t' -> 'order_date'
    """
    out = df
    for c in df.columns:
        normalized = c.strip()
        if normalized != c:
            out = out.withColumnRenamed(c, normalized)
    return out


def _safe_rename(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    out = df
    for src, dst in rename_map.items():
        if src in out.columns and src != dst:
            out = out.withColumnRenamed(src, dst)
    return out


def _apply_output_from_source_mapping(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """
    Some configs use mapping as: {output_column: source_column}.
    Example: {"discount_code": "code"} means create discount_code from code.
    """
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
    if not valid:
        return df
    return df.fillna(valid)


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
    agg_exprs = [_build_agg_expr(m) for m in metrics]
    return df.groupBy(*group_by_cols).agg(*agg_exprs)


# ------------------------------
# Client-specific orchestration
# ------------------------------

def _pick_primary_and_lookup(client_name: str, file_map: Dict[str, str]) -> Tuple[str, str]:
    names = list(file_map.keys())

    if client_name == "alpha":
        primary = next(n for n in names if "orders" in n)
        lookup = next(n for n in names if "codes" in n)
        return primary, lookup

    if client_name == "beta":
        primary = next(n for n in names if "sales" in n and "shows" not in n)
        lookup = next(n for n in names if "shows_and_codes" in n)
        return primary, lookup

    if client_name == "gamma":
        primary = next(n for n in names if "creator_gamma_sales" in n)
        lookup = next(n for n in names if "salesforce_data" in n)
        return primary, lookup

    raise ValueError(f"Unsupported client_name: {client_name}")


def _run_client_transform(
    glue_ctx: GlueContext,
    client_name: str,
    bucket_name: str,
    event_input: Dict[str, Any],
    cfg: Dict[str, Any],
) -> DataFrame:
    file_names: List[str] = event_input.get("file_names", [])
    object_keys: List[str] = event_input.get("object_keys", [])

    if len(file_names) != len(object_keys):
        raise ValueError("event_input file_names and object_keys length mismatch")

    file_map = {file_names[i]: object_keys[i] for i in range(len(file_names))}

    required_files = set(cfg.get("required_files", []))
    missing_required = sorted([f for f in required_files if f not in file_map])
    if missing_required:
        raise ValueError(f"Missing required files for {client_name}: {missing_required}")

    primary_file, lookup_file = _pick_primary_and_lookup(client_name, file_map)

    primary_df = _read_csv(glue_ctx, bucket_name, file_map[primary_file])
    lookup_df = _read_csv(glue_ctx, bucket_name, file_map[lookup_file])
    primary_df = _normalize_header_columns(primary_df)
    lookup_df = _normalize_header_columns(lookup_df)

    primary_df = _safe_rename(primary_df, cfg.get("rename_columns", {}))

    join_cfg = (cfg.get("joins") or [None])[0]
    if not join_cfg:
        raise ValueError("Config must contain at least one join definition")

    lookup_rename = join_cfg.get("column_mapping", {})
    lookup_df = _safe_rename(lookup_df, lookup_rename)

    norm_cfg = join_cfg.get("normalization", {})
    trim_cols = []
    lower_cols = []
    if norm_cfg.get("trim"):
        trim_cols = [join_cfg["keys"][0]]
    if norm_cfg.get("lowercase"):
        lower_cols = [join_cfg["keys"][0]]

    # Also include explicit cleaning rules from config.
    for col_name, rule in (cfg.get("cleaning_rules") or {}).items():
        if "trim" in str(rule).lower() and col_name not in trim_cols:
            trim_cols.append(col_name)
        if "lower" in str(rule).lower() and col_name not in lower_cols:
            lower_cols.append(col_name)

    primary_df = _normalize_columns(primary_df, trim_cols, lower_cols)
    lookup_df = _normalize_columns(lookup_df, trim_cols, lower_cols)

    join_key = join_cfg["keys"][0]
    join_how = join_cfg.get("how", "left")

    joined = primary_df.alias("left").join(
        lookup_df.alias("right"),
        F.col(f"left.{join_key}") == F.col(f"right.{join_key}"),
        join_how,
    )

    if join_cfg.get("quarantine_on_unmatched", False):
        joined = joined.filter(F.col(f"right.{join_key}").isNotNull())

    # Resolve duplicate join key column by keeping left-side key.
    joined = joined.drop(F.col(f"right.{join_key}"))

    fill_nulls = cfg.get("transformations", {}).get("fill_nulls", {})
    joined = _apply_fill_nulls(joined, fill_nulls)

    derived_cols = cfg.get("transformations", {}).get("derived_columns", [])
    joined = _apply_derived_columns(joined, derived_cols)

    filters = cfg.get("transformations", {}).get("filter_expressions", [])
    joined = _apply_filters(joined, filters)

    post_join_columns = cfg.get("post_join_columns", {})
    joined = _apply_output_from_source_mapping(joined, post_join_columns)

    agg_cfg = cfg.get("gold_aggregations") or cfg.get("aggregation")
    if not agg_cfg:
        raise ValueError("Missing aggregation config")

    aggregated = _aggregate(joined, agg_cfg.get("group_by", []), agg_cfg.get("metrics", []))

    # Unify business date column name for partitioning requirement.
    if "order_date" not in aggregated.columns:
        if "date" in aggregated.columns:
            aggregated = aggregated.withColumn("order_date", F.col("date").cast("date"))
        else:
            raise ValueError("Transformed dataset must contain 'date' or 'order_date'")

    aggregated = aggregated.withColumn("client_name", F.lit(client_name))
    return aggregated


# ------------------------------
# Output and Redshift load
# ------------------------------

def _write_processed_to_s3(df: DataFrame, bucket_name: str, client_name: str) -> str:
    out = (
        df.withColumn("order_date", F.to_date(F.col("order_date")))
        .filter(F.col("order_date").isNotNull())
        .withColumn("year", F.date_format(F.col("order_date"), "yyyy"))
        .withColumn("month", F.date_format(F.col("order_date"), "MM"))
        .withColumn("day", F.date_format(F.col("order_date"), "dd"))
    )

    processed_prefix = f"processed/client_uploads/{client_name}/"
    processed_uri = f"s3://{bucket_name}/{processed_prefix}"

    out.write.mode("append").partitionBy("year", "month", "day").parquet(processed_uri)
    return processed_uri


def _type_expr(type_name: str) -> T.DataType:
    mapping = {
        "string": T.StringType(),
        "double": T.DoubleType(),
        "date": T.DateType(),
        "bigint": T.LongType(),
    }
    if type_name not in mapping:
        raise ValueError(f"Unsupported type in FINAL_SCHEMA: {type_name}")
    return mapping[type_name]


def _enforce_final_schema(df: DataFrame, client_name: str, bucket_name: str) -> DataFrame:
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

    for col_name, col_type in FINAL_SCHEMA:
        dtype = _type_expr(col_type)
        if col_name not in out.columns:
            out = out.withColumn(col_name, F.lit(None).cast(dtype))
        else:
            out = out.withColumn(col_name, F.col(col_name).cast(dtype))

    # Redshift target requires non-null orders and client_name.
    out = out.fillna({"orders": 0})
    out = out.withColumn(
        "client_name",
        F.when(F.col("client_name").isNull() | (F.length(F.trim(F.col("client_name"))) == 0), F.lit(client_name)).otherwise(
            F.col("client_name")
        ),
    )

    # Keep only final columns in exact order.
    out = out.select(*[F.col(c) for c, _ in FINAL_SCHEMA])
    return out


def _write_schema_drift_quarantine(
    df: DataFrame,
    bucket_name: str,
    client_name: str,
    extra_columns: List[str],
    missing_columns: List[str],
    existing_columns: List[str],
    expected_columns: List[str],
) -> None:
    key = f"quarantine/client={client_name}/columns/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    payload = {
        "type": "schema_drift",
        "client_name": client_name,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "extra_columns": extra_columns,
        "missing_columns": missing_columns,
        "existing_columns": existing_columns,
        "expected_columns": expected_columns,
    }
    boto3.client("s3").put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    if extra_columns:
        # Persist actual extra-column values for investigation.
        columns_df = df
        if "date" in columns_df.columns:
            columns_df = columns_df.withColumn("order_date", F.col("date").cast("date"))
        elif "order_date" not in columns_df.columns:
            columns_df = columns_df.withColumn("order_date", F.lit(None).cast("date"))

        columns_df = columns_df.withColumn("client_name", F.lit(client_name))
        keep_cols = ["client_name", "order_date"] + [c for c in extra_columns if c in columns_df.columns]
        quarantine_uri = f"s3://{bucket_name}/quarantine/client={client_name}/columns/"
        (
            columns_df.select(*keep_cols)
            .withColumn("year", F.date_format(F.col("order_date"), "yyyy"))
            .withColumn("month", F.date_format(F.col("order_date"), "MM"))
            .withColumn("day", F.date_format(F.col("order_date"), "dd"))
            .write.mode("append")
            .partitionBy("year", "month", "day")
            .parquet(quarantine_uri)
        )


def _redshift_jdbc_url(options: Dict[str, str]) -> str:
    if options.get("REDSHIFT_JDBC_URL"):
        jdbc_url = options["REDSHIFT_JDBC_URL"]
        if "your-redshift-cluster" in jdbc_url or "your_database" in jdbc_url:
            raise ValueError(
                "REDSHIFT_JDBC_URL is still a placeholder. "
                "Set it to the real endpoint, e.g. jdbc:redshift://<endpoint>:5439/<database>."
            )
        return jdbc_url

    host = options.get("REDSHIFT_HOST")
    port = options.get("REDSHIFT_PORT", "5439")
    database = options.get("REDSHIFT_DATABASE")
    if not host or not database:
        raise ValueError("Missing Redshift connection details. Set REDSHIFT_JDBC_URL or REDSHIFT_HOST + REDSHIFT_DATABASE")

    return f"jdbc:redshift://{host}:{port}/{database}"


def _redshift_table(options: Dict[str, str]) -> str:
    if options.get("REDSHIFT_TABLE"):
        return options["REDSHIFT_TABLE"]

    schema = options.get("REDSHIFT_SCHEMA")
    table = options.get("REDSHIFT_TABLE_NAME")
    if not schema or not table:
        raise ValueError("Missing Redshift target table. Set REDSHIFT_TABLE or REDSHIFT_SCHEMA + REDSHIFT_TABLE_NAME")

    return f"{schema}.{table}"


def _write_to_redshift(df: DataFrame, options: Dict[str, str]) -> None:
    jdbc_url = _redshift_jdbc_url(options)
    table_name = _redshift_table(options)
    user = options.get("REDSHIFT_USER")
    password = options.get("REDSHIFT_PASSWORD")

    if not user or not password:
        raise ValueError("Missing Redshift credentials. Set REDSHIFT_USER and REDSHIFT_PASSWORD")

    try:
        (
            df.drop("year", "month", "day")
            .write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.amazon.redshift.jdbc.Driver")
            .mode("append")
            .save()
        )
    except Exception as exc:
        raise RuntimeError(
            "Redshift load failed. Check JDBC endpoint/database, credentials, and Glue network access "
            "(VPC/subnet/security-group/NACL) to Redshift."
        ) from exc


# ------------------------------
# Main
# ------------------------------

def main() -> None:
    required = getResolvedOptions(sys.argv, ["JOB_NAME", "EVENT_INPUT"])
    optional = _parse_optional_args(sys.argv)

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(required["JOB_NAME"], required)

    try:
        event_input = json.loads(required["EVENT_INPUT"])
        client_name = event_input["client_name"].strip().lower()
        bucket_name = event_input["bucket_name"]

        config_base_prefix = optional.get("CONFIG_BASE_PREFIX", "config")
        config_version_file = optional.get("CONFIG_VERSION_FILE", "v1.json")
        config_uri = _build_client_config_s3_uri(bucket_name, client_name, config_base_prefix, config_version_file)

        print(json.dumps({"stage": "config_load", "config_uri": config_uri}))
        cfg = _load_json_from_s3(config_uri)

        transformed_df = _run_client_transform(
            glue_ctx=glue_ctx,
            client_name=client_name,
            bucket_name=bucket_name,
            event_input=event_input,
            cfg=cfg,
        )

        final_df = _enforce_final_schema(transformed_df, client_name, bucket_name)
        processed_uri = _write_processed_to_s3(final_df, bucket_name, client_name)
        print(json.dumps({"stage": "processed_write", "processed_uri": processed_uri}))

        _write_to_redshift(final_df, optional)
        print(json.dumps({"stage": "redshift_load", "status": "SUCCESS", "timestamp": datetime.utcnow().isoformat()}))

        job.commit()
    except Exception as exc:
        print(json.dumps({"stage": "failed", "error": str(exc)}))
        raise


if __name__ == "__main__":
    main()
