import json
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    without = uri.replace("s3://", "", 1)
    bucket, key = without.split("/", 1)
    return bucket, key


def _load_config_from_s3(config_s3_uri: str) -> Dict[str, Any]:
    bucket, key = _parse_s3_uri(config_s3_uri)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return json.loads(body)


def _clean_currency(col: F.Column) -> F.Column:
    return F.regexp_replace(F.trim(col.cast("string")), r"[^0-9.\-]", "").cast("double")


def _clean_date(col: F.Column) -> F.Column:
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "yyyy/MM/dd"),
        F.to_date(col, "MM/dd/yyyy"),
        F.to_date(col, "dd-MM-yyyy"),
        F.to_date(F.substring(col, 1, 10), "yyyy-MM-dd")
    )


def _normalize_text(col: F.Column) -> F.Column:
    return F.lower(F.trim(col.cast("string")))


def _apply_column_mapping(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    out = df
    for src, dst in mapping.items():
        if src in out.columns and src != dst:
            out = out.withColumnRenamed(src, dst)
    return out


def _read_csv(glue_ctx: GlueContext, bucket: str, key: str) -> DataFrame:
    return (
        glue_ctx.spark_session.read.option("header", "true").option("inferSchema", "true").csv(f"s3://{bucket}/{key}")
    )


def _derive_partitioned_prefix(prefix_pattern: str, client_id: str) -> str:
    now = datetime.now(timezone.utc)
    return prefix_pattern.format(
        client_id=client_id,
        year=f"{now.year:04d}",
        month=f"{now.month:02d}",
        day=f"{now.day:02d}"
    )


def _transform_alpha(files: Dict[str, Any], cfg: Dict[str, Any], glue_ctx: GlueContext, bucket: str) -> Tuple[DataFrame, DataFrame]:
    orders = _read_csv(glue_ctx, bucket, files["orders"]["key"])
    codes = _read_csv(glue_ctx, bucket, files["codes"]["key"])

    orders = _apply_column_mapping(orders, cfg["column_mappings"]["orders"])
    codes = _apply_column_mapping(codes, cfg["column_mappings"]["codes"])

    orders = (
        orders.withColumn("event_date", _clean_date(F.col("event_date")))
        .withColumn("discount_code", _normalize_text(F.col("discount_code")))
        .withColumn("revenue", _clean_currency(F.col("revenue")))
        .filter(F.col("event_date").isNotNull())
        .filter(F.col("discount_code").isNotNull())
    )

    codes = codes.withColumn("code", _normalize_text(F.col("code")))
    codes = codes.withColumn("discount_code", F.coalesce(F.col("discount_code"), F.col("code")))

    joined = orders.alias("o").join(codes.alias("c"), F.col("o.discount_code") == F.col("c.code"), "left")
    quarantine = joined.filter(F.col("c.code").isNull()).select("o.*")
    loaded = (
        joined.filter(F.col("c.code").isNotNull())
        .withColumn("discount_code", F.col("c.discount_code"))
        .groupBy("event_date", "discount_code")
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.sum("revenue").alias("revenue")
        )
    )
    return loaded, quarantine


def _transform_beta(files: Dict[str, Any], cfg: Dict[str, Any], glue_ctx: GlueContext, bucket: str) -> Tuple[DataFrame, DataFrame]:
    sales = _read_csv(glue_ctx, bucket, files["sales"]["key"])
    shows = _read_csv(glue_ctx, bucket, files["shows_and_codes"]["key"])

    sales = _apply_column_mapping(sales, cfg["column_mappings"]["sales"])
    shows = _apply_column_mapping(shows, cfg["column_mappings"]["shows_and_codes"])

    sales = (
        sales.withColumn("event_date", _clean_date(F.col("event_date")))
        .withColumn("show_name", _normalize_text(F.col("show_name")))
        .withColumn("revenue", _clean_currency(F.col("revenue")))
        .withColumn("orders", F.coalesce(F.col("orders").cast("double"), F.lit(0.0)))
        .withColumn("new_value", F.coalesce(_clean_currency(F.col("new_value")), F.lit(0.0)))
        .withColumn("lapsed_value", F.coalesce(_clean_currency(F.col("lapsed_value")), F.lit(0.0)))
        .withColumn("active_value", F.coalesce(_clean_currency(F.col("active_value")), F.lit(0.0)))
        .filter(F.col("event_date").isNotNull())
    )

    shows = (
        shows.withColumn("show_name", _normalize_text(F.col("show_name")))
        .withColumn("discount_code", _normalize_text(F.col("discount_code")))
    )

    joined = sales.alias("s").join(shows.alias("c"), "show_name", "left")
    quarantine = joined.filter(F.col("c.discount_code").isNull()).select("s.*")
    matched = joined.filter(F.col("c.discount_code").isNotNull()).withColumn("discount_code", F.col("c.discount_code"))

    active_segments = (
        F.when(F.col("new_value") > 0, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("lapsed_value") > 0, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("active_value") > 0, F.lit(1)).otherwise(F.lit(0))
    )
    share = F.when(active_segments > 0, F.col("orders") / active_segments).otherwise(F.lit(0.0))

    matched = (
        matched.withColumn("new_orders", F.when(F.col("new_value") > 0, share).otherwise(F.lit(0.0)))
        .withColumn("lapsed_orders", F.when(F.col("lapsed_value") > 0, share).otherwise(F.lit(0.0)))
        .withColumn("active_orders", F.when(F.col("active_value") > 0, share).otherwise(F.lit(0.0)))
    )

    loaded = (
        matched.groupBy("event_date", "discount_code")
        .agg(
            F.sum("revenue").alias("revenue"),
            F.sum("orders").alias("orders"),
            F.sum("new_orders").alias("new_orders"),
            F.sum("lapsed_orders").alias("lapsed_orders"),
            F.sum("active_orders").alias("active_orders")
        )
    )
    return loaded, quarantine


def _transform_gamma(files: Dict[str, Any], cfg: Dict[str, Any], glue_ctx: GlueContext, bucket: str) -> Tuple[DataFrame, DataFrame]:
    sales = _read_csv(glue_ctx, bucket, files["sales"]["key"])
    lookup = _read_csv(glue_ctx, bucket, files["salesforce_data"]["key"])

    sales = _apply_column_mapping(sales, cfg["column_mappings"]["sales"])
    lookup = _apply_column_mapping(lookup, cfg["column_mappings"]["salesforce_data"])

    sales = (
        sales.withColumn("event_date", _clean_date(F.col("event_date")))
        .withColumn("show_name", _normalize_text(F.col("show_name")))
        .withColumn("revenue", _clean_currency(F.col("revenue")))
        .withColumn("orders", F.coalesce(F.col("orders").cast("double"), F.lit(0.0)))
        .filter(F.col("event_date").isNotNull())
        .filter(F.col("id").isNotNull())
    )

    lookup = lookup.withColumn("discount_code", _normalize_text(F.col("discount_code")))
    joined = sales.alias("s").join(lookup.alias("l"), "id", "left")

    quarantine = joined.filter(F.col("l.discount_code").isNull()).select("s.*")
    loaded = (
        joined.filter(F.col("l.discount_code").isNotNull())
        .withColumn("discount_code", F.col("l.discount_code"))
        .groupBy("event_date", "discount_code")
        .agg(
            F.sum("revenue").alias("revenue"),
            F.sum("orders").alias("orders")
        )
    )
    return loaded, quarantine


def _to_common_schema(df: DataFrame, client_id: str, batch_id: str) -> DataFrame:
    now_ts = datetime.now(timezone.utc).isoformat()
    standard_cols = [
        F.col("event_date"),
        F.lit(client_id).alias("client_id"),
        F.col("discount_code"),
        F.coalesce(F.col("show_name"), F.lit(None)).alias("show_name"),
        F.coalesce(F.col("revenue"), F.lit(0.0)).alias("revenue"),
        F.coalesce(F.col("orders"), F.lit(0.0)).alias("orders"),
        F.coalesce(F.col("new_orders"), F.lit(0.0)).alias("new_orders"),
        F.coalesce(F.col("lapsed_orders"), F.lit(0.0)).alias("lapsed_orders"),
        F.coalesce(F.col("active_orders"), F.lit(0.0)).alias("active_orders"),
        F.lit(now_ts).alias("ingestion_ts"),
        F.lit(batch_id).alias("source_batch_id")
    ]
    return df.select(*standard_cols)


def _assert_s3_prefix_has_objects(bucket: str, prefix: str) -> None:
    resp = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        raise RuntimeError(f"No objects written to expected prefix: s3://{bucket}/{prefix}")


def _load_to_redshift(df: DataFrame, jdbc_url: str, table: str, user: str, password: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.amazon.redshift.jdbc.Driver")
        .mode("append")
        .save()
    )


def main() -> None:
    args = getResolvedOptions(
        __import__("sys").argv,
        [
            "JOB_NAME",
            "CLIENT_ID",
            "SOURCE_BUCKET",
            "FILES_JSON",
            "CONFIG_S3_URI",
            "BATCH_ID",
            "REDSHIFT_JDBC_URL",
            "REDSHIFT_TABLE",
            "REDSHIFT_USER",
            "REDSHIFT_PASSWORD"
        ]
    )

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    config = _load_config_from_s3(args["CONFIG_S3_URI"])
    client_id = args["CLIENT_ID"].lower()
    client_cfg = config["clients"][client_id]
    files = json.loads(args["FILES_JSON"])
    source_bucket = args["SOURCE_BUCKET"]
    batch_id = args["BATCH_ID"]

    if client_id == "alpha":
        loaded_df, quarantine_df = _transform_alpha(files, client_cfg, glue_ctx, source_bucket)
    elif client_id == "beta":
        loaded_df, quarantine_df = _transform_beta(files, client_cfg, glue_ctx, source_bucket)
    elif client_id == "gamma":
        loaded_df, quarantine_df = _transform_gamma(files, client_cfg, glue_ctx, source_bucket)
    else:
        raise ValueError(f"Unsupported client_id: {client_id}")

    loaded_df = _to_common_schema(loaded_df, client_id, batch_id)
    quarantine_df = quarantine_df.withColumn("client_id", F.lit(client_id))

    processed_prefix = _derive_partitioned_prefix(client_cfg["output"]["processed_prefix_pattern"], client_id)
    quarantine_prefix = _derive_partitioned_prefix(client_cfg["output"]["quarantine_prefix_pattern"], client_id)

    loaded_df.write.mode("overwrite").parquet(f"s3://{source_bucket}/{processed_prefix}")
    quarantine_df.write.mode("overwrite").parquet(f"s3://{source_bucket}/{quarantine_prefix}")

    _assert_s3_prefix_has_objects(source_bucket, processed_prefix)

    _load_to_redshift(
        loaded_df,
        jdbc_url=args["REDSHIFT_JDBC_URL"],
        table=args["REDSHIFT_TABLE"],
        user=args["REDSHIFT_USER"],
        password=args["REDSHIFT_PASSWORD"]
    )

    print(
        json.dumps(
            {
                "status": "SUCCESS",
                "client_id": client_id,
                "processed_prefix": f"s3://{source_bucket}/{processed_prefix}",
                "quarantine_prefix": f"s3://{source_bucket}/{quarantine_prefix}",
                "batch_id": batch_id
            }
        )
    )
    job.commit()


if __name__ == "__main__":
    main()
