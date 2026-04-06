import json
import sys
from typing import Any, Dict, List, Tuple

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


REQUIRED_ARGS = [
    "JOB_NAME",
    "EVENT_INPUT",
    "REDSHIFT_JDBC_URL",
    "REDSHIFT_USER",
    "REDSHIFT_PASSWORD",
    "REDSHIFT_SCHEMA",
    "REDSHIFT_STAGING_TABLE",
]


def _require_str(payload: Dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"Missing required field in EVENT_INPUT: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Missing required field in EVENT_INPUT: {key}")
    return text


def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket_and_key = s3_uri[5:]
    bucket, key = bucket_and_key.split("/", 1)
    return bucket, key


def _manifest_uri(event_input: Dict[str, Any]) -> str:
    provided = event_input.get("manifest_s3_uri")
    if provided:
        return str(provided)
    bucket_name = _require_str(event_input, "bucket_name")
    client_name = _require_str(event_input, "client_name")
    load_id = _require_str(event_input, "load_id")
    return f"s3://{bucket_name}/processed/client_uploads/{client_name}/_manifests/{load_id}.manifest.json"


def _read_manifest_paths(glue_ctx: GlueContext, manifest_uri: str) -> List[str]:
    spark = glue_ctx.spark_session
    manifest_df = spark.read.option("multiline", "true").json(manifest_uri)
    entries = manifest_df.select(F.explode(F.col("entries")).alias("entry")).select(F.col("entry.url").alias("url"))
    paths = [row["url"] for row in entries.collect() if row["url"]]
    if not paths:
        raise ValueError(f"No parquet entries found in manifest: {manifest_uri}")
    return paths


def _prepare_staging_df(glue_ctx: GlueContext, parquet_paths: List[str], load_id: str, event_name: str) -> DataFrame:
    spark = glue_ctx.spark_session
    df = spark.read.parquet(*parquet_paths).filter(F.col("load_id") == F.lit(load_id))
    if df.rdd.isEmpty():
        raise ValueError(f"No rows found for load_id={load_id} in processed parquet paths")

    return (
        df.withColumn("event_name", F.coalesce(F.col("event_name"), F.lit(event_name)))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "discount_code",
            "orders",
            "revenue",
            "order_date",
            "client_name",
            "event_name",
            "load_id",
            "event_date",
            "created_at",
        )
    )


def _write_to_redshift(df: DataFrame, jdbc_url: str, user: str, password: str, table_name: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.amazon.redshift.jdbc.Driver")
        .mode("append")
        .save()
    )


def _execute_jdbc_sql(
    spark_context: SparkContext,
    jdbc_url: str,
    user: str,
    password: str,
    sql_statements: list[str],
) -> None:
    jvm = spark_context._gateway.jvm
    jvm.java.lang.Class.forName("com.amazon.redshift.jdbc.Driver")
    conn = None
    stmt = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        for sql in sql_statements:
            stmt.execute(sql)
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    event_input = json.loads(args["EVENT_INPUT"])
    load_id = _require_str(event_input, "load_id")
    event_name = str(event_input.get("event_name", "ObjectCreated")).strip() or "ObjectCreated"

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    manifest_uri = _manifest_uri(event_input)
    parquet_paths = _read_manifest_paths(glue_ctx, manifest_uri)
    staging_df = _prepare_staging_df(glue_ctx, parquet_paths, load_id, event_name)
    table_name = f"{args['REDSHIFT_SCHEMA']}.{args['REDSHIFT_STAGING_TABLE']}"
    load_id_sql = load_id.replace("'", "''")
    create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  discount_code VARCHAR(256),
  orders BIGINT,
  revenue DOUBLE PRECISION,
  order_date DATE,
  client_name VARCHAR(256) NOT NULL,
  event_name VARCHAR(128),
  load_id VARCHAR(128) NOT NULL,
  event_date DATE,
  created_at TIMESTAMP
);
"""
    delete_for_load_id_sql = f"DELETE FROM {table_name} WHERE load_id = '{load_id_sql}';"
    _execute_jdbc_sql(sc, args["REDSHIFT_JDBC_URL"], args["REDSHIFT_USER"], args["REDSHIFT_PASSWORD"], [create_table_sql, delete_for_load_id_sql])
    _write_to_redshift(staging_df, args["REDSHIFT_JDBC_URL"], args["REDSHIFT_USER"], args["REDSHIFT_PASSWORD"], table_name)

    print(json.dumps({"status": "SUCCESS", "stage": "staging_load", "load_id": load_id, "manifest_s3_uri": manifest_uri}))
    job.commit()


if __name__ == "__main__":
    main()
