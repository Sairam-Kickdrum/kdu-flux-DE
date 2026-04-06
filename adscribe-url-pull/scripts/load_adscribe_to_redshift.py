from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import redshift_connector


LOGGER = logging.getLogger("load_adscribe_to_redshift")
DEFAULT_BUCKET = "kduflux-de-bucket"
DEFAULT_DYNAMODB_TABLE = "kdu-flux-dynamodb-table-de"
DEFAULT_PROCESSED_S3_PREFIX = f"s3://{DEFAULT_BUCKET}/processed/adscribe/"
DEFAULT_REDSHIFT_HOST = "main-workgroup.743298171118.ap-southeast-1.redshift-serverless.amazonaws.com"
DEFAULT_REDSHIFT_PORT = 5439
DEFAULT_REDSHIFT_DATABASE = "main_db"
FINAL_TABLE = "fact_adscribe_performance"
STAGING_TABLE = "stg_adscribe_performance"
DDL_FILE = Path(__file__).resolve().parents[1] / "sql" / "redshift_adscribe_tables.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load processed Adscribe parquet data from S3 into Redshift using staging + delete/insert."
    )
    parser.add_argument("--batch_id", required=True)
    parser.add_argument("--start_date", required=True)
    parser.add_argument("--end_date", required=True)
    parser.add_argument("--processed_s3_prefix", default=DEFAULT_PROCESSED_S3_PREFIX)
    parser.add_argument("--redshift_host", default=DEFAULT_REDSHIFT_HOST)
    parser.add_argument("--redshift_port", type=int, default=DEFAULT_REDSHIFT_PORT)
    parser.add_argument("--redshift_database", default=DEFAULT_REDSHIFT_DATABASE)
    parser.add_argument("--redshift_user", required=True)
    parser.add_argument("--redshift_password", required=True)
    parser.add_argument("--redshift_iam_role_arn", required=True)
    parser.add_argument("--dynamodb_table", default=DEFAULT_DYNAMODB_TABLE)
    parser.add_argument("--skip_dynamodb_update", action="store_true")
    return parser.parse_args()


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def normalize_s3_prefix(prefix: str) -> str:
    normalized = prefix.strip()
    if not normalized.startswith("s3://"):
        raise ValueError("processed_s3_prefix must start with s3://")
    if not normalized.endswith("/"):
        normalized = f"{normalized}/"
    return normalized


def load_ddl_sql() -> str:
    return DDL_FILE.read_text(encoding="utf-8")


def execute_statements(cursor, sql_text: str) -> None:
    for statement in sql_text.split(";"):
        trimmed = statement.strip()
        if trimmed:
            cursor.execute(trimmed)


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def copy_to_staging(cursor, processed_prefix: str, iam_role_arn: str) -> None:
    # Redshift COPY accepts the base processed prefix or a narrower partition prefix.
    for prefix in [processed_prefix]:
        LOGGER.info("Copying processed Adscribe parquet from %s", prefix)
        cursor.execute(
            f"""
            COPY {STAGING_TABLE}
            FROM {quote_literal(prefix)}
            IAM_ROLE {quote_literal(iam_role_arn)}
            FORMAT AS PARQUET;
            """
        )


def refresh_final_table(connection, start_date: str, end_date: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN;")
        cursor.execute(
            f"""
            DELETE FROM {FINAL_TABLE}
            WHERE source_type = 'adscribe'
              AND date BETWEEN %s AND %s;
            """,
            (start_date, end_date),
        )
        cursor.execute(
            f"""
            INSERT INTO {FINAL_TABLE} (
                date,
                source_type,
                client_name,
                show_name,
                discount_code,
                campaign_name,
                campaign_item_id,
                revenue,
                orders,
                impressions,
                revenue_per_order,
                revenue_per_impression,
                impressions_per_order,
                batch_id,
                run_id,
                source_key,
                processed_at
            )
            SELECT
                date,
                source_type,
                client_name,
                show_name,
                discount_code,
                campaign_name,
                campaign_item_id,
                revenue,
                orders,
                impressions,
                revenue_per_order,
                revenue_per_impression,
                impressions_per_order,
                batch_id,
                run_id,
                source_key,
                processed_at
            FROM {STAGING_TABLE}
            WHERE source_type = 'adscribe'
              AND date BETWEEN %s AND %s;
            """,
            (start_date, end_date),
        )
        cursor.execute("COMMIT;")
    except Exception:
        cursor.execute("ROLLBACK;")
        raise
    finally:
        cursor.close()


def update_dynamodb_status(table_name: str, batch_id: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    dynamodb_client = boto3.client("dynamodb")
    dynamodb_client.update_item(
        TableName=table_name,
        Key={"key": {"S": batch_id}},
        UpdateExpression=(
            "SET #status = :status, "
            "#redshift_table = :redshift_table, "
            "#redshift_load_strategy = :redshift_load_strategy, "
            "#updated_at = :updated_at"
        ),
        ExpressionAttributeNames={
            "#status": "status",
            "#redshift_table": "redshift_table",
            "#redshift_load_strategy": "redshift_load_strategy",
            "#updated_at": "updated_at",
        },
        ExpressionAttributeValues={
            ":status": {"S": "LOADED_TO_REDSHIFT"},
            ":redshift_table": {"S": FINAL_TABLE},
            ":redshift_load_strategy": {"S": "DELETE_INSERT"},
            ":updated_at": {"S": timestamp},
        },
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    processed_s3_prefix = normalize_s3_prefix(args.processed_s3_prefix)

    LOGGER.info(
        "Loading Adscribe batch_id=%s for date window %s to %s",
        args.batch_id,
        args.start_date,
        args.end_date,
    )
    LOGGER.info("Using Redshift endpoint %s:%s/%s", args.redshift_host, args.redshift_port, args.redshift_database)

    connection = redshift_connector.connect(
        host=args.redshift_host,
        port=args.redshift_port,
        database=args.redshift_database,
        user=args.redshift_user,
        password=args.redshift_password,
    )

    try:
        cursor = connection.cursor()
        try:
            execute_statements(cursor, load_ddl_sql())
            cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE};")
            copy_to_staging(cursor, processed_s3_prefix, args.redshift_iam_role_arn)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

        refresh_final_table(connection, args.start_date, args.end_date)

        if not args.skip_dynamodb_update:
            update_dynamodb_status(args.dynamodb_table, args.batch_id)

        LOGGER.info("Adscribe Redshift load completed successfully")
    except Exception:
        LOGGER.exception("Adscribe Redshift load failed")
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
