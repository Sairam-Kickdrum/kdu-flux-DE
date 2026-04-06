import json
import sys
from typing import Any, Dict, List

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


REQUIRED_ARGS = [
    "JOB_NAME",
    "EVENT_INPUT",
    "REDSHIFT_JDBC_URL",
    "REDSHIFT_USER",
    "REDSHIFT_PASSWORD",
    "REDSHIFT_SCHEMA",
    "REDSHIFT_STAGING_TABLE",
    "REDSHIFT_FINAL_TABLE",
    "CLEANUP_STAGING",
]


def _require_str(payload: Dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"Missing required field in EVENT_INPUT: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Missing required field in EVENT_INPUT: {key}")
    return text


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _execute_transaction_sql(
    spark_context: SparkContext,
    jdbc_url: str,
    user: str,
    password: str,
    sql_statements: List[str],
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
    load_id = _sql_escape(_require_str(event_input, "load_id"))
    cleanup_staging = args["CLEANUP_STAGING"].lower() == "true"

    schema = args["REDSHIFT_SCHEMA"]
    staging_table = f"{schema}.{args['REDSHIFT_STAGING_TABLE']}"
    final_table = f"{schema}.{args['REDSHIFT_FINAL_TABLE']}"

    delete_sql = f"""
DELETE FROM {final_table}
USING (
  SELECT client_name, MIN(order_date) AS min_order_date, MAX(order_date) AS max_order_date
  FROM {staging_table}
  WHERE load_id = '{load_id}'
  GROUP BY client_name
) s
WHERE {final_table}.client_name = s.client_name
  AND {final_table}.order_date BETWEEN s.min_order_date AND s.max_order_date;
"""
    insert_sql = f"""
INSERT INTO {final_table} (discount_code, orders, revenue, order_date, client_name)
SELECT discount_code, orders, revenue, order_date, client_name
FROM {staging_table}
WHERE load_id = '{load_id}';
"""
    create_final_table_sql = f"""
CREATE TABLE IF NOT EXISTS {final_table} (
  discount_code VARCHAR(256),
  orders BIGINT NOT NULL,
  revenue DOUBLE PRECISION,
  order_date DATE,
  client_name VARCHAR(256) NOT NULL
);
"""
    sql_statements = [create_final_table_sql, delete_sql, insert_sql]
    if cleanup_staging:
        sql_statements.append(f"DELETE FROM {staging_table} WHERE load_id = '{load_id}';")

    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    _execute_transaction_sql(sc, args["REDSHIFT_JDBC_URL"], args["REDSHIFT_USER"], args["REDSHIFT_PASSWORD"], sql_statements)
    print(json.dumps({"status": "SUCCESS", "stage": "final_promote", "load_id": event_input["load_id"]}))
    job.commit()


if __name__ == "__main__":
    main()
