# kdu-flux-DE

## Data Enginnering Project 

## bootstrap

### tf_lock_table_name = "kdu-flux-tf-state-locks-de"
### tf_state_bucket_name = "kdu-flux-tf-state-de"
### tf_state_bucket_region = "ap-southeast-1"

## adscribe-url-pull

Terraform stack for the `flux-de-adscribe-url-pull` Lambda in `ap-southeast-1`.

## Adscribe Redshift Load

Additive files for loading processed Adscribe output from `s3://kduflux-de-bucket/processed/adscribe/` into Redshift live under [adscribe-url-pull/sql/redshift_adscribe_tables.sql](/c:/Users/Admin/Desktop/data-engineering-project/kdu-flux-DE/adscribe-url-pull/sql/redshift_adscribe_tables.sql) and [adscribe-url-pull/scripts/load_adscribe_to_redshift.py](/c:/Users/Admin/Desktop/data-engineering-project/kdu-flux-DE/adscribe-url-pull/scripts/load_adscribe_to_redshift.py).

Runtime parameters:
- `--batch_id` required for batch tracking and optional DynamoDB metadata update.
- `--start_date` and `--end_date` are required and drive the idempotent affected window.
- `--processed_s3_prefix` defaults to `s3://kduflux-de-bucket/processed/adscribe/`. You can also pass a more specific partition root, but it must remain an `s3://.../` prefix.
- `--redshift_host` defaults to `main-workgroup.743298171118.ap-southeast-1.redshift-serverless.amazonaws.com`.
- `--redshift_port` defaults to `5439`.
- `--redshift_database` defaults to `main_db`.
- `--redshift_user` and `--redshift_password` are required runtime inputs and are not hardcoded.
- `--redshift_iam_role_arn` is required so Redshift `COPY` can read processed parquet from S3.

Load behavior:
- The script creates `stg_adscribe_performance` and `fact_adscribe_performance` if they do not exist.
- It truncates staging, copies processed parquet into staging, then runs `DELETE + INSERT` inside one transaction for `source_type = 'adscribe'` and the requested date range.
- This keeps the raw landing and existing processing flow intact while making the final table dashboard-ready and idempotent for overlapping pulls.
- On success, the script updates DynamoDB status to `LOADED_TO_REDSHIFT` with `redshift_table = fact_adscribe_performance` and `redshift_load_strategy = DELETE_INSERT`. Use `--skip_dynamodb_update` if you only want the Redshift load.
