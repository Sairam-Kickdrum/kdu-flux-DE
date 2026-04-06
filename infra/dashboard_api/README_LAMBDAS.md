# Analytics Lambda Backend (API Gateway already exists)

This package contains only Lambda code and shared Python utilities for the existing API Gateway methods.

## Domain routing choice

Use `domain` as a **query parameter** (`?domain=client_upload|ascribe`).

Why this is best here:
- Your API paths are already created as fixed routes (`/analytics/...`) and do not include `{domain}` path variables.
- Query param keeps the existing routes unchanged and still allows one API family to serve both analytics domains.

## Folder structure

```text
lambda_src/
  common/
    __init__.py
    db.py
    logging_utils.py
    responses.py
    sql_builders.py
    validation.py
  handlers/
    analytics_kpi.py
    analytics_revenue_daily.py
    analytics_revenue_monthly.py
    analytics_breakdown.py
    analytics_details.py
tests/
  events/
    kpi_client_upload.json
    kpi_ascribe.json
    details_ascribe.json
requirements.txt
```

## Environment variables

Set these on each Lambda:

- `AWS_REGION` (example: `ap-southeast-1`)
- `REDSHIFT_HOST`
- `REDSHIFT_PORT` (default `5439`)
- `REDSHIFT_DATABASE`
- `REDSHIFT_SECRET_ARN` (Secrets Manager JSON with `username`/`password`)
- `CLIENT_UPLOAD_TABLE` (default `fact_client_uploads`)
- `ASCRIBE_TABLE` (default `flux_ascribe_performance`)

Secret JSON example:

```json
{
  "username": "user_flux",
  "password": "<your-password>"
}
```

## Handler names (attach to existing API methods)

| API method | Lambda | Handler string |
|---|---|---|
| GET `/analytics/kpi` | analytics_kpi | `handler.lambda_handler` |
| GET `/analytics/revenue/daily` | analytics_revenue_daily | `handler.lambda_handler` |
| GET `/analytics/revenue/monthly` | analytics_revenue_monthly | `handler.lambda_handler` |
| GET `/analytics/breakdown` | analytics_breakdown | `handler.lambda_handler` |
| GET `/analytics/details` | analytics_details | `handler.lambda_handler` |

Deployment note:
- For each Lambda zip, copy one handler file as `handler.py` plus the whole `common/` folder.

## Packaging example (PowerShell)

From repo folder `infra/dashboard_api`:

```powershell
$dist = "lambda_dist_v2"
New-Item -ItemType Directory -Force $dist | Out-Null

$handlers = @(
  "analytics_kpi",
  "analytics_revenue_daily",
  "analytics_revenue_monthly",
  "analytics_breakdown",
  "analytics_details"
)

foreach ($h in $handlers) {
  $build = Join-Path $dist ("build_" + $h)
  $zip = Join-Path $dist ($h + ".zip")
  if (Test-Path $build) { Remove-Item -Recurse -Force $build }
  if (Test-Path $zip) { Remove-Item -Force $zip }

  New-Item -ItemType Directory -Force $build | Out-Null
  Copy-Item -Recurse -Force "lambda_src/common" (Join-Path $build "common")
  Copy-Item -Force ("lambda_src/handlers/" + $h + ".py") (Join-Path $build "handler.py")
  Compress-Archive -Path (Join-Path $build "*") -DestinationPath $zip -Force
}
```

Then upload each zip to its Lambda function.

## Query params by endpoint

Common:
- `domain=client_upload|ascribe`
- `start_date=YYYY-MM-DD`
- `end_date=YYYY-MM-DD`

Client upload specific:
- `client=all|alpha|beta|gamma`

Breakdown:
- `dimension`
- `top_n` (optional)

Details:
- `limit` (default 50, max 500)
- `offset` (default 0)
- `sort_by`
- `sort_order=asc|desc`

## Example curl

```bash
curl "https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/analytics/kpi?domain=client_upload&client=alpha"
curl "https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/analytics/kpi?domain=ascribe&start_date=2025-01-01&end_date=2025-01-31"
curl "https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/analytics/breakdown?domain=ascribe&start_date=2025-01-01&end_date=2025-01-31&dimension=show_name&top_n=5"
curl "https://<api-id>.execute-api.<region>.amazonaws.com/<stage>/analytics/details?domain=client_upload&client=all&limit=50&offset=0&sort_by=date&sort_order=desc"
```

## Local test events

Use files in `tests/events/`.

## Error behavior

- `400` for validation/filter errors
- `500` for unexpected/database errors

Error payload format:

```json
{
  "error": {
    "code": "BAD_REQUEST",
    "message": "dimension 'show' is not supported for domain 'client_upload'"
  }
}
```

## Assumptions

- API Gateway already routes each GET endpoint to its Lambda.
- OPTIONS is already configured in API Gateway.
- Redshift endpoint is reachable from Lambda runtime network.
- `fact_client_uploads` does not contain `show`/`product`; therefore those are rejected for `client_upload` breakdown.
