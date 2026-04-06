# Dashboard Analytics API (API Gateway + Lambda + Redshift)

Production-oriented Terraform + Python implementation for dashboard analytics tiles:
- KPI: total revenue / total orders
- Revenue over time (daily / monthly)
- Breakdown by `discount_code|show|product`
- Details table with pagination

## Folder Structure
```text
infra/dashboard_api/
  main.tf
  provider.tf
  versions.tf
  variables.tf
  outputs.tf
  terraform.tfvars.example
  modules/
    lambda_endpoint/
  lambda_src/
    common/
    handlers/
  scripts/
    package_lambdas.ps1
```

## API Contract
All endpoints are `GET`, support CORS, and return JSON.

Endpoints:
- `/analytics/kpi`
- `/analytics/revenue/daily`
- `/analytics/revenue/monthly`
- `/analytics/breakdown`
- `/analytics/details`

Query params:
- `client`: `All|Alpha|Beta|Gamma` (case-insensitive, default `All`)
- `start_date`: `YYYY-MM-DD` (optional)
- `end_date`: `YYYY-MM-DD` (optional)
- `group_by`: `discount_code|show|product` (breakdown only)
- `limit`: `1..500` (details only, default `100`)
- `offset`: `>=0` (details only, default `0`)

## Response Schemas
Success:
```json
{
  "success": true,
  "data": {}
}
```

Bad request:
```json
{
  "success": false,
  "error": {
    "code": "BAD_REQUEST",
    "message": "Validation message"
  }
}
```

Internal error:
```json
{
  "success": false,
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "Internal server error"
  }
}
```

## SQL Patterns Used
- KPI:
  - `SUM(revenue)`, `SUM(orders)`
- Daily:
  - `GROUP BY order_date`
- Monthly:
  - `GROUP BY DATE_TRUNC('month', order_date)`
- Breakdown:
  - Group by one of `discount_code`, `show`, `product`
- Details:
  - projected columns + `LIMIT/OFFSET`

All data filters (`client/start_date/end_date`) use Redshift Data API parameters.

## Deployment
1. Copy variables:
```powershell
cd infra/dashboard_api
Copy-Item terraform.tfvars.example terraform.tfvars
```
2. Update `terraform.tfvars` with your values (especially `redshift_secret_arn` and `redshift_fact_table`).
3. Package Lambdas:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\package_lambdas.ps1
```
4. Deploy:
```powershell
terraform init
terraform plan
terraform apply
```

## Sample Requests
```http
GET /analytics/kpi?client=All&start_date=2024-01-01&end_date=2024-12-31
GET /analytics/revenue/daily?client=alpha
GET /analytics/revenue/monthly?client=beta
GET /analytics/breakdown?client=gamma&group_by=product
GET /analytics/details?client=all&start_date=2024-01-01&end_date=2024-12-31&limit=50&offset=0
```

## Sample Responses
KPI:
```json
{
  "success": true,
  "data": {
    "total_revenue": 124578.44,
    "total_orders": 1821
  }
}
```

Breakdown:
```json
{
  "success": true,
  "data": {
    "group_by": "discount_code",
    "rows": [
      {"group_key": "SHOW10", "revenue": 10000.12, "orders": 120}
    ]
  }
}
```

## Error Handling
- Input validation errors => HTTP `400`
- Runtime/query failures => HTTP `500`
- Structured JSON logs are emitted to CloudWatch.

## Testing Strategy
- Unit-test validation helpers and SQL builder logic.
- Unit-test each handler with mocked Redshift Data API responses.
- Integration-test each endpoint against a dev Redshift dataset.
- Smoke-test with API Gateway invoke URL after deploy.

## One Lambda Per Endpoint vs Router Lambda
- Current implementation uses **one Lambda per endpoint**:
  - Better isolation, easier ownership and rollout.
  - Smaller handlers and clearer metrics.
- Router Lambda is better when:
  - You need fewer deployed artifacts.
  - You share complex auth/middleware in one place.

## Assumptions
- Redshift table has columns: `order_date, client_name, discount_code, show, product, revenue, orders`.
- Secret in Secrets Manager contains Redshift credentials usable by Redshift Data API.
- Redshift workgroup and database already exist.
