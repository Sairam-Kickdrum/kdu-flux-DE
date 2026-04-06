# Client Upload Pipeline Assets

## Lambda package build
Run from repository root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\package_lambda.ps1
```

This creates:

`pipeline/lambda/dist/client_upload_orchestrator.zip`

Update `infra/terraform.tfvars` value of `lambda_package_file` to this zip path when deploying the real code package.
