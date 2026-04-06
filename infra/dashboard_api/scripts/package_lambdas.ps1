$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$srcRoot = Join-Path $root "lambda_src"
$distRoot = Join-Path $root "lambda_dist"
$requirementsFile = Join-Path $root "requirements.txt"
$vendorDir = Join-Path $distRoot "_vendor"

$handlers = @(
  @{ Name = "kpi"; File = "analytics_kpi.py" },
  @{ Name = "revenue_daily"; File = "analytics_revenue_daily.py" },
  @{ Name = "revenue_monthly"; File = "analytics_revenue_monthly.py" },
  @{ Name = "breakdown"; File = "analytics_breakdown.py" },
  @{ Name = "details"; File = "analytics_details.py" }
)

New-Item -ItemType Directory -Force $distRoot | Out-Null
if (Test-Path $vendorDir) { Remove-Item -Recurse -Force $vendorDir }
New-Item -ItemType Directory -Force $vendorDir | Out-Null

# Install third-party dependencies only if requirements contain real packages.
$requirementsContent = Get-Content $requirementsFile | Where-Object {
  $line = $_.Trim()
  $line -ne "" -and -not $line.StartsWith("#")
}
if ($requirementsContent.Count -gt 0) {
  python -m pip install -r $requirementsFile -t $vendorDir --upgrade
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to install Python dependencies for lambda packaging."
  }
}

foreach ($handler in $handlers) {
  $buildDir = Join-Path $distRoot ("build_" + $handler.Name)
  $zipPath = Join-Path $distRoot ($handler.Name + ".zip")
  if (Test-Path $buildDir) { Remove-Item -Recurse -Force $buildDir }
  if (Test-Path $zipPath) { Remove-Item -Force $zipPath }

  New-Item -ItemType Directory -Force $buildDir | Out-Null
  Copy-Item -Recurse -Force (Join-Path $vendorDir "*") $buildDir
  Copy-Item -Recurse -Force (Join-Path $srcRoot "common") (Join-Path $buildDir "common")
  Copy-Item -Force (Join-Path $srcRoot ("handlers\" + $handler.File)) (Join-Path $buildDir "handler.py")

  Get-ChildItem -Path $buildDir -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
  Get-ChildItem -Path $buildDir -Recurse -File -Filter "*.pyc" | Remove-Item -Force

  Compress-Archive -Path (Join-Path $buildDir "*") -DestinationPath $zipPath -Force
  Write-Output "Created: $zipPath"
}
