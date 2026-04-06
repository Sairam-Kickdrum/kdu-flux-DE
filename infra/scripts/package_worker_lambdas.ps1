$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

$stagingSrcDir = Join-Path $root "lambda\staging_loader"
$stagingSrc = Join-Path $stagingSrcDir "handler.py"
$stagingDistDir = Join-Path $root "lambda\staging_loader\dist"
$stagingZip = Join-Path $stagingDistDir "staging_loader.zip"

$finalSrcDir = Join-Path $root "lambda\final_promote"
$finalSrc = Join-Path $finalSrcDir "handler.py"
$finalDistDir = Join-Path $root "lambda\final_promote\dist"
$finalZip = Join-Path $finalDistDir "final_promote.zip"

New-Item -ItemType Directory -Force $stagingDistDir, $finalDistDir | Out-Null
if (Test-Path $stagingZip) { Remove-Item -Force $stagingZip }
if (Test-Path $finalZip) { Remove-Item -Force $finalZip }

Compress-Archive -Path $stagingSrc -DestinationPath $stagingZip -Force
Compress-Archive -Path $finalSrc -DestinationPath $finalZip -Force

Write-Output "Created: $stagingZip"
Write-Output "Created: $finalZip"
