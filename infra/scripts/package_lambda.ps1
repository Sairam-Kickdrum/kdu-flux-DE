param(
  [string]$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path
)

$srcDir = Join-Path $ProjectRoot "pipeline\lambda\src"
$configDir = Join-Path $ProjectRoot "pipeline\config"
$buildDir = Join-Path $ProjectRoot "pipeline\lambda\build"
$distDir = Join-Path $ProjectRoot "pipeline\lambda\dist"
$zipPath = Join-Path $distDir "client_upload_orchestrator.zip"

New-Item -ItemType Directory -Force $buildDir, $distDir | Out-Null
Remove-Item -Recurse -Force "$buildDir\*" -ErrorAction SilentlyContinue

Copy-Item -Recurse -Force "$srcDir\*" $buildDir
New-Item -ItemType Directory -Force (Join-Path $buildDir "config") | Out-Null
Copy-Item -Force (Join-Path $configDir "client_pipeline_config.json") (Join-Path $buildDir "config\client_pipeline_config.json")

if (Test-Path $zipPath) {
  Remove-Item -Force $zipPath
}

Compress-Archive -Path "$buildDir\*" -DestinationPath $zipPath -Force
Write-Output "Lambda package created: $zipPath"
