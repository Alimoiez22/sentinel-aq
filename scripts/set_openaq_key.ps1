# Store the OpenAQ API key in .env without showing it on screen or in shell history.
# Usage (from the repo root, PowerShell 7):  & .\scripts\set_openaq_key.ps1
# The key never leaves this machine: .env is ignored by git (see .gitignore).
$ErrorActionPreference = 'Stop'
$envFile = Join-Path (Split-Path -Parent $PSScriptRoot) '.env'
if (-not (Test-Path $envFile)) { throw ".env not found at $envFile; run 'make setup' first" }

$key = Read-Host 'OpenAQ API key (input hidden)' -MaskInput
if ([string]::IsNullOrWhiteSpace($key)) { throw 'No key entered; .env unchanged.' }
$key = $key.Trim()

$lines = Get-Content -LiteralPath $envFile
if (-not ($lines -match '^OPENAQ_API_KEY=')) { $lines += 'OPENAQ_API_KEY=' }
# Escape '$' so the key is inserted literally by -replace.
$lines = $lines -replace '^OPENAQ_API_KEY=.*', ('OPENAQ_API_KEY=' + $key.Replace('$', '$$'))
# LF line endings: Docker Compose reads this file inside Linux tooling.
[IO.File]::WriteAllText($envFile, ($lines -join "`n") + "`n")
Remove-Variable key, lines

$ok = (Select-String -LiteralPath $envFile -Pattern '^OPENAQ_API_KEY=.+').Count -eq 1
if ($ok) { Write-Host 'Saved: OPENAQ_API_KEY is set in .env (value not shown).' }
else { Write-Host 'Not saved: OPENAQ_API_KEY line is still empty.' }
