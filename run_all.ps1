param(
  [string]$PythonExe = "C:\Python313\python.exe"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

$Jobs = @(
  @{ Name = "philip_run_live_football_betting";  Script = "UI_webscraping\Betting\philip_run_live_football_betting.py" }
  @{ Name = "football_scraper";                 Script = "Database\Get_data\football.py" }
  @{ Name = "ezekiel_run_live_football_betting"; Script = "UI_webscraping\Betting\ezekiel_run_live_football_betting.py" }
)

$Started = @()

Write-Host "[run_all] Root: $Root"
Write-Host "[run_all] Python: $PythonExe"

foreach ($j in $Jobs) {
  $scriptPath = Join-Path $Root $j.Script
  if (-not (Test-Path $scriptPath)) {
    throw "Script not found: $scriptPath"
  }

  Write-Host "[run_all] Starting $($j.Name): $($j.Script)"
  $p = Start-Process -FilePath $PythonExe -WorkingDirectory $Root -ArgumentList @($scriptPath) -PassThru
  $Started += [pscustomobject]@{
    name = $j.Name
    script = $j.Script
    pid = $p.Id
    started_at = (Get-Date).ToString("s")
  }
  Write-Host ("[run_all] Started {0} pid={1}" -f $j.Name, $p.Id)
}

$pidFile = Join-Path $Root "run_all.pids.json"
$Started | ConvertTo-Json -Depth 4 | Set-Content -Path $pidFile -Encoding UTF8
Write-Host "[run_all] Wrote PIDs to: $pidFile"

Write-Host ""
Write-Host "To stop one script (example):"
Write-Host "  Stop-Process -Id <pid>"
Write-Host ""
