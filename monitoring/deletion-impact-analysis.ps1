#requires -Version 5.1
<#
.SYNOPSIS
    Cross-references Hexaware's proposed deletion list against Tenable findings and ADF
    linked-service usage to produce a per-path impact assessment.

.DESCRIPTION
    THIS IS THE ARTIFACT TO PUT IN FRONT OF A SKEPTIC.

    For each path in Hexaware's hardcoded $files array, this script answers:
      1. Is this path actually a Tenable finding? (i.e. is it even vulnerable per the scan?)
      2. Which ODBC driver family does this path belong to?
      3. How many ADF/Synapse linked services depend on that driver?
      4. Of those linked services, how many run via a SHIR (the relevant ones)?
      5. Has any pipeline using this driver run successfully in the recent window?

    Each row gets a Verdict:
      DELETE_BLOCKED  — backed by SHIR linked services with recent successful runs.
                        Running Hexaware's script will cause production failure.
      RISKY_DELETE    — backed by linked services but no recent runs (low-traffic / dormant).
                        Could still break the next scheduled run.
      ORPHAN          — Tenable-flagged but no linked-service references found.
                        Safe to remove (or better: properly uninstall).
      NOT_VULNERABLE  — path is in Hexaware's list but NOT in Tenable findings.
                        Why is this being deleted at all?
      UNKNOWN         — couldn't classify (missing data).

.PARAMETER HexawareScript
    Path to the Hexaware PS1 to analyse. The script extracts the $files = @( "..." ) array
    by regex without executing the file (safe).

.PARAMETER TenablePivotCsv
    CSV with at minimum these columns: Path, DriverFamily, Sub Cat (or 'SubCat'), asset.host_name (or 'Host').
    Easiest source: Close & Load the Power Query output from your existing Tenable pivot to a CSV.

.PARAMETER LinkedServiceCsv
    Output from adf-linked-service-discovery.ps1 — usually:
    C:\Dev\gv228-evidence\adf-evidence-<ts>-linked-services.csv

.PARAMETER PipelineRunsCsv
    Output from adf-linked-service-discovery.ps1 — usually:
    C:\Dev\gv228-evidence\adf-evidence-<ts>-pipeline-runs-30d.csv
    Optional. If omitted, RecentRunCount column is blank.

.PARAMETER OutDir
    Where to drop the impact CSVs. Defaults to the same directory as the linked-service CSV.

.EXAMPLE
    .\deletion-impact-analysis.ps1 `
        -HexawareScript    'C:\Dev\hexaware-libcurl-cleanup.ps1' `
        -TenablePivotCsv   'C:\Dev\tenable-flat.csv' `
        -LinkedServiceCsv  'C:\Dev\gv228-evidence\adf-evidence-...-linked-services.csv' `
        -PipelineRunsCsv   'C:\Dev\gv228-evidence\adf-evidence-...-pipeline-runs-30d.csv'
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$HexawareScript,
    [Parameter(Mandatory)] [string]$TenablePivotCsv,
    [Parameter(Mandatory)] [string]$LinkedServiceCsv,
    [string]$PipelineRunsCsv,
    [string]$OutDir
)

$ErrorActionPreference = 'Stop'

if (-not $OutDir) { $OutDir = Split-Path -Parent $LinkedServiceCsv }
if (-not (Test-Path -LiteralPath $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
$prefix = Join-Path $OutDir "deletion-impact-$ts"

# ---------- 1. Extract Hexaware's deletion list (without executing the script) ----------
Write-Host "Parsing Hexaware deletion list from $HexawareScript..." -ForegroundColor Cyan
$content = Get-Content -LiteralPath $HexawareScript -Raw
if ($content -notmatch '(?ms)\$files\s*=\s*@\(\s*(.*?)\s*\)') {
    throw "Could not locate `$files = @( ... ) array in $HexawareScript"
}
$arrayBody = $matches[1]
$hexawarePaths = [regex]::Matches($arrayBody, '"([^"]+)"') |
    ForEach-Object { $_.Groups[1].Value }
Write-Host "  Found $($hexawarePaths.Count) paths in Hexaware's deletion list" -ForegroundColor Green

# ---------- 2. Load Tenable findings ----------
Write-Host "Loading Tenable findings from $TenablePivotCsv..." -ForegroundColor Cyan
$tenable = Import-Csv -LiteralPath $TenablePivotCsv
# Normalise column names — accept variants
$pathCol  = ($tenable[0].PSObject.Properties.Name | Where-Object { $_ -in 'Path','LibcurlPath','output.1' })[0]
$famCol   = ($tenable[0].PSObject.Properties.Name | Where-Object { $_ -in 'DriverFamily','Driver Family','driver_family','Driver' })[0]
$subCol   = ($tenable[0].PSObject.Properties.Name | Where-Object { $_ -in 'Sub Cat','SubCat','Subcat','Product' })[0]
$hostCol  = ($tenable[0].PSObject.Properties.Name | Where-Object { $_ -in 'asset.host_name','Host','HostName','asset_host_name' })[0]
if (-not $pathCol) { throw "Tenable CSV must have a path column (Path / LibcurlPath / output.1). Columns found: $($tenable[0].PSObject.Properties.Name -join ', ')" }
Write-Host "  Tenable: $($tenable.Count) findings (path=$pathCol, family=$famCol, subcat=$subCol, host=$hostCol)" -ForegroundColor Green

# Build lookups: path -> Tenable row, driver family -> Tenable rows
$tenableByPath = @{}
foreach ($t in $tenable) {
    $p = $t.$pathCol
    if ($p) { $tenableByPath[$p.Trim().ToLowerInvariant()] = $t }
}

# ---------- 3. Load linked services and (optional) pipeline runs ----------
Write-Host "Loading ADF linked services from $LinkedServiceCsv..." -ForegroundColor Cyan
$lses = Import-Csv -LiteralPath $LinkedServiceCsv
Write-Host "  Linked services: $($lses.Count) total" -ForegroundColor Green

# Group linked services by inferred driver name. Skip rows with no DriverInferred.
$lsByDriver = $lses | Where-Object { $_.DriverInferred -and $_.DriverInferred -notmatch '^<' } |
    Group-Object DriverInferred -AsHashTable -AsString
if (-not $lsByDriver) { $lsByDriver = @{} }

$pipelineRuns = @()
if ($PipelineRunsCsv -and (Test-Path -LiteralPath $PipelineRunsCsv)) {
    $pipelineRuns = Import-Csv -LiteralPath $PipelineRunsCsv
    Write-Host "  Pipeline runs (recent window): $($pipelineRuns.Count)" -ForegroundColor Green
} else {
    Write-Host "  Pipeline runs CSV not provided — RecentRunCount column will be blank" -ForegroundColor Yellow
}

# ---------- 4. Driver-family extraction helper ----------
function Get-DriverFamilyFromPath {
    param([string]$Path)
    if ($Path -match '\\ODBC Drivers\\([^\\]+?)(?:_[\d.]+)?\\') {
        return $matches[1]
    }
    return $null
}

function Get-DriverFolderFromPath {
    param([string]$Path)
    if ($Path -match '\\ODBC Drivers\\([^\\]+)\\') { return $matches[1] }
    return $null
}

# Map "Microsoft Hive ODBC Driver" (folder name) to the connector driver name in linked services.
# The ADF linked-service mapping uses "Microsoft Hive ODBC Driver" (matches the folder).
function Resolve-LinkedServicesForDriver {
    param([string]$DriverFamily)
    if (-not $DriverFamily) { return @() }
    # Try exact match first
    if ($lsByDriver.ContainsKey($DriverFamily)) { return $lsByDriver[$DriverFamily] }
    # Try fuzzy: ignore "Microsoft " prefix and trailing version stamps
    $needle = $DriverFamily -replace '^Microsoft\s+',''
    foreach ($k in $lsByDriver.Keys) {
        $hay = $k -replace '^Microsoft\s+',''
        if ($hay -eq $needle) { return $lsByDriver[$k] }
    }
    return @()
}

# ---------- 5. Build the impact rows ----------
Write-Host "Building per-path impact analysis..." -ForegroundColor Cyan
$impact = New-Object System.Collections.Generic.List[object]
foreach ($p in $hexawarePaths) {
    $lc = $p.Trim().ToLowerInvariant()
    $isInTenable = $tenableByPath.ContainsKey($lc)

    $folder = Get-DriverFolderFromPath -Path $p
    $family = Get-DriverFamilyFromPath -Path $p

    $matchingLses = Resolve-LinkedServicesForDriver -DriverFamily $family
    $shirLses     = @($matchingLses | Where-Object { $_.IntegrationRuntimeKind -eq 'SelfHosted' })
    $managedLses  = @($matchingLses | Where-Object { $_.IntegrationRuntimeKind -ne 'SelfHosted' })

    # Pipeline run count for the data factories that host these linked services
    $recentRunCount = $null
    if ($pipelineRuns -and $matchingLses) {
        $relevantContainers = $matchingLses | Select-Object -ExpandProperty Container -Unique
        $recentRunCount = @($pipelineRuns | Where-Object { $_.Container -in $relevantContainers -and $_.Status -eq 'Succeeded' }).Count
    }

    # Verdict
    $verdict = 'UNKNOWN'
    $rationale = ''
    if (-not $isInTenable) {
        $verdict = 'NOT_VULNERABLE'
        $rationale = "Path is in Hexaware's deletion list but does NOT appear in Tenable findings — why is this being deleted?"
    } elseif ($shirLses.Count -gt 0 -and $recentRunCount -gt 0) {
        $verdict = 'DELETE_BLOCKED'
        $rationale = "$($shirLses.Count) SHIR linked services depend on $family with $recentRunCount successful pipeline runs in the recent window. Deletion will break production."
    } elseif ($shirLses.Count -gt 0) {
        $verdict = 'RISKY_DELETE'
        $rationale = "$($shirLses.Count) SHIR linked services depend on $family but no recent successful runs detected. Still risky — next scheduled run will fail."
    } elseif ($matchingLses.Count -gt 0) {
        $verdict = 'RISKY_DELETE'
        $rationale = "$($matchingLses.Count) linked services reference $family but on non-SHIR runtimes. May or may not break — investigate."
    } else {
        $verdict = 'ORPHAN'
        $rationale = "Tenable-flagged but no linked-service references found. Driver appears unused — preferred remediation is uninstall, not file deletion."
    }

    $impact.Add([PSCustomObject]@{
        HexawarePath          = $p
        InTenableFindings     = $isInTenable
        DriverFolder          = $folder
        DriverFamily          = $family
        LinkedServicesTotal   = $matchingLses.Count
        SHIRLinkedServices    = $shirLses.Count
        NonSHIRLinkedServices = $managedLses.Count
        RecentSuccessfulRuns  = $recentRunCount
        Verdict               = $verdict
        Rationale             = $rationale
        ExampleLinkedServices = (($matchingLses | Select-Object -First 5 |
            ForEach-Object { "$($_.Container)/$($_.LinkedServiceName) [$($_.IntegrationRuntime)]" }) -join '; ')
    })
}

$impact | Export-Csv -NoTypeInformation -Path "$prefix-per-path.csv"

# Verdict summary
$verdictSummary = $impact | Group-Object Verdict | Sort-Object Count -Descending |
    ForEach-Object {
        [PSCustomObject]@{
            Verdict = $_.Name
            Count   = $_.Count
            Paths   = $_.Count
        }
    }
$verdictSummary | Export-Csv -NoTypeInformation -Path "$prefix-summary.csv"

# Driver-family summary (de-duplicated by family)
$familySummary = $impact | Where-Object DriverFamily |
    Group-Object DriverFamily | ForEach-Object {
        $worst = ($_.Group | ForEach-Object {
            switch ($_.Verdict) {
                'DELETE_BLOCKED' { 4 } 'RISKY_DELETE' { 3 } 'ORPHAN' { 2 } 'NOT_VULNERABLE' { 1 } default { 0 }
            }
        } | Measure-Object -Maximum).Maximum
        $worstName = switch ($worst) { 4{'DELETE_BLOCKED'} 3{'RISKY_DELETE'} 2{'ORPHAN'} 1{'NOT_VULNERABLE'} default{'UNKNOWN'} }
        [PSCustomObject]@{
            DriverFamily        = $_.Name
            HexawarePathsCount  = $_.Count
            SHIRLinkedServices  = ($_.Group | Select-Object -First 1).SHIRLinkedServices
            WorstVerdict        = $worstName
        }
    } | Sort-Object @{e='SHIRLinkedServices';desc=$true}, DriverFamily
$familySummary | Export-Csv -NoTypeInformation -Path "$prefix-family-summary.csv"

# Console summary
Write-Host ""
Write-Host "=== DELETION IMPACT SUMMARY ===" -ForegroundColor Green
$verdictSummary | Format-Table -AutoSize

$blocked = ($impact | Where-Object Verdict -eq 'DELETE_BLOCKED').Count
$risky   = ($impact | Where-Object Verdict -eq 'RISKY_DELETE').Count
$orphan  = ($impact | Where-Object Verdict -eq 'ORPHAN').Count
$nv      = ($impact | Where-Object Verdict -eq 'NOT_VULNERABLE').Count

Write-Host ""
if ($blocked -gt 0) {
    Write-Host "[STOP] $blocked path(s) backed by ACTIVE SHIR linked services with recent successful runs." -ForegroundColor Red
    Write-Host "       Running Hexaware's script will break production." -ForegroundColor Red
}
if ($risky -gt 0) {
    Write-Host "[WARN] $risky path(s) backed by linked services without recent runs — next scheduled run will fail." -ForegroundColor Yellow
}
if ($orphan -gt 0) {
    Write-Host "[INFO] $orphan path(s) Tenable-flagged but with no linked-service references — proper uninstall recommended over file deletion." -ForegroundColor Cyan
}
if ($nv -gt 0) {
    Write-Host "[QUESTION] $nv path(s) in Hexaware's list but NOT in Tenable findings — why are these being deleted?" -ForegroundColor Magenta
}
Write-Host ""
Write-Host "Outputs:" -ForegroundColor Green
Write-Host "  $prefix-per-path.csv         (one row per Hexaware path with verdict)"
Write-Host "  $prefix-summary.csv          (verdict counts)"
Write-Host "  $prefix-family-summary.csv   (rolled up by driver family)"
