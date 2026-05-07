#requires -Version 5.1
<#
.SYNOPSIS
    Consolidates per-host evidence ZIPs from odbc-discovery-v2 into estate-wide
    master CSVs, verifying tamper-evident manifests along the way.

.DESCRIPTION
    Workflow:
      1. Hexaware runs odbc-discovery-v2.ps1 on each of N servers and emails back
         a folder of <hostname>-<ts>-evidence.zip files.
      2. You run THIS script pointing -EvidenceFolder at that folder.
      3. The script:
           - Extracts each ZIP into a per-host subfolder.
           - Checks each CSV's SHA256 against the manifest (transit-corruption check).
           - Merges all per-host CSVs into estate-wide master CSVs in -OutDir.
           - Emits a per-host status report.

.PARAMETER EvidenceFolder
    Folder containing the per-host ZIPs Hexaware sent back.

.PARAMETER OutDir
    Where master CSVs and the status report go. Defaults to <EvidenceFolder>\consolidated.

.EXAMPLE
    .\consolidator.ps1 -EvidenceFolder 'C:\Dev\hexaware-returns\2026-05-08'
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$EvidenceFolder,
    [string]$OutDir
)

$ErrorActionPreference = 'Stop'

if (-not $OutDir) { $OutDir = Join-Path $EvidenceFolder 'consolidated' }
if (-not (Test-Path -LiteralPath $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"

# ---------- 1. Find every ZIP and extract ----------
$zips = Get-ChildItem -LiteralPath $EvidenceFolder -Filter '*-evidence.zip' -File
Write-Host "Found $($zips.Count) evidence ZIPs in $EvidenceFolder" -ForegroundColor Cyan

$verificationRows = New-Object System.Collections.Generic.List[object]
$extractRoot = Join-Path $OutDir "extracted-$ts"
New-Item -ItemType Directory -Path $extractRoot -Force | Out-Null

foreach ($zip in $zips) {
    $hostName = $zip.BaseName -replace '-\d{8}_\d{6}Z-evidence$',''
    $extractDir = Join-Path $extractRoot $hostName
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

    Write-Host "  Extracting $($zip.Name) -> $extractDir" -ForegroundColor DarkGray
    try {
        Expand-Archive -LiteralPath $zip.FullName -DestinationPath $extractDir -Force
    } catch {
        $verificationRows.Add([PSCustomObject]@{ Host=$hostName; Status='EXTRACT_FAILED'; Detail="$_" })
        continue
    }

    # ---------- 2. Manifest + transit-corruption check ----------
    $manifestPath = Join-Path $extractDir 'manifest.json'
    if (-not (Test-Path -LiteralPath $manifestPath)) {
        # No manifest — still merge the CSVs but flag it.
        $verificationRows.Add([PSCustomObject]@{ Host=$hostName; Status='NO_MANIFEST'; Detail='manifest.json missing — proceeding with CSV merge but no integrity check.' })
        continue
    }
    try {
        $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    } catch {
        $verificationRows.Add([PSCustomObject]@{ Host=$hostName; Status='MANIFEST_INVALID'; Detail="$_" })
        continue
    }

    $fileVerification = @()
    foreach ($f in $manifest.files) {
        $localPath = Join-Path $extractDir $f.Name
        if (-not (Test-Path -LiteralPath $localPath)) {
            $fileVerification += [PSCustomObject]@{ File=$f.Name; Status='MISSING' }; continue
        }
        $localHash = (Get-FileHash -LiteralPath $localPath -Algorithm SHA256).Hash
        if ($localHash -ne $f.Sha256) {
            $fileVerification += [PSCustomObject]@{ File=$f.Name; Status='HASH_MISMATCH' }
        } else {
            $fileVerification += [PSCustomObject]@{ File=$f.Name; Status='OK' }
        }
    }
    $bad = @($fileVerification | Where-Object Status -ne 'OK')
    if ($bad.Count -gt 0) {
        $verificationRows.Add([PSCustomObject]@{
            Host=$hostName; Status='TRANSIT_CORRUPTION'
            Detail="File hash check failed for: $((($bad | ForEach-Object { $_.File }) -join '; ')) — request a re-send."
        })
        continue
    }

    $verificationRows.Add([PSCustomObject]@{
        Host=$hostName; Status='OK'
        Detail="Files=$($manifest.files.Count); RanBy=$($manifest.user); Elevated=$($manifest.elevated)"
    })
}

$verificationRows | Export-Csv -NoTypeInformation -Path (Join-Path $OutDir "verification-report-$ts.csv")

# ---------- 3. Merge per-host CSVs into estate-wide master CSVs ----------
$verifiedHosts = $verificationRows | Where-Object Status -in 'OK','NO_MANIFEST' | Select-Object -ExpandProperty Host
Write-Host ""
Write-Host "Merging CSVs from $($verifiedHosts.Count) hosts..." -ForegroundColor Cyan

# CSV categories follow the suffix pattern from odbc-discovery-v2
$categories = @(
    'product-roots', 'libcurl-inventory', 'loaded-libcurl-MODULES',
    'registered-drivers', 'shir-events-30d', 'system-dsns-INFORMATIONAL',
    'run-metadata'
)
foreach ($cat in $categories) {
    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($h in $verifiedHosts) {
        $hostDir = Join-Path $extractRoot $h
        $candidate = Get-ChildItem -LiteralPath $hostDir -Filter "$h-$cat.csv" -File -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $candidate) {
            # Fall back to any -shir-events-*d.csv pattern
            $candidate = Get-ChildItem -LiteralPath $hostDir -Filter "$h-$cat*.csv" -File -ErrorAction SilentlyContinue | Select-Object -First 1
        }
        if ($candidate) {
            try { $rows.AddRange([object[]](Import-Csv -LiteralPath $candidate.FullName)) } catch {}
        }
    }
    if ($rows.Count -gt 0) {
        $masterPath = Join-Path $OutDir "master-$cat.csv"
        $rows | Export-Csv -NoTypeInformation -Path $masterPath
        Write-Host "  master-$cat.csv      $($rows.Count) rows from $($verifiedHosts.Count) hosts" -ForegroundColor Green
    }
}

# ---------- 4. Console summary ----------
Write-Host ""
Write-Host "=== CONSOLIDATION SUMMARY ===" -ForegroundColor Green
$verificationRows | Group-Object Status | ForEach-Object {
    $colour = if ($_.Name -eq 'OK') { 'Green' } elseif ($_.Name -eq 'NO_MANIFEST') { 'Yellow' } else { 'Red' }
    Write-Host ("{0,-25} {1}" -f $_.Name, $_.Count) -ForegroundColor $colour
}
Write-Host ""
Write-Host "Status report:        $OutDir\verification-report-$ts.csv"
Write-Host "Master CSVs:          $OutDir\master-*.csv"
Write-Host "Extracted evidence:   $extractRoot"
$bad = @($verificationRows | Where-Object Status -in 'TRANSIT_CORRUPTION','EXTRACT_FAILED','MANIFEST_INVALID')
if ($bad.Count -gt 0) {
    Write-Host ""
    Write-Host "[!] $($bad.Count) host(s) had transit/extraction issues — review the report and request a re-send." -ForegroundColor Red
}
