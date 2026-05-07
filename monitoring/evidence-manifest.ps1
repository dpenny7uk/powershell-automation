#requires -Version 5.1
<#
.SYNOPSIS
    Generate or verify a tamper-evident manifest of a folder's contents (SHA256).

.DESCRIPTION
    Two modes:

    GENERATE (default):
        Walks $Folder, hashes every file, writes manifest.json with file metadata
        and the running user/host/timestamp. Use this to seal a folder of evidence
        before sending it to anyone.

    VERIFY:
        Reads $Folder\manifest.json and re-hashes every file listed. Reports
        any missing, modified, or unexpected files.

.PARAMETER Folder
    Folder to seal (Generate mode) or verify (Verify mode).

.PARAMETER Verify
    Switch - flips the script into verify mode.

.EXAMPLE
    # Seal an evidence folder before sending
    .\evidence-manifest.ps1 -Folder C:\Dev\gv228-evidence\adf-evidence-...

    # Verify a returned folder against its manifest
    .\evidence-manifest.ps1 -Folder C:\Dev\hexaware-returns\SHIR01 -Verify
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$Folder,
    [switch]$Verify
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Folder)) { throw "Folder not found: $Folder" }
$manifestPath = Join-Path $Folder 'manifest.json'

if (-not $Verify) {
    # ---------- GENERATE ----------
    $files = Get-ChildItem -LiteralPath $Folder -File -Recurse | Where-Object { $_.Name -ne 'manifest.json' }
    $entries = foreach ($f in $files) {
        $rel = $f.FullName.Substring($Folder.Length).TrimStart('\','/')
        [PSCustomObject]@{
            Name         = $rel
            SizeBytes    = $f.Length
            Sha256       = (Get-FileHash -LiteralPath $f.FullName -Algorithm SHA256).Hash
            LastWriteUtc = $f.LastWriteTimeUtc.ToString('o')
        }
    }
    $manifest = [ordered]@{
        schema      = 'evidence-manifest/v1'
        runUser     = "$env:USERDOMAIN\$env:USERNAME"
        runHost     = $env:COMPUTERNAME
        sealedUtc   = (Get-Date).ToUniversalTime().ToString('o')
        psVersion   = "$($PSVersionTable.PSVersion)"
        files       = $entries
    }
    $manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $manifestPath -Encoding UTF8
    Write-Host "Sealed: $manifestPath" -ForegroundColor Green
    Write-Host "  Files hashed: $($entries.Count)"
    Write-Host "  By:           $env:USERDOMAIN\$env:USERNAME on $env:COMPUTERNAME"
    return
}

# ---------- VERIFY ----------
if (-not (Test-Path -LiteralPath $manifestPath)) { throw "manifest.json missing in $Folder" }
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json

Write-Host "Verifying $Folder against manifest..." -ForegroundColor Cyan
Write-Host "  Manifest schema:   $($manifest.schema)"
Write-Host "  Sealed by:         $($manifest.runUser) on $($manifest.runHost)"
Write-Host "  Sealed UTC:        $($manifest.sealedUtc)"
Write-Host ""

$results = New-Object System.Collections.Generic.List[object]
foreach ($f in $manifest.files) {
    $localPath = Join-Path $Folder $f.Name
    if (-not (Test-Path -LiteralPath $localPath)) {
        $results.Add([PSCustomObject]@{ File=$f.Name; Status='MISSING'; Expected=$f.Sha256; Actual=$null })
        continue
    }
    $localHash = (Get-FileHash -LiteralPath $localPath -Algorithm SHA256).Hash
    $status = if ($localHash -eq $f.Sha256) { 'OK' } else { 'MODIFIED' }
    $results.Add([PSCustomObject]@{ File=$f.Name; Status=$status; Expected=$f.Sha256; Actual=$localHash })
}

# Detect unexpected files (files in folder but not in manifest)
$expectedNames = $manifest.files | ForEach-Object { $_.Name }
$onDisk = Get-ChildItem -LiteralPath $Folder -File -Recurse |
    Where-Object { $_.Name -ne 'manifest.json' } |
    ForEach-Object { $_.FullName.Substring($Folder.Length).TrimStart('\','/') }
$unexpected = $onDisk | Where-Object { $_ -notin $expectedNames }
foreach ($u in $unexpected) { $results.Add([PSCustomObject]@{ File=$u; Status='UNEXPECTED'; Expected=$null; Actual=(Get-FileHash -LiteralPath (Join-Path $Folder $u) -Algorithm SHA256).Hash }) }

$ok       = @($results | Where-Object Status -eq 'OK').Count
$modified = @($results | Where-Object Status -eq 'MODIFIED').Count
$missing  = @($results | Where-Object Status -eq 'MISSING').Count
$extra    = @($results | Where-Object Status -eq 'UNEXPECTED').Count

Write-Host "Verification result:" -ForegroundColor Cyan
Write-Host ("  OK:         {0}" -f $ok) -ForegroundColor Green
if ($modified) { Write-Host ("  MODIFIED:   {0}" -f $modified) -ForegroundColor Red }
if ($missing)  { Write-Host ("  MISSING:    {0}" -f $missing) -ForegroundColor Red }
if ($extra)    { Write-Host ("  UNEXPECTED: {0}" -f $extra) -ForegroundColor Yellow }

if ($modified + $missing -gt 0) {
    Write-Host ""
    Write-Host "Failures:" -ForegroundColor Red
    $results | Where-Object Status -in 'MODIFIED','MISSING' | Format-Table -AutoSize
    exit 1
}
