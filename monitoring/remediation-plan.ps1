#requires -Version 5.1
<#
.SYNOPSIS
    Produces a per-path remediation plan across the full Tenable libcurl scope.
    For each vulnerable libcurl.dll path, recommends the right action:
    upgrade, uninstall, vendor-update, investigate, leave alone, etc.

.DESCRIPTION
    The deletion-impact-analysis script answers "what breaks if Hexaware's script runs?"
    This script answers the broader question: "for each vulnerable libcurl path in the
    estate, what should we actually do about it?"

    Combines three streams of evidence:
      1. Tenable findings (the vulnerable paths)
      2. ADF linked services and recent pipeline runs (production dependency)
      3. Hexaware's proposed deletion list (so each row is tagged whether their
         script would have hit it)

    Output classifies every path into one of these recommended actions:

      UPGRADE_DRIVER_MSI    Vulnerable + production-critical SHIR driver in active use.
                            Proper MSI uninstall + install of patched version during
                            maintenance window. The right answer for active drivers.

      UNINSTALL_CLEAN_MSI   Vulnerable SHIR/Gateway driver with no observed LS dependency.
                            Driver appears unused; remove via Add/Remove Programs (or
                            msiexec /x). Cleaner than file deletion - leaves no registered
                            but broken driver behind.

      INVESTIGATE_KV_REFS   Vulnerable SHIR driver where ADF has OdbcLinkedServices using
                            KV-secured connection strings whose underlying driver isn't
                            visible. Could be referenced by these LSes - needs KV secret
                            lookup or ADF author check before any action.

      VENDOR_UPDATE_*       Vulnerable but in a separately-managed product (Tableau, IBM
                            Cognos, Google Chrome, etc.). Different team, different update
                            cadence. Route to product owner.

      MCAFEE_AGENT_UPDATE   McAfee Agent libcurl - endpoint security team owns this.

      IGNORE_INACTIVE       Backup folders, archives. Libcurl on disk but never loaded.
                            No remediation needed (could optionally archive/delete the
                            whole backup but that's a separate cleanup task).

      ALREADY_PATCHED       LibcurlFileVersion is >= 8.4.0. False positive in Tenable
                            (or Tenable hasn't rescanned since the patch landed).

      NEEDS_REVIEW          Could not classify automatically. Manual inspection.

.PARAMETER TenableFlatCsv
    The flat CSV produced from your Tenable Power Query pivot. Columns expected:
    Path, asset.host_name (or Host), and optionally Product / DriverFamily.
    If Product / DriverFamily aren't present they will be derived from Path.

.PARAMETER LinkedServiceCsv
    Output of adf-linked-service-discovery.ps1 / query-adf-via-azcli.ps1.
    Optional but strongly recommended - without it every SHIR row defaults to UNINSTALL_CLEAN_MSI.

.PARAMETER PipelineRunsCsv
    Pipeline-runs CSV from the ADF discovery. Optional.
    If present, distinguishes UPGRADE_DRIVER_MSI (recent runs) from UNINSTALL_CLEAN_MSI.

.PARAMETER HexawareScript
    Optional. If supplied, each output row is tagged InHexawareDeletionList = True/False
    so you can see which actions overlap with their proposed deletion paths.

.PARAMETER OutDir
    Defaults to E:\Libcurl_Remediation\Output\remediation-plan-<ts>.

.EXAMPLE
    .\remediation-plan.ps1 `
        -TenableFlatCsv    'E:\Libcurl_Remediation\Output\tenable-flat.csv' `
        -LinkedServiceCsv  'E:\Libcurl_Remediation\Output\adf-evidence-20260513_100616Z\linked-services.csv' `
        -PipelineRunsCsv   'E:\Libcurl_Remediation\Output\adf-evidence-20260513_100616Z\pipeline-runs-30d.csv' `
        -HexawareScript    'E:\Libcurl_Remediation\Scripts\hexaware-libcurl-cleanup.ps1'
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$TenableFlatCsv,
    [string]$LinkedServiceCsv,
    [string]$PipelineRunsCsv,
    [string]$HexawareScript,
    [string]$OutDir
)

$ErrorActionPreference = 'Stop'
$ScriptVersion = '1.0.1'
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
if (-not $OutDir) { $OutDir = "E:\Libcurl_Remediation\Output\remediation-plan-$ts" }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$prefix = Join-Path $OutDir "remediation-plan-$ts"

# ---------- Load inputs ----------
Write-Host "Loading Tenable flat CSV..." -ForegroundColor Cyan
$tenable = Import-Csv -LiteralPath $TenableFlatCsv
if (-not $tenable) { throw "Tenable CSV empty or unreadable: $TenableFlatCsv" }
$tenableCols = $tenable[0].PSObject.Properties.Name

# Resolve column names defensively.
# NOTE: Use Select-Object -First 1 rather than [0]. When Where-Object returns a single
# string, [0] indexes into the string and returns its first CHARACTER. Subtle PS gotcha.
$pathCol    = $tenableCols | Where-Object { $_ -in 'Path','LibcurlPath','output.1' }                          | Select-Object -First 1
$hostCol    = $tenableCols | Where-Object { $_ -in 'asset.host_name','Host','HostName','asset_host_name' }   | Select-Object -First 1
$famCol     = $tenableCols | Where-Object { $_ -in 'DriverFamily','Driver Family','driver_family','Driver' } | Select-Object -First 1
$productCol = $tenableCols | Where-Object { $_ -in 'Product','Sub Cat','SubCat' }                            | Select-Object -First 1
$versionCol = $tenableCols | Where-Object { $_ -in 'LibcurlFileVersion','InstalledVersion','Version' }       | Select-Object -First 1
if (-not $pathCol) { throw "Tenable CSV must have a Path column. Found: $($tenableCols -join ', ')" }
Write-Host "  Rows: $($tenable.Count) (path=$pathCol; host=$hostCol; family=$famCol; product=$productCol; version=$versionCol)" -ForegroundColor Green

# Linked services
$lsByDriver = @{}
$lsKvUnresolved = New-Object System.Collections.Generic.List[object]
if ($LinkedServiceCsv -and (Test-Path -LiteralPath $LinkedServiceCsv)) {
    Write-Host "Loading linked services..." -ForegroundColor Cyan
    $lses = Import-Csv -LiteralPath $LinkedServiceCsv
    $shirLses = $lses | Where-Object { $_.IntegrationRuntimeKind -eq 'SelfHosted' }
    foreach ($ls in $shirLses) {
        if ($ls.DriverInferred -and $ls.DriverInferred -notlike '<see*') {
            $key = $ls.DriverInferred
            if (-not $lsByDriver.ContainsKey($key)) { $lsByDriver[$key] = @() }
            $lsByDriver[$key] += $ls
        } elseif ($ls.LinkedServiceType -eq 'OdbcLinkedService' -or $ls.DriverInferred -like '<see*') {
            $lsKvUnresolved.Add($ls)
        }
    }
    Write-Host "  SHIR LSes resolved to driver: $(($lsByDriver.Values | ForEach-Object { $_.Count } | Measure-Object -Sum).Sum)" -ForegroundColor Green
    Write-Host "  SHIR LSes with unresolved drivers (KV refs etc.): $($lsKvUnresolved.Count)" -ForegroundColor Yellow
} else {
    Write-Host "(No LinkedServiceCsv provided - SHIR rows will default to UNINSTALL_CLEAN_MSI)" -ForegroundColor Yellow
}

# Pipeline runs - count successful runs per container, last available window
$runsByContainer = @{}
if ($PipelineRunsCsv -and (Test-Path -LiteralPath $PipelineRunsCsv)) {
    Write-Host "Loading pipeline runs..." -ForegroundColor Cyan
    $runs = Import-Csv -LiteralPath $PipelineRunsCsv
    $byContainer = $runs | Where-Object { $_.Status -eq 'Succeeded' } | Group-Object Container
    foreach ($g in $byContainer) { $runsByContainer[$g.Name] = $g.Count }
    Write-Host "  Successful runs grouped across $($runsByContainer.Count) container(s)" -ForegroundColor Green
}

# Hexaware deletion list
$hexawarePaths = @()
if ($HexawareScript -and (Test-Path -LiteralPath $HexawareScript)) {
    Write-Host "Parsing Hexaware deletion list..." -ForegroundColor Cyan
    $content = Get-Content -LiteralPath $HexawareScript -Raw
    if ($content -match '(?ms)\$files\s*=\s*@\(\s*(.*?)\s*\)') {
        $arrayBody = $matches[1]
        $hexawarePaths = [regex]::Matches($arrayBody, '"([^"]+)"') | ForEach-Object { $_.Groups[1].Value.ToLowerInvariant() }
        Write-Host "  Paths in Hexaware deletion list: $($hexawarePaths.Count)" -ForegroundColor Green
    }
}
$hexawareSet = @{}
foreach ($p in $hexawarePaths) { $hexawareSet[$p] = $true }

# ---------- Helpers ----------
function Get-DriverFolderFromPath { param([string]$p)
    if ($p -match '\\ODBC Drivers\\([^\\]+)\\') { return $matches[1] }
    return $null
}
function Get-DriverFamilyFromFolder { param([string]$folder)
    if (-not $folder) { return $null }
    if ($folder -match '^(.+?)_[\d.]+$') { return $matches[1] }
    return $folder
}
function Get-ProductFromPath { param([string]$p)
    if ($p -match 'Microsoft Integration Runtime')          { return 'SHIR' }
    if ($p -match 'On-premises data gateway')               { return 'Power BI Gateway' }
    if ($p -match 'Power BI Desktop')                       { return 'Power BI Desktop' }
    if ($p -match 'SQL Server Management Studio')           { return 'SSMS' }
    if ($p -match 'Visual Studio')                          { return 'SSDT' }
    if ($p -match 'McAfee')                                 { return 'McAfee Agent' }
    if ($p -match '\\Tableau\\')                            { return 'Tableau Server' }
    if ($p -match '(?i)\\cognos\\')                         { return 'IBM Cognos' }
    if ($p -match '(?i)Client Access')                      { return 'IBM Client Access' }
    if ($p -match '(?i)SoapUI')                             { return 'SmartBear SoapUI' }
    if ($p -match '\\Chrome\\')                             { return 'Google Chrome' }
    if ($p -match '(?i)\\Backup')                           { return 'Backup (inactive)' }
    return 'Other'
}
function Resolve-LsesForDriverFamily { param([string]$family)
    if (-not $family) { return @() }
    # Exact match
    if ($lsByDriver.ContainsKey($family)) { return $lsByDriver[$family] }
    # Fuzzy: strip "Microsoft " prefix and try
    $needle = $family -replace '^Microsoft\s+',''
    foreach ($k in $lsByDriver.Keys) {
        if (($k -replace '^Microsoft\s+','') -eq $needle) { return $lsByDriver[$k] }
    }
    return @()
}
function Test-VersionVulnerable { param([string]$v)
    if (-not $v) { return $null }
    # libcurl version compare: anything < 8.4.0 is vulnerable to CVE-2023-38545
    if ($v -match '^(\d+)\.(\d+)\.(\d+)') {
        $major=[int]$matches[1]; $minor=[int]$matches[2]
        if ($major -gt 8) { return $false }
        if ($major -eq 8 -and $minor -ge 4) { return $false }
        return $true
    }
    return $null
}

# ---------- Build the plan ----------
Write-Host "Building remediation plan..." -ForegroundColor Cyan
$plan = New-Object System.Collections.Generic.List[object]
foreach ($row in $tenable) {
    $path     = $row.$pathCol
    if (-not $path) { continue }
    $hostName = if ($hostCol)  { $row.$hostCol }  else { $null }
    $family   = if ($famCol)   { $row.$famCol }   else { Get-DriverFamilyFromFolder (Get-DriverFolderFromPath $path) }
    $product  = if ($productCol -and $row.$productCol) { $row.$productCol } else { Get-ProductFromPath $path }
    $version  = if ($versionCol) { $row.$versionCol } else { $null }
    $folder   = Get-DriverFolderFromPath $path

    $isVuln = Test-VersionVulnerable $version
    $inHex  = $hexawareSet.ContainsKey(($path.Trim().ToLowerInvariant()))

    $lses          = Resolve-LsesForDriverFamily $family
    $shirLsCount   = $lses.Count
    $runsForLs     = 0
    foreach ($ls in $lses) {
        if ($ls.Container -and $runsByContainer.ContainsKey($ls.Container)) {
            $runsForLs += [int]$runsByContainer[$ls.Container]
        }
    }
    $kvUnresolvedCount = $lsKvUnresolved.Count

    # --- Decide action ---
    $action = 'NEEDS_REVIEW'
    $rationale = ''

    if ($isVuln -eq $false) {
        $action = 'ALREADY_PATCHED'
        $rationale = "Libcurl version $version is >= 8.4.0; not vulnerable to CVE-2023-38545."
    }
    elseif ($product -eq 'Backup (inactive)') {
        $action = 'IGNORE_INACTIVE'
        $rationale = "Backup folder. Libcurl on disk in archived files only; never loaded into a process."
    }
    elseif ($product -eq 'Google Chrome') {
        $action = 'VENDOR_UPDATE_Chrome'
        $rationale = "Chrome bundles libcurl and updates itself via its own update service. No manual action."
    }
    elseif ($product -eq 'Tableau Server') {
        $action = 'VENDOR_UPDATE_Tableau'
        $rationale = "Tableau Server bundles libcurl. Patch via the next Tableau Server upgrade cycle."
    }
    elseif ($product -eq 'IBM Cognos') {
        $action = 'VENDOR_UPDATE_Cognos'
        $rationale = "IBM Cognos bundles libcurl. Patch requires an IBM-issued fix pack."
    }
    elseif ($product -eq 'IBM Client Access') {
        $action = 'VENDOR_UPDATE_IBM_ClientAccess'
        $rationale = "IBM iSeries Client Access bundles libcurl. Patch requires an IBM-issued update."
    }
    elseif ($product -eq 'SmartBear SoapUI') {
        $action = 'VENDOR_UPDATE_SoapUI'
        $rationale = "SoapUI bundles libcurl. Update via SmartBear's release cadence."
    }
    elseif ($product -eq 'McAfee Agent') {
        $action = 'MCAFEE_AGENT_UPDATE'
        $rationale = "McAfee Agent libcurl. Endpoint security team owns the McAfee Agent update cycle."
    }
    elseif ($product -eq 'Power BI Desktop') {
        $action = 'VENDOR_UPDATE_PowerBIDesktop'
        $rationale = "Power BI Desktop on user workstation. EUC team to push update via comms. Microsoft Store install auto-updates; direct-download installs need manual update."
    }
    elseif ($product -in @('SHIR','Power BI Gateway','SSMS','SSDT')) {
        # In-scope for GV-228
        if ($shirLsCount -gt 0 -and $runsForLs -gt 0) {
            $action = 'UPGRADE_DRIVER_MSI'
            $rationale = "Driver family '$family' is referenced by $shirLsCount SHIR linked service(s) with $runsForLs successful pipeline runs in the recent window. Upgrade via vendor-supplied MSI during maintenance window. NOT deletion."
        }
        elseif ($shirLsCount -gt 0) {
            $action = 'UPGRADE_DRIVER_MSI'
            $rationale = "Driver family '$family' is referenced by $shirLsCount SHIR linked service(s) but no recent successful runs observed. Still in-use per LS config; upgrade via MSI."
        }
        elseif ($kvUnresolvedCount -gt 0 -and $product -eq 'SHIR') {
            $action = 'INVESTIGATE_KV_REFS'
            $rationale = "No resolved LS depends on '$family', but $kvUnresolvedCount SHIR ODBC linked service(s) use Key-Vault-secured connection strings whose underlying driver is not visible from the API. Could reference this driver. Read the KV secrets or check with ADF authors before uninstalling."
        }
        else {
            $action = 'UNINSTALL_CLEAN_MSI'
            $rationale = "Vulnerable libcurl on disk but no observed LS dependency. Driver appears unused. Uninstall cleanly via Add/Remove Programs (or msiexec /x) - leaves no registered-but-broken driver behind. Better than file deletion."
        }
    }
    else {
        $action = 'NEEDS_REVIEW'
        $rationale = "Path classified as '$product' but no specific remediation rule. Manual review."
    }

    $plan.Add([PSCustomObject]@{
        Path                       = $path
        Host                       = $hostName
        Product                    = $product
        DriverFolder               = $folder
        DriverFamily               = $family
        LibcurlFileVersion         = $version
        IsVulnerable               = $isVuln
        InHexawareDeletionList     = $inHex
        SHIRLinkedServicesForFamily= $shirLsCount
        RecentRunsForThoseLSes     = $runsForLs
        KVUnresolvedLSesInEstate   = $kvUnresolvedCount
        RecommendedAction          = $action
        Rationale                  = $rationale
    })
}

# ---------- Outputs ----------
$plan | Export-Csv -NoTypeInformation -Path "$prefix-per-path.csv"

$actionSummary = $plan | Group-Object RecommendedAction | Sort-Object Count -Descending | ForEach-Object {
    [PSCustomObject]@{
        RecommendedAction = $_.Name
        PathCount         = $_.Count
        UniqueHosts       = ($_.Group | Select-Object -ExpandProperty Host -Unique).Count
        UniqueDriverFamilies = ($_.Group | Where-Object DriverFamily | Select-Object -ExpandProperty DriverFamily -Unique).Count
    }
}
$actionSummary | Export-Csv -NoTypeInformation -Path "$prefix-action-summary.csv"

$driverRollup = $plan | Where-Object DriverFamily | Group-Object DriverFamily | ForEach-Object {
    $worstAction = ($_.Group | ForEach-Object {
        switch ($_.RecommendedAction) {
            'UPGRADE_DRIVER_MSI'    { 5 }
            'INVESTIGATE_KV_REFS'   { 4 }
            'UNINSTALL_CLEAN_MSI'   { 3 }
            'NEEDS_REVIEW'          { 2 }
            default                 { 1 }
        }
    } | Measure-Object -Maximum).Maximum
    $worstName = switch ($worstAction) {
        5 {'UPGRADE_DRIVER_MSI'} 4 {'INVESTIGATE_KV_REFS'} 3 {'UNINSTALL_CLEAN_MSI'} 2 {'NEEDS_REVIEW'} default {'OTHER'}
    }
    [PSCustomObject]@{
        DriverFamily        = $_.Name
        PathCount           = $_.Count
        UniqueHosts         = ($_.Group | Select-Object -ExpandProperty Host -Unique).Count
        WorstRecommendation = $worstName
        InHexawareList      = (($_.Group | Where-Object InHexawareDeletionList).Count)
    }
} | Sort-Object PathCount -Descending
$driverRollup | Export-Csv -NoTypeInformation -Path "$prefix-driver-rollup.csv"

# Manifest
$manifestFiles = Get-ChildItem -LiteralPath $OutDir -File | ForEach-Object {
    [PSCustomObject]@{
        Name=$_.Name; SizeBytes=$_.Length
        Sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        LastWriteUtc=$_.LastWriteTimeUtc.ToString('o')
    }
}
$manifest = [ordered]@{
    schema='remediation-plan-manifest/v1'
    scriptName='remediation-plan.ps1'; scriptVersion=$ScriptVersion
    runUser="$env:USERDOMAIN\$env:USERNAME"; runHost=$env:COMPUTERNAME
    startedUtc=$ts; finishedUtc=(Get-Date).ToUniversalTime().ToString('o')
    inputs=@{
        TenableFlatCsv=$TenableFlatCsv
        LinkedServiceCsv=$LinkedServiceCsv
        PipelineRunsCsv=$PipelineRunsCsv
        HexawareScript=$HexawareScript
    }
    files=$manifestFiles
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutDir 'manifest.json') -Encoding UTF8

# Console summary
Write-Host ""
Write-Host "=== REMEDIATION PLAN SUMMARY ===" -ForegroundColor Green
$actionSummary | Format-Table -AutoSize
Write-Host "Output: $OutDir" -ForegroundColor Green
