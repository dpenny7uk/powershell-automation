#requires -Version 5.1
<#
.SYNOPSIS
    Runs odbc-discovery-v2.ps1 across a list of remote servers via PowerShell
    remoting, retrieves the per-host evidence ZIP from each, and assembles them
    in a single local folder ready for consolidator.ps1.

.DESCRIPTION
    Single-command fan-out for Hexaware:
      - Accepts a list of servers (file, array, or CSV column).
      - Opens a PSSession to each in parallel (throttled).
      - Sends odbc-discovery-v2.ps1 content over the wire (-FilePath) so no
        script file persists on the remote disk.
      - Discovery runs on each remote in C:\Windows\Temp.
      - Once complete, the wrapper Copy-Item -FromSession's the resulting ZIP
        back to the local return folder.
      - Tears down sessions and writes a per-server status report.

    Requirements (Hexaware side):
      - PowerShell remoting / WinRM enabled on every target server (5985 HTTP
        or 5986 HTTPS) and reachable from the runner box.
      - The running account has Local Administrators on every target server
        (loaded-modules check needs admin to read other-user process memory).
      - odbc-discovery-v2.ps1 in the same folder as this wrapper, OR pass
        -ScriptPath explicitly.

    Requirements (Hiscox side, before sending):
      - Decide on the server list. Send Hexaware a CSV named e.g. shir-servers.csv
        with one column "Server" and one host per row.

.PARAMETER Servers
    Inline list of server names. e.g. -Servers 'SHIR01','SHIR02'

.PARAMETER ServerListPath
    Path to a file with one server per line, or a CSV with a 'Server' column.

.PARAMETER ScriptPath
    Path to the canonical odbc-discovery-v2.ps1. Defaults to a file of that
    name in the same folder as this wrapper.

.PARAMETER ReturnDir
    Local folder to drop the per-host evidence ZIPs and status report.
    Defaults to E:\Libcurl_Remediation\Output\returns-<UTC timestamp>.

.PARAMETER Credential
    Optional. If omitted, uses the current user (Kerberos). Pass an explicit
    credential with -Credential (Get-Credential) for delegated access.

.PARAMETER ThrottleLimit
    Max parallel remote sessions. Default 8.

.PARAMETER UseSSL
    Use WinRM-over-HTTPS (port 5986). Requires the runner trusts the remote
    cert. Default off (HTTP / 5985).

.PARAMETER DryRun
    Connectivity test only. Opens sessions, reports success/failure, closes.
    Does not run the discovery script.

.EXAMPLE
    # Inline servers, current user, default return dir
    .\run-discovery-remote.ps1 -Servers 'SHIR01','SHIR02','SHIR03'

.EXAMPLE
    # Server list from CSV, explicit credential, parallel up to 12
    $cred = Get-Credential
    .\run-discovery-remote.ps1 `
        -ServerListPath 'C:\Dev\shir-servers.csv' `
        -Credential $cred `
        -ThrottleLimit 12

.EXAMPLE
    # Just check who's reachable
    .\run-discovery-remote.ps1 -ServerListPath '.\servers.txt' -DryRun
#>

[CmdletBinding()]
param(
    [string[]]$Servers,
    [string]$ServerListPath,
    [string]$ScriptPath,
    [string]$ReturnDir,
    [System.Management.Automation.PSCredential]$Credential,
    [int]$ThrottleLimit = 8,
    [switch]$UseSSL,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"

# ---------- Resolve inputs ----------
if (-not $Servers -and -not $ServerListPath) {
    throw "Provide either -Servers or -ServerListPath."
}
if ($ServerListPath) {
    if (-not (Test-Path -LiteralPath $ServerListPath)) { throw "Server list not found: $ServerListPath" }
    if ($ServerListPath -match '\.csv$') {
        $Servers = (Import-Csv -LiteralPath $ServerListPath).Server | Where-Object { $_ }
    } else {
        $Servers = Get-Content -LiteralPath $ServerListPath | Where-Object { $_.Trim() -and -not $_.StartsWith('#') } | ForEach-Object { $_.Trim() }
    }
}
$Servers = $Servers | Where-Object { $_ } | Select-Object -Unique
if (-not $Servers) { throw "No servers resolved from inputs." }

if (-not $ScriptPath) {
    $ScriptPath = Join-Path (Split-Path -Parent $PSCommandPath) 'odbc-discovery-v2.ps1'
}
if (-not (Test-Path -LiteralPath $ScriptPath)) { throw "Discovery script not found: $ScriptPath" }

if (-not $ReturnDir) { $ReturnDir = "E:\Libcurl_Remediation\Output\returns-$ts" }
New-Item -ItemType Directory -Path $ReturnDir -Force | Out-Null

$logPath = Join-Path $ReturnDir "remote-execution.log"
function Log { param([string]$Message,[string]$Level='INFO')
    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format 's'), $Level, $Message
    Add-Content -LiteralPath $logPath -Value $line
    Write-Host $line
}

Log "================================================================="
Log "remote-discovery-runner starting"
Log "Servers:        $($Servers.Count) ($($Servers -join ', '))"
Log "ScriptPath:     $ScriptPath"
Log "ReturnDir:      $ReturnDir"
Log "ThrottleLimit:  $ThrottleLimit"
Log "UseSSL:         $UseSSL"
Log "DryRun:         $DryRun"
Log "Credential:     $(if ($Credential) { $Credential.UserName } else { '<current user / Kerberos>' })"
Log "================================================================="

# ---------- Open sessions in parallel ----------
$sessionParams = @{
    ComputerName  = $Servers
    ThrottleLimit = $ThrottleLimit
    ErrorAction   = 'SilentlyContinue'
    ErrorVariable = 'sessionErrors'
}
if ($Credential) { $sessionParams.Credential = $Credential }
if ($UseSSL)     { $sessionParams.UseSSL = $true }

Write-Host ""
Write-Host "Opening sessions to $($Servers.Count) servers (throttle=$ThrottleLimit)..." -ForegroundColor Cyan
$sessions = New-PSSession @sessionParams

$reachable   = @($sessions | ForEach-Object { $_.ComputerName })
$unreachable = $Servers | Where-Object { $_ -notin $reachable }

Log "Sessions opened:  $($sessions.Count) / $($Servers.Count)"
foreach ($u in $unreachable) {
    $errMsg = ($sessionErrors | Where-Object { $_.TargetObject -eq $u } | Select-Object -First 1).Exception.Message
    Log "  UNREACHABLE: $u ($errMsg)" 'WARN'
}

$status = New-Object System.Collections.Generic.List[object]
foreach ($u in $unreachable) {
    $status.Add([PSCustomObject]@{ Server=$u; SessionOpened=$false; DiscoveryRan=$false; ZipReturned=$false; Detail='Could not open PSSession' })
}

if (-not $sessions) {
    Log "No reachable servers. Aborting." 'ERROR'
    $status | Export-Csv -NoTypeInformation -Path (Join-Path $ReturnDir "remote-execution-status-$ts.csv")
    return
}

if ($DryRun) {
    foreach ($s in $sessions) {
        $status.Add([PSCustomObject]@{ Server=$s.ComputerName; SessionOpened=$true; DiscoveryRan=$false; ZipReturned=$false; Detail='DryRun — connectivity OK' })
    }
    $sessions | Remove-PSSession
    $status | Export-Csv -NoTypeInformation -Path (Join-Path $ReturnDir "remote-execution-status-$ts.csv")
    Write-Host ""
    Write-Host "=== DRY RUN COMPLETE ===" -ForegroundColor Green
    $status | Format-Table -AutoSize
    return
}

# ---------- Run discovery in parallel ----------
Write-Host ""
Write-Host "Running discovery on $($sessions.Count) servers (this typically takes 2-5 min/server, parallel)..." -ForegroundColor Cyan
$discoveryErrors = $null
$null = Invoke-Command -Session $sessions -FilePath $ScriptPath -ErrorAction SilentlyContinue -ErrorVariable discoveryErrors

# ---------- Retrieve ZIPs ----------
Write-Host ""
Write-Host "Retrieving evidence ZIPs..." -ForegroundColor Cyan
foreach ($s in $sessions) {
    $row = [PSCustomObject]@{
        Server        = $s.ComputerName
        SessionOpened = $true
        DiscoveryRan  = $true
        ZipReturned   = $false
        Detail        = ''
    }

    $errOnHost = $discoveryErrors | Where-Object { $_.OriginInfo.PSComputerName -eq $s.ComputerName -or $_.TargetObject.PSComputerName -eq $s.ComputerName }
    if ($errOnHost) {
        $row.DiscoveryRan = $false
        $row.Detail = "Discovery error: $($errOnHost[0].Exception.Message)"
        $status.Add($row); continue
    }

    try {
        $remoteZip = Invoke-Command -Session $s -ScriptBlock {
            Get-ChildItem -Path C:\Windows\Temp -Filter '*-evidence.zip' -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending |
                Select-Object -First 1 -ExpandProperty FullName
        } -ErrorAction Stop
    } catch {
        $row.Detail = "Could not list remote ZIPs: $_"
        $status.Add($row); continue
    }

    if (-not $remoteZip) {
        $row.Detail = 'Discovery completed but no evidence ZIP found in C:\Windows\Temp'
        $status.Add($row); continue
    }

    try {
        Copy-Item -FromSession $s -Path $remoteZip -Destination $ReturnDir -Force -ErrorAction Stop
        $localZipName = Split-Path -Leaf $remoteZip
        if (Test-Path -LiteralPath (Join-Path $ReturnDir $localZipName)) {
            $row.ZipReturned = $true
            $row.Detail = $localZipName
        } else {
            $row.Detail = "Copy reported success but file not present: $localZipName"
        }
    } catch {
        $row.Detail = "Copy-Item -FromSession failed: $_"
    }
    $status.Add($row)
    Log ("{0,-25} {1}" -f $s.ComputerName, $(if ($row.ZipReturned) { 'OK    ' + $row.Detail } else { 'FAILED ' + $row.Detail }))
}

# ---------- Cleanup ----------
$sessions | Remove-PSSession

$statusPath = Join-Path $ReturnDir "remote-execution-status-$ts.csv"
$status | Export-Csv -NoTypeInformation -Path $statusPath

# ---------- Summary ----------
Write-Host ""
Write-Host "=== REMOTE DISCOVERY COMPLETE ===" -ForegroundColor Green
$ok        = @($status | Where-Object ZipReturned -eq $true).Count
$noZip     = @($status | Where-Object { $_.SessionOpened -and $_.DiscoveryRan -and -not $_.ZipReturned }).Count
$discFail  = @($status | Where-Object { $_.SessionOpened -and -not $_.DiscoveryRan }).Count
$noSession = @($status | Where-Object { -not $_.SessionOpened }).Count
Write-Host ("ZIP retrieved:        {0}" -f $ok)        -ForegroundColor Green
if ($noZip)     { Write-Host ("Discovery ran, no ZIP: {0}" -f $noZip)     -ForegroundColor Yellow }
if ($discFail)  { Write-Host ("Discovery error:      {0}" -f $discFail)  -ForegroundColor Red }
if ($noSession) { Write-Host ("Could not connect:    {0}" -f $noSession) -ForegroundColor Red }
Write-Host ""
Write-Host "Returns folder:       $ReturnDir"
Write-Host "Status report:        $statusPath"
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "  .\consolidator.ps1 -EvidenceFolder '$ReturnDir'"
