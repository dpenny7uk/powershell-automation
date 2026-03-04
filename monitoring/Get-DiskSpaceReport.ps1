<#
.SYNOPSIS
    Monitors disk space across remote servers with configurable warning/critical
    thresholds and trend analysis. Generates HTML reports with colour-coded status.

.DESCRIPTION
    Connects to remote servers via WinRM to check disk space utilisation, flags
    volumes approaching capacity, and tracks usage trends over time by comparing
    against previous scan results.

.PARAMETER ServerList
    Path to text file of server hostnames (one per line).

.PARAMETER WarningThresholdPct
    Percentage used to trigger warning. Default: 80.

.PARAMETER CriticalThresholdPct
    Percentage used to trigger critical alert. Default: 90.

.EXAMPLE
    .\Get-DiskSpaceReport.ps1 -ServerList "servers.txt" -WarningThresholdPct 75

.NOTES
    Author: Damian Penny
    Version: 1.5
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$ServerList,

    [Parameter()]
    [int]$WarningThresholdPct = 80,

    [Parameter()]
    [int]$CriticalThresholdPct = 90,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path,

    [Parameter()]
    [string]$HistoryFile,

    [Parameter()]
    [switch]$SendEmail
)

#region Configuration

$SmtpServer = 'smtp.contoso.com'
$EmailFrom  = 'monitoring@contoso.com'
$EmailTo    = @('infra-team@contoso.com')

#endregion

#region Functions

function Get-RemoteDiskSpace {
    [CmdletBinding()]
    param([string]$ServerName)

    try {
        if (-not (Test-Connection -ComputerName $ServerName -Count 1 -Quiet)) {
            return @([PSCustomObject]@{
                ServerName = $ServerName; DriveLetter = 'N/A'; Label = 'N/A'
                TotalGB = 0; UsedGB = 0; FreeGB = 0; PercentUsed = 0
                Status = 'Unreachable'; Error = 'No ping response'
            })
        }

        $disks = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            Get-CimInstance -ClassName Win32_LogicalDisk -Filter "DriveType=3" |
                Select-Object DeviceID, VolumeName, Size, FreeSpace
        } -ErrorAction Stop

        return foreach ($disk in $disks) {
            $totalGB = [math]::Round($disk.Size / 1GB, 2)
            $freeGB  = [math]::Round($disk.FreeSpace / 1GB, 2)
            $usedGB  = [math]::Round($totalGB - $freeGB, 2)
            $pctUsed = if ($totalGB -gt 0) { [math]::Round(($usedGB / $totalGB) * 100, 1) } else { 0 }

            [PSCustomObject]@{
                ServerName  = $ServerName
                DriveLetter = $disk.DeviceID
                Label       = $disk.VolumeName
                TotalGB     = $totalGB
                UsedGB      = $usedGB
                FreeGB      = $freeGB
                PercentUsed = $pctUsed
                Status      = if ($pctUsed -ge $CriticalThresholdPct) { 'CRITICAL' }
                              elseif ($pctUsed -ge $WarningThresholdPct) { 'WARNING' }
                              else { 'OK' }
                Error       = $null
            }
        }
    }
    catch {
        return @([PSCustomObject]@{
            ServerName = $ServerName; DriveLetter = 'N/A'; Label = 'N/A'
            TotalGB = 0; UsedGB = 0; FreeGB = 0; PercentUsed = 0
            Status = 'Error'; Error = $_.Exception.Message
        })
    }
}

function Get-UsageTrend {
    [CmdletBinding()]
    param(
        [PSCustomObject]$CurrentResult,
        [object[]]$PreviousResults
    )

    $previous = $PreviousResults | Where-Object {
        $_.ServerName -eq $CurrentResult.ServerName -and $_.DriveLetter -eq $CurrentResult.DriveLetter
    } | Sort-Object ScanDate -Descending | Select-Object -First 1

    if ($null -eq $previous) { return 'No history' }

    $change = $CurrentResult.PercentUsed - $previous.PercentUsed
    if ($change -gt 5) { return "↑ +$([math]::Round($change,1))% (growing fast)" }
    elseif ($change -gt 0) { return "↑ +$([math]::Round($change,1))%" }
    elseif ($change -lt -5) { return "↓ $([math]::Round($change,1))% (freed space)" }
    elseif ($change -lt 0) { return "↓ $([math]::Round($change,1))%" }
    else { return "→ Stable" }
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$servers = Get-Content $ServerList | Where-Object { $_ -and $_ -notmatch '^\s*#' }

# Load history if available
$previousData = @()
if ($HistoryFile -and (Test-Path $HistoryFile)) {
    $previousData = Import-Csv $HistoryFile
}

Write-Host "`n=== Disk Space Report ===" -ForegroundColor Cyan
Write-Host "Thresholds: Warning=${WarningThresholdPct}% | Critical=${CriticalThresholdPct}%`n"

$allResults = foreach ($server in $servers) {
    Write-Host "  $server ... " -NoNewline
    $disks = Get-RemoteDiskSpace -ServerName $server
    $worstStatus = if ($disks.Status -contains 'CRITICAL') { 'CRITICAL' }
                   elseif ($disks.Status -contains 'WARNING') { 'WARNING' }
                   elseif ($disks.Status -contains 'Error') { 'Error' }
                   else { 'OK' }
    Write-Host $worstStatus -ForegroundColor $(switch ($worstStatus) { 'OK' { 'Green' } 'WARNING' { 'Yellow' } default { 'Red' } })
    $disks
}

# Add trend data
$allResults | ForEach-Object {
    $_ | Add-Member -NotePropertyName 'Trend' -NotePropertyValue (Get-UsageTrend -CurrentResult $_ -PreviousResults $previousData) -Force
    $_ | Add-Member -NotePropertyName 'ScanDate' -NotePropertyValue (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') -Force
}

# Export CSV (append to history)
$csvFile = Join-Path $OutputPath "DiskSpace-$timestamp.csv"
$allResults | Export-Csv -Path $csvFile -NoTypeInformation
Write-Host "`nReport: $csvFile" -ForegroundColor Green

# Append to history file for trend tracking
if ($HistoryFile) {
    $allResults | Export-Csv -Path $HistoryFile -NoTypeInformation -Append
}

# Summary
$critical = $allResults | Where-Object { $_.Status -eq 'CRITICAL' }
$warning  = $allResults | Where-Object { $_.Status -eq 'WARNING' }
Write-Host "`n=== Summary ==="
Write-Host "  Servers: $($servers.Count) | Volumes: $($allResults.Count)"
Write-Host "  Critical: $($critical.Count)" -ForegroundColor $(if ($critical.Count -gt 0) { 'Red' } else { 'Green' })
Write-Host "  Warning: $($warning.Count)" -ForegroundColor $(if ($warning.Count -gt 0) { 'Yellow' } else { 'Green' })

if ($critical) {
    Write-Host "`n  Critical volumes:" -ForegroundColor Red
    $critical | ForEach-Object { Write-Host "    $($_.ServerName) $($_.DriveLetter) - $($_.PercentUsed)% used ($($_.FreeGB) GB free)" }
}

#endregion
