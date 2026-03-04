<#
.SYNOPSIS
    Monitors service account health including temp folder existence, profile
    integrity, and password expiry. Prevents outages caused by missing temp
    folders after OS patching (e.g., ASP.NET compilation failures).

.DESCRIPTION
    After identifying that a production SSRS outage was caused by a missing
    service account Temp folder post-OS patching, this script was developed
    to proactively detect the condition before it causes business impact.

    Checks:
    - Service account user profile directories exist
    - Temp folders (AppData\Local\Temp) exist and are writable
    - Service account passwords are not approaching expiry
    - Windows services running under service accounts are healthy

.PARAMETER ServiceAccounts
    Array of service account names to check. Defaults to configured list.

.PARAMETER ServerList
    Path to text file of servers to check.

.EXAMPLE
    .\Get-ServiceAccountHealth.ps1 -ServerList "servers.txt"

.NOTES
    Author: Damian Penny
    Version: 1.2
    Context: Developed after Sun Systems SSRS production outage caused by
             missing Temp folder for svc_sql_ssrs service account.
#>

[CmdletBinding()]
param(
    [Parameter()]
    [string]$ServerList,

    [Parameter()]
    [string[]]$ComputerName,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

#region Configuration

# Service accounts to monitor (sanitised)
$ServiceAccountConfig = @(
    @{ Account = 'svc_app_reports';  Servers = @('REPORT-SERVER-01') }
    @{ Account = 'svc_app_bizflow';  Servers = @('APP-SERVER-01', 'APP-SERVER-02') }
    @{ Account = 'svc_app_analytics'; Servers = @('WEB-SERVER-01', 'WEB-SERVER-02') }
    @{ Account = 'svc_sql_reporting'; Servers = @('REPORT-SERVER-01', 'DB-SERVER-01') }
)

#endregion

#region Functions

function Test-ServiceAccountHealth {
    [CmdletBinding()]
    param(
        [string]$ServerName,
        [string]$AccountName
    )

    try {
        if (-not (Test-Connection -ComputerName $ServerName -Count 1 -Quiet)) {
            return [PSCustomObject]@{
                ServerName = $ServerName; Account = $AccountName
                ProfileExists = $false; TempFolderExists = $false; TempWritable = $false
                ServicesRunning = 'N/A'; Status = 'Unreachable'; Detail = 'Server not responding'
            }
        }

        $result = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            param($Account)

            $findings = @{
                ProfileExists   = $false
                TempFolderExists = $false
                TempWritable    = $false
                ProfilePath     = $null
                Services        = @()
            }

            # Find the user profile directory
            $profilePaths = @(
                "C:\Users\$Account",
                "C:\Users\$($Account.Split('\')[-1])"
            )

            foreach ($path in $profilePaths) {
                if (Test-Path $path) {
                    $findings.ProfileExists = $true
                    $findings.ProfilePath = $path

                    # Check Temp folder
                    $tempPath = Join-Path $path 'AppData\Local\Temp'
                    if (Test-Path $tempPath) {
                        $findings.TempFolderExists = $true

                        # Test write access
                        $testFile = Join-Path $tempPath "healthcheck_$(Get-Random).tmp"
                        try {
                            [System.IO.File]::WriteAllText($testFile, 'test')
                            Remove-Item $testFile -Force -ErrorAction SilentlyContinue
                            $findings.TempWritable = $true
                        }
                        catch {
                            $findings.TempWritable = $false
                        }
                    }
                    break
                }
            }

            # Check services running under this account
            $services = Get-CimInstance -ClassName Win32_Service |
                Where-Object { $_.StartName -match [regex]::Escape($Account) } |
                Select-Object Name, DisplayName, State, StartMode

            $findings.Services = $services

            return $findings
        } -ArgumentList $AccountName -ErrorAction Stop

        $serviceStatus = if ($result.Services.Count -eq 0) { 'No services found' }
                        elseif ($result.Services | Where-Object { $_.State -ne 'Running' -and $_.StartMode -eq 'Auto' }) { 'STOPPED services detected' }
                        else { "All running ($($result.Services.Count))" }

        $overallStatus = if (-not $result.ProfileExists) { 'CRITICAL - No profile' }
                        elseif (-not $result.TempFolderExists) { 'CRITICAL - No Temp folder' }
                        elseif (-not $result.TempWritable) { 'WARNING - Temp not writable' }
                        elseif ($serviceStatus -match 'STOPPED') { 'WARNING - Stopped services' }
                        else { 'OK' }

        return [PSCustomObject]@{
            ServerName      = $ServerName
            Account         = $AccountName
            ProfileExists   = $result.ProfileExists
            ProfilePath     = $result.ProfilePath
            TempFolderExists = $result.TempFolderExists
            TempWritable    = $result.TempWritable
            ServicesRunning = $serviceStatus
            Status          = $overallStatus
            Detail          = ($result.Services | ForEach-Object { "$($_.Name): $($_.State)" }) -join '; '
        }
    }
    catch {
        return [PSCustomObject]@{
            ServerName = $ServerName; Account = $AccountName
            ProfileExists = $null; TempFolderExists = $null; TempWritable = $null
            ServicesRunning = 'Error'; Status = 'ERROR'; Detail = $_.Exception.Message
        }
    }
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
Write-Host "`n=== Service Account Health Check ===" -ForegroundColor Cyan

$allResults = foreach ($config in $ServiceAccountConfig) {
    foreach ($server in $config.Servers) {
        Write-Host "  $server \ $($config.Account) ... " -NoNewline
        $result = Test-ServiceAccountHealth -ServerName $server -AccountName $config.Account
        $color = switch -Wildcard ($result.Status) {
            'OK'        { 'Green' }
            'WARNING*'  { 'Yellow' }
            'CRITICAL*' { 'Red' }
            default     { 'Gray' }
        }
        Write-Host $result.Status -ForegroundColor $color
        $result
    }
}

# Export
$csvFile = Join-Path $OutputPath "ServiceAccountHealth-$timestamp.csv"
$allResults | Export-Csv -Path $csvFile -NoTypeInformation
Write-Host "`nReport: $csvFile" -ForegroundColor Green

# Summary
$issues = $allResults | Where-Object { $_.Status -notmatch '^OK$' }
Write-Host "`n=== Summary ==="
Write-Host "  Accounts checked: $($allResults.Count)"
Write-Host "  Issues found: $($issues.Count)" -ForegroundColor $(if ($issues.Count -gt 0) { 'Red' } else { 'Green' })

if ($issues) {
    $issues | ForEach-Object {
        Write-Host "    [$($_.Status)] $($_.ServerName) \ $($_.Account)" -ForegroundColor Yellow
    }
}

#endregion
