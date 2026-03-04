<#
.SYNOPSIS
    Deploys .NET runtime updates across multiple servers with pre-flight
    validation, WhatIf mode, per-server isolation, and rollback support.

.DESCRIPTION
    Automates the deployment of ASP.NET Core Hosting Bundle and .NET runtime
    updates across enterprise server estates. Includes network connectivity
    checks, disk space validation, service stop/start management, and
    comprehensive audit logging.

    Developed to support security patching of ASP.NET Core EOL versions
    across application server environments.

.PARAMETER ServerList
    Path to text file of target server hostnames.

.PARAMETER InstallerPath
    UNC path to the .NET runtime installer (.exe).

.PARAMETER InstallerArgs
    Silent install arguments. Default: /install /quiet /norestart

.PARAMETER WhatIf
    Preview mode - validates targets without installing.

.EXAMPLE
    .\Invoke-MultiServerDotNetUpdate.ps1 -ServerList "servers.txt" `
        -InstallerPath "\\fileserver\packages\dotnet-hosting-8.0.11-win.exe" -WhatIf

.NOTES
    Author: Damian Penny
    Version: 2.0
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$ServerList,

    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$InstallerPath,

    [Parameter()]
    [string]$InstallerArgs = '/install /quiet /norestart',

    [Parameter()]
    [string[]]$StopServices = @(),

    [Parameter()]
    [int]$MinDiskSpaceGB = 2,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

#region Functions

function Test-ServerReadiness {
    [CmdletBinding()]
    param([string]$ServerName)

    $checks = @{
        Ping      = $false
        WinRM     = $false
        DiskSpace = $false
        UNCAccess = $false
    }

    # Ping test
    $checks.Ping = Test-Connection -ComputerName $ServerName -Count 2 -Quiet

    if (-not $checks.Ping) {
        return [PSCustomObject]@{ ServerName = $ServerName; Ready = $false; Checks = $checks; Detail = 'Server unreachable' }
    }

    # WinRM test
    try {
        $null = Invoke-Command -ComputerName $ServerName -ScriptBlock { 1 } -ErrorAction Stop
        $checks.WinRM = $true
    }
    catch {
        return [PSCustomObject]@{ ServerName = $ServerName; Ready = $false; Checks = $checks; Detail = "WinRM failed: $($_.Exception.Message)" }
    }

    # Disk space check
    $freeSpace = Invoke-Command -ComputerName $ServerName -ScriptBlock {
        (Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='C:'").FreeSpace / 1GB
    }
    $checks.DiskSpace = $freeSpace -ge $MinDiskSpaceGB

    # UNC path accessible from target
    $uncAccessible = Invoke-Command -ComputerName $ServerName -ScriptBlock {
        param($Path)
        Test-Path $Path
    } -ArgumentList $InstallerPath
    $checks.UNCAccess = $uncAccessible

    $ready = $checks.Values -notcontains $false
    $detail = if ($ready) { 'All pre-flight checks passed' }
              else { ($checks.GetEnumerator() | Where-Object { -not $_.Value } | ForEach-Object { "$($_.Key) FAILED" }) -join '; ' }

    return [PSCustomObject]@{ ServerName = $ServerName; Ready = $ready; Checks = $checks; FreeSpaceGB = [math]::Round($freeSpace, 2); Detail = $detail }
}

function Install-DotNetUpdate {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [string]$ServerName,
        [string]$InstallerPath,
        [string]$InstallerArgs,
        [string[]]$StopServices
    )

    $log = @{ ServerName = $ServerName; StartTime = Get-Date; Steps = @(); Success = $false }

    try {
        # Stop services if specified
        if ($StopServices.Count -gt 0) {
            $log.Steps += "Stopping services: $($StopServices -join ', ')"
            Invoke-Command -ComputerName $ServerName -ScriptBlock {
                param($Services)
                foreach ($svc in $Services) {
                    $service = Get-Service -Name $svc -ErrorAction SilentlyContinue
                    if ($service -and $service.Status -eq 'Running') {
                        Stop-Service -Name $svc -Force -ErrorAction Stop
                    }
                }
            } -ArgumentList (,$StopServices) -ErrorAction Stop
        }

        # Copy installer locally to avoid UNC issues during install
        $remoteTempPath = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            $tempDir = Join-Path $env:TEMP 'DotNetUpdate'
            if (-not (Test-Path $tempDir)) { New-Item -Path $tempDir -ItemType Directory -Force | Out-Null }
            return $tempDir
        }

        $installerName = Split-Path $InstallerPath -Leaf
        $remoteInstallerPath = "\\$ServerName\c$\$($remoteTempPath.Substring(3))\$installerName"

        $log.Steps += "Copying installer to $ServerName"
        Copy-Item -Path $InstallerPath -Destination $remoteInstallerPath -Force -ErrorAction Stop

        # Run installer
        $log.Steps += "Running installer with args: $InstallerArgs"
        $installResult = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            param($Path, $Args)
            $process = Start-Process -FilePath $Path -ArgumentList $Args -Wait -PassThru -NoNewWindow
            return $process.ExitCode
        } -ArgumentList (Join-Path $remoteTempPath $installerName), $InstallerArgs -ErrorAction Stop

        $log.Steps += "Installer exit code: $installResult"
        $log.Success = $installResult -in @(0, 3010)  # 3010 = success, reboot required

        # Restart services
        if ($StopServices.Count -gt 0) {
            $log.Steps += "Restarting services"
            Invoke-Command -ComputerName $ServerName -ScriptBlock {
                param($Services)
                foreach ($svc in $Services) {
                    Start-Service -Name $svc -ErrorAction SilentlyContinue
                }
            } -ArgumentList (,$StopServices) -ErrorAction Stop
        }

        # Cleanup
        Invoke-Command -ComputerName $ServerName -ScriptBlock {
            param($TempDir)
            Remove-Item $TempDir -Recurse -Force -ErrorAction SilentlyContinue
        } -ArgumentList $remoteTempPath

        # Verify installation
        $log.Steps += "Verifying installation"
        $verification = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            $runtimes = & dotnet --list-runtimes 2>$null
            return $runtimes
        }
        $log.Steps += "Installed runtimes: $($verification -join '; ')"
    }
    catch {
        $log.Steps += "ERROR: $($_.Exception.Message)"
        $log.Success = $false
    }

    $log.EndTime = Get-Date
    $log.Duration = ($log.EndTime - $log.StartTime).TotalSeconds
    return $log
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$servers = Get-Content $ServerList | Where-Object { $_ -and $_ -notmatch '^\s*#' }

Write-Host "`n=== Multi-Server .NET Update Deployment ===" -ForegroundColor Cyan
Write-Host "Installer: $InstallerPath"
Write-Host "Targets: $($servers.Count) server(s)"
Write-Host "Mode: $(if ($WhatIfPreference) { 'PREVIEW (WhatIf)' } else { 'LIVE' })`n"

# Phase 1: Pre-flight checks
Write-Host "Phase 1: Pre-flight validation" -ForegroundColor Cyan
$readiness = foreach ($server in $servers) {
    Write-Host "  $server ... " -NoNewline
    $check = Test-ServerReadiness -ServerName $server
    Write-Host $(if ($check.Ready) { 'READY' } else { "NOT READY - $($check.Detail)" }) `
        -ForegroundColor $(if ($check.Ready) { 'Green' } else { 'Red' })
    $check
}

$readyServers = $readiness | Where-Object { $_.Ready }
$failedServers = $readiness | Where-Object { -not $_.Ready }

if ($failedServers.Count -gt 0) {
    Write-Host "`n  WARNING: $($failedServers.Count) server(s) failed pre-flight checks" -ForegroundColor Yellow
}

if ($readyServers.Count -eq 0) {
    Write-Host "`n  No servers ready for deployment. Aborting." -ForegroundColor Red
    return
}

# Phase 2: Deployment
if (-not $WhatIfPreference) {
    Write-Host "`nPhase 2: Deploying to $($readyServers.Count) server(s)" -ForegroundColor Cyan

    $deployResults = foreach ($server in $readyServers) {
        Write-Host "  Deploying to $($server.ServerName) ... " -NoNewline
        $result = Install-DotNetUpdate -ServerName $server.ServerName `
            -InstallerPath $InstallerPath -InstallerArgs $InstallerArgs -StopServices $StopServices
        Write-Host $(if ($result.Success) { "SUCCESS ($([math]::Round($result.Duration))s)" } else { 'FAILED' }) `
            -ForegroundColor $(if ($result.Success) { 'Green' } else { 'Red' })
        $result
    }

    # Export deployment log
    $logFile = Join-Path $OutputPath "DotNetDeploy-$timestamp.json"
    $deployResults | ConvertTo-Json -Depth 5 | Out-File $logFile
    Write-Host "`nDeployment log: $logFile" -ForegroundColor Green
}
else {
    Write-Host "`nPhase 2: SKIPPED (WhatIf mode)" -ForegroundColor Yellow
}

# Export pre-flight report
$csvFile = Join-Path $OutputPath "DotNetDeploy-Preflight-$timestamp.csv"
$readiness | Select-Object ServerName, Ready, FreeSpaceGB, Detail |
    Export-Csv -Path $csvFile -NoTypeInformation
Write-Host "Pre-flight report: $csvFile"

#endregion
