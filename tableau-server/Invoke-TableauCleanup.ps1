<#
======================================================================================
Script Name: TableauCleanup.ps1
Description: Tableau Server log cleanup and maintenance with Azure DevOps support
Author: Your Name
Environment: Azure DevOps Pipeline / Windows Server
Version: 3.1.0
Created Date: 2025-12-08
Last Modified: 2026-02-11
Execution: Azure DevOps Pipeline or manual execution
Dependencies: PowerShell 5+, TSM CLI, SMTP Relay
Log Location: Configured via config file or environment variables
Notes: Supports both config file and Azure DevOps variable groups
       Runs TSM cleanup and ziplogs commands
       Cleans up old log files and archives

Change Log:
  v3.1.0 (2026-02-11) - Bug fix: PowerShell 5.1 compatibility (removed ?? operators)
                       - Bug fix: Config file merge now uses correct JSON property names
                       - Bug fix: Remove-OldFiles no longer inflates counts in WhatIf mode
                       - Bug fix: TSM exit code now validated inside Start-Job
                       - Added secondary server cleanup via PowerShell remoting
                       - Improved disk space reporting for negative improvements
  v3.0.0 (2025-12-08) - Azure DevOps pipeline integration
                      - Support for environment variables from variable groups
                      - Multi-server support (production cluster)
                      - Enhanced disk space monitoring
  v2.0.3 (2025-11-06) - Original Task Scheduler version
======================================================================================
#>

#Requires -Version 5.0

[CmdletBinding()]
param(
    [string]$ConfigFile,
    [switch]$SkipEmailNotification,
    [switch]$WhatIf,
    
    # Azure DevOps pipeline parameters
    [string]$ServerName,
    [string]$SecondaryServerName,
    [string]$LogPath,
    [string]$ZipLogPath,
    [string]$TableauZipLogPath,
    [int]$LogRetentionDays,
    [int]$ZipLogRetentionDays,
    [string]$EmailTo,
    [string]$EmailFrom,
    [string]$SmtpServer
)

$ErrorActionPreference = 'Stop'
$InformationPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'

#region Configuration
function Get-CleanupConfiguration {
    param([string]$ConfigPath)
    
    # Check for Azure DevOps environment variables
    $useEnvironmentVars = $false
    if ($env:TableauServerName -or $env:TableauPrimaryServer) {
        $useEnvironmentVars = $true
        Write-Information "Using configuration from Azure DevOps environment variables"
    }
    
    # Default configuration
    $defaultConfig = @{
        PrimaryServer = if ($useEnvironmentVars) {
            if ($env:TableauPrimaryServer) { $env:TableauPrimaryServer }
            elseif ($env:TableauServerName) { $env:TableauServerName }
            else { $env:COMPUTERNAME }
        } else {
            $env:COMPUTERNAME
        }
        
        SecondaryServer = $env:TableauSecondaryServer
        
        Paths = @{
            LogDirectory = if ($env:TableauLogPathPrimary) {
                $env:TableauLogPathPrimary
            } elseif ($LogPath) {
                $LogPath
            } else {
                "D:\Scripts\Logs"
            }
            
            LogFilePrefix = "TableauCleanup"
            
            ZipLogDirectory = if ($ZipLogPath) {
                $ZipLogPath
            } else {
                "D:\Scripts\Logs\ZipLogs"
            }
            
            TableauZipLogDirectory = if ($TableauZipLogPath) {
                $TableauZipLogPath
            } else {
                "D:\Tableau\data\tabsvc\files\log-archives"
            }
            
            MonitorDrive = "D"
        }
        
        Retention = @{
            LogFilesRetentionDays = if ($env:RetentionDays) {
                [int]$env:RetentionDays
            } elseif ($LogRetentionDays) {
                $LogRetentionDays
            } else {
                7
            }
            
            ZipLogsRetentionDays = if ($ZipLogRetentionDays) {
                $ZipLogRetentionDays
            } else {
                30
            }
        }
        
        Email = @{
            SendNotifications = $true
            SmtpServer = if ($env:EmailServer) {
                $env:EmailServer
            } elseif ($SmtpServer) {
                $SmtpServer
            } else {
                "smtp-relay.contoso.com"
            }
            
            From = if ($env:EmailFrom) {
                $env:EmailFrom
            } elseif ($EmailFrom) {
                $EmailFrom
            } else {
                "App_Support_Team@contoso.com"
            }
            
            To = if ($env:EmailTo) {
                @($env:EmailTo -split ',')
            } elseif ($EmailTo) {
                @($EmailTo -split ',')
            } else {
                @("App_Support_Team@contoso.com")
            }
            
            CC = @()
            Port = $null
        }
        
        TSM = @{
            StopCommand = "tsm stop"
            StartCommand = "tsm start"
            ZipLogsCommand = "tsm maintenance ziplogs -l"
            CleanupCommand = "tsm maintenance cleanup -l -t"
            TimeoutSeconds = 600
        }
        
        Monitoring = @{
            DiskSpaceWarningThresholdGB = 50
        }
        
        Environment = if ($env:Environment) { $env:Environment } else { "Staging" }
    }
    
    # Load from file if provided
    if ($ConfigPath -and (Test-Path $ConfigPath)) {
        try {
            $fileConfig = Get-Content $ConfigPath -Raw | ConvertFrom-Json
            
            # Merge with defaults if not using environment variables
            if (-not $useEnvironmentVars) {
                # Top-level properties
                if ($fileConfig.PrimaryServer) {
                    $defaultConfig.PrimaryServer = $fileConfig.PrimaryServer
                }
                if ($fileConfig.SecondaryServer) {
                    $defaultConfig.SecondaryServer = $fileConfig.SecondaryServer
                }
                
                # Paths: JSON uses 'LogLocation', script uses 'LogDirectory'
                if ($fileConfig.Paths.LogLocation) {
                    $defaultConfig.Paths.LogDirectory = $fileConfig.Paths.LogLocation
                }
                if ($fileConfig.Paths.ZipLogDirectory) {
                    $defaultConfig.Paths.ZipLogDirectory = $fileConfig.Paths.ZipLogDirectory
                }
                if ($fileConfig.Paths.TableauZipLogDirectory) {
                    $defaultConfig.Paths.TableauZipLogDirectory = $fileConfig.Paths.TableauZipLogDirectory
                }
                
                # Retention: JSON uses 'RetentionDays.Logs', script uses 'Retention.LogFilesRetentionDays'
                if ($fileConfig.RetentionDays.Logs) {
                    $defaultConfig.Retention.LogFilesRetentionDays = [int]$fileConfig.RetentionDays.Logs
                }
                # Note: JSON has no ZipLogsRetentionDays - default (30) is used
            }
            
            Write-Information "Configuration loaded from $ConfigPath"
        }
        catch {
            Write-Warning "Failed to load config from $ConfigPath. Using defaults/environment variables. Error: $($_.Exception.Message)"
        }
    }
    
    return $defaultConfig
}

$Config = Get-CleanupConfiguration -ConfigPath $ConfigFile
#endregion

#region Logging Functions
function Write-Log {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$Message,
        
        [ValidateSet("INFO", "WARN", "ERROR", "SUCCESS")]
        [string]$Level = "INFO",
        
        [string]$LogFile
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $server = $env:COMPUTERNAME
    $logMessage = "$timestamp [$Level] [$server] - $Message"
    
    # Console output with color
    switch ($Level) {
        "ERROR"   { Write-Host $logMessage -ForegroundColor Red }
        "WARN"    { Write-Host $logMessage -ForegroundColor Yellow }
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        default   { Write-Host $logMessage }
    }
    
    # Azure DevOps pipeline logging
    if ($env:TF_BUILD -eq 'True') {
        switch ($Level) {
            "ERROR" { Write-Host "##vso[task.logissue type=error]$Message" }
            "WARN" { Write-Host "##vso[task.logissue type=warning]$Message" }
        }
    }
    
    # Write to file
    if ($LogFile) {
        try {
            Add-Content -Path $LogFile -Value $logMessage -ErrorAction Stop
        }
        catch {
            Write-Warning "Failed to write to log file: $_"
        }
    }
}

function Initialize-LogFile {
    param(
        [string]$LogDirectory,
        [string]$LogPrefix
    )
    
    try {
        if (-not (Test-Path $LogDirectory)) {
            New-Item -Path $LogDirectory -ItemType Directory -Force | Out-Null
            Write-Log "Created log directory: $LogDirectory" -Level "INFO"
        }
        
        $dateStamp = Get-Date -Format "yyyyMMdd"
        $logFileName = "${LogPrefix}_${dateStamp}.log"
        $logFilePath = Join-Path $LogDirectory $logFileName
        
        $header = @"
================================================================================
Tableau $($Config.Environment) Cleanup Process
Started: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Server: $env:COMPUTERNAME
User: $env:USERNAME
$(if ($env:TF_BUILD -eq 'True') { "Pipeline: $($env:BUILD_DEFINITIONNAME)" })
$(if ($env:BUILD_BUILDNUMBER) { "Build: $($env:BUILD_BUILDNUMBER)" })
================================================================================
"@
        Add-Content -Path $logFilePath -Value $header
        
        return $logFilePath
    }
    catch {
        Write-Error "Failed to initialize log file: $_"
        throw
    }
}
#endregion

#region Disk Space Functions
function Get-DiskSpaceInfo {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$DriveLetter,
        
        [string]$LogFile
    )
    
    try {
        $drive = Get-PSDrive -Name $DriveLetter -ErrorAction Stop
        
        $usedBytes = $drive.Used
        $freeBytes = $drive.Free
        $totalBytes = $usedBytes + $freeBytes
        
        $result = @{
            DriveLetter = $DriveLetter
            UsedGB = [math]::Round($usedBytes / 1GB, 2)
            FreeGB = [math]::Round($freeBytes / 1GB, 2)
            TotalGB = [math]::Round($totalBytes / 1GB, 2)
            UsedPercent = [math]::Round(($usedBytes / $totalBytes) * 100, 2)
        }
        
        Write-Log "Disk $DriveLetter : Total=$($result.TotalGB)GB, Used=$($result.UsedGB)GB ($($result.UsedPercent)%), Free=$($result.FreeGB)GB" -LogFile $LogFile
        
        return $result
    }
    catch {
        Write-Log "Failed to get disk space for drive '$DriveLetter': $_" -Level "ERROR" -LogFile $LogFile
        throw
    }
}

function Get-FolderSize {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$FolderPath,
        
        [string]$Label = "Folder",
        [string]$LogFile
    )
    
    try {
        if (-not (Test-Path $FolderPath)) {
            Write-Log "Folder does not exist: $FolderPath" -Level "WARN" -LogFile $LogFile
            return 0
        }
        
        $sizeBytes = (Get-ChildItem -Path $FolderPath -Recurse -File -ErrorAction SilentlyContinue | 
                      Measure-Object -Property Length -Sum).Sum
        
        if ($null -eq $sizeBytes) { $sizeBytes = 0 }
        
        $sizeMB = [math]::Round($sizeBytes / 1MB, 2)
        Write-Log "$Label Size: $sizeMB MB ($FolderPath)" -LogFile $LogFile
        
        return $sizeMB
    }
    catch {
        Write-Log "Failed to get folder size: $_" -Level "ERROR" -LogFile $LogFile
        return 0
    }
}
#endregion

#region Cleanup Functions
function Remove-OldFiles {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$Path,
        
        [Parameter(Mandatory)]
        [int]$RetentionDays,
        
        [string]$FilePattern = "*",
        [string]$Description,
        [string]$LogFile
    )
    
    Write-Log "Cleaning up $Description (retention: $RetentionDays days)" -LogFile $LogFile
    
    $stats = @{
        FilesDeleted = 0
        SpaceFreedMB = 0
        Errors = 0
    }
    
    try {
        if (-not (Test-Path $Path)) {
            Write-Log "Path does not exist: $Path" -Level "WARN" -LogFile $LogFile
            return $stats
        }
        
        $cutoffDate = (Get-Date).AddDays(-$RetentionDays)
        $files = Get-ChildItem -Path $Path -Filter $FilePattern -File -ErrorAction Stop |
                 Where-Object { $_.LastWriteTime -lt $cutoffDate }
        
        Write-Log "Found $($files.Count) files to delete" -LogFile $LogFile
        
        foreach ($file in $files) {
            try {
                $sizeMB = [math]::Round($file.Length / 1MB, 2)
                
                if ($WhatIf) {
                    Write-Log "WHATIF: Would delete $($file.Name) ($sizeMB MB)" -LogFile $LogFile
                }
                else {
                    Remove-Item $file.FullName -Force -ErrorAction Stop
                    Write-Log "Deleted: $($file.Name) ($sizeMB MB)" -LogFile $LogFile
                    $stats.FilesDeleted++
                    $stats.SpaceFreedMB += $sizeMB
                }
            }
            catch {
                Write-Log "Error deleting $($file.Name): $_" -Level "ERROR" -LogFile $LogFile
                $stats.Errors++
            }
        }
        
        Write-Log "$Description cleanup complete: $($stats.FilesDeleted) files, $($stats.SpaceFreedMB) MB freed" -Level "SUCCESS" -LogFile $LogFile
    }
    catch {
        Write-Log "Error during cleanup: $_" -Level "ERROR" -LogFile $LogFile
        $stats.Errors++
    }
    
    return $stats
}

function Invoke-RemoteCleanup {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$ServerName,
        
        [Parameter(Mandatory)]
        [hashtable]$Paths,
        
        [Parameter(Mandatory)]
        [hashtable]$Retention,
        
        [string]$LogFile
    )
    
    Write-Log "Starting remote cleanup on secondary server: $ServerName" -LogFile $LogFile
    
    $remoteStats = @{
        FilesDeleted = 0
        SpaceFreedMB = 0
        Errors = 0
    }
    
    try {
        # Test connectivity first
        if (-not (Test-Connection -ComputerName $ServerName -Count 1 -Quiet)) {
            Write-Log "Secondary server $ServerName is not reachable - skipping remote cleanup" -Level "WARN" -LogFile $LogFile
            return $remoteStats
        }
        
        $result = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            param($LogDir, $ZipLogDir, $TableauZipLogDir, $LogRetention, $ZipRetention)
            
            $stats = @{ FilesDeleted = 0; SpaceFreedMB = 0; Errors = 0 }
            
            $locations = @(
                @{ Path = $LogDir; Days = $LogRetention; Pattern = "*.log"; Name = "Script Logs" }
                @{ Path = $ZipLogDir; Days = $ZipRetention; Pattern = "*.zip"; Name = "Zip Logs" }
                @{ Path = $TableauZipLogDir; Days = $ZipRetention; Pattern = "*.zip"; Name = "Tableau Zip Logs" }
            )
            
            foreach ($loc in $locations) {
                if (-not (Test-Path $loc.Path)) { continue }
                
                $cutoffDate = (Get-Date).AddDays(-$loc.Days)
                $files = Get-ChildItem -Path $loc.Path -Filter $loc.Pattern -File -ErrorAction SilentlyContinue |
                         Where-Object { $_.LastWriteTime -lt $cutoffDate }
                
                foreach ($file in $files) {
                    try {
                        $sizeMB = [math]::Round($file.Length / 1MB, 2)
                        Remove-Item $file.FullName -Force -ErrorAction Stop
                        $stats.FilesDeleted++
                        $stats.SpaceFreedMB += $sizeMB
                    }
                    catch {
                        $stats.Errors++
                    }
                }
            }
            
            return $stats
        } -ArgumentList $Paths.LogDirectory, $Paths.ZipLogDirectory, $Paths.TableauZipLogDirectory, $Retention.LogFilesRetentionDays, $Retention.ZipLogsRetentionDays
        
        $remoteStats.FilesDeleted = $result.FilesDeleted
        $remoteStats.SpaceFreedMB = $result.SpaceFreedMB
        $remoteStats.Errors = $result.Errors
        
        Write-Log "Secondary server cleanup complete: $($remoteStats.FilesDeleted) files, $([math]::Round($remoteStats.SpaceFreedMB, 2)) MB freed" -Level "SUCCESS" -LogFile $LogFile
        
        if ($remoteStats.Errors -gt 0) {
            Write-Log "Secondary server had $($remoteStats.Errors) deletion errors" -Level "WARN" -LogFile $LogFile
        }
    }
    catch {
        Write-Log "Remote cleanup failed on $ServerName : $_" -Level "ERROR" -LogFile $LogFile
        $remoteStats.Errors++
    }
    
    return $remoteStats
}

function Invoke-TSMCommand {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$Command,
        
        [Parameter(Mandatory)]
        [string]$Description,
        
        [int]$TimeoutSeconds = 600,
        [string]$LogFile
    )
    
    Write-Log "Executing: $Description" -LogFile $LogFile
    Write-Log "Command: $Command" -LogFile $LogFile
    
    if ($WhatIf) {
        Write-Log "WHATIF: Would execute TSM command" -LogFile $LogFile
        return $true
    }
    
    try {
        $job = Start-Job -ScriptBlock {
            param($cmd)
            $output = Invoke-Expression $cmd 2>&1
            [PSCustomObject]@{
                Output = ($output | Out-String)
                ExitCode = $LASTEXITCODE
            }
        } -ArgumentList $Command
        
        $completed = Wait-Job $job -Timeout $TimeoutSeconds
        
        if ($completed) {
            $jobResult = Receive-Job $job
            Remove-Job $job
            
            if ($jobResult.ExitCode -ne 0) {
                Write-Log "$Description failed with exit code $($jobResult.ExitCode): $($jobResult.Output)" -Level "ERROR" -LogFile $LogFile
                return $false
            }
            
            if ($jobResult.Output) {
                Write-Log "TSM Output: $($jobResult.Output)" -LogFile $LogFile
            }
            
            Write-Log "$Description completed successfully" -Level "SUCCESS" -LogFile $LogFile
            return $true
        }
        else {
            Stop-Job $job
            Remove-Job $job
            throw "Command timed out after $TimeoutSeconds seconds"
        }
    }
    catch {
        Write-Log "$Description failed: $_" -Level "ERROR" -LogFile $LogFile
        return $false
    }
}
#endregion

#region Email Functions
function Send-CleanupNotification {
    param(
        [ValidateSet("Success", "Warning", "Failure")]
        [string]$Status,
        [hashtable]$Stats,
        [hashtable]$Configuration,
        [string]$LogFile
    )
    
    if (-not $Configuration.Email.SendNotifications -or $SkipEmailNotification) {
        Write-Information "Email notification skipped"
        return
    }
    
    $statusColor = switch ($Status) {
        "Success" { "#28a745" }
        "Warning" { "#ffc107" }
        "Failure" { "#dc3545" }
    }
    
    $subject = "Tableau $($Configuration.Environment) Cleanup - $Status"
    $dateStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    
    # Build table rows
    $tableRows = @()
    $tableRows += "<tr><td>Status</td><td style='color: $statusColor; font-weight: bold;'>$Status</td></tr>"
    $tableRows += "<tr><td>Environment</td><td>$($Configuration.Environment)</td></tr>"
    $tableRows += "<tr><td>Server</td><td>$($Stats.Server)</td></tr>"
    
    if ($Stats.Duration) {
        $tableRows += "<tr><td>Duration</td><td>$([math]::Round($Stats.Duration.TotalMinutes, 2)) minutes</td></tr>"
    }
    
    if ($Stats.DiskSpaceBefore) {
        $tableRows += "<tr><td>Disk Space Before</td><td>$($Stats.DiskSpaceBefore.FreeGB) GB free ($($Stats.DiskSpaceBefore.UsedPercent)% used)</td></tr>"
    }
    
    if ($Stats.DiskSpaceAfter) {
        $tableRows += "<tr><td>Disk Space After</td><td>$($Stats.DiskSpaceAfter.FreeGB) GB free ($($Stats.DiskSpaceAfter.UsedPercent)% used)</td></tr>"
    }
    
    if ($Stats.SpaceFreed) {
        $tableRows += "<tr><td>Space Freed</td><td>$([math]::Round($Stats.SpaceFreed, 2)) MB</td></tr>"
    }
    
    if ($Stats.FilesDeleted) {
        $tableRows += "<tr><td>Files Deleted</td><td>$($Stats.FilesDeleted)</td></tr>"
    }
    
    if ($Stats.TSMOperations) {
        $tableRows += "<tr><td>TSM Operations</td><td>$($Stats.TSMOperations)</td></tr>"
    }
    
    if ($Stats.SecondaryServer) {
        $tableRows += "<tr><td>Secondary Server</td><td>$($Stats.SecondaryServer.Server): $($Stats.SecondaryServer.FilesDeleted) files, $([math]::Round($Stats.SecondaryServer.SpaceFreedMB, 2)) MB freed</td></tr>"
    }
    
    $tableRows += "<tr><td>Log File</td><td>$LogFile</td></tr>"
    
    # HTML body
    $body = @"
<!DOCTYPE html>
<html>
<head>
<style>
  body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
  h2 { color: $statusColor; }
  table { border-collapse: collapse; width: 100%; margin-top: 20px; }
  th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
  th { background-color: #f2f2f2; font-weight: bold; }
  .metadata { background-color: #f8f9fa; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
  .footer { margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }
</style>
</head>
<body>
  <h2>Tableau $($Configuration.Environment) Cleanup $Status</h2>
  
  <div class="metadata">
    <strong>Timestamp:</strong> $dateStamp<br>
    <strong>Script Version:</strong> 3.1.0<br>
    <strong>Environment:</strong> $($Configuration.Environment)<br>
    $(if ($env:BUILD_BUILDNUMBER) { "<strong>Build:</strong> $($env:BUILD_BUILDNUMBER)<br>" })
  </div>

  <table>
    $($tableRows -join "`n    ")
  </table>
  
  <div class="footer">
    This is an automated notification from the operations-automation automation platform.<br>
    For issues or questions, please contact the Onshore Group App Support team.
  </div>
</body>
</html>
"@

    try {
        $mailParams = @{
            From = $Configuration.Email.From
            To = $Configuration.Email.To
            Subject = $subject
            SmtpServer = $Configuration.Email.SmtpServer
            Body = $body
            BodyAsHtml = $true
            ErrorAction = 'Stop'
        }
        
        if ($Configuration.Email.CC -and $Configuration.Email.CC.Count -gt 0) {
            $mailParams.Cc = $Configuration.Email.CC
        }
        
        Send-MailMessage @mailParams
        Write-Information "Email notification sent successfully"
    }
    catch {
        Write-Warning "Failed to send email notification: $_"
    }
}
#endregion

#region Main Script
try {
    $startTime = Get-Date
    $currentServer = $env:COMPUTERNAME
    
    # Initialize logging
    $logFile = Initialize-LogFile -LogDirectory $Config.Paths.LogDirectory -LogPrefix $Config.Paths.LogFilePrefix
    
    Write-Log "=============================================================================" -LogFile $logFile
    Write-Log "Tableau Cleanup Script v3.1.0 (Azure DevOps Edition)" -LogFile $logFile
    Write-Log "Environment: $($Config.Environment)" -LogFile $logFile
    Write-Log "Server: $currentServer" -LogFile $logFile
    Write-Log "WhatIf Mode: $WhatIf" -LogFile $logFile
    Write-Log "=============================================================================" -LogFile $logFile
    
    # Get disk space before
    $diskSpaceBefore = Get-DiskSpaceInfo -DriveLetter $Config.Paths.MonitorDrive -LogFile $logFile
    
    # Check disk space threshold
    if ($diskSpaceBefore.FreeGB -lt $Config.Monitoring.DiskSpaceWarningThresholdGB) {
        Write-Log "WARNING: Free disk space ($($diskSpaceBefore.FreeGB) GB) is below threshold ($($Config.Monitoring.DiskSpaceWarningThresholdGB) GB)" -Level "WARN" -LogFile $logFile
    }
    
    # Statistics tracking
    $totalStats = @{
        Server = $currentServer
        FilesDeleted = 0
        SpaceFreed = 0
        TSMOperations = 0
        DiskSpaceBefore = $diskSpaceBefore
    }
    
    # Run TSM maintenance ziplogs
    Write-Log "Running TSM ziplogs..." -LogFile $logFile
    $zipLogsSuccess = Invoke-TSMCommand -Command $Config.TSM.ZipLogsCommand `
                                        -Description "TSM ZipLogs" `
                                        -TimeoutSeconds $Config.TSM.TimeoutSeconds `
                                        -LogFile $logFile
    if ($zipLogsSuccess) { $totalStats.TSMOperations++ }
    
    # Run TSM maintenance cleanup
    Write-Log "Running TSM cleanup..." -LogFile $logFile
    $cleanupSuccess = Invoke-TSMCommand -Command $Config.TSM.CleanupCommand `
                                       -Description "TSM Cleanup" `
                                       -TimeoutSeconds $Config.TSM.TimeoutSeconds `
                                       -LogFile $logFile
    if ($cleanupSuccess) { $totalStats.TSMOperations++ }
    
    # Clean up old log files
    $logCleanup = Remove-OldFiles -Path $Config.Paths.LogDirectory `
                                   -RetentionDays $Config.Retention.LogFilesRetentionDays `
                                   -FilePattern "*.log" `
                                   -Description "Script Logs" `
                                   -LogFile $logFile
    
    $totalStats.FilesDeleted += $logCleanup.FilesDeleted
    $totalStats.SpaceFreed += $logCleanup.SpaceFreedMB
    
    # Clean up old zip logs
    $zipCleanup = Remove-OldFiles -Path $Config.Paths.ZipLogDirectory `
                                   -RetentionDays $Config.Retention.ZipLogsRetentionDays `
                                   -FilePattern "*.zip" `
                                   -Description "Zip Logs" `
                                   -LogFile $logFile
    
    $totalStats.FilesDeleted += $zipCleanup.FilesDeleted
    $totalStats.SpaceFreed += $zipCleanup.SpaceFreedMB
    
    # Clean up Tableau zip logs
    $tableauZipCleanup = Remove-OldFiles -Path $Config.Paths.TableauZipLogDirectory `
                                         -RetentionDays $Config.Retention.ZipLogsRetentionDays `
                                         -FilePattern "*.zip" `
                                         -Description "Tableau Zip Logs" `
                                         -LogFile $logFile
    
    $totalStats.FilesDeleted += $tableauZipCleanup.FilesDeleted
    $totalStats.SpaceFreed += $tableauZipCleanup.SpaceFreedMB
    
    # Clean up secondary server (production only)
    if ($Config.SecondaryServer) {
        Write-Log "Secondary server configured: $($Config.SecondaryServer)" -LogFile $logFile
        
        if (-not $WhatIf) {
            $remoteCleanup = Invoke-RemoteCleanup -ServerName $Config.SecondaryServer `
                                                   -Paths $Config.Paths `
                                                   -Retention $Config.Retention `
                                                   -LogFile $logFile
            
            $totalStats.FilesDeleted += $remoteCleanup.FilesDeleted
            $totalStats.SpaceFreed += $remoteCleanup.SpaceFreedMB
            $totalStats.SecondaryServer = @{
                Server = $Config.SecondaryServer
                FilesDeleted = $remoteCleanup.FilesDeleted
                SpaceFreedMB = $remoteCleanup.SpaceFreedMB
            }
        }
        else {
            Write-Log "WHATIF: Would run remote cleanup on $($Config.SecondaryServer)" -LogFile $logFile
        }
    }
    
    # Get disk space after
    $diskSpaceAfter = Get-DiskSpaceInfo -DriveLetter $Config.Paths.MonitorDrive -LogFile $logFile
    $totalStats.DiskSpaceAfter = $diskSpaceAfter
    
    # Calculate space improvement
    $spaceImprovement = $diskSpaceAfter.FreeGB - $diskSpaceBefore.FreeGB
    if ($spaceImprovement -lt 0) {
        Write-Log "Disk space decreased by $([math]::Round([math]::Abs($spaceImprovement), 2)) GB (TSM operations may have created archives or other processes consumed space)" -Level "WARN" -LogFile $logFile
    }
    else {
        Write-Log "Disk space improvement: $([math]::Round($spaceImprovement, 2)) GB" -Level "SUCCESS" -LogFile $logFile
    }
    
    $endTime = Get-Date
    $duration = $endTime - $startTime
    $totalStats.Duration = $duration
    
    Write-Log "=============================================================================" -LogFile $logFile
    Write-Log "Cleanup Process Completed Successfully" -Level "SUCCESS" -LogFile $logFile
    Write-Log "Total execution time: $([math]::Round($duration.TotalMinutes, 2)) minutes" -LogFile $logFile
    Write-Log "Files deleted: $($totalStats.FilesDeleted)" -LogFile $logFile
    Write-Log "Space freed: $([math]::Round($totalStats.SpaceFreed, 2)) MB" -LogFile $logFile
    Write-Log "TSM operations: $($totalStats.TSMOperations)" -LogFile $logFile
    Write-Log "=============================================================================" -LogFile $logFile
    
    # Send notification
    Send-CleanupNotification -Status "Success" -Stats $totalStats -Configuration $Config -LogFile $logFile
    
    # Publish log file as artifact (if in Azure DevOps)
    if ($env:TF_BUILD -eq 'True' -and (Test-Path $logFile)) {
        Write-Host "##vso[artifact.upload containerfolder=logs;artifactname=logs]$logFile"
    }
    
    exit 0
}
catch {
    $errorMessage = $_.Exception.Message
    $endTime = Get-Date
    $duration = if ($startTime) { $endTime - $startTime } else { $null }
    
    Write-Log "=============================================================================" -Level "ERROR" -LogFile $logFile
    Write-Log "CRITICAL ERROR: $errorMessage" -Level "ERROR" -LogFile $logFile
    Write-Log "Stack Trace: $($_.ScriptStackTrace)" -Level "ERROR" -LogFile $logFile
    Write-Log "Cleanup Process Failed" -Level "ERROR" -LogFile $logFile
    Write-Log "=============================================================================" -Level "ERROR" -LogFile $logFile
    
    # Send failure notification
    $failureStats = @{
        Server = $currentServer
        Duration = $duration
        ErrorMessage = $errorMessage
    }
    
    Send-CleanupNotification -Status "Failure" -Stats $failureStats -Configuration $Config -LogFile $logFile
    
    # Publish log file even on failure
    if ($env:TF_BUILD -eq 'True' -and (Test-Path $logFile)) {
        Write-Host "##vso[artifact.upload containerfolder=logs;artifactname=logs]$logFile"
    }
    
    exit 1
}
#endregion
