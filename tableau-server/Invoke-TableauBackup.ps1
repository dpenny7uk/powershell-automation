<#
======================================================================================
Script Name: TableauBackup.ps1
Description: Tableau Server backup with Azure DevOps pipeline support
Author: Your Name
Environment: Azure DevOps Pipeline / Windows Server
Version: 3.1.0
Created Date: 2025-12-08
Last Modified: 2026-02-11
Execution: Azure DevOps Pipeline or manual execution
Dependencies: PowerShell 5+, TSM CLI, SMTP Relay
Log Location: Configured via config file or environment variables
Notes: Supports both config file and Azure DevOps variable groups
       Designed for multi-environment deployment (staging/production)

Change Log:
  v3.1.0 (2026-02-11) - Bug fix: Write-Log return value polluted Remove-OldFiles output
                         causing production email to show 0 files deleted
                       - Bug fix: PowerShell 5.1 compatibility (removed ?? and -AsHashtable)
                       - Bug fix: Config file merge now works with PSCustomObject and
                         corrected property name filter
                       - Bug fix: Remove-OldFiles no longer inflates count in WhatIf mode
                       - Bug fix: Failure email now includes cleanup statistics
                       - Bug fix: TSM backup exit code now validated inside Start-Job
                       - Bug fix: Move-BackupFile now filters by expected filename first
                       - Bug fix: Corrupted UTF-8 bullet character in warning emails
                       - Bug fix: UNC backup paths no longer fail disk space pre-check
  v3.0.0 (2025-12-08) - Azure DevOps pipeline integration
                      - Support for environment variables from variable groups
                      - Improved error handling and logging
                      - Multi-server support (production cluster)
  v2.1.0 (2025-11-05) - Original Task Scheduler version
======================================================================================
#>

#Requires -Version 5.0

[CmdletBinding()]
param(
    [string]$ConfigFile,
    [switch]$WhatIf,
    [switch]$SkipEmailNotification,
    [switch]$ValidateOnly,
    
    # Azure DevOps pipeline parameters (override config file)
    [string]$ServerName,
    [string]$BackupPath,
    [string]$TableauBackupPath,
    [string]$LogPath,
    [int]$RetentionDays,
    [string]$EmailTo,
    [string]$EmailFrom,
    [string]$SmtpServer
)

$ErrorActionPreference = 'Stop'
$InformationPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'

#region Configuration
function Get-BackupConfiguration {
    param([string]$ConfigPath)
   
    # Check for Azure DevOps environment variables first
    $useEnvironmentVars = $false
    if ($env:TableauServerName -or $env:TableauPrimaryServer) {
        $useEnvironmentVars = $true
        Write-Information "Using configuration from Azure DevOps environment variables"
    }
    
    # Default configuration template
    $defaultConfig = @{
        PrimaryServer = if ($useEnvironmentVars) { 
            if ($env:TableauPrimaryServer) { $env:TableauPrimaryServer }
            elseif ($env:TableauServerName) { $env:TableauServerName }
            else { "localhost" }
        } else { 
            "TABLEAU-STG-01" 
        }
        SecondaryServer = $env:TableauSecondaryServer
       
        # Retention periods (days)
        RetentionDays = @{
            Logs = if ($env:RetentionDays) { [int]$env:RetentionDays } else { 7 }
            Backups = 1
        }
       
        # Paths - prioritize environment variables
        Paths = @{
            LogLocation = if ($env:TableauLogPath) { 
                $env:TableauLogPath 
            } elseif ($LogPath) { 
                $LogPath 
            } else { 
                "D:\Scripts\Logs\" 
            }
            
            BackupLocation = if ($env:TableauBackupPath) { 
                $env:TableauBackupPath 
            } elseif ($BackupPath) { 
                $BackupPath 
            } else { 
                "D:\BACKUPS\" 
            }
            
            TableauBackupLocation = if ($TableauBackupPath) { 
                $TableauBackupPath 
            } else { 
                "D:\Tableau\Tableau Server\data\tabsvc\files\backups\" 
            }
        }
       
        # Email configuration - prioritize environment variables
        Email = @{
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
            
            Cc = @()
        }
       
        # Backup settings
        Backup = @{
            FileNamePattern = "Tableau_Backup_{Server}_{Date}.tsbak"
            MinimumSizeMB = 1
            MaximumAgeDays = 1
            TsmTimeout = 7200  # 2 hours in seconds
        }
       
        # Environment identifier
        Environment = if ($env:Environment) { $env:Environment } else { "Staging" }
    }
   
    # Load from file if provided and exists
    if ($ConfigPath -and (Test-Path $ConfigPath)) {
        try {
            $fileConfig = Get-Content $ConfigPath -Raw | ConvertFrom-Json
            
            # Merge file config with defaults (environment variables take precedence)
            if (-not $useEnvironmentVars) {
                foreach ($prop in $fileConfig.PSObject.Properties) {
                    if ($prop.Name -notin @('_comment', '_note')) {
                        if ($prop.Value -is [PSCustomObject]) {
                            # Merge nested properties into existing hashtable
                            if ($defaultConfig.ContainsKey($prop.Name) -and $defaultConfig[$prop.Name] -is [hashtable]) {
                                foreach ($nested in $prop.Value.PSObject.Properties) {
                                    $defaultConfig[$prop.Name][$nested.Name] = $nested.Value
                                }
                            }
                        } else {
                            $defaultConfig[$prop.Name] = $prop.Value
                        }
                    }
                }
            }
            
            Write-Information "Configuration loaded from $ConfigPath"
        }
        catch {
            Write-Warning "Failed to load config from $ConfigPath. Using defaults/environment variables. Error: $($_.Exception.Message)"
        }
    }
    
    return $defaultConfig
}

$Config = Get-BackupConfiguration -ConfigPath $ConfigFile
#endregion

#region Utility Functions  
function Write-Log {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory, ValueFromPipeline)]
        [string]$Message,
       
        [Parameter(Mandatory)]
        [string]$Server,
       
        [ValidateSet("INFO", "WARNING", "ERROR", "DEBUG", "SUCCESS")]
        [string]$Level = "INFO",
       
        [string]$LogFile
    )
   
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp [$Level] - SERVER: $Server - $Message"
   
    # Console output with color
    $color = switch ($Level) {
        "ERROR" { "Red" }
        "WARNING" { "Yellow" }
        "SUCCESS" { "Green" }
        "DEBUG" { "Gray" }
        default { "White" }
    }
    Write-Host $logMessage -ForegroundColor $color
   
    # Azure DevOps pipeline logging commands
    if ($env:TF_BUILD -eq 'True') {
        switch ($Level) {
            "ERROR" { Write-Host "##vso[task.logissue type=error]$Message" }
            "WARNING" { Write-Host "##vso[task.logissue type=warning]$Message" }
        }
    }
   
    if ($LogFile) {
        try {
            Add-Content -Path $LogFile -Value $logMessage -ErrorAction Stop
        }
        catch {
            Write-Warning "Failed to write to log file: $($_.Exception.Message)"
        }
    }
}

function Test-Prerequisites {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [hashtable]$Configuration,
       
        [Parameter(Mandatory)]
        [string]$Server,
       
        [string]$LogFile
    )
   
    Write-Log "Validating prerequisites..." $Server -LogFile $LogFile
    $issues = @()
   
    # Check if TSM is available
    try {
        $tsmVersion = & tsm version 2>&1
        Write-Log "TSM version: $tsmVersion" $Server -LogFile $LogFile
    }
    catch {
        $issues += "TSM command not found or not in PATH"
    }
   
    # Check paths accessibility
    $pathsToCheck = @(
        @{ Path = $Configuration.Paths.LogLocation; Name = "Log Location" },
        @{ Path = $Configuration.Paths.BackupLocation; Name = "Backup Location" },
        @{ Path = $Configuration.Paths.TableauBackupLocation; Name = "Tableau Backup Location" }
    )
   
    foreach ($item in $pathsToCheck) {
        if (-not (Test-Path $item.Path)) {
            try {
                New-Item -Path $item.Path -ItemType Directory -Force | Out-Null
                Write-Log "Created missing directory: $($item.Path)" $Server -LogFile $LogFile
            }
            catch {
                $issues += "$($item.Name) not accessible and could not be created: $($item.Path)"
            }
        }
        else {
            Write-Log "Path accessible: $($item.Name) - $($item.Path)" $Server "DEBUG" -LogFile $LogFile
        }
    }
   
    # Check disk space (skip for UNC paths - they point to remote servers)
    if ($Configuration.Paths.BackupLocation -match '^\\\\') {
        Write-Log "Backup location is a UNC path - skipping local disk space check" $Server "DEBUG" -LogFile $LogFile
    }
    else {
        $backupDrive = ($Configuration.Paths.BackupLocation -split '[\\/]')[0]
        if ($backupDrive -and ($backupDrive -match '^[A-Z]:$')) {
            try {
                $driveLetter = $backupDrive.TrimEnd(':')
                $drive = Get-PSDrive $driveLetter -ErrorAction Stop
                $freeSpaceGB = [math]::Round($drive.Free / 1GB, 2)
                Write-Log "Free space on backup drive $($driveLetter): $freeSpaceGB GB" $Server -LogFile $LogFile
               
                if ($freeSpaceGB -lt 10) {
                    $issues += "Low disk space on backup location: $freeSpaceGB GB"
                }
            }
            catch {
                Write-Log "Could not check disk space: $($_.Exception.Message)" $Server "WARNING" -LogFile $LogFile
            }
        }
    }
   
    # Check SMTP connectivity
    if (-not $SkipEmailNotification) {
        try {
            $tcpClient = New-Object System.Net.Sockets.TcpClient
            $tcpClient.Connect($Configuration.Email.SmtpServer, 25)
            $tcpClient.Close()
            Write-Log "SMTP server is reachable: $($Configuration.Email.SmtpServer)" $Server -LogFile $LogFile
        }
        catch {
            $issues += "SMTP server not reachable: $($Configuration.Email.SmtpServer)"
        }
    }
   
    if ($issues.Count -gt 0) {
        Write-Log "Prerequisites validation FAILED:" $Server "ERROR" -LogFile $LogFile
        foreach ($issue in $issues) {
            Write-Log "  - $issue" $Server "ERROR" -LogFile $LogFile
        }
        return $false
    }
   
    Write-Log "Prerequisites validation PASSED" $Server "SUCCESS" -LogFile $LogFile
    return $true
}

function Remove-OldFiles {
    [CmdletBinding()]
    param(
        [string]$Path,
        [int]$DaysOld,
        [string]$Filter = "*.*",
        [string]$Server,
        [string]$LogFile,
        [switch]$WhatIf
    )
   
    Write-Log "Cleaning up files older than $DaysOld days in: $Path" $Server -LogFile $LogFile
   
    $result = @{
        Deleted = 0
        SpaceFreedMB = 0
        Errors = 0
    }
   
    try {
        $cutoffDate = (Get-Date).AddDays(-$DaysOld)
        $files = Get-ChildItem -Path $Path -Filter $Filter -File -ErrorAction Stop | 
                 Where-Object { $_.LastWriteTime -lt $cutoffDate }
       
        Write-Log "Found $($files.Count) files to delete" $Server -LogFile $LogFile
       
        foreach ($file in $files) {
            try {
                $sizeMB = [math]::Round($file.Length / 1MB, 2)
               
                if ($WhatIf) {
                    Write-Log "WHATIF: Would delete $($file.Name) ($sizeMB MB)" $Server "DEBUG" -LogFile $LogFile
                }
                else {
                    Remove-Item $file.FullName -Force -ErrorAction Stop
                    Write-Log "Deleted: $($file.Name) ($sizeMB MB)" $Server "DEBUG" -LogFile $LogFile
                    $result.SpaceFreedMB += $sizeMB
                    $result.Deleted++
                }
            }
            catch {
                Write-Log "Error deleting $($file.Name): $($_.Exception.Message)" $Server "WARNING" -LogFile $LogFile
                $result.Errors++
            }
        }
    }
    catch {
        Write-Log "Error accessing path $Path $($_.Exception.Message)" $Server "ERROR" -LogFile $LogFile
        $result.Errors++
    }
   
    return $result
}

function Invoke-TableauBackup {
    [CmdletBinding()]
    param(
        [string]$BackupFileName,
        [string]$Server,
        [string]$LogFile,
        [int]$TimeoutSeconds = 7200,
        [switch]$WhatIf
    )
   
    Write-Log "Starting Tableau backup: $BackupFileName" $Server -LogFile $LogFile
   
    if ($WhatIf) {
        Write-Log "WHATIF: Would execute: tsm maintenance backup -f $BackupFileName -d" $Server "DEBUG" -LogFile $LogFile
        return
    }
   
    try {
        $command = "tsm maintenance backup -f $BackupFileName -d"
        Write-Log "Executing: $command" $Server -LogFile $LogFile
       
        $job = Start-Job -ScriptBlock {
            param($cmd)
            $output = Invoke-Expression $cmd 2>&1
            [PSCustomObject]@{
                Output = ($output | Out-String)
                ExitCode = $LASTEXITCODE
            }
        } -ArgumentList $command
       
        $completed = Wait-Job $job -Timeout $TimeoutSeconds
       
        if ($completed) {
            $jobResult = Receive-Job $job
            Remove-Job $job
           
            if ($jobResult.ExitCode -ne 0) {
                throw "TSM backup failed with exit code $($jobResult.ExitCode): $($jobResult.Output)"
            }
           
            Write-Log "Tableau backup completed" $Server "SUCCESS" -LogFile $LogFile
            if ($jobResult.Output) {
                Write-Log "TSM Output: $($jobResult.Output)" $Server "DEBUG" -LogFile $LogFile
            }
        }
        else {
            Stop-Job $job
            Remove-Job $job
            throw "Tableau backup timed out after $TimeoutSeconds seconds"
        }
    }
    catch {
        Write-Log "Tableau backup FAILED: $($_.Exception.Message)" $Server "ERROR" -LogFile $LogFile
        throw
    }
}

function Move-BackupFile {
    [CmdletBinding()]
    param(
        [string]$SourcePath,
        [string]$DestinationPath,
        [string]$ExpectedFileName,
        [string]$Server,
        [string]$LogFile,
        [switch]$WhatIf
    )
   
    Write-Log "Moving backup file from Tableau default location to storage" $Server -LogFile $LogFile
   
    try {
        # Try to find the expected file first
        $latestBackup = $null
        if ($ExpectedFileName) {
            $latestBackup = Get-ChildItem -Path $SourcePath -Filter $ExpectedFileName -File -ErrorAction SilentlyContinue
            if ($latestBackup) {
                Write-Log "Found expected backup file: $ExpectedFileName" $Server -LogFile $LogFile
            }
        }
        
        # Fall back to newest .tsbak if expected file not found
        if (-not $latestBackup) {
            if ($ExpectedFileName) {
                Write-Log "Expected file '$ExpectedFileName' not found, falling back to newest .tsbak" $Server "WARNING" -LogFile $LogFile
            }
            $sourceFiles = Get-ChildItem -Path $SourcePath -Filter "*.tsbak" -File | 
                           Sort-Object LastWriteTime -Descending
            
            if ($sourceFiles.Count -eq 0) {
                throw "No backup files found in $SourcePath"
            }
            $latestBackup = $sourceFiles[0]
        }
       
        $destinationFile = Join-Path $DestinationPath $latestBackup.Name
       
        Write-Log "Source: $($latestBackup.FullName)" $Server -LogFile $LogFile
        Write-Log "Destination: $destinationFile" $Server -LogFile $LogFile
       
        if ($WhatIf) {
            Write-Log "WHATIF: Would move $($latestBackup.Name) to $DestinationPath" $Server "DEBUG" -LogFile $LogFile
        }
        else {
            Move-Item -Path $latestBackup.FullName -Destination $destinationFile -Force -ErrorAction Stop
            Write-Log "Backup file moved successfully" $Server "SUCCESS" -LogFile $LogFile
        }
       
        return $destinationFile
    }
    catch {
        Write-Log "Failed to move backup file: $($_.Exception.Message)" $Server "ERROR" -LogFile $LogFile
        throw
    }
}

function Get-BackupMetrics {
    [CmdletBinding()]
    param([string]$BackupFilePath)
   
    try {
        if (Test-Path $BackupFilePath) {
            $file = Get-Item $BackupFilePath
            return @{
                SizeMB = [math]::Round($file.Length / 1MB, 2)
                SizeGB = [math]::Round($file.Length / 1GB, 2)
                Created = $file.CreationTime
                Modified = $file.LastWriteTime
            }
        }
        return $null
    }
    catch {
        return $null
    }
}

function Send-EmailNotification {
    param(
        [ValidateSet("Success", "Warning", "Failure")]
        [string]$Status,
        [hashtable]$Details,
        [hashtable]$Configuration
    )
   
    if ($SkipEmailNotification) {
        Write-Information "Email notification skipped (SkipEmailNotification flag set)"
        return
    }
   
    $isSuccess = $Status -eq "Success"
    $statusColor = switch ($Status) {
        "Success" { "#28a745" }
        "Warning" { "#ffc107" }
        "Failure" { "#dc3545" }
    }
   
    $subject = "Tableau $($Configuration.Environment) Backup - $Status"
    if ($WhatIf) {
        $subject += " [WHATIF MODE]"
    }
   
    $dateStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
   
    # Build table rows
    $tableRows = @()
    $tableRows += "<tr><td>Status</td><td style='color: $statusColor; font-weight: bold;'>$Status</td></tr>"
    $tableRows += "<tr><td>Environment</td><td>$($Configuration.Environment)</td></tr>"
    $tableRows += "<tr><td>Server</td><td>$($Details.Server)</td></tr>"
   
    if ($Details.BackupFileName) {
        $tableRows += "<tr><td>Backup File</td><td>$($Details.BackupFileName)</td></tr>"
    }
   
    if ($Details.BackupSizeGB) {
        $tableRows += "<tr><td>Backup Size</td><td>$($Details.BackupSizeGB) GB ($($Details.BackupSizeMB) MB)</td></tr>"
    }
   
    if ($Details.Duration) {
        $tableRows += "<tr><td>Duration</td><td>$([math]::Round($Details.Duration.TotalMinutes, 2)) minutes</td></tr>"
    }
   
    if ($Details.CleanupStats) {
        $tableRows += "<tr><td>Files Cleaned</td><td>$($Details.CleanupStats.TotalDeleted)</td></tr>"
        $tableRows += "<tr><td>Space Freed</td><td>$([math]::Round($Details.CleanupStats.TotalSpaceFreedMB, 2)) MB</td></tr>"
    }
   
    if ($Details.ErrorMessage) {
        $tableRows += "<tr><td>Error</td><td style='color: red;'>$($Details.ErrorMessage)</td></tr>"
    }
   
    if ($Details.Warnings -and $Details.Warnings.Count -gt 0) {
        $warningsList = ($Details.Warnings | ForEach-Object { "&bull; $_" }) -join "<br>"
        $tableRows += "<tr><td>Warnings</td><td style='color: orange;'>$warningsList</td></tr>"
    }
   
    $tableRows += "<tr><td>Log File</td><td>$($Details.LogFile)</td></tr>"
   
    # HTML email body
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
  .action-required { background-color: #fff3cd; border: 1px solid #ffc107; padding: 15px; margin-top: 20px; border-radius: 4px; }
</style>
</head>
<body>
  <h2>Tableau $($Configuration.Environment) Backup $Status</h2>
 
  <div class="metadata">
    <strong>Timestamp:</strong> $dateStamp<br>
    <strong>Script Version:</strong> 3.1.0<br>
    <strong>Environment:</strong> $($Configuration.Environment)<br>
    $(if ($env:BUILD_BUILDNUMBER) { "<strong>Build:</strong> $($env:BUILD_BUILDNUMBER)<br>" })
    $(if ($env:BUILD_DEFINITIONNAME) { "<strong>Pipeline:</strong> $($env:BUILD_DEFINITIONNAME)<br>" })
  </div>

  <table>
    $($tableRows -join "`n    ")
  </table>
 
  $(if (-not $isSuccess) {
    "<div class='action-required'><strong>Action Required:</strong> Please review the log file for detailed error information and take appropriate action.</div>"
  })
 
  <div class="footer">
    This is an automated notification from the operations-automation automation platform.<br>
    $(if ($env:BUILD_BUILDNUMBER) { 
        "View pipeline run: <a href='$($env:SYSTEM_TEAMFOUNDATIONCOLLECTIONURI)$($env:SYSTEM_TEAMPROJECT)/_build/results?buildId=$($env:BUILD_BUILDID)'>Build $($env:BUILD_BUILDNUMBER)</a><br>"
    })
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
       
        if ($Configuration.Email.Cc -and $Configuration.Email.Cc.Count -gt 0) {
            $mailParams.Cc = $Configuration.Email.Cc
        }
       
        Send-MailMessage @mailParams
        Write-Information "Email notification sent successfully to $($Configuration.Email.To -join ', ')"
    }
    catch {
        Write-Warning "Failed to send email notification: $($_.Exception.Message)"
    }
}
#endregion

#region Main Script
try {
    $startTime = Get-Date
    $currentServer = $env:COMPUTERNAME
    $warnings = @()
   
    $date = Get-Date -Format "yyyyMMdd"
    $logFile = Join-Path $Config.Paths.LogLocation "TableauBackup_$date.log"
   
    # Generate backup filename
    $backupFileName = $Config.Backup.FileNamePattern -replace '\{Server\}', $Config.PrimaryServer -replace '\{Date\}', $date
   
    Write-Log "=============================================================================" $currentServer -LogFile $logFile
    Write-Log "Tableau Backup Script v3.1.0 (Azure DevOps Edition)" $currentServer -LogFile $logFile
    Write-Log "Execution started at $startTime" $currentServer -LogFile $logFile
    Write-Log "=============================================================================" $currentServer -LogFile $logFile
    Write-Log "Current Server: $currentServer" $currentServer -LogFile $logFile
    Write-Log "Primary Server: $($Config.PrimaryServer)" $currentServer -LogFile $logFile
    Write-Log "Environment: $($Config.Environment)" $currentServer -LogFile $logFile
    Write-Log "WhatIf Mode: $WhatIf" $currentServer -LogFile $logFile
    Write-Log "Validate Only: $ValidateOnly" $currentServer -LogFile $logFile
   
    if ($env:TF_BUILD -eq 'True') {
        Write-Log "Running in Azure DevOps Pipeline" $currentServer -LogFile $logFile
        Write-Log "Build: $($env:BUILD_BUILDNUMBER)" $currentServer -LogFile $logFile
        Write-Log "Pipeline: $($env:BUILD_DEFINITIONNAME)" $currentServer -LogFile $logFile
    }
   
    # Validate prerequisites
    $prereqsPassed = Test-Prerequisites -Configuration $Config -Server $currentServer -LogFile $logFile
   
    if (-not $prereqsPassed) {
        throw "Prerequisites validation failed. Please check the log for details."
    }
   
    if ($ValidateOnly) {
        Write-Log "Validation complete. Exiting (ValidateOnly mode)." $currentServer -LogFile $logFile
        exit 0
    }
   
    # Only run backup on primary server (or current server if no primary defined)
    if ($Config.PrimaryServer -and ($currentServer -ne $Config.PrimaryServer)) {
        Write-Log "This server ($currentServer) is not the primary server ($($Config.PrimaryServer)). Exiting." $currentServer "WARNING" -LogFile $logFile
        exit 0
    }

    # Cleanup old files
    Write-Log "Starting cleanup phase..." $currentServer -LogFile $logFile
    $cleanupStats = @{
        TotalDeleted = 0
        TotalSpaceFreedMB = 0
    }
   
    $locations = @(
        @{ Path = $Config.Paths.LogLocation; Days = $Config.RetentionDays.Logs; Name = "Logs" }
        @{ Path = $Config.Paths.BackupLocation; Days = $Config.RetentionDays.Backups; Name = "Backups"; Filter = "*.tsbak" }
    )
   
    foreach ($location in $locations) {
        Write-Log "Cleaning up $($location.Name)..." $currentServer -LogFile $logFile
        $params = @{
            Path = $location.Path
            DaysOld = $location.Days
            Server = $currentServer
            LogFile = $logFile
            WhatIf = $WhatIf
        }
        if ($location.Filter) { $params.Filter = $location.Filter }
       
        $result = Remove-OldFiles @params
        $cleanupStats.TotalDeleted += $result.Deleted
        $cleanupStats.TotalSpaceFreedMB += $result.SpaceFreedMB
       
        if ($result.Errors -gt 0) {
            $warnings += "$($result.Errors) errors during cleanup of $($location.Name)"
        }
    }
   
    Write-Log "Cleanup phase complete. Total: $($cleanupStats.TotalDeleted) files deleted, $([math]::Round($cleanupStats.TotalSpaceFreedMB, 2)) MB freed" $currentServer -LogFile $logFile

    # Run Tableau backup
    Write-Log "Starting backup phase..." $currentServer -LogFile $logFile
    Invoke-TableauBackup -BackupFileName $backupFileName `
                         -Server $currentServer `
                         -LogFile $logFile `
                         -TimeoutSeconds $Config.Backup.TsmTimeout `
                         -WhatIf:$WhatIf

    # Move backup file
    Write-Log "Moving backup file to storage location..." $currentServer -LogFile $logFile
    $movedBackupPath = Move-BackupFile -SourcePath $Config.Paths.TableauBackupLocation `
                                        -DestinationPath $Config.Paths.BackupLocation `
                                        -ExpectedFileName $backupFileName `
                                        -Server $currentServer `
                                        -LogFile $logFile `
                                        -WhatIf:$WhatIf

    # Get backup metrics
    $backupMetrics = Get-BackupMetrics -BackupFilePath $movedBackupPath

    if (-not $backupMetrics) {
        throw "Backup file not found or metrics could not be retrieved: $movedBackupPath"
    }
   
    Write-Log "Backup file metrics: $($backupMetrics.SizeMB) MB ($($backupMetrics.SizeGB) GB)" $currentServer -LogFile $logFile
   
    # Validate backup size
    if ($backupMetrics.SizeMB -lt $Config.Backup.MinimumSizeMB) {
        $warnings += "Backup file size ($($backupMetrics.SizeMB) MB) is below minimum threshold ($($Config.Backup.MinimumSizeMB) MB)"
        Write-Log "WARNING: Backup file size is unexpectedly small" $currentServer "WARNING" -LogFile $logFile
    }

    $endTime = Get-Date
    $duration = $endTime - $startTime

    Write-Log "=============================================================================" $currentServer -LogFile $logFile
    Write-Log "Backup Process Completed Successfully" $currentServer "SUCCESS" -LogFile $logFile
    Write-Log "Total execution time: $([math]::Round($duration.TotalMinutes, 2)) minutes" $currentServer -LogFile $logFile
    Write-Log "=============================================================================" $currentServer -LogFile $logFile

    # Send success notification
    $notificationDetails = @{
        Server = $currentServer
        BackupFileName = $backupFileName
        BackupSizeMB = $backupMetrics.SizeMB
        BackupSizeGB = $backupMetrics.SizeGB
        Duration = $duration
        LogFile = $logFile
        CleanupStats = $cleanupStats
    }
   
    if ($warnings.Count -gt 0) {
        $notificationDetails.Warnings = $warnings
        Send-EmailNotification -Status "Warning" -Details $notificationDetails -Configuration $Config
    }
    else {
        Send-EmailNotification -Status "Success" -Details $notificationDetails -Configuration $Config
    }
   
    # Publish log file as pipeline artifact (if in Azure DevOps)
    if ($env:TF_BUILD -eq 'True' -and (Test-Path $logFile)) {
        Write-Host "##vso[artifact.upload containerfolder=logs;artifactname=logs]$logFile"
    }
   
    exit 0
}
catch {
    $errorMessage = $_.Exception.Message
    $endTime = Get-Date
    $duration = if ($startTime) { $endTime - $startTime } else { $null }
   
    Write-Log "=============================================================================" $currentServer "ERROR" -LogFile $logFile
    Write-Log "CRITICAL ERROR: $errorMessage" $currentServer "ERROR" -LogFile $logFile
    Write-Log "Stack Trace: $($_.ScriptStackTrace)" $currentServer "ERROR" -LogFile $logFile
    Write-Log "Backup Process Failed" $currentServer "ERROR" -LogFile $logFile
    Write-Log "=============================================================================" $currentServer "ERROR" -LogFile $logFile
   
    # Send failure notification
    $failureDetails = @{
        Server = $currentServer
        BackupFileName = $backupFileName
        ErrorMessage = $errorMessage
        LogFile = $logFile
    }
   
    if ($duration) {
        $failureDetails.Duration = $duration
    }
   
    if ($cleanupStats) {
        $failureDetails.CleanupStats = $cleanupStats
    }
   
    if ($warnings.Count -gt 0) {
        $failureDetails.Warnings = $warnings
    }
   
    Send-EmailNotification -Status "Failure" -Details $failureDetails -Configuration $Config
   
    # Publish log file even on failure
    if ($env:TF_BUILD -eq 'True' -and (Test-Path $logFile)) {
        Write-Host "##vso[artifact.upload containerfolder=logs;artifactname=logs]$logFile"
    }
   
    exit 1
}
#endregion
