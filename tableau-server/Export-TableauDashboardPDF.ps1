#Requires -Version 5.1
<#
.SYNOPSIS
    Export Tableau Server dashboards to PDF with parameterised filtering
.DESCRIPTION
    Automated PDF export of Tableau dashboards for multiple business entities.
    Called by Azure DevOps pipeline with credentials passed as parameters.
    Email notification handled by pipeline, not this script.

    Use Case: Monthly coverholder/partner reports exported as individual PDFs,
    one per entity, using Tableau's tabcmd export with URL parameter filtering.

    Partner list is maintained in partners.json alongside this script.

.PARAMETER TableauPassword
    Service account password (sourced from Azure DevOps secret variable)
.PARAMETER TableauServerHost
    Hostname of the Tableau Server where tabcmd runs (sourced from variable group)
.PARAMETER ExportCount
    Number of partners to export. 0 = all (default). Use a small number for quick testing.
.PARAMETER DryRun
    Preview mode - logs what would happen without executing exports
.EXAMPLE
    .\Export-TableauDashboardPDF.ps1 -TableauPassword $env:TABLEAU_PASSWORD
.EXAMPLE
    .\Export-TableauDashboardPDF.ps1 -TableauPassword $env:TABLEAU_PASSWORD -ExportCount 3
.EXAMPLE
    .\Export-TableauDashboardPDF.ps1 -TableauPassword $env:TABLEAU_PASSWORD -DryRun
#>

[CmdletBinding()]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingPlainTextForPassword', 'TableauPassword',
    Justification = 'Password sourced from Azure DevOps secret variable and must be written to file for tabcmd. SecureString adds no practical security in this pipeline context.')]
param(
    [Parameter(Mandatory)]
    [string]$TableauPassword,

    [Parameter(Mandatory)]
    [string]$TableauServerHost,

    [int]$ExportCount = 0,

    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

#region Configuration
$TableauServer = "http://tableau-prod-01.contoso.com/"
$TableauSite = "BusinessSite"
$TableauUser = "SVC_Tableau"
$DashboardPath = "InsuranceReportPack/CoverSummary"
$ParameterName = "Parameters.Partner%20Name"

$LocalBasePath = "D:\FloodCover"

$MaxRetries = 3
$RetryDelaySeconds = 10
$ExportTimeoutMs = 900000     # 15 minutes per export
$MinFileSizeKB = 10           # PDFs smaller than this are flagged as suspect
#endregion

#region Load Partners
$PartnersFile = Join-Path $PSScriptRoot "partners.json"
if (-not (Test-Path $PartnersFile)) {
    throw "Partner list not found: $PartnersFile"
}
$Partners = Get-Content $PartnersFile -Raw | ConvertFrom-Json
if ($Partners.Count -eq 0) {
    throw "Partner list is empty"
}

# Limit partner count for testing
if ($ExportCount -gt 0 -and $ExportCount -lt $Partners.Count) {
    $totalPartners = $Partners.Count
    $Partners = $Partners[0..($ExportCount - 1)]
}
#endregion

#region Logging
$IsADO = $env:TF_BUILD -eq 'True'

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logLine = "$timestamp [$Level] $Message"

    if ($IsADO) {
        switch ($Level) {
            "ERROR"   { Write-Host "##vso[task.logissue type=error]$Message" }
            "WARN"    { Write-Host "##vso[task.logissue type=warning]$Message" }
            default   { Write-Host $logLine }
        }
    } else {
        switch ($Level) {
            "ERROR"   { Write-Host $logLine -ForegroundColor Red }
            "WARN"    { Write-Host $logLine -ForegroundColor Yellow }
            "SUCCESS" { Write-Host $logLine -ForegroundColor Green }
            default   { Write-Host $logLine }
        }
    }
}
#endregion

#region Main
$startTime = Get-Date
$success = 0
$failed = 0

try {
    Write-Log "========================================"
    Write-Log "Tableau Dashboard PDF Export"
    Write-Log "========================================"
    Write-Log "Running as: $env:USERNAME"

    $ReportDate = (Get-Date).AddMonths(-1)
    $Year = $ReportDate.ToString("yyyy")
    $MonthNum = $ReportDate.ToString("MM")
    $MonthName = $ReportDate.ToString("MMMM")
    $FolderName = "$MonthNum - $MonthName"
    $ReportPeriod = "$MonthName $Year"

    Write-Log "Report Period: $ReportPeriod"
    Write-Log "Partners: $($Partners.Count)"
    if ($ExportCount -gt 0 -and $ExportCount -lt $totalPartners) {
        Write-Log "TEST MODE: Limited to first $ExportCount of $totalPartners partners" -Level WARN
    }
    if ($DryRun) { Write-Log "MODE: DRY RUN - No actual exports" -Level WARN }

    $RemoteExportPath = Join-Path (Join-Path $LocalBasePath $Year) $FolderName
    $UncExportPath = "\\$TableauServerHost\" + ($RemoteExportPath -replace '^([A-Z]):', '$1$')

    if ($DryRun) {
        # Dry run - simulate locally
        for ($i = 0; $i -lt $Partners.Count; $i++) {
            $partner = $Partners[$i]
            $counter = $i + 1
            Write-Log "[$counter/$($Partners.Count)] Exporting $($partner.FileName)..."
            Write-Log "  [DRY RUN] Would export: $($partner.FileName)"
            $success++
        }
    } else {
        # Run tabcmd on Tableau Server via PSRemoting
        Write-Log "Connecting to Tableau Server: $TableauServerHost"
        $session = New-PSSession -ComputerName $TableauServerHost -ErrorAction Stop
        Write-Log "Connected to remote session" -Level SUCCESS

        try {
            $remoteResult = Invoke-Command -Session $session -ArgumentList @(
                $TableauServer, $TableauSite, $TableauUser, $TableauPassword,
                $DashboardPath, $ParameterName, $RemoteExportPath,
                $MaxRetries, $RetryDelaySeconds, $ExportTimeoutMs, $MinFileSizeKB,
                $Partners
            ) -ScriptBlock {
                param(
                    $TableauServer, $TableauSite, $TableauUser, $TableauPassword,
                    $DashboardPath, $ParameterName, $ExportPath,
                    $MaxRetries, $RetryDelaySeconds, $ExportTimeoutMs, $MinFileSizeKB,
                    $Partners
                )

                $ErrorActionPreference = 'Stop'
                $success = 0
                $failed = 0

                # Create export folder
                New-Item -Path $ExportPath -ItemType Directory -Force | Out-Null
                Write-Host "Remote export folder: $ExportPath"

                # Write password file
                $PasswordFile = Join-Path $env:TEMP "tableau_pwd_$([guid]::NewGuid().ToString('N')).txt"
                $TableauPassword | Out-File -FilePath $PasswordFile -Encoding ASCII -NoNewline

                try {
                    # Login
                    Write-Host "Authenticating to Tableau Server..."
                    $loginArgs = @(
                        "login"
                        "--server", $TableauServer
                        "--site", $TableauSite
                        "--username", $TableauUser
                        "--password-file", $PasswordFile
                        "--no-certcheck"
                    )
                    $loginResult = & tabcmd @loginArgs 2>&1
                    if ($LASTEXITCODE -ne 0) {
                        throw "Login failed: $loginResult"
                    }
                    Write-Host "Authenticated successfully"

                    # Export each partner
                    for ($i = 0; $i -lt $Partners.Count; $i++) {
                        $partner = $Partners[$i]
                        $counter = $i + 1
                        $outputFile = Join-Path $ExportPath $partner.FileName
                        $url = "$DashboardPath`?$ParameterName=$($partner.Name)"

                        Write-Host "[$counter/$($Partners.Count)] Exporting $($partner.FileName)..."

                        $exported = $false
                        for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
                            if ($attempt -gt 1) {
                                Write-Host "  Retry $attempt/$MaxRetries for $($partner.FileName)..."
                                Start-Sleep -Seconds $RetryDelaySeconds
                            }

                            $exportArgString = "export `"$url`" --fullpdf --pagelayout portrait --pagesize a4 --filename `"$outputFile`""
                            $proc = Start-Process -FilePath "tabcmd" -ArgumentList $exportArgString -NoNewWindow -PassThru
                            $exited = $proc.WaitForExit($ExportTimeoutMs)

                            if (-not $exited) {
                                try { $proc.Kill() } catch {}
                                Write-Host "  TIMEOUT after $([math]::Round($ExportTimeoutMs / 60000))m: $($partner.FileName)"
                                continue
                            }

                            if ($proc.ExitCode -eq 0 -and (Test-Path $outputFile)) {
                                $sizeKB = [math]::Round((Get-Item $outputFile).Length / 1KB, 1)
                                if ($sizeKB -lt $MinFileSizeKB) {
                                    Write-Host "  SUSPECT: $($partner.FileName) is only ${sizeKB} KB - possible blank export"
                                    Remove-Item $outputFile -Force -ErrorAction SilentlyContinue
                                    continue
                                }
                                Write-Host "  Exported: $($partner.FileName) (${sizeKB} KB)"
                                $exported = $true
                                break
                            }

                            Write-Host "  FAILED (attempt $attempt): $($partner.FileName) - exit code $($proc.ExitCode)"
                        }

                        if ($exported) { $success++ } else { $failed++ }
                    }

                    # Logout
                    & tabcmd logout 2>&1 | Out-Null
                    Write-Host "Logged out of Tableau"

                } finally {
                    if (Test-Path $PasswordFile) {
                        Remove-Item $PasswordFile -Force -ErrorAction SilentlyContinue
                    }
                }

                return @{ Success = $success; Failed = $failed; ExportPath = $ExportPath }
            }

            $success = $remoteResult.Success
            $failed = $remoteResult.Failed
            Write-Log "Remote export complete: $success succeeded, $failed failed"

        } finally {
            Remove-PSSession -Session $session -ErrorAction SilentlyContinue
        }
    }

    $ExportPath = $UncExportPath
    $duration = "{0:hh\:mm\:ss}" -f ((Get-Date) - $startTime)
    Write-Log "========================================"
    Write-Log "COMPLETE: $success succeeded, $failed failed (Duration: $duration)" -Level $(if ($failed -eq 0) { "SUCCESS" } else { "WARN" })
    Write-Log "========================================"

    # Set ADO variables for downstream steps
    if ($IsADO) {
        Write-Host "##vso[task.setvariable variable=ExportPath]$ExportPath"
        Write-Host "##vso[task.setvariable variable=ExportSuccess]$success"
        Write-Host "##vso[task.setvariable variable=ExportFailed]$failed"
    }

    if ($failed -gt 0) { exit 1 }

} catch {
    $errorMsg = $_.Exception.Message
    Write-Log "FATAL ERROR: $errorMsg" -Level ERROR
    exit 1
}
#endregion
