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
    
.PARAMETER TableauPassword
    Service account password (sourced from Azure DevOps secret variable)
.PARAMETER DryRun
    Preview mode - logs what would happen without executing exports
.EXAMPLE
    .\Export-TableauDashboardPDF.ps1 -TableauPassword $env:TABLEAU_PASSWORD
.EXAMPLE
    .\Export-TableauDashboardPDF.ps1 -TableauPassword $env:TABLEAU_PASSWORD -DryRun
#>

[CmdletBinding()]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingPlainTextForPassword', 'TableauPassword',
    Justification = 'Password sourced from Azure DevOps secret variable and must be written to file for tabcmd. SecureString adds no practical security in this pipeline context.')]
param(
    [Parameter(Mandatory)]
    [string]$TableauPassword,
    
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

#region Configuration
$TableauServer = "http://tableau-prod-01.contoso.com/"
$TableauSite = "BusinessSite"
$TableauUser = "SVC_Tableau"
$DashboardPath = "InsuranceReportPack/CoverSummary"
$ParameterName = "Parameters.Partner%20Name"

$LocalBasePath = "D:\Reports\PartnerExports"

# Partner list - edit here to add/remove entities
$Partners = @(
    @{Name = "Acme%20Insurance";    FileName = "AcmeInsurance.pdf"}
    @{Name = "Atlas%20Partners";    FileName = "AtlasPartners.pdf"}
    @{Name = "Bridgewater";         FileName = "Bridgewater.pdf"}
    @{Name = "Cascade%20Risk";      FileName = "CascadeRisk.pdf"}
    @{Name = "Cornerstone";         FileName = "Cornerstone.pdf"}
    @{Name = "Delta%20%26%20West";  FileName = "DeltaWest.pdf"}
    @{Name = "Evergreen";           FileName = "Evergreen.pdf"}
    @{Name = "Falcon";              FileName = "Falcon.pdf"}
    @{Name = "Gateway";             FileName = "Gateway.pdf"}
    @{Name = "Harbour%20Re";        FileName = "HarbourRe.pdf"}
    @{Name = "Ironclad";            FileName = "Ironclad.pdf"}
    @{Name = "Keystone";            FileName = "Keystone.pdf"}
    @{Name = "Lakeview";            FileName = "Lakeview.pdf"}
    @{Name = "Meridian";            FileName = "Meridian.pdf"}
    @{Name = "Northgate";           FileName = "Northgate.pdf"}
    @{Name = "Oakwood";             FileName = "Oakwood.pdf"}
    @{Name = "Pacific%20Specialty"; FileName = "PacificSpecialty.pdf"}
    @{Name = "Ridgeline";           FileName = "Ridgeline.pdf"}
    @{Name = "Summit";              FileName = "Summit.pdf"}
    @{Name = "Tidewater";           FileName = "Tidewater.pdf"}
    @{Name = "Union%20General";     FileName = "UnionGeneral.pdf"}
    @{Name = "Vanguard";            FileName = "Vanguard.pdf"}
    @{Name = "Westfield";           FileName = "Westfield.pdf"}
    @{Name = "York%20Specialty";    FileName = "YorkSpecialty.pdf"}
)
#endregion

#region Logging
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logLine = "$timestamp [$Level] $Message"
    
    switch ($Level) {
        "ERROR"   { Write-Host $logLine -ForegroundColor Red }
        "WARN"    { Write-Host $logLine -ForegroundColor Yellow }
        "SUCCESS" { Write-Host $logLine -ForegroundColor Green }
        default   { Write-Host $logLine }
    }
    
    # Azure DevOps logging commands
    if ($env:TF_BUILD -eq 'True') {
        if ($Level -eq "ERROR") { Write-Host "##vso[task.logissue type=error]$Message" }
        if ($Level -eq "WARN")  { Write-Host "##vso[task.logissue type=warning]$Message" }
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
    
    # Calculate previous month (reports are for previous month)
    $ReportDate = (Get-Date).AddMonths(-1)
    $Year = $ReportDate.ToString("yyyy")
    $MonthNum = $ReportDate.ToString("MM")
    $MonthName = $ReportDate.ToString("MMMM")
    $FolderName = "$MonthNum - $MonthName"
    $ReportPeriod = "$MonthName $Year"
    
    Write-Log "Report Period: $ReportPeriod"
    Write-Log "Partners: $($Partners.Count)"
    if ($DryRun) { Write-Log "MODE: DRY RUN - No actual exports" -Level WARN }
    
    # Create folder structure (PowerShell 5.1 compatible)
    $ExportPath = Join-Path (Join-Path $LocalBasePath $Year) $FolderName
    
    if (-not $DryRun) {
        New-Item -Path $ExportPath -ItemType Directory -Force | Out-Null
        Write-Log "Local folder: $ExportPath"
    }
    
    # Create password file for tabcmd (avoids password in command line)
    $PasswordFile = Join-Path $env:TEMP "tableau_pwd_$([guid]::NewGuid().ToString('N')).txt"
    if (-not $DryRun) {
        $TableauPassword | Out-File -FilePath $PasswordFile -Encoding ASCII -NoNewline
    }
    
    try {
        # Login to Tableau
        Write-Log "Authenticating to Tableau Server..."
        if (-not $DryRun) {
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
        }
        Write-Log "Authenticated successfully" -Level SUCCESS
        
        # Export each dashboard
        $counter = 0
        foreach ($partner in $Partners) {
            $counter++
            $outputFile = Join-Path $ExportPath $partner.FileName
            $url = "$DashboardPath`?$ParameterName=$($partner.Name)"
            
            Write-Log "[$counter/$($Partners.Count)] Exporting $($partner.FileName)..."
            
            if (-not $DryRun) {
                $exportArgs = @(
                    "export", $url
                    "--fullpdf"
                    "--pagelayout", "portrait"
                    "--pagesize", "a4"
                    "--filename", $outputFile
                )
                
                $exportResult = & tabcmd @exportArgs 2>&1
                
                if ($LASTEXITCODE -eq 0 -and (Test-Path $outputFile)) {
                    $size = [math]::Round((Get-Item $outputFile).Length / 1KB, 1)
                    Write-Log "  Exported: $($partner.FileName) (${size} KB)" -Level SUCCESS
                    $success++
                } else {
                    Write-Log "  FAILED: $($partner.FileName) - $exportResult" -Level ERROR
                    $failed++
                }
            } else {
                Write-Log "  [DRY RUN] Would export: $($partner.FileName)"
                $success++
            }
        }
        
        # Logout
        if (-not $DryRun) {
            & tabcmd logout 2>&1 | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Write-Log "tabcmd logout returned exit code $LASTEXITCODE (non-critical)" -Level WARN
            }
        }
        Write-Log "Logged out of Tableau"
        
        # Summary
        $duration = "{0:hh\:mm\:ss}" -f ((Get-Date) - $startTime)
        Write-Log "========================================"
        Write-Log "COMPLETE: $success succeeded, $failed failed (Duration: $duration)" -Level $(if ($failed -eq 0) { "SUCCESS" } else { "WARN" })
        Write-Log "========================================"
        
        if ($failed -gt 0) { exit 1 }
        
    } finally {
        # Clean up password file
        if (Test-Path $PasswordFile) {
            Remove-Item $PasswordFile -Force -ErrorAction SilentlyContinue
        }
    }
    
} catch {
    $errorMsg = $_.Exception.Message
    Write-Log "FATAL ERROR: $errorMsg" -Level ERROR
    exit 1
}
#endregion
