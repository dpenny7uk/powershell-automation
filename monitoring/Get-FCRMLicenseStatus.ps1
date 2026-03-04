<#
.SYNOPSIS
    Monitors application license file expiry dates to prevent system-wide
    login failures caused by expired licenses.

.DESCRIPTION
    Developed after a production incident where an expired license file
    caused a system-wide login failure (FK_UserSessionOrgUnit constraint
    error). Proactively monitors license files and alerts before expiry.

.PARAMETER LicenseConfigs
    Array of license file configurations to monitor.

.PARAMETER ThresholdDays
    Days before expiry to raise warning. Default: 30.

.EXAMPLE
    .\Get-FCRMLicenseStatus.ps1 -ThresholdDays 30

.NOTES
    Author: Damian Penny
    Version: 1.0
#>

[CmdletBinding()]
param(
    [Parameter()]
    [int]$ThresholdDays = 30,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path,

    [Parameter()]
    [switch]$SendEmail
)

#region Configuration

# License files to monitor (sanitised)
$LicenseConfigs = @(
    @{
        Application  = 'CorpCRM'
        Server       = 'APP-SERVER-01'
        LicensePath  = 'D:\Applications\CorpCRM\Config\license.xml'
        ExpiryXPath  = '//License/ExpiryDate'
        DateFormat   = 'yyyy-MM-dd'
        Owner        = 'crm-team@contoso.com'
    }
    @{
        Application  = 'RiskPlatform'
        Server       = 'APP-SERVER-03'
        LicensePath  = 'D:\Applications\RiskPlatform\license.lic'
        ExpiryRegex  = 'EXPIRY_DATE=(\d{4}-\d{2}-\d{2})'
        DateFormat   = 'yyyy-MM-dd'
        Owner        = 'risk-team@contoso.com'
    }
)

$SmtpServer = 'smtp.contoso.com'
$EmailFrom  = 'monitoring@contoso.com'
$EmailTo    = @('infra-team@contoso.com')

#endregion

#region Functions

function Get-LicenseExpiry {
    [CmdletBinding()]
    param([hashtable]$Config)

    try {
        if (-not (Test-Connection -ComputerName $Config.Server -Count 1 -Quiet)) {
            return [PSCustomObject]@{
                Application = $Config.Application; Server = $Config.Server
                ExpiryDate = $null; DaysRemaining = $null
                Status = 'Unreachable'; Error = 'Server not responding'
            }
        }

        $expiryDate = Invoke-Command -ComputerName $Config.Server -ScriptBlock {
            param($LicPath, $XPath, $Regex, $DateFmt)

            if (-not (Test-Path $LicPath)) {
                return @{ Error = "License file not found: $LicPath" }
            }

            $content = Get-Content $LicPath -Raw

            # XML-based license
            if ($XPath) {
                [xml]$xml = $content
                $dateStr = $xml.SelectSingleNode($XPath).InnerText
                return @{ Date = [DateTime]::ParseExact($dateStr, $DateFmt, $null) }
            }

            # Regex-based license
            if ($Regex -and $content -match $Regex) {
                $dateStr = $Matches[1]
                return @{ Date = [DateTime]::ParseExact($dateStr, $DateFmt, $null) }
            }

            return @{ Error = 'Could not parse expiry date from license file' }
        } -ArgumentList $Config.LicensePath, $Config.ExpiryXPath, $Config.ExpiryRegex, $Config.DateFormat -ErrorAction Stop

        if ($expiryDate.Error) {
            return [PSCustomObject]@{
                Application = $Config.Application; Server = $Config.Server
                ExpiryDate = $null; DaysRemaining = $null
                Status = 'Error'; Error = $expiryDate.Error
            }
        }

        $daysRemaining = ($expiryDate.Date - (Get-Date)).Days
        $status = if ($daysRemaining -le 0) { 'EXPIRED' }
                  elseif ($daysRemaining -le 7) { 'CRITICAL' }
                  elseif ($daysRemaining -le $ThresholdDays) { 'WARNING' }
                  else { 'OK' }

        return [PSCustomObject]@{
            Application   = $Config.Application
            Server        = $Config.Server
            ExpiryDate    = $expiryDate.Date.ToString('yyyy-MM-dd')
            DaysRemaining = $daysRemaining
            Status        = $status
            Owner         = $Config.Owner
            Error         = $null
        }
    }
    catch {
        return [PSCustomObject]@{
            Application = $Config.Application; Server = $Config.Server
            ExpiryDate = $null; DaysRemaining = $null
            Status = 'Error'; Error = $_.Exception.Message
        }
    }
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
Write-Host "`n=== License Expiry Monitor ===" -ForegroundColor Cyan

$results = foreach ($config in $LicenseConfigs) {
    Write-Host "  $($config.Application) ($($config.Server)): " -NoNewline
    $result = Get-LicenseExpiry -Config $config
    $color = switch ($result.Status) {
        'OK'       { 'Green' }
        'WARNING'  { 'Yellow' }
        'CRITICAL' { 'Red' }
        'EXPIRED'  { 'DarkRed' }
        default    { 'Gray' }
    }
    Write-Host "$($result.Status) $(if ($result.DaysRemaining) { "($($result.DaysRemaining) days)" })" -ForegroundColor $color
    $result
}

$csvFile = Join-Path $OutputPath "LicenseExpiry-$timestamp.csv"
$results | Export-Csv -Path $csvFile -NoTypeInformation
Write-Host "`nReport: $csvFile" -ForegroundColor Green

$expiring = $results | Where-Object { $_.Status -in @('WARNING', 'CRITICAL', 'EXPIRED') }
if ($expiring -and $SendEmail) {
    $body = $expiring | ConvertTo-Html -Property Application, Server, ExpiryDate, DaysRemaining, Status, Owner | Out-String
    Send-MailMessage -SmtpServer $SmtpServer -From $EmailFrom -To $EmailTo `
        -Subject "License Expiry Alert - $($expiring.Count) license(s) expiring" -Body $body -BodyAsHtml
}

#endregion
