<#
.SYNOPSIS
    Monitors SSL certificate expiry dates across servers and endpoints,
    alerting at configurable thresholds (30/14/7 days).

.DESCRIPTION
    Connects to specified endpoints via HTTPS to retrieve certificate details,
    checks expiry dates against threshold values, and generates reports.
    Designed to prevent outages caused by expired certificates being missed
    by centralised certificate management teams.

    Can check both HTTPS endpoints and certificates in the Windows certificate
    store on remote servers via WinRM.

.PARAMETER Endpoints
    Array of hashtables with Name and URL keys for HTTPS endpoint checks.

.PARAMETER ServerList
    Path to text file of server hostnames for Windows cert store checks.

.PARAMETER ThresholdDays
    Days before expiry to flag as warning. Default: 30.

.PARAMETER CriticalDays
    Days before expiry to flag as critical. Default: 7.

.PARAMETER SendEmail
    Send HTML email report if certificates are expiring.

.EXAMPLE
    .\Get-SSLCertificateExpiry.ps1 -ThresholdDays 30

.NOTES
    Author: Damian Penny
    Version: 2.0
#>

[CmdletBinding()]
param(
    [Parameter()]
    [int]$ThresholdDays = 30,

    [Parameter()]
    [int]$CriticalDays = 7,

    [Parameter()]
    [string]$ServerList,

    [Parameter()]
    [switch]$SendEmail,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

#region Configuration

# Endpoints to check via HTTPS (sanitised examples)
$Endpoints = @(
    @{ Name = 'AnalyticsPlatform-Prod'; URL = 'https://analytics.contoso.com' }
    @{ Name = 'AnalyticsPlatform-Staging'; URL = 'https://analytics-staging.contoso.com' }
    @{ Name = 'BizFlow-Prod'; URL = 'https://bizflow.contoso.com' }
    @{ Name = 'BizFlow-DEV'; URL = 'https://bizflow-dev.contoso.com' }
    @{ Name = 'ReportServer-Prod'; URL = 'https://reports.contoso.com' }
    @{ Name = 'FileTransfer-Prod'; URL = 'https://eft.contoso.com' }
)

# Servers to check Windows certificate store (for IIS bindings, etc.)
$CertStoreServers = @(
    'APP-SERVER-01', 'APP-SERVER-02',
    'WEB-SERVER-01', 'WEB-SERVER-02',
    'REPORT-SERVER-01'
)

# Email settings
$SmtpServer  = 'smtp.contoso.com'
$EmailFrom   = 'monitoring@contoso.com'
$EmailTo     = @('infra-team@contoso.com')

#endregion

#region Functions

function Get-EndpointCertificate {
    <#
    .SYNOPSIS
        Retrieves SSL certificate details from an HTTPS endpoint.
    #>
    [CmdletBinding()]
    param(
        [string]$Name,
        [string]$URL
    )

    try {
        $uri = [System.Uri]$URL
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.Connect($uri.Host, 443)

        $sslStream = New-Object System.Net.Security.SslStream($tcpClient.GetStream(), $false, {
            param($sender, $certificate, $chain, $sslPolicyErrors)
            return $true  # Accept all certs for inspection
        })

        $sslStream.AuthenticateAsClient($uri.Host)
        $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($sslStream.RemoteCertificate)

        $daysRemaining = ($cert.NotAfter - (Get-Date)).Days

        $result = [PSCustomObject]@{
            Name           = $Name
            Source         = 'HTTPS Endpoint'
            URL            = $URL
            Subject        = $cert.Subject
            Issuer         = $cert.Issuer
            Thumbprint     = $cert.Thumbprint
            NotBefore      = $cert.NotBefore
            NotAfter       = $cert.NotAfter
            DaysRemaining  = $daysRemaining
            Status         = if ($daysRemaining -le 0) { 'EXPIRED' }
                            elseif ($daysRemaining -le $CriticalDays) { 'CRITICAL' }
                            elseif ($daysRemaining -le $ThresholdDays) { 'WARNING' }
                            else { 'OK' }
            Error          = $null
        }

        $sslStream.Close()
        $tcpClient.Close()

        return $result
    }
    catch {
        return [PSCustomObject]@{
            Name           = $Name
            Source         = 'HTTPS Endpoint'
            URL            = $URL
            Subject        = $null
            Issuer         = $null
            Thumbprint     = $null
            NotBefore      = $null
            NotAfter       = $null
            DaysRemaining  = $null
            Status         = 'ERROR'
            Error          = $_.Exception.Message
        }
    }
}

function Get-RemoteCertificates {
    <#
    .SYNOPSIS
        Retrieves certificates from the Windows certificate store on a remote server.
    #>
    [CmdletBinding()]
    param([string]$ServerName)

    try {
        if (-not (Test-Connection -ComputerName $ServerName -Count 1 -Quiet)) {
            return @([PSCustomObject]@{
                Name = $ServerName; Source = 'Cert Store'; URL = 'N/A'
                Subject = $null; Issuer = $null; Thumbprint = $null
                NotBefore = $null; NotAfter = $null; DaysRemaining = $null
                Status = 'UNREACHABLE'; Error = 'Server did not respond to ping'
            })
        }

        $certs = Invoke-Command -ComputerName $ServerName -ScriptBlock {
            # Get certs from LocalMachine\My (Personal) store - these are the IIS/service certs
            Get-ChildItem Cert:\LocalMachine\My | Where-Object {
                $_.NotAfter -gt (Get-Date).AddDays(-30)  # Include recently expired
            } | Select-Object Subject, Issuer, Thumbprint, NotBefore, NotAfter,
                @{ N = 'SANs'; E = { ($_.Extensions | Where-Object { $_.Oid.FriendlyName -eq 'Subject Alternative Name' }).Format($false) } }
        } -ErrorAction Stop

        return foreach ($cert in $certs) {
            $daysRemaining = ($cert.NotAfter - (Get-Date)).Days
            [PSCustomObject]@{
                Name           = $ServerName
                Source         = 'Windows Cert Store'
                URL            = 'LocalMachine\My'
                Subject        = $cert.Subject
                Issuer         = $cert.Issuer
                Thumbprint     = $cert.Thumbprint
                NotBefore      = $cert.NotBefore
                NotAfter       = $cert.NotAfter
                DaysRemaining  = $daysRemaining
                Status         = if ($daysRemaining -le 0) { 'EXPIRED' }
                                elseif ($daysRemaining -le $CriticalDays) { 'CRITICAL' }
                                elseif ($daysRemaining -le $ThresholdDays) { 'WARNING' }
                                else { 'OK' }
                Error          = $null
            }
        }
    }
    catch {
        return @([PSCustomObject]@{
            Name = $ServerName; Source = 'Cert Store'; URL = 'N/A'
            Subject = $null; Issuer = $null; Thumbprint = $null
            NotBefore = $null; NotAfter = $null; DaysRemaining = $null
            Status = 'ERROR'; Error = $_.Exception.Message
        })
    }
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$allResults = @()

Write-Host "`n=== SSL Certificate Expiry Check ===" -ForegroundColor Cyan
Write-Host "Threshold: ${ThresholdDays} days | Critical: ${CriticalDays} days`n"

# Check HTTPS endpoints
Write-Host "Checking HTTPS endpoints..." -ForegroundColor Cyan
foreach ($ep in $Endpoints) {
    Write-Host "  $($ep.Name): " -NoNewline
    $result = Get-EndpointCertificate -Name $ep.Name -URL $ep.URL
    $color = switch ($result.Status) {
        'OK'       { 'Green' }
        'WARNING'  { 'Yellow' }
        'CRITICAL' { 'Red' }
        'EXPIRED'  { 'DarkRed' }
        default    { 'Gray' }
    }
    Write-Host "$($result.Status) $(if ($result.DaysRemaining) { "($($result.DaysRemaining) days)" })" -ForegroundColor $color
    $allResults += $result
}

# Check Windows certificate stores
Write-Host "`nChecking certificate stores..." -ForegroundColor Cyan
foreach ($server in $CertStoreServers) {
    Write-Host "  ${server}: " -NoNewline
    $certs = Get-RemoteCertificates -ServerName $server
    $worstStatus = ($certs | Sort-Object DaysRemaining | Select-Object -First 1).Status
    Write-Host "$worstStatus ($($certs.Count) certs)" -ForegroundColor $(
        switch ($worstStatus) { 'OK' { 'Green' } 'WARNING' { 'Yellow' } default { 'Red' } }
    )
    $allResults += $certs
}

# Export CSV
$csvFile = Join-Path $OutputPath "SSL-CertExpiry-$timestamp.csv"
$allResults | Export-Csv -Path $csvFile -NoTypeInformation
Write-Host "`nCSV report: $csvFile" -ForegroundColor Green

# Summary
$expiring = $allResults | Where-Object { $_.Status -in @('WARNING', 'CRITICAL', 'EXPIRED') }
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "  Total certificates checked: $($allResults.Count)"
Write-Host "  Expiring/Expired: $($expiring.Count)" -ForegroundColor $(if ($expiring.Count -gt 0) { 'Red' } else { 'Green' })

if ($expiring.Count -gt 0) {
    Write-Host "`n  Certificates requiring attention:" -ForegroundColor Yellow
    $expiring | ForEach-Object {
        Write-Host "    [$($_.Status)] $($_.Name) - $($_.Subject) - Expires: $($_.NotAfter)" -ForegroundColor Yellow
    }
}

# Send email if requested and there are findings
if ($SendEmail -and $expiring.Count -gt 0) {
    $htmlBody = $expiring | ConvertTo-Html -Property Name, Subject, NotAfter, DaysRemaining, Status -Head '<style>table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:8px}th{background:#1F4E79;color:white}</style>' | Out-String
    Send-MailMessage -SmtpServer $SmtpServer -From $EmailFrom -To $EmailTo `
        -Subject "SSL Certificate Expiry Alert - $($expiring.Count) certificate(s) expiring" `
        -Body $htmlBody -BodyAsHtml
    Write-Host "  Email alert sent." -ForegroundColor Green
}

#endregion
