<#
.SYNOPSIS
    Resolves hostnames to IPs and IPs to hostnames in bulk, identifying
    stale DNS records and validating CMDB server entries.

.PARAMETER InputFile
    Text file with one hostname or IP per line.

.PARAMETER InputList
    Array of hostnames or IPs to resolve.

.EXAMPLE
    .\Invoke-HostnameResolution.ps1 -InputFile "servers.txt"
    .\Invoke-HostnameResolution.ps1 -InputList @("APP-SERVER-01","10.0.1.50")

.NOTES
    Author: Damian Penny
    Version: 1.0
#>

[CmdletBinding(DefaultParameterSetName = 'ByFile')]
param(
    [Parameter(ParameterSetName = 'ByFile', Mandatory)]
    [ValidateScript({ Test-Path $_ })]
    [string]$InputFile,

    [Parameter(ParameterSetName = 'ByList', Mandatory)]
    [string[]]$InputList,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$entries = if ($InputFile) { Get-Content $InputFile | Where-Object { $_ -and $_ -notmatch '^\s*#' } } else { $InputList }

Write-Host "`n=== Hostname Resolution ===" -ForegroundColor Cyan
Write-Host "Resolving $($entries.Count) entries...`n"

$results = foreach ($entry in $entries) {
    $entry = $entry.Trim()
    Write-Host "  $entry ... " -NoNewline

    try {
        $isIP = $entry -match '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
        $dns = [System.Net.Dns]::GetHostEntry($entry)

        $result = [PSCustomObject]@{
            Input       = $entry
            Type        = if ($isIP) { 'IP' } else { 'Hostname' }
            Hostname    = $dns.HostName
            IPAddresses = ($dns.AddressList | ForEach-Object { $_.IPAddressToString }) -join '; '
            Pingable    = Test-Connection -ComputerName $entry -Count 1 -Quiet
            Status      = 'Resolved'
        }
        Write-Host "$($dns.HostName) → $($result.IPAddresses)" -ForegroundColor Green
        $result
    }
    catch {
        Write-Host "FAILED" -ForegroundColor Red
        [PSCustomObject]@{
            Input = $entry; Type = if ($entry -match '^\d') { 'IP' } else { 'Hostname' }
            Hostname = 'UNRESOLVED'; IPAddresses = 'N/A'
            Pingable = $false; Status = 'DNS lookup failed'
        }
    }
}

$csvFile = Join-Path $OutputPath "HostnameResolution-$timestamp.csv"
$results | Export-Csv -Path $csvFile -NoTypeInformation

$failed = ($results | Where-Object { $_.Status -ne 'Resolved' }).Count
Write-Host "`nResolved: $(($results | Where-Object { $_.Status -eq 'Resolved' }).Count) | Failed: $failed"
Write-Host "Report: $csvFile" -ForegroundColor Green
