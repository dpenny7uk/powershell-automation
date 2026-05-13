#requires -Version 5.1
<#
.SYNOPSIS
    Read-only: pulls SHIR registration details from the local registry.
    Identifies which Data Factory each SHIR node is registered to, so you
    can determine whether cross-subscription ADF discovery is needed.

.DESCRIPTION
    Designed to run via run-discovery-remote.ps1 against the same server list
    you already used:

      .\run-discovery-remote.ps1 `
          -ServerListPath '.\shir-servers.csv' `
          -ScriptPath     '.\get-shir-registration.ps1' `
          -ReturnDir      'E:\Libcurl_Remediation\Output\shir-registration-<ts>'

    OR run locally on a single box for spot-checks.

    Outputs one CSV per host into C:\Windows\Temp (then the wrapper retrieves it).
    No secrets, no auth keys, no tokens - only the non-sensitive registration
    fields useful for working out which ADF the SHIR talks to.
#>

[CmdletBinding()]
param(
    [string]$OutDir = "C:\Windows\Temp"
)

$ErrorActionPreference = 'Continue'
$h  = $env:COMPUTERNAME
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
$outFile = Join-Path $OutDir "$h-$ts-shir-registration.csv"

$regPath = 'HKLM:\SOFTWARE\Microsoft\DataTransfer\DataManagementGateway\ConfigurationManager'
$row = [PSCustomObject]@{
    Host                    = $h
    RegistryFound           = $false
    IntegrationRuntimeName  = $null
    ClusterId               = $null
    DataFactoryEndpoint     = $null
    DataFactoryName         = $null   # parsed from endpoint where possible
    Region                  = $null
    Cloud                   = $null
    ConnectionStatus        = $null
    Version                 = $null
    InstallationPath        = $null
    AllNonSecretProperties  = $null
}

if (Test-Path -LiteralPath $regPath) {
    $row.RegistryFound = $true
    try {
        $reg = Get-ItemProperty -Path $regPath -ErrorAction Stop

        # Pull commonly-present, non-sensitive properties. Names vary slightly across SHIR versions.
        foreach ($prop in 'IntegrationRuntimeName','GatewayName','NodeName') {
            if ($reg.PSObject.Properties[$prop] -and $reg.$prop) { $row.IntegrationRuntimeName = $reg.$prop; break }
        }
        foreach ($prop in 'ClusterId','GatewayClusterId') {
            if ($reg.PSObject.Properties[$prop] -and $reg.$prop) { $row.ClusterId = $reg.$prop; break }
        }
        foreach ($prop in 'HostServiceUri','HostServiceUriPrefix','ServiceUrls','DispatcherServiceUrl','WssEndpoint') {
            if ($reg.PSObject.Properties[$prop] -and $reg.$prop) { $row.DataFactoryEndpoint = $reg.$prop; break }
        }
        foreach ($prop in 'ConnectionStatus','Status') {
            if ($reg.PSObject.Properties[$prop] -and $reg.$prop) { $row.ConnectionStatus = $reg.$prop; break }
        }
        foreach ($prop in 'Version','GatewayVersion','RuntimeVersion') {
            if ($reg.PSObject.Properties[$prop] -and $reg.$prop) { $row.Version = $reg.$prop; break }
        }
        foreach ($prop in 'InstallationPath','InstallPath') {
            if ($reg.PSObject.Properties[$prop] -and $reg.$prop) { $row.InstallationPath = $reg.$prop; break }
        }

        # Parse DataFactoryName + Region from endpoint URL where possible.
        # Typical formats:
        #   https://<adfname>.svc.datafactory.azure.com
        #   https://<region>.frontdoor.datafactory.azure.com/...
        #   https://<adfname>-<region>.svc.datafactory.azure.com
        if ($row.DataFactoryEndpoint) {
            if ($row.DataFactoryEndpoint -match 'https?://([a-z0-9-]+)\.svc\.datafactory\.(?:azure\.com|usgovcloudapi\.net|chinacloudapi\.cn)') {
                $row.DataFactoryName = $matches[1]
            } elseif ($row.DataFactoryEndpoint -match 'https?://([a-z0-9-]+)\.([a-z0-9-]+)\.datafactory') {
                $row.DataFactoryName = $matches[1]
                $row.Region = $matches[2]
            }
            if ($row.DataFactoryEndpoint -match 'datafactory\.azure\.com')        { $row.Cloud = 'AzurePublic' }
            elseif ($row.DataFactoryEndpoint -match 'datafactory\.usgovcloudapi') { $row.Cloud = 'AzureGov' }
            elseif ($row.DataFactoryEndpoint -match 'datafactory\.chinacloudapi') { $row.Cloud = 'AzureChina' }
        }

        # Catch-all dump of non-secret properties for diagnostics.
        $secretPatterns = @('Key','Secret','Token','Credential','Password','Pwd','Cert','Encrypt','Hash')
        $nonSecret = $reg.PSObject.Properties |
            Where-Object {
                $name = $_.Name
                $name -notlike 'PS*' -and -not ($secretPatterns | Where-Object { $name -like "*$_*" })
            } |
            ForEach-Object { "$($_.Name)=$($_.Value)" }
        $row.AllNonSecretProperties = ($nonSecret -join '; ')
    } catch {
        $row.AllNonSecretProperties = "Read failed: $_"
    }
}

$row | Export-Csv -NoTypeInformation -Path $outFile
Write-Host "Wrote: $outFile" -ForegroundColor Green
$row | Format-List
