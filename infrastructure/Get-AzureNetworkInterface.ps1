<#
.SYNOPSIS
    Searches for Azure Network Interfaces across multiple subscriptions,
    matching by hostname, IP address, or resource group pattern.

.DESCRIPTION
    Useful for tracking down server resources in large Azure estates where
    resources span multiple subscriptions. Supports Azure Resource Graph
    queries for fast cross-subscription searching.

.PARAMETER SearchTerm
    Hostname, IP address, or pattern to search for.

.PARAMETER SubscriptionIds
    Specific subscription IDs to search. If omitted, searches all accessible.

.EXAMPLE
    .\Get-AzureNetworkInterface.ps1 -SearchTerm "APP-SERVER-01"
    .\Get-AzureNetworkInterface.ps1 -SearchTerm "10.0.1.50"

.NOTES
    Author: Damian Penny
    Version: 1.1
    Requires: Az.ResourceGraph, Az.Network modules
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SearchTerm,

    [Parameter()]
    [string[]]$SubscriptionIds,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

# Ensure connected to Azure
$context = Get-AzContext -ErrorAction SilentlyContinue
if (-not $context) {
    Write-Host "Not connected to Azure. Running Connect-AzAccount..." -ForegroundColor Yellow
    Connect-AzAccount
}

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
Write-Host "`n=== Azure Network Interface Search ===" -ForegroundColor Cyan
Write-Host "Searching for: $SearchTerm`n"

# Method 1: Azure Resource Graph (fastest for cross-subscription)
try {
    $query = @"
Resources
| where type =~ 'microsoft.network/networkinterfaces'
| where name contains '$SearchTerm'
    or properties.ipConfigurations[0].properties.privateIPAddress == '$SearchTerm'
    or tags contains '$SearchTerm'
| project name, resourceGroup, subscriptionId, location,
    privateIP = properties.ipConfigurations[0].properties.privateIPAddress,
    subnet = properties.ipConfigurations[0].properties.subnet.id,
    vmId = properties.virtualMachine.id
| order by name asc
"@

    $graphParams = @{ Query = $query }
    if ($SubscriptionIds) { $graphParams['Subscription'] = $SubscriptionIds }

    $results = Search-AzGraph @graphParams

    if ($results.Count -gt 0) {
        Write-Host "Found $($results.Count) network interface(s):" -ForegroundColor Green
        $results | Format-Table name, privateIP, resourceGroup, subscriptionId, location -AutoSize

        # Export
        $csvFile = Join-Path $OutputPath "AzureNIC-Search-$timestamp.csv"
        $results | Export-Csv -Path $csvFile -NoTypeInformation
        Write-Host "Exported to: $csvFile" -ForegroundColor Green
    }
    else {
        Write-Host "No results found via Resource Graph." -ForegroundColor Yellow

        # Method 2: Iterate subscriptions directly
        Write-Host "Trying direct subscription search..." -ForegroundColor Cyan
        $subs = if ($SubscriptionIds) { $SubscriptionIds } else { (Get-AzSubscription).Id }

        $allNics = foreach ($sub in $subs) {
            Set-AzContext -SubscriptionId $sub -ErrorAction SilentlyContinue | Out-Null
            $subName = (Get-AzContext).Subscription.Name

            Get-AzNetworkInterface -ErrorAction SilentlyContinue | Where-Object {
                $_.Name -match $SearchTerm -or
                ($_.IpConfigurations | Where-Object { $_.PrivateIpAddress -eq $SearchTerm })
            } | ForEach-Object {
                [PSCustomObject]@{
                    Name           = $_.Name
                    PrivateIP      = ($_.IpConfigurations[0]).PrivateIpAddress
                    ResourceGroup  = $_.ResourceGroupName
                    Subscription   = $subName
                    Location       = $_.Location
                    VMAttached     = if ($_.VirtualMachine) { $_.VirtualMachine.Id.Split('/')[-1] } else { 'None' }
                }
            }
        }

        if ($allNics) {
            Write-Host "Found $($allNics.Count) NIC(s):" -ForegroundColor Green
            $allNics | Format-Table -AutoSize
        }
        else {
            Write-Host "No network interfaces found matching '$SearchTerm'." -ForegroundColor Yellow
        }
    }
}
catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Ensure Az.ResourceGraph module is installed: Install-Module Az.ResourceGraph" -ForegroundColor Yellow
}
