<#
.SYNOPSIS
    Shared HTML email report generator used by monitoring and audit scripts.

.DESCRIPTION
    Provides a consistent, professional HTML template for all automated reports.
    Supports colour-coded status indicators, summary statistics, and responsive
    table layouts suitable for email delivery.

.PARAMETER Title
    Report title displayed in the header.

.PARAMETER SummaryData
    Hashtable of key-value pairs for the summary section.

.PARAMETER TableData
    Array of PSCustomObjects to render as the main data table.

.PARAMETER StatusColumn
    Column name containing status values for colour coding.

.EXAMPLE
    $html = Write-HTMLReport -Title "SSL Certificate Report" `
        -SummaryData @{ 'Total Certs' = 15; 'Expiring' = 3 } `
        -TableData $results -StatusColumn 'Status'

.NOTES
    Author: Damian Penny
    Version: 1.0
#>

function Write-HTMLReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Title,

        [Parameter()]
        [hashtable]$SummaryData,

        [Parameter(Mandatory)]
        [object[]]$TableData,

        [Parameter()]
        [string]$StatusColumn = 'Status',

        [Parameter()]
        [string]$FooterText
    )

    $statusColors = @{
        'OK'          = '#eafaf1'
        'Clean'       = '#eafaf1'
        'Supported'   = '#eafaf1'
        'WARNING'     = '#fef3e2'
        'CRITICAL'    = '#fde8e8'
        'EXPIRED'     = '#f5b7b1'
        'ERROR'       = '#f5f5f5'
        'Unreachable' = '#f5f5f5'
    }

    # Build summary section
    $summaryHtml = if ($SummaryData) {
        $items = $SummaryData.GetEnumerator() | ForEach-Object {
            "<span><strong>$($_.Key):</strong> $($_.Value)</span>"
        }
        "<div class='summary'>$($items -join '  |  ')</div>"
    }

    # Build table headers
    $columns = $TableData[0].PSObject.Properties.Name
    $headerRow = ($columns | ForEach-Object { "<th>$_</th>" }) -join ''

    # Build table rows with status colouring
    $dataRows = foreach ($row in $TableData) {
        $statusValue = if ($StatusColumn -and $row.PSObject.Properties[$StatusColumn]) {
            $row.$StatusColumn
        }
        $bgColor = if ($statusValue -and $statusColors.ContainsKey($statusValue)) {
            $statusColors[$statusValue]
        } else { '#ffffff' }

        $cells = $columns | ForEach-Object { "<td>$($row.$_)</td>" }
        "<tr style='background-color: $bgColor'>$($cells -join '')</tr>"
    }

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $footer = if ($FooterText) { $FooterText } else { "Report generated: $timestamp" }

    return @"
<!DOCTYPE html>
<html>
<head><style>
    body { font-family: Arial, sans-serif; margin: 20px; color: #333; }
    h1 { color: #1F4E79; margin-bottom: 5px; }
    .summary { background: #f0f4f8; padding: 12px 15px; border-radius: 5px; margin: 15px 0; font-size: 0.95em; }
    .summary span { margin-right: 15px; }
    table { border-collapse: collapse; width: 100%; margin-top: 10px; }
    th { background: #1F4E79; color: white; padding: 10px 8px; text-align: left; font-size: 0.9em; }
    td { padding: 8px; border-bottom: 1px solid #e0e0e0; font-size: 0.9em; }
    tr:hover { background-color: #f5f5f5 !important; }
    .footer { color: #888; font-size: 0.85em; margin-top: 15px; padding-top: 10px; border-top: 1px solid #eee; }
</style></head>
<body>
    <h1>$Title</h1>
    $summaryHtml
    <table>
        <tr>$headerRow</tr>
        $($dataRows -join "`n")
    </table>
    <p class='footer'>$footer</p>
</body>
</html>
"@
}

Export-ModuleMember -Function Write-HTMLReport
