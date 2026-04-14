# Export Alteryx Server Schedules

$OutputCsv    = "C:\Dev\AlteryxExport\schedules.csv"
$PageSize     = 100

# Authenticate
. "$PSScriptRoot\Get-AlteryxAuth.ps1"
$auth    = Get-AlteryxAuth
$headers = $auth.Headers
$BaseUrl = $auth.BaseUrl

# Get all schedules (paginated)
$scheduleList = [System.Collections.Generic.List[object]]::new()
$skip = 0
do {
    $raw = Invoke-RestMethod -Uri "$BaseUrl/v3/schedules?skip=$skip&take=$PageSize" -Headers $headers -Method Get
    $response = if ($raw -is [array]) { $raw } elseif ($null -ne $raw) { @($raw) } else { @() }
    if ($response.Count -gt 0) {
        $scheduleList.AddRange($response)
        $skip += $PageSize
    } else { break }
} while ($response.Count -eq $PageSize)
Write-Host "Total Schedules: $($scheduleList.Count)"

$unique = ($scheduleList.workflowId | Sort-Object -Unique).Count
Write-Host "Unique workflows with schedules: $unique"

# Get full detail for each schedule
Write-Host "`nFetching schedule details..."
$results = [System.Collections.Generic.List[object]]::new()
$i = 0

foreach ($sched in $scheduleList) {
    $i++
    Write-Host "  [$i/$($scheduleList.Count)] $($sched.name)" -ForegroundColor Gray

    try {
        $detail = Invoke-RestMethod -Uri "$BaseUrl/v3/schedules/$($sched.id)" -Headers $headers -Method Get
    } catch {
        Write-Warning "  Failed to get detail for $($sched.name): $($_.Exception.Message)"
        $detail = $null
    }

    $results.Add([PSCustomObject]@{
        id            = $sched.id
        name          = $sched.name
        workflowId    = $sched.workflowId
        ownerId       = $sched.ownerId
        runDateTime    = $sched.runDateTime
        timeZone      = $sched.timeZone
        enabled       = if ($detail) { $detail.enabled } else { $null }
        state         = if ($detail) { $detail.state } else { "" }
        priority      = if ($detail) { $detail.priority } else { "" }
        frequency     = if ($detail) { $detail.frequency } else { "" }
        lastRunTime   = if ($detail) { $detail.lastRunTime } else { "" }
        runCount      = if ($detail) { $detail.runCount } else { "" }
        lastError     = if ($detail) { $detail.lastError } else { "" }
        creationTime  = if ($detail) { $detail.creationTime } else { "" }
        comment       = if ($detail) { $detail.comment } else { "" }
    })

    Start-Sleep -Milliseconds 150
}

# Summary
$enabledCount = ($results | Where-Object { $_.enabled -is [bool] -and $_.enabled -eq $true }).Count
$disabledCount = ($results | Where-Object { $_.enabled -is [bool] -and $_.enabled -eq $false }).Count
$activeCount = ($results | Where-Object { $_.state -eq "Active" }).Count

Write-Host "`n── Schedule Summary ──" -ForegroundColor Cyan
Write-Host "  Enabled: $enabledCount"
Write-Host "  Disabled: $disabledCount"
Write-Host "  State Active: $activeCount"
Write-Host "  Unique workflows (all schedules): $unique"

$enabledWorkflows = ($results | Where-Object { $_.enabled -eq $true } | Select-Object -Property workflowId -Unique).Count
Write-Host "  Unique workflows (enabled only): $enabledWorkflows"

# Export
$results | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

Write-Host "`nExported to: $OutputCsv" -ForegroundColor Green