# Export Alteryx Server Collections with Workflow Mappings

$OutputCsv    = "C:\Dev\AlteryxExport\collections.csv"
$PageSize     = 100

# Authenticate
. "$PSScriptRoot\Get-AlteryxAuth.ps1"
$auth    = Get-AlteryxAuth
$headers = $auth.Headers
$BaseUrl = $auth.BaseUrl

# Get all collections (paginated)
$collections = [System.Collections.Generic.List[object]]::new()
$skip = 0
do {
    $raw = Invoke-RestMethod -Uri "$BaseUrl/v3/collections?skip=$skip&take=$PageSize" -Headers $headers -Method Get
    $response = if ($raw -is [array]) { $raw } elseif ($null -ne $raw) { @($raw) } else { @() }
    if ($response.Count -gt 0) {
        $collections.AddRange($response)
        $skip += $PageSize
    } else { break }
} while ($response.Count -eq $PageSize)
Write-Host "Total Collections: $($collections.Count)"

# Get detail for each collection
$results = [System.Collections.Generic.List[object]]::new()
$i = 0

foreach ($coll in $collections) {
    $i++
    Write-Host "  [$i/$($collections.Count)] $($coll.name)" -ForegroundColor Gray

    try {
        $detail = Invoke-RestMethod -Uri "$BaseUrl/v3/collections/$($coll.id)" -Headers $headers -Method Get
    } catch {
        Write-Warning "  Failed to get detail for $($coll.name)"
        continue
    }

    $wfIds = $detail.workflowIds
    $userNames = @()
    if ($detail.users) {
        $userNames = $detail.users | ForEach-Object {
            $displayName = ""
            if ($_.activeDirectoryObject) {
                $displayName = $_.activeDirectoryObject.displayName
            }
            if (-not $displayName) { $displayName = $_.userId }
            $displayName
        }
    }

    $userGroupIds = if ($detail.userGroupIds) { $detail.userGroupIds } else { @() }
    $scheduleIds = if ($detail.scheduleIds) { $detail.scheduleIds } else { @() }

    # One row per workflow in the collection
    if ($wfIds -and $wfIds.Count -gt 0) {
        foreach ($wfId in $wfIds) {
            $results.Add([PSCustomObject]@{
                collectionId    = $coll.id
                collectionName  = $coll.name
                collectionOwner = $coll.ownerId
                workflowId      = $wfId
                users           = ($userNames -join " | ")
                userGroupIds    = ($userGroupIds -join " | ")
                scheduleCount   = $scheduleIds.Count
                dateAdded       = $coll.dateAdded
            })
        }
    } else {
        # Empty collection
        $results.Add([PSCustomObject]@{
            collectionId    = $coll.id
            collectionName  = $coll.name
            collectionOwner = $coll.ownerId
            workflowId      = ""
            users           = ($userNames -join " | ")
            userGroupIds    = ($userGroupIds -join " | ")
            scheduleCount   = $scheduleIds.Count
            dateAdded       = $coll.dateAdded
        })
    }

    Start-Sleep -Milliseconds 150
}

# Summary
$totalWorkflows = ($results | Where-Object { $_.workflowId -ne "" } | Select-Object -Property workflowId -Unique).Count
Write-Host "`n── Collections Summary ──" -ForegroundColor Cyan
Write-Host "  Collections: $($collections.Count)"
Write-Host "  Unique workflows in collections: $totalWorkflows"
Write-Host "  Total collection-workflow mappings: $(($results | Where-Object { $_.workflowId -ne '' }).Count)"

$results | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
Write-Host "`nExported to: $OutputCsv" -ForegroundColor Green