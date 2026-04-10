# Export Alteryx Server Collections with Workflow Mappings

$BaseUrl      = "https://alteryx.contoso.com/webapi"
$TokenUrl     = "https://alteryx.contoso.com/webapi/oauth2/token"
$ClientId     = "YOUR_CLIENT_ID"
$ClientSecret = "YOUR_CLIENT_SECRET"
$OutputCsv    = "C:\Dev\AlteryxExport\collections.csv"

# Authenticate
$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
}
$tokenResponse = Invoke-RestMethod -Uri $TokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
$headers = @{ "Authorization" = "Bearer $($tokenResponse.access_token)" }

# Get all collections
$collections = Invoke-RestMethod -Uri "$BaseUrl/v3/collections" -Headers $headers -Method Get
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