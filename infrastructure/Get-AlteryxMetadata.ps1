# Re-pull Alteryx workflow metadata with correct field names
# Run from the same machine as the export script

$BaseUrl      = "https://alteryx.contoso.com/webapi"
$TokenUrl     = "https://alteryx.contoso.com/webapi/oauth2/token"
$ClientId     = "YOUR_CLIENT_ID"
$ClientSecret = "YOUR_CLIENT_SECRET"
$OutputCsv    = "C:\Dev\AlteryxExport\workflow_metadata.csv"
$PageSize     = 100

# Authenticate
$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
}
$tokenResponse = Invoke-RestMethod -Uri $TokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
$headers = @{ "Authorization" = "Bearer $($tokenResponse.access_token)" }

# Get all workflows
$allWorkflows = [System.Collections.Generic.List[object]]::new()
$skip = 0
do {
    $response = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows?skip=$skip&take=$PageSize" -Headers $headers -Method Get
    if ($response -and $response.Count -gt 0) {
        $allWorkflows.AddRange($response)
        Write-Host "Retrieved $($allWorkflows.Count)..."
        $skip += $PageSize
    } else { break }
} while ($response.Count -eq $PageSize)

Write-Host "Total: $($allWorkflows.Count) workflows"

# Now get individual details for each (to capture runCount and job history)
$results = [System.Collections.Generic.List[object]]::new()
$i = 0

foreach ($wf in $allWorkflows) {
    $i++
    Write-Host "  [$i/$($allWorkflows.Count)] $($wf.name)" -ForegroundColor Gray

    # Get workflow detail
    try {
        $detail = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)" -Headers $headers -Method Get
    } catch {
        $detail = $null
    }

    # Count actual jobs by paginating (runCount from API is unreliable)
    $actualRunCount = 0
    $lastJob = $null
    try {
        # Get first page to check if any jobs exist
        $firstPage = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=0&take=$PageSize" -Headers $headers -Method Get
        if ($firstPage -and $firstPage.Count -gt 0) {
            $actualRunCount = $firstPage.Count

            if ($firstPage.Count -eq $PageSize) {
                # More pages exist — keep counting
                $countSkip = $PageSize
                do {
                    $nextPage = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=$countSkip&take=$PageSize" -Headers $headers -Method Get
                    if ($nextPage -and $nextPage.Count -gt 0) {
                        $actualRunCount += $nextPage.Count
                        $countSkip += $PageSize
                    } else { break }
                    Start-Sleep -Milliseconds 150
                } while ($nextPage.Count -eq $PageSize)

                # Get the actual last job
                $lastPage = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=$($actualRunCount - 1)&take=1" -Headers $headers -Method Get
                if ($lastPage -and $lastPage.Count -gt 0) { $lastJob = $lastPage[-1] }
            } else {
                # All jobs fit in one page — last job is the last element
                $lastJob = $firstPage[-1]
            }
        }
    } catch {}

    $results.Add([PSCustomObject]@{
        id                  = $wf.id
        name                = $wf.name
        ownerId             = if ($detail) { $detail.ownerId } else { "" }
        dateCreated         = $wf.dateCreated
        runCount            = $actualRunCount
        packageType         = if ($detail -and $detail.versions) { $detail.versions[0].packageWorkflowType } else { "" }
        fileName            = if ($detail -and $detail.details) { $detail.details.fileName } else { "" }
        published           = if ($detail -and $detail.versions) { $detail.versions[0].published } else { "" }
        runDisabled         = if ($detail -and $detail.versions) { $detail.versions[0].runDisabled } else { "" }
        lastJobStatus       = if ($lastJob) { $lastJob.status } else { "No jobs" }
        lastJobDate         = if ($lastJob) { $lastJob.createDate } else { "" }
    })

    Start-Sleep -Milliseconds 150
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
Write-Host "`nMetadata saved: $OutputCsv" -ForegroundColor Green