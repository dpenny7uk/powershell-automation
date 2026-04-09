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

    # Get last job for this workflow
    $lastJob = $null
    try {
        $rc = if ($detail) { $detail.runCount } else { 0 }
        $skipTo = if ($rc -gt 1) { $rc - 1 } else { 0 }
        $jobs = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=$skipTo&take=1" -Headers $headers -Method Get
        if ($jobs -and $jobs.Count -gt 0) {
            $lastJob = $jobs[-1]
        }
    } catch {}

    $results.Add([PSCustomObject]@{
        id                  = $wf.id
        name                = $wf.name
        dateCreated         = $wf.dateCreated
        runCount            = if ($detail) { $detail.runCount } else { "" }
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