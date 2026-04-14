# Re-pull Alteryx workflow metadata with correct field names
# Run from the same machine as the export script

$OutputCsv    = "C:\Dev\AlteryxExport\workflow_metadata.csv"
$PageSize     = 100

# Authenticate
. "$PSScriptRoot\Get-AlteryxAuth.ps1"
$auth    = Get-AlteryxAuth
$headers = $auth.Headers
$BaseUrl = $auth.BaseUrl

# Get all workflows
$allWorkflows = [System.Collections.Generic.List[object]]::new()
$skip = 0
do {
    $response = @(Invoke-RestMethod -Uri "$BaseUrl/v3/workflows?skip=$skip&take=$PageSize" -Headers $headers -Method Get)
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
        Write-Warning "  Failed to get detail for $($wf.name): $($_.Exception.Message)"
        $detail = $null
    }

    # Count actual jobs by paginating (runCount from API is unreliable)
    # Fetch sorted descending so the first result is the most recent job
    $actualRunCount = 0
    $lastJob = $null
    try {
        $firstPage = @(Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=0&take=$PageSize&sortField=createDateTime&direction=desc" -Headers $headers -Method Get)
        if ($firstPage -and $firstPage.Count -gt 0) {
            $lastJob = $firstPage[0]
            $actualRunCount = $firstPage.Count

            if ($firstPage.Count -eq $PageSize) {
                # More pages exist — keep counting
                $countSkip = $PageSize
                do {
                    $nextPage = @(Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=$countSkip&take=$PageSize&sortField=createDateTime&direction=desc" -Headers $headers -Method Get)
                    if ($nextPage -and $nextPage.Count -gt 0) {
                        $actualRunCount += $nextPage.Count
                        $countSkip += $PageSize
                    } else { break }
                    Start-Sleep -Milliseconds 150
                } while ($nextPage.Count -eq $PageSize)
            }
        }
    } catch {
        Write-Warning "Failed to fetch jobs for $($wf.name): $($_.Exception.Message)"
    }

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
        lastJobDate         = if ($lastJob) { if ($lastJob.createDateTime) { $lastJob.createDateTime } elseif ($lastJob.createDate) { $lastJob.createDate } else { "" } } else { "" }
    })

    Start-Sleep -Milliseconds 150
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
Write-Host "`nMetadata saved: $OutputCsv" -ForegroundColor Green