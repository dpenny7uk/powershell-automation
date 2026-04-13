# Audit unscheduled Alteryx workflows with recent activity (2025+)
# Captures job history and workflow creator (ownerId) for ownership assignment
# Note: the Alteryx Server API does not expose who triggered individual jobs,
#       so we use ownerId as "Created By" — the person who published the workflow.
#
# Outputs:
#   job_triggers.csv         — one row per job (workflow, creator, job date/status)
#   job_triggers_summary.csv — one row per workflow (creator, total runs, date range)

$BaseUrl      = "https://alteryx.contoso.com/webapi"
$TokenUrl     = "https://alteryx.contoso.com/webapi/oauth2/token"
$ClientId     = "YOUR_CLIENT_ID"
$ClientSecret = "YOUR_CLIENT_SECRET"
$OutputCsv    = "C:\Dev\AlteryxExport\job_triggers.csv"
$SummaryCsv   = "C:\Dev\AlteryxExport\job_triggers_summary.csv"
$PageSize     = 100

# ── Authenticate ──────────────────────────────────────────────────────────
$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
}
$tokenResponse = Invoke-RestMethod -Uri $TokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
$headers = @{ "Authorization" = "Bearer $($tokenResponse.access_token)" }

# ── Fetch all workflows (paginated) ──────────────────────────────────────
$allWorkflows = [System.Collections.Generic.List[object]]::new()
$skip = 0
do {
    $response = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows?skip=$skip&take=$PageSize" -Headers $headers -Method Get
    if ($response -and $response.Count -gt 0) {
        $allWorkflows.AddRange($response)
        Write-Host "Retrieved $($allWorkflows.Count) workflows..."
        $skip += $PageSize
    } else { break }
} while ($response.Count -eq $PageSize)

Write-Host "Total workflows: $($allWorkflows.Count)"

# ── Fetch all schedules ──────────────────────────────────────────────────
$schedules = Invoke-RestMethod -Uri "$BaseUrl/v3/schedules" -Headers $headers -Method Get
$scheduledWorkflowIds = [System.Collections.Generic.HashSet[string]]::new()
foreach ($s in $schedules) {
    [void]$scheduledWorkflowIds.Add($s.workflowId)
}
Write-Host "Scheduled workflows: $($scheduledWorkflowIds.Count)"

# ── Fetch all users and build lookup ─────────────────────────────────────
$users = Invoke-RestMethod -Uri "$BaseUrl/v3/users?view=Full" -Headers $headers -Method Get
$userLookup = @{}
foreach ($u in $users) {
    $userLookup[$u.id] = $u
}
Write-Host "Users loaded: $($userLookup.Count)"

# ── Identify unscheduled workflows with recent runs (2025+) ──────────────
# Note: runCount from the API is unreliable (often 0 for workflows that have
# actually run). Instead, we fetch the most recent job directly and check its date.
$ActiveSince = [datetime]"2025-01-01"
Write-Host "`nChecking for recent jobs (filtering to runs since $($ActiveSince.ToString('yyyy-MM-dd')))..."
$candidates = [System.Collections.Generic.List[object]]::new()
$skippedHistoric = 0
$skippedNoJobs = 0
$i = 0

foreach ($wf in $allWorkflows) {
    $i++
    if ($scheduledWorkflowIds.Contains($wf.id)) { continue }

    # Fetch the most recent job directly — don't rely on runCount
    $lastJob = $null
    try {
        $jobs = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=0&take=1&sortField=createDateTime&direction=desc" -Headers $headers -Method Get
        if ($jobs -and $jobs.Count -gt 0) { $lastJob = $jobs[0] }
    } catch {}

    if (-not $lastJob) {
        $skippedNoJobs++
    } else {
        $lastJobDate = if ($lastJob.createDateTime) { [datetime]$lastJob.createDateTime } elseif ($lastJob.createDate) { [datetime]$lastJob.createDate } else { $null }

        if ($lastJobDate -and $lastJobDate -ge $ActiveSince) {
            # Get ownerId from workflow detail
            $ownerId = ""
            try {
                $detail = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)" -Headers $headers -Method Get
                $ownerId = if ($detail) { $detail.ownerId } else { "" }
            } catch {}

            $candidates.Add([PSCustomObject]@{
                id       = $wf.id
                name     = $wf.name
                ownerId  = $ownerId
            })
        } else {
            $skippedHistoric++
        }
    }

    if ($i % 50 -eq 0) {
        Write-Host "  Checked $i/$($allWorkflows.Count) — found $($candidates.Count) recent, skipped $skippedHistoric historic, $skippedNoJobs no jobs"
    }

    Start-Sleep -Milliseconds 150
}

Write-Host "`nUnscheduled workflows with recent runs (2025+): $($candidates.Count)"
Write-Host "Skipped (last run before 2025): $skippedHistoric"
Write-Host "Skipped (no jobs at all): $skippedNoJobs"

# ── Fetch full job history for each candidate ────────────────────────────
# Note: the Alteryx Server API does not expose who triggered each job.
# We capture job details + the workflow ownerId (who published it) as "Created By".
$results = [System.Collections.Generic.List[object]]::new()
$wfIndex = 0

foreach ($wf in $candidates) {
    $wfIndex++
    Write-Host "  [$wfIndex/$($candidates.Count)] $($wf.name)" -ForegroundColor Gray

    # Resolve workflow owner
    $owner = if ($wf.ownerId) { $userLookup[$wf.ownerId] } else { $null }

    $skip = 0
    do {
        try {
            $jobs = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=$skip&take=$PageSize" -Headers $headers -Method Get
        } catch {
            Write-Warning "    Failed to fetch jobs for $($wf.name) at skip=$skip"
            break
        }

        if (-not $jobs -or $jobs.Count -eq 0) { break }

        foreach ($job in $jobs) {
            $jobDate = if ($job.createDateTime) { $job.createDateTime } elseif ($job.createDate) { $job.createDate } else { "" }
            $results.Add([PSCustomObject]@{
                workflowId      = $wf.id
                workflowName    = $wf.name
                createdByUserId = if ($wf.ownerId) { $wf.ownerId } else { "" }
                createdByName   = if ($owner) { "$($owner.firstName) $($owner.lastName)" } else { "Unknown" }
                createdByEmail  = if ($owner) { $owner.email } else { "" }
                jobId           = $job.id
                jobDate         = $jobDate
                jobStatus       = $job.status
            })
        }

        $skip += $PageSize
        Start-Sleep -Milliseconds 150
    } while ($jobs.Count -eq $PageSize)
}

# ── Export detailed CSV ──────────────────────────────────────────────────
$results | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

# ── Build and export summary CSV ─────────────────────────────────────────
$summary = $results | Group-Object workflowId | ForEach-Object {
    $group = $_.Group

    [PSCustomObject]@{
        workflowId     = $_.Name
        workflowName   = $group[0].workflowName
        createdByName  = $group[0].createdByName
        createdByEmail = $group[0].createdByEmail
        totalRuns      = $group.Count
        firstRunDate   = ($group | Sort-Object jobDate | Select-Object -First 1).jobDate
        lastRunDate    = ($group | Sort-Object jobDate -Descending | Select-Object -First 1).jobDate
    }
}

$summary | Export-Csv -Path $SummaryCsv -NoTypeInformation -Encoding UTF8

# ── Console summary ──────────────────────────────────────────────────────
$knownOwners = ($results | Where-Object { $_.createdByName -ne "Unknown" } | Select-Object createdByUserId -Unique).Count

Write-Host "`n── Unscheduled Workflow Summary ──" -ForegroundColor Cyan
Write-Host "  Total workflows in gallery:      $($allWorkflows.Count)"
Write-Host "  Scheduled workflows (excluded):   $($scheduledWorkflowIds.Count)"
Write-Host "  Unscheduled with recent runs:     $($candidates.Count)"
Write-Host "  Total jobs found:                 $($results.Count)"
Write-Host "  Workflows with known creator:     $knownOwners"
Write-Host "`nDetailed export: $OutputCsv" -ForegroundColor Green
Write-Host "Summary export:  $SummaryCsv" -ForegroundColor Green
