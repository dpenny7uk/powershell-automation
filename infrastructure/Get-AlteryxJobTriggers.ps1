# Identify who triggered unscheduled Alteryx workflow runs
# Cross-references job history with user directory to produce an audit trail
#
# Outputs:
#   job_triggers.csv         — one row per job (who ran what, when)
#   job_triggers_summary.csv — one row per workflow (unique triggerers, run counts)

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
$ActiveSince = [datetime]"2025-01-01"
Write-Host "`nChecking workflow run counts (filtering to runs since $($ActiveSince.ToString('yyyy-MM-dd')))..."
$candidates = [System.Collections.Generic.List[object]]::new()
$skippedHistoric = 0
$i = 0

foreach ($wf in $allWorkflows) {
    $i++
    if ($scheduledWorkflowIds.Contains($wf.id)) { continue }

    try {
        $detail = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)" -Headers $headers -Method Get
    } catch {
        $detail = $null
    }

    $rc = if ($detail) { $detail.runCount } else { 0 }
    if ($rc -gt 0) {
        # Check last job date — only include if most recent run is 2025+
        $lastJob = $null
        try {
            $skipTo = if ($rc -gt 1) { $rc - 1 } else { 0 }
            $jobs = Invoke-RestMethod -Uri "$BaseUrl/v3/workflows/$($wf.id)/jobs?skip=$skipTo&take=1" -Headers $headers -Method Get
            if ($jobs -and $jobs.Count -gt 0) { $lastJob = $jobs[-1] }
        } catch {}

        $lastJobDate = if ($lastJob -and $lastJob.createDate) { [datetime]$lastJob.createDate } else { $null }

        if ($lastJobDate -and $lastJobDate -ge $ActiveSince) {
            $candidates.Add([PSCustomObject]@{
                id       = $wf.id
                name     = $wf.name
                runCount = $rc
            })
        } else {
            $skippedHistoric++
        }
    }

    if ($i % 50 -eq 0) {
        Write-Host "  Checked $i/$($allWorkflows.Count) — found $($candidates.Count) recent, skipped $skippedHistoric historic"
    }

    Start-Sleep -Milliseconds 150
}

Write-Host "`nUnscheduled workflows with recent runs (2025+): $($candidates.Count)"
Write-Host "Skipped (last run before 2025): $skippedHistoric"

# ── Fetch full job history for each candidate ────────────────────────────
$results = [System.Collections.Generic.List[object]]::new()
$wfIndex = 0

foreach ($wf in $candidates) {
    $wfIndex++
    Write-Host "  [$wfIndex/$($candidates.Count)] $($wf.name) ($($wf.runCount) runs)" -ForegroundColor Gray

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
            $user = if ($job.userId) { $userLookup[$job.userId] } else { $null }
            $results.Add([PSCustomObject]@{
                workflowId        = $wf.id
                workflowName      = $wf.name
                jobId             = $job.id
                jobDate           = $job.createDate
                jobStatus         = $job.status
                triggeredByUserId = if ($job.userId) { $job.userId } else { "" }
                triggeredByName   = if ($user) { "$($user.firstName) $($user.lastName)" } else { "Unknown" }
                triggeredByEmail  = if ($user) { $user.email } else { "" }
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

    # Find the user who triggered the most runs
    $topTriggerer = $group |
        Where-Object { $_.triggeredByName -ne "Unknown" } |
        Group-Object triggeredByName |
        Sort-Object Count -Descending |
        Select-Object -First 1

    [PSCustomObject]@{
        workflowId       = $_.Name
        workflowName     = $group[0].workflowName
        totalRuns        = $group.Count
        uniqueTriggerers = ($group | Select-Object triggeredByUserId -Unique).Count
        triggererNames   = ($group | Select-Object triggeredByName -Unique | ForEach-Object { $_.triggeredByName }) -join " | "
        triggererEmails  = ($group | Where-Object { $_.triggeredByEmail } | Select-Object triggeredByEmail -Unique | ForEach-Object { $_.triggeredByEmail }) -join " | "
        mostLikelyOwner  = if ($topTriggerer) { "$($topTriggerer.Name) ($($topTriggerer.Count) runs)" } else { "Unknown" }
        firstRunDate     = ($group | Sort-Object jobDate | Select-Object -First 1).jobDate
        lastRunDate      = ($group | Sort-Object jobDate -Descending | Select-Object -First 1).jobDate
    }
}

$summary | Export-Csv -Path $SummaryCsv -NoTypeInformation -Encoding UTF8

# ── Console summary ──────────────────────────────────────────────────────
$uniqueUsers = ($results | Where-Object { $_.triggeredByName -ne "Unknown" } | Select-Object triggeredByUserId -Unique).Count

Write-Host "`n── Job Trigger Summary ──" -ForegroundColor Cyan
Write-Host "  Total workflows in gallery:      $($allWorkflows.Count)"
Write-Host "  Scheduled workflows (excluded):   $($scheduledWorkflowIds.Count)"
Write-Host "  Unscheduled with runs:            $($candidates.Count)"
Write-Host "  Total jobs found:                 $($results.Count)"
Write-Host "  Unique users who triggered runs:  $uniqueUsers"
Write-Host "`nDetailed export: $OutputCsv" -ForegroundColor Green
Write-Host "Summary export:  $SummaryCsv" -ForegroundColor Green
