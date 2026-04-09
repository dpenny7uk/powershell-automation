#Requires -Version 5.1
<#
.SYNOPSIS
    Exports all workflows from Alteryx Server via the v3 API.

.DESCRIPTION
    Authenticates using OAuth2 client credentials, paginates through all workflows,
    and downloads each workflow package (.yxzp) to a local directory.

.NOTES
    1. Paste your API Key (client_id) and API Secret (client_secret) below
    2. Run from a machine that can reach alteryx.contoso.com
#>

# ── Configuration ─────────────────────────────────────────────────────────────
$BaseUrl        = "https://alteryx.contoso.com/webapi"
$TokenUrl       = "https://alteryx.contoso.com/webapi/oauth2/token"
$ClientId       = "YOUR_CLIENT_ID"       # API Key from your profile
$ClientSecret   = "YOUR_CLIENT_SECRET"   # API Secret from your profile
$OutputDir      = "C:\AlteryxExport\workflows"
$PageSize       = 100

# ── Setup ─────────────────────────────────────────────────────────────────────
$ErrorActionPreference = "Stop"

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    Write-Host "Created output directory: $OutputDir" -ForegroundColor Cyan
}

# ── Authenticate (OAuth2 Client Credentials) ──────────────────────────────────
Write-Host "Authenticating..." -ForegroundColor Cyan

$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
}

try {
    $tokenResponse = Invoke-RestMethod -Uri $TokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
    $accessToken = $tokenResponse.access_token
    Write-Host "Authentication successful." -ForegroundColor Green
}
catch {
    Write-Error "Failed to authenticate. Check your Client ID/Secret.`n$($_.Exception.Message)"
    exit 1
}

$headers = @{
    "Authorization" = "Bearer $accessToken"
}

# ── Retrieve all workflow metadata ────────────────────────────────────────────
Write-Host "Retrieving workflow list..." -ForegroundColor Cyan

$allWorkflows = [System.Collections.Generic.List[object]]::new()
$skip = 0

do {
    $url = "$BaseUrl/v3/workflows?skip=$skip&take=$PageSize"

    try {
        $response = Invoke-RestMethod -Uri $url -Headers $headers -Method Get
    }
    catch {
        Write-Warning "Failed to fetch workflows at offset $skip : $($_.Exception.Message)"
        break
    }

    if ($response -and $response.Count -gt 0) {
        $allWorkflows.AddRange($response)
        Write-Host "  Retrieved $($allWorkflows.Count) workflows so far..." -ForegroundColor Gray
        $skip += $PageSize
    }
    else {
        break
    }

} while ($response.Count -eq $PageSize)

Write-Host "Total workflows found: $($allWorkflows.Count)" -ForegroundColor Green

# ── Export metadata to CSV for reference ──────────────────────────────────────
$metadataCsv = Join-Path $OutputDir "_workflow_metadata.csv"
$allWorkflows | Select-Object id, name, isPublic, runCount,
    @{N='owner';E={$_.owner.email}},
    @{N='dateCreated';E={$_.dateCreated}},
    @{N='lastRunDate';E={$_.lastRunDate}} |
    Export-Csv -Path $metadataCsv -NoTypeInformation -Encoding UTF8

Write-Host "Metadata exported to: $metadataCsv" -ForegroundColor Cyan

# ── Download each workflow package ────────────────────────────────────────────
Write-Host "`nDownloading workflow packages..." -ForegroundColor Cyan

$success = 0
$failed  = 0
$failedList = [System.Collections.Generic.List[string]]::new()

foreach ($wf in $allWorkflows) {
    $wfId   = $wf.id
    $safeName = ($wf.name -replace '[\\/:*?"<>|]', '_').Trim()
    $fileName = "${safeName}__${wfId}.yxzp"
    $outPath  = Join-Path $OutputDir $fileName

    if (Test-Path $outPath) {
        Write-Host "  SKIP (exists): $safeName" -ForegroundColor DarkGray
        $success++
        continue
    }

    $downloadUrl = "$BaseUrl/v3/workflows/$wfId/package"

    try {
        Invoke-RestMethod -Uri $downloadUrl -Headers $headers -Method Get -OutFile $outPath
        Write-Host "  OK: $safeName" -ForegroundColor Green
        $success++
    }
    catch {
        Write-Warning "  FAIL: $safeName - $($_.Exception.Message)"
        $failed++
        $failedList.Add("$wfId | $safeName | $($_.Exception.Message)")
    }

    Start-Sleep -Milliseconds 200
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "`n════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "Export complete." -ForegroundColor Green
Write-Host "  Success: $success" -ForegroundColor Green
Write-Host "  Failed:  $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })
Write-Host "  Output:  $OutputDir" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════" -ForegroundColor Cyan

if ($failedList.Count -gt 0) {
    $failLog = Join-Path $OutputDir "_failed_downloads.log"
    $failedList | Out-File -FilePath $failLog -Encoding UTF8
    Write-Host "Failed downloads logged to: $failLog" -ForegroundColor Yellow
}