<#
.SYNOPSIS
    Analyses Tableau Server storage usage by site, project, and content type
    via PostgreSQL repository queries. Identifies top consumers and cleanup
    candidates.

.PARAMETER TableauServer
    Hostname of Tableau Server (repository on port 8060).

.PARAMETER SiteName
    Site to analyse. Default: all sites.

.PARAMETER TopN
    Number of top consumers to report. Default: 20.

.EXAMPLE
    .\Get-TableauStorageAnalysis.ps1 -TableauServer "analytics-prod-01" -TopN 30

.NOTES
    Author: Damian Penny
    Version: 1.1
    Repository port: 8060
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$TableauServer,

    [Parameter()]
    [string]$SiteName,

    [Parameter()]
    [int]$TopN = 20,

    [Parameter()]
    [string]$RepositoryPassword,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

$RepoPort = 8060
$RepoUser = 'readonly'
$RepoDatabase = 'workgroup'

if (-not $RepositoryPassword) {
    $sec = Read-Host "Enter readonly repository password" -AsSecureString
    $RepositoryPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec))
}

# Site filter
$siteFilter = if ($SiteName) { "WHERE s.name = '$SiteName'" } else { '' }

$queries = @{
    SiteSummary = @"
SELECT s.name AS site_name,
       COUNT(DISTINCT w.id) AS workbook_count,
       COUNT(DISTINCT d.id) AS datasource_count,
       pg_size_pretty(SUM(w.data_size)) AS workbook_size,
       pg_size_pretty(SUM(d.data_size)) AS datasource_size
FROM sites s
LEFT JOIN workbooks w ON w.site_id = s.id
LEFT JOIN datasources d ON d.site_id = s.id
$siteFilter
GROUP BY s.name ORDER BY SUM(w.data_size) DESC NULLS LAST;
"@

    TopWorkbooks = @"
SELECT w.name AS workbook_name, p.name AS project_name, s.name AS site_name,
       su.friendly_name AS owner,
       pg_size_pretty(w.data_size) AS size,
       w.data_size AS size_bytes,
       w.updated_at, w.created_at
FROM workbooks w
JOIN projects p ON w.project_id = p.id
JOIN sites s ON w.site_id = s.id
JOIN system_users su ON w.owner_id = su.id
$siteFilter
ORDER BY w.data_size DESC NULLS LAST
LIMIT $TopN;
"@

    ProjectSummary = @"
SELECT p.name AS project_name, s.name AS site_name,
       COUNT(w.id) AS workbook_count,
       pg_size_pretty(SUM(w.data_size)) AS total_size,
       SUM(w.data_size) AS size_bytes
FROM projects p
JOIN sites s ON p.site_id = s.id
LEFT JOIN workbooks w ON w.project_id = p.id
$siteFilter
GROUP BY p.name, s.name
ORDER BY SUM(w.data_size) DESC NULLS LAST
LIMIT $TopN;
"@
}

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
Write-Host "`n=== Tableau Storage Analysis ===" -ForegroundColor Cyan
Write-Host "Server: $TableauServer | Site: $(if ($SiteName) { $SiteName } else { 'All' })`n"

try {
    foreach ($queryName in $queries.Keys) {
        Write-Host "Running: $queryName ... " -NoNewline
        $env:PGPASSWORD = $RepositoryPassword
        $result = & psql -h $TableauServer -p $RepoPort -U $RepoUser -d $RepoDatabase `
            -t -A -F ',' -c $queries[$queryName] 2>&1
        $env:PGPASSWORD = $null

        $outFile = Join-Path $OutputPath "Tableau-Storage-$queryName-$timestamp.csv"
        $result | Out-File $outFile
        Write-Host "Done → $outFile" -ForegroundColor Green
    }
    Write-Host "`nAnalysis complete. Review CSV files for storage optimisation candidates." -ForegroundColor Cyan
}
catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}
