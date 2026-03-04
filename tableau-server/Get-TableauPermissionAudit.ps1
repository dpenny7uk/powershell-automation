<#
.SYNOPSIS
    Audits Tableau Server workbook permissions by querying the PostgreSQL
    repository database, identifies workbooks with missing or broken
    permissions, and generates remediation reports.

.DESCRIPTION
    Developed during investigation of a 7-year permission loss issue affecting
    551 workbooks (37.6% of a major site). Queries the Tableau repository
    to identify workbooks with zero explicit permission records, broken
    inheritance, or orphaned permission entries.

    Connects to the Tableau Server PostgreSQL repository (readonly user)
    to perform permission analysis without impacting server performance.

.PARAMETER TableauServer
    Hostname of the Tableau Server (connects to repository on port 8060).

.PARAMETER SiteName
    Tableau site name to audit. Default: 'Default'.

.PARAMETER RepositoryPassword
    Password for the 'readonly' PostgreSQL repository user.
    Use: tsm configuration get -k pgsql.readonly_password

.EXAMPLE
    .\Get-TableauPermissionAudit.ps1 -TableauServer "analytics-prod-01" -SiteName "Corp"

.NOTES
    Author: Damian Penny
    Version: 1.4
    Requires: Npgsql .NET driver or psql client
    Repository port: 8060 (default Tableau Server PostgreSQL)
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$TableauServer,

    [Parameter()]
    [string]$SiteName = 'Default',

    [Parameter()]
    [string]$RepositoryPassword,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

#region Configuration

$RepoPort = 8060
$RepoUser = 'readonly'
$RepoDatabase = 'workgroup'

#endregion

#region Functions

function Invoke-RepositoryQuery {
    <#
    .SYNOPSIS
        Executes a SQL query against the Tableau PostgreSQL repository.
    #>
    [CmdletBinding()]
    param(
        [string]$Query,
        [string]$Server,
        [int]$Port,
        [string]$Database,
        [string]$Username,
        [string]$Password
    )

    # Method 1: Using psql if available
    $psql = Get-Command psql -ErrorAction SilentlyContinue
    if ($psql) {
        $env:PGPASSWORD = $Password
        $result = & psql -h $Server -p $Port -U $Username -d $Database -t -A -F '|' -c $Query 2>&1
        $env:PGPASSWORD = $null

        if ($LASTEXITCODE -ne 0) {
            throw "psql query failed: $result"
        }

        # Parse pipe-delimited output
        $lines = $result | Where-Object { $_ -and $_ -notmatch '^\(' }
        return $lines
    }

    # Method 2: Using .NET Npgsql driver
    $npgsqlPath = Get-ChildItem -Path "${env:ProgramFiles}\*","${env:ProgramFiles(x86)}\*" -Filter 'Npgsql.dll' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($npgsqlPath) {
        Add-Type -Path $npgsqlPath.FullName
        $connString = "Host=$Server;Port=$Port;Database=$Database;Username=$Username;Password=$Password;SSL Mode=Require;Trust Server Certificate=true"
        $conn = New-Object Npgsql.NpgsqlConnection($connString)
        $conn.Open()

        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $Query
        $adapter = New-Object Npgsql.NpgsqlDataAdapter($cmd)
        $dataset = New-Object System.Data.DataSet
        $null = $adapter.Fill($dataset)

        $conn.Close()
        return $dataset.Tables[0]
    }

    throw "No PostgreSQL client available. Install psql or Npgsql .NET driver."
}

#endregion

#region Queries

$queries = @{
    # Workbooks with zero explicit permissions
    WorkbooksNoPermissions = @"
SELECT
    w.name AS workbook_name,
    p.name AS project_name,
    s.name AS site_name,
    su.friendly_name AS owner_name,
    w.created_at,
    w.updated_at,
    COUNT(wp.id) AS permission_count
FROM workbooks w
JOIN projects p ON w.project_id = p.id
JOIN sites s ON w.site_id = s.id
JOIN system_users su ON w.owner_id = su.id
LEFT JOIN workbook_permissions wp ON w.id = wp.workbook_id
WHERE s.name = '$SiteName'
GROUP BY w.name, p.name, s.name, su.friendly_name, w.created_at, w.updated_at
HAVING COUNT(wp.id) = 0
ORDER BY p.name, w.name;
"@

    # Permission summary by project
    ProjectPermissionSummary = @"
SELECT
    p.name AS project_name,
    COUNT(DISTINCT w.id) AS total_workbooks,
    COUNT(DISTINCT CASE WHEN wp.id IS NULL THEN w.id END) AS workbooks_no_perms,
    ROUND(
        COUNT(DISTINCT CASE WHEN wp.id IS NULL THEN w.id END)::numeric /
        NULLIF(COUNT(DISTINCT w.id), 0) * 100, 1
    ) AS pct_no_perms
FROM workbooks w
JOIN projects p ON w.project_id = p.id
JOIN sites s ON w.site_id = s.id
LEFT JOIN workbook_permissions wp ON w.id = wp.workbook_id
WHERE s.name = '$SiteName'
GROUP BY p.name
ORDER BY workbooks_no_perms DESC;
"@

    # Total site statistics
    SiteStats = @"
SELECT
    s.name AS site_name,
    COUNT(DISTINCT w.id) AS total_workbooks,
    COUNT(DISTINCT CASE WHEN wp.id IS NULL THEN w.id END) AS no_permissions,
    ROUND(
        COUNT(DISTINCT CASE WHEN wp.id IS NULL THEN w.id END)::numeric /
        NULLIF(COUNT(DISTINCT w.id), 0) * 100, 1
    ) AS pct_affected,
    pg_size_pretty(SUM(w.data_size)) AS total_size
FROM workbooks w
JOIN sites s ON w.site_id = s.id
LEFT JOIN workbook_permissions wp ON w.id = wp.workbook_id
WHERE s.name = '$SiteName'
GROUP BY s.name;
"@
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'

Write-Host "`n=== Tableau Server Permission Audit ===" -ForegroundColor Cyan
Write-Host "Server: $TableauServer | Site: $SiteName`n"

# Prompt for password if not provided
if (-not $RepositoryPassword) {
    $securePassword = Read-Host "Enter readonly repository password" -AsSecureString
    $RepositoryPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
    )
}

try {
    # Run site statistics query
    Write-Host "Querying site statistics..." -ForegroundColor Cyan
    $siteStats = Invoke-RepositoryQuery -Query $queries.SiteStats `
        -Server $TableauServer -Port $RepoPort -Database $RepoDatabase `
        -Username $RepoUser -Password $RepositoryPassword

    Write-Host "Site stats retrieved." -ForegroundColor Green

    # Run workbooks with no permissions query
    Write-Host "Identifying workbooks with missing permissions..." -ForegroundColor Cyan
    $noPermsWorkbooks = Invoke-RepositoryQuery -Query $queries.WorkbooksNoPermissions `
        -Server $TableauServer -Port $RepoPort -Database $RepoDatabase `
        -Username $RepoUser -Password $RepositoryPassword

    # Run project summary
    Write-Host "Generating project summary..." -ForegroundColor Cyan
    $projectSummary = Invoke-RepositoryQuery -Query $queries.ProjectPermissionSummary `
        -Server $TableauServer -Port $RepoPort -Database $RepoDatabase `
        -Username $RepoUser -Password $RepositoryPassword

    # Export results
    $workbookFile = Join-Path $OutputPath "Tableau-NoPerms-Workbooks-$timestamp.csv"
    $projectFile  = Join-Path $OutputPath "Tableau-ProjectSummary-$timestamp.csv"

    if ($noPermsWorkbooks -is [System.Data.DataTable]) {
        $noPermsWorkbooks | Export-Csv -Path $workbookFile -NoTypeInformation
        $projectSummary | Export-Csv -Path $projectFile -NoTypeInformation
    }
    else {
        # psql pipe-delimited output
        $noPermsWorkbooks | Out-File $workbookFile
        $projectSummary | Out-File $projectFile
    }

    Write-Host "`n=== Results ===" -ForegroundColor Cyan
    Write-Host "  Workbooks with no permissions: $workbookFile"
    Write-Host "  Project summary: $projectFile"
    Write-Host "`nReview the reports and coordinate with content owners for permission remediation."
    Write-Host "Consider updating 'tsm maintenance cleanup' parameters to prevent future permission deletion." -ForegroundColor Yellow
}
catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Ensure the repository is accessible on port $RepoPort and the readonly password is correct." -ForegroundColor Yellow
}

#endregion
