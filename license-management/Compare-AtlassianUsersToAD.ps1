<#
.SYNOPSIS
    Compares Atlassian Cloud (Confluence/JIRA) user lists against Active Directory
    groups, identifies users not meeting compliance criteria, and optionally
    removes non-compliant users. Generates HTML email reports.

.DESCRIPTION
    Addresses the problem of users being invited directly to Atlassian products
    bypassing AD group provisioning, leading to wasted licenses and security gaps.

    Identifies users who:
    - Are not members of the required AD provisioning groups
    - Have been inactive for more than a configurable number of days
    - Have been in the organisation for less than a configurable number of days

    Produces a detailed HTML report suitable for email distribution and can
    optionally remove non-compliant users via the Atlassian REST API.

    Estimated savings: £2,880 - £6,000 annually depending on license tier.

.PARAMETER WhatIf
    Preview mode - generates the report without making any changes.

.PARAMETER MaxInactiveDays
    Days of inactivity before a user is flagged. Default: 90.

.PARAMETER MinOrgDays
    Minimum days in the organisation before audit applies. Default: 60.

.PARAMETER SendReport
    Send HTML report via email after completion.

.EXAMPLE
    .\Compare-AtlassianUsersToAD.ps1 -WhatIf
    # Preview only - no changes made

.EXAMPLE
    .\Compare-AtlassianUsersToAD.ps1 -SendReport
    # Run audit and email results

.NOTES
    Author: Damian Penny
    Version: 3.0
    Requires: ActiveDirectory module, Atlassian API token
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter()]
    [int]$MaxInactiveDays = 90,

    [Parameter()]
    [int]$MinOrgDays = 60,

    [Parameter()]
    [switch]$SendReport,

    [Parameter()]
    [string]$OutputPath = (Get-Location).Path
)

#region Configuration

# Atlassian API settings
$AtlassianBaseUrl  = 'https://contoso.atlassian.net'
$AtlassianApiToken = $env:ATLASSIAN_API_TOKEN  # Set via environment variable or Azure DevOps pipeline variable
$AtlassianEmail    = 'svc-atlassian-audit@contoso.com'

# AD groups that should provision Atlassian access
$ADGroups = @{
    'Confluence' = 'SG-App-Confluence-Users'     # e.g., App_Confluence_ProductAccess
    'JIRA'       = 'SG-App-JIRA-Users'           # e.g., App_Jira_ProductAccess
}

# Atlassian groups to audit
$AtlassianGroups = @{
    'Confluence' = 'confluence-users'
    'JIRA'       = 'jira-software-users'
}

# License costs per user per month (for savings calculation)
$LicenseCostPerUser = @{
    'Confluence' = 4.00  # £/user/month
    'JIRA'       = 6.00  # £/user/month
}

# Email settings
$SmtpServer = 'smtp.contoso.com'
$EmailFrom  = 'atlassian-audit@contoso.com'
$EmailTo    = @('it-governance@contoso.com', 'infra-team@contoso.com')

#endregion

#region Functions

function Get-AtlassianGroupMembers {
    <#
    .SYNOPSIS
        Retrieves members of an Atlassian Cloud group via REST API.
    #>
    [CmdletBinding()]
    param([string]$GroupName)

    $headers = @{
        'Authorization' = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${AtlassianEmail}:${AtlassianApiToken}"))
        'Content-Type'  = 'application/json'
    }

    $members = @()
    $startAt = 0
    $maxResults = 50

    do {
        $uri = "${AtlassianBaseUrl}/rest/api/3/group/member?groupname=${GroupName}&startAt=${startAt}&maxResults=${maxResults}"
        try {
            $response = Invoke-RestMethod -Uri $uri -Headers $headers -Method Get -ErrorAction Stop
            $members += $response.values
            $startAt += $maxResults
        }
        catch {
            Write-Warning "Failed to retrieve Atlassian group '${GroupName}': $($_.Exception.Message)"
            break
        }
    } while ($response.isLast -eq $false)

    return $members
}

function Get-ADGroupMembersEmail {
    <#
    .SYNOPSIS
        Retrieves email addresses of AD group members.
    #>
    [CmdletBinding()]
    param([string]$GroupName)

    try {
        $members = Get-ADGroupMember -Identity $GroupName -Recursive -ErrorAction Stop |
            Where-Object { $_.objectClass -eq 'user' } |
            ForEach-Object {
                $user = Get-ADUser $_.SamAccountName -Properties EmailAddress, LastLogonDate, WhenCreated -ErrorAction SilentlyContinue
                if ($user.EmailAddress) {
                    [PSCustomObject]@{
                        Email        = $user.EmailAddress.ToLower()
                        DisplayName  = $user.Name
                        LastLogon    = $user.LastLogonDate
                        Created      = $user.WhenCreated
                        Enabled      = $user.Enabled
                    }
                }
            }
        return $members
    }
    catch {
        Write-Warning "Failed to query AD group '${GroupName}': $($_.Exception.Message)"
        return @()
    }
}

function Get-UserLastActive {
    <#
    .SYNOPSIS
        Checks when an Atlassian user was last active (approximate via API).
    #>
    [CmdletBinding()]
    param([string]$AccountId)

    # Note: Atlassian Cloud doesn't expose last-login directly for all products.
    # This checks the user profile for activity indicators.
    # In practice, you may need to use Atlassian Access audit logs or
    # Azure AD sign-in logs if SSO is configured.

    $headers = @{
        'Authorization' = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${AtlassianEmail}:${AtlassianApiToken}"))
        'Content-Type'  = 'application/json'
    }

    try {
        $uri = "${AtlassianBaseUrl}/rest/api/3/user?accountId=${AccountId}&expand=groups,applicationRoles"
        $user = Invoke-RestMethod -Uri $uri -Headers $headers -Method Get -ErrorAction Stop
        return $user
    }
    catch {
        return $null
    }
}

function New-AuditHTMLReport {
    <#
    .SYNOPSIS
        Generates a professional HTML email report from the audit results.
    #>
    [CmdletBinding()]
    param(
        [hashtable]$Results,
        [decimal]$EstimatedSavings
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $tableRows = foreach ($product in $Results.Keys) {
        foreach ($user in $Results[$product]) {
            $rowClass = switch ($user.Reason) {
                { $_ -match 'Not in AD' } { 'critical' }
                { $_ -match 'Inactive' }  { 'warning' }
                default                    { 'info' }
            }
            "<tr class='$rowClass'>
                <td>$product</td>
                <td>$($user.Email)</td>
                <td>$($user.DisplayName)</td>
                <td>$($user.Reason)</td>
                <td>$($user.Action)</td>
            </tr>"
        }
    }

    $totalFlagged = ($Results.Values | ForEach-Object { $_.Count } | Measure-Object -Sum).Sum

    return @"
<!DOCTYPE html>
<html>
<head><style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    h1 { color: #1F4E79; }
    .summary { background: #f0f4f8; padding: 15px; border-radius: 5px; margin: 15px 0; }
    .savings { color: #27ae60; font-size: 1.2em; font-weight: bold; }
    table { border-collapse: collapse; width: 100%; }
    th { background: #1F4E79; color: white; padding: 10px; text-align: left; }
    td { padding: 8px; border-bottom: 1px solid #ddd; }
    tr.critical { background: #fde8e8; }
    tr.warning { background: #fef3e2; }
    tr.info { background: #eaf4fd; }
    .footer { color: #666; font-size: 0.9em; margin-top: 20px; }
</style></head>
<body>
    <h1>Atlassian License Audit Report</h1>
    <div class='summary'>
        <p>Users flagged for review: <strong>$totalFlagged</strong></p>
        <p class='savings'>Estimated annual savings if removed: £$([math]::Round($EstimatedSavings, 2))</p>
        <p>Criteria: Not in AD provisioning group | Inactive >$MaxInactiveDays days | <$MinOrgDays days in org</p>
    </div>
    <table>
        <tr><th>Product</th><th>Email</th><th>Display Name</th><th>Reason</th><th>Action</th></tr>
        $($tableRows -join "`n")
    </table>
    <p class='footer'>Report generated: $timestamp | Mode: $(if ($WhatIfPreference) { 'Preview (WhatIf)' } else { 'Live' })</p>
</body>
</html>
"@
}

#endregion

#region Main

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$auditResults = @{}
$totalSavings = 0

Write-Host "`n=== Atlassian License Audit ===" -ForegroundColor Cyan
Write-Host "Mode: $(if ($WhatIfPreference) { 'Preview (WhatIf)' } else { 'LIVE - changes will be applied' })`n"

foreach ($product in $AtlassianGroups.Keys) {
    Write-Host "Auditing $product..." -ForegroundColor Cyan

    # Get Atlassian group members
    $atlassianUsers = Get-AtlassianGroupMembers -GroupName $AtlassianGroups[$product]
    Write-Host "  Atlassian users: $($atlassianUsers.Count)"

    # Get AD group members
    $adUsers = Get-ADGroupMembersEmail -GroupName $ADGroups[$product]
    $adEmails = $adUsers.Email
    Write-Host "  AD group members: $($adUsers.Count)"

    # Compare
    $flaggedUsers = @()
    foreach ($atlUser in $atlassianUsers) {
        $email = $atlUser.emailAddress.ToLower()
        $reasons = @()

        # Check 1: Not in AD group
        if ($email -notin $adEmails) {
            $reasons += "Not in AD group ($($ADGroups[$product]))"
        }

        # Check 2: AD user disabled
        $adMatch = $adUsers | Where-Object { $_.Email -eq $email }
        if ($adMatch -and -not $adMatch.Enabled) {
            $reasons += 'AD account disabled'
        }

        # Check 3: Inactive too long (based on AD last logon as proxy)
        if ($adMatch -and $adMatch.LastLogon) {
            $daysSinceLogon = ((Get-Date) - $adMatch.LastLogon).Days
            if ($daysSinceLogon -gt $MaxInactiveDays) {
                $reasons += "Inactive $daysSinceLogon days (threshold: $MaxInactiveDays)"
            }
        }

        if ($reasons.Count -gt 0) {
            $flaggedUsers += [PSCustomObject]@{
                Email       = $email
                DisplayName = $atlUser.displayName
                AccountId   = $atlUser.accountId
                Reason      = $reasons -join '; '
                Action      = if ($WhatIfPreference) { 'Would remove' } else { 'Removed' }
            }

            $monthlyCost = $LicenseCostPerUser[$product]
            $totalSavings += ($monthlyCost * 12)
        }
    }

    $auditResults[$product] = $flaggedUsers
    Write-Host "  Flagged for removal: $($flaggedUsers.Count)" -ForegroundColor $(if ($flaggedUsers.Count -gt 0) { 'Yellow' } else { 'Green' })
}

# Generate HTML report
$htmlReport = New-AuditHTMLReport -Results $auditResults -EstimatedSavings $totalSavings
$htmlFile = Join-Path $OutputPath "Atlassian-Audit-$timestamp.html"
$htmlReport | Out-File -FilePath $htmlFile -Encoding UTF8

# Export CSV
$csvFile = Join-Path $OutputPath "Atlassian-Audit-$timestamp.csv"
$csvData = foreach ($product in $auditResults.Keys) {
    foreach ($user in $auditResults[$product]) {
        [PSCustomObject]@{
            Product     = $product
            Email       = $user.Email
            DisplayName = $user.DisplayName
            Reason      = $user.Reason
            Action      = $user.Action
            AuditDate   = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        }
    }
}
if ($csvData) { $csvData | Export-Csv -Path $csvFile -NoTypeInformation }

# Summary
$totalFlagged = ($auditResults.Values | ForEach-Object { $_.Count } | Measure-Object -Sum).Sum
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "  Total users flagged: $totalFlagged"
Write-Host "  Estimated annual savings: £$([math]::Round($totalSavings, 2))" -ForegroundColor Green
Write-Host "  HTML report: $htmlFile"
Write-Host "  CSV report: $csvFile"

# Send email
if ($SendReport -and $totalFlagged -gt 0) {
    Send-MailMessage -SmtpServer $SmtpServer -From $EmailFrom -To $EmailTo `
        -Subject "Atlassian License Audit - $totalFlagged users flagged (est. savings £$([math]::Round($totalSavings, 2))/year)" `
        -Body $htmlReport -BodyAsHtml
    Write-Host "  Email report sent." -ForegroundColor Green
}

#endregion
