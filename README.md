# PowerShell Automation

Infrastructure automation scripts for enterprise Windows Server environments. Covers security vulnerability auditing, SSL certificate monitoring, license compliance, multi-server deployments, and application health checks.

All server names, IPs, domains, and application names have been sanitised. These scripts are based on real production tooling — adapt the configuration variables at the top of each script for your environment.

## Structure

```
├── security/
│   ├── Invoke-DotNetVulnerabilityAudit.ps1    # Scan servers for vulnerable .NET versions
│   └── Invoke-JavaVulnerabilityAudit.ps1      # Scan servers for vulnerable Java/JRE installs
├── monitoring/
│   ├── Get-SSLCertificateExpiry.ps1           # Proactive SSL certificate expiry monitoring
│   ├── Get-DiskSpaceReport.ps1                # Disk space monitoring with threshold alerting
│   ├── Get-ServiceAccountHealth.ps1           # Service account temp folder monitoring
│   └── Get-FCRMLicenseStatus.ps1              # Application license file expiry monitoring
├── deployment/
│   └── Invoke-MultiServerDotNetUpdate.ps1     # Multi-server .NET runtime deployment
├── license-management/
│   └── Compare-AtlassianUsersToAD.ps1         # Atlassian license audit against AD groups
├── infrastructure/
│   ├── Get-AzureNetworkInterface.ps1          # Cross-subscription Azure NIC search
│   └── Invoke-HostnameResolution.ps1          # Bulk hostname/IP resolution
├── tableau-server/
│   ├── Get-TableauPermissionAudit.ps1         # Audit workbook permissions via repository
│   └── Get-TableauStorageAnalysis.ps1         # Storage usage analysis by site/project
└── common/
    └── Write-HTMLReport.ps1                   # Shared HTML email report generator
```

## Prerequisites

- PowerShell 5.1+ (Windows) or PowerShell 7+ (cross-platform)
- Active Directory module (`RSAT-AD-PowerShell`) for AD-related scripts
- Az PowerShell modules for Azure scripts: `Install-Module Az -Scope CurrentUser`
- Remote server access (WinRM enabled) for multi-server scripts
- Appropriate AD group membership or service account credentials

## Usage

Each script is self-contained with configuration variables at the top. Example:

```powershell
# Run vulnerability audit against a list of servers
.\security\Invoke-DotNetVulnerabilityAudit.ps1 -ServerList "servers.txt" -OutputPath "C:\Reports"

# Check SSL certificates expiring within 30 days
.\monitoring\Get-SSLCertificateExpiry.ps1 -ThresholdDays 30

# Compare Atlassian users against AD groups and generate removal report
.\license-management\Compare-AtlassianUsersToAD.ps1 -WhatIf
```
