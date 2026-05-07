#requires -Version 5.1
<#
.SYNOPSIS
    Enumerates ADF/Synapse linked services, datasets, triggers, pipeline runs and
    activity-run-level usage. Identifies which use SHIR + which ODBC drivers.

.DESCRIPTION
    Run by the analyst against their own Azure tenant (NOT for hand-off to a third
    party — the running identity needs Reader access on every subscription that
    contains a relevant ADF or Synapse workspace).

    Outputs (CSVs prefixed with timestamp in $OutDir):
      *-linked-services.csv         every LS across every ADF/Synapse with inferred
                                    driver, IR name + IR kind, connection-string
                                    type (PlainString | SecureString | KeyVaultRef)
      *-shir-linked-services.csv    filtered to LSes routed via a SelfHosted IR
      *-datasets.csv                datasets and which LS they reference
      *-triggers.csv                trigger inventory: schedule + pipelines + state
                                    (proves FUTURE scheduled use)
      *-pipeline-runs-Nd.csv        pipeline run history (last N days)
      *-activity-runs-Md.csv        activity-level runs for the last M days
                                    (proves PAST use at activity granularity)
      *-driver-summary.csv          pivot: driver -> LS counts / IRs / containers
      manifest.json                 hashes of all outputs (tamper-evident)

.PARAMETER RecentRunDays
    Pipeline-run lookback window. Default 30.

.PARAMETER ActivityRunDays
    Activity-run lookback window. Default 7 (smaller because it's per-pipeline-run
    and produces a lot of API calls). Set 0 to skip activity-level capture.

.PARAMETER OutDir
    Default C:\Dev\gv228-evidence.

.PARAMETER SubscriptionFilter
    Optional list of subscription names or IDs to limit scope.

.NOTES
    Requirements:
      Install-Module Az.Accounts, Az.DataFactory, Az.Synapse -Scope CurrentUser
#>

[CmdletBinding()]
param(
    [int]$RecentRunDays = 30,
    [int]$ActivityRunDays = 7,
    [string]$OutDir = "C:\Dev\gv228-evidence",
    [string[]]$SubscriptionFilter = @()
)

$ErrorActionPreference = 'Continue'
$ScriptVersion = '2.1.0'
if (-not (Test-Path -LiteralPath $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
$workDir = Join-Path $OutDir "adf-evidence-$ts"
New-Item -ItemType Directory -Path $workDir -Force | Out-Null

# Linked-service .NET type -> Microsoft ODBC driver bundled with SHIR
$typeToDriver = @{
    'OdbcLinkedService'                     = '<see ConnectionString Driver={...}>'
    'GoogleBigQueryLinkedService'           = 'Microsoft Google BigQuery ODBC Driver'
    'GoogleBigQueryV2LinkedService'         = 'Microsoft Google BigQuery ODBC Driver'
    'GoogleAdWordsLinkedService'            = 'Microsoft Google Ads ODBC Driver'
    'HiveLinkedService'                     = 'Microsoft Hive ODBC Driver'
    'HBaseLinkedService'                    = 'Microsoft HBase ODBC Driver'
    'SparkLinkedService'                    = 'Microsoft Spark ODBC Driver'
    'ImpalaLinkedService'                   = 'Microsoft Impala ODBC Driver'
    'PrestoLinkedService'                   = 'Microsoft Presto ODBC Driver'
    'PhoenixLinkedService'                  = 'Microsoft Phoenix ODBC Driver'
    'ServiceNowLinkedService'               = 'Microsoft ServiceNow ODBC Driver'
    'ServiceNowV2LinkedService'             = 'Microsoft ServiceNow ODBC Driver'
    'SalesforceLinkedService'               = 'Microsoft Salesforce ODBC Driver'
    'SalesforceV2LinkedService'             = 'Microsoft Salesforce ODBC Driver'
    'SalesforceMarketingCloudLinkedService' = 'Microsoft Salesforce Marketing Cloud ODBC Driver'
    'SalesforceServiceCloudLinkedService'   = 'Microsoft Salesforce ODBC Driver'
    'OracleServiceCloudLinkedService'       = 'Microsoft Oracle Service Cloud ODBC Driver'
    'EloquaLinkedService'                   = 'Microsoft Eloqua ODBC Driver'
    'ConcurLinkedService'                   = 'Microsoft Concur ODBC Driver'
    'CouchbaseLinkedService'                = 'Microsoft Couchbase ODBC Driver'
    'JiraLinkedService'                     = 'Microsoft Jira ODBC Driver'
    'HubspotLinkedService'                  = 'Microsoft Hubspot ODBC Driver'
    'MarketoLinkedService'                  = 'Microsoft Marketo ODBC Driver'
    'MagentoLinkedService'                  = 'Microsoft Magento ODBC Driver'
    'PaypalLinkedService'                   = 'Microsoft PayPal ODBC Driver'
    'QuickBooksLinkedService'               = 'Microsoft Quickbooks ODBC Driver'
    'ResponsysLinkedService'                = 'Microsoft Responsys ODBC Driver'
    'ShopifyLinkedService'                  = 'Microsoft Shopify ODBC Driver'
    'SquareLinkedService'                   = 'Microsoft Square ODBC Driver'
    'XeroLinkedService'                     = 'Microsoft Xero ODBC Driver'
    'ZohoLinkedService'                     = 'Microsoft Zoho ODBC Driver'
    'AmazonRedshiftLinkedService'           = 'Microsoft Amazon Redshift ODBC Driver'
}

function Hide-Secrets { param([string]$ConnectionString)
    if ([string]::IsNullOrWhiteSpace($ConnectionString)) { return '' }
    $secretKey = '^\s*(PWD|PASSWORD|PASSWD|TOKEN|APIKEY|API_KEY|SECRET|CREDENTIAL|AUTHTOKEN|PRIVATEKEY)\s*='
    ($ConnectionString -split ';' | ForEach-Object {
        if ($_ -match $secretKey) { ($_ -split '=',2)[0] + '=<redacted>' } else { $_ }
    }) -join ';'
}

# (C) Connection-string type detection: handles plain strings, SecureString objects,
# and AzureKeyVaultSecretReference objects. Returns a structured record so KV refs
# don't silently disappear.
function Get-ConnectionInfo { param($Properties)
    $result = [ordered]@{ Type='None'; PlainString=$null; KvStore=$null; KvSecretName=$null; KvSecretVersion=$null }
    if (-not $Properties) { return $result }
    if (-not $Properties.PSObject.Properties['ConnectionString']) { return $result }
    $cs = $Properties.ConnectionString
    if ($null -eq $cs) { return $result }
    if ($cs -is [string]) { $result.Type='PlainString'; $result.PlainString=$cs; return $result }
    if ($cs.PSObject.Properties['SecretName']) {
        $result.Type           = 'KeyVaultReference'
        $result.KvStore        = $cs.Store.ReferenceName
        $result.KvSecretName   = $cs.SecretName
        $result.KvSecretVersion= $cs.SecretVersion
        return $result
    }
    if ($cs.PSObject.Properties['Value']) {
        $val = $cs.Value
        if ($val -is [string]) { $result.Type='SecureString'; $result.PlainString=$val; return $result }
    }
    $result.Type = 'Unknown'
    return $result
}

function Get-DriverFromConnectionString { param([string]$ConnectionString)
    if ([string]::IsNullOrWhiteSpace($ConnectionString)) { return $null }
    if ($ConnectionString -match 'Driver\s*=\s*\{?([^;}]+?)\}?\s*(?:;|$)') { return $matches[1].Trim() }
    return $null
}

# --- Auth ---
if (-not (Get-AzContext -ErrorAction SilentlyContinue)) {
    Write-Host "Not signed in to Azure — running Connect-AzAccount..." -ForegroundColor Yellow
    Connect-AzAccount | Out-Null
}

$linkedServiceRows = New-Object System.Collections.Generic.List[object]
$datasetRows       = New-Object System.Collections.Generic.List[object]
$triggerRows       = New-Object System.Collections.Generic.List[object]
$pipelineRunRows   = New-Object System.Collections.Generic.List[object]
$activityRunRows   = New-Object System.Collections.Generic.List[object]
$irKindLookup      = @{}

$subs = Get-AzSubscription
if ($SubscriptionFilter) { $subs = $subs | Where-Object { $_.Name -in $SubscriptionFilter -or $_.Id -in $SubscriptionFilter } }

foreach ($sub in $subs) {
    Set-AzContext -Subscription $sub.Id -ErrorAction SilentlyContinue | Out-Null
    Write-Host "=== Subscription: $($sub.Name) ===" -ForegroundColor Cyan

    # ---------- Data Factories ----------
    $dataFactories = @()
    try { $dataFactories = Get-AzDataFactoryV2 -ErrorAction Stop } catch { Write-Warning "  Get-AzDataFactoryV2: $_" }

    foreach ($df in $dataFactories) {
        Write-Host "  ADF: $($df.DataFactoryName)" -ForegroundColor Yellow

        # IR kinds (used to tag linked services)
        try {
            foreach ($ir in (Get-AzDataFactoryV2IntegrationRuntime -DataFactoryName $df.DataFactoryName -ResourceGroupName $df.ResourceGroupName -ErrorAction Stop)) {
                $irKindLookup["$($df.DataFactoryName)|$($ir.Name)"] = $ir.Type
            }
        } catch {}

        # Linked services
        try {
            $lses = Get-AzDataFactoryV2LinkedService -DataFactoryName $df.DataFactoryName -ResourceGroupName $df.ResourceGroupName -ErrorAction Stop
        } catch { Write-Warning "    LinkedServices: $_"; $lses = @() }
        foreach ($ls in $lses) {
            $type    = $ls.Properties.GetType().Name
            $connInfo= Get-ConnectionInfo -Properties $ls.Properties
            $driver  = $typeToDriver[$type]
            if ($type -eq 'OdbcLinkedService' -and $connInfo.PlainString) {
                $parsed = Get-DriverFromConnectionString -ConnectionString $connInfo.PlainString
                if ($parsed) { $driver = $parsed }
            }
            $ir = if ($ls.Properties.ConnectVia) { $ls.Properties.ConnectVia.ReferenceName } else { $null }
            $linkedServiceRows.Add([PSCustomObject]@{
                Subscription            = $sub.Name
                ResourceGroup           = $df.ResourceGroupName
                Container               = $df.DataFactoryName
                ContainerKind           = 'ADF'
                LinkedServiceName       = $ls.Name
                LinkedServiceType       = $type
                IntegrationRuntime      = $ir
                IntegrationRuntimeKind  = $irKindLookup["$($df.DataFactoryName)|$ir"]
                DriverInferred          = $driver
                ConnectionStringType    = $connInfo.Type
                KvStore                 = $connInfo.KvStore
                KvSecretName            = $connInfo.KvSecretName
                KvSecretVersion         = $connInfo.KvSecretVersion
                ConnectionStringRedacted= Hide-Secrets -ConnectionString $connInfo.PlainString
            })
        }

        # Datasets
        try {
            foreach ($ds in (Get-AzDataFactoryV2Dataset -DataFactoryName $df.DataFactoryName -ResourceGroupName $df.ResourceGroupName -ErrorAction Stop)) {
                $linkedTo = if ($ds.Properties.LinkedServiceName) { $ds.Properties.LinkedServiceName.ReferenceName } else { $null }
                $datasetRows.Add([PSCustomObject]@{
                    Container=$df.DataFactoryName; ContainerKind='ADF'
                    DatasetName=$ds.Name; DatasetType=$ds.Properties.GetType().Name
                    LinkedServiceName=$linkedTo
                })
            }
        } catch {}

        # (B) Triggers — proves scheduled future use
        try {
            foreach ($t in (Get-AzDataFactoryV2Trigger -DataFactoryName $df.DataFactoryName -ResourceGroupName $df.ResourceGroupName -ErrorAction Stop)) {
                $tProps = $t.Properties
                $triggerType = $tProps.GetType().Name
                $recurrence = $null; $interval = $null; $frequency = $null
                if ($tProps.PSObject.Properties['Recurrence']) {
                    $recurrence = $tProps.Recurrence.StartTime
                    $frequency  = $tProps.Recurrence.Frequency
                    $interval   = $tProps.Recurrence.Interval
                }
                $pipelines = @()
                if ($tProps.PSObject.Properties['Pipelines']) {
                    foreach ($p in $tProps.Pipelines) { $pipelines += $p.PipelineReference.ReferenceName }
                } elseif ($tProps.PSObject.Properties['PipelineProperty']) {
                    $pipelines += $tProps.PipelineProperty.PipelineReference.ReferenceName
                }
                $triggerRows.Add([PSCustomObject]@{
                    Container=$df.DataFactoryName; ContainerKind='ADF'
                    TriggerName=$t.Name; TriggerType=$triggerType; RuntimeState=$tProps.RuntimeState
                    Frequency=$frequency; Interval=$interval; StartTime=$recurrence
                    Pipelines=($pipelines -join '; ')
                })
            }
        } catch { Write-Warning "    Triggers: $_" }

        # Pipeline runs (last N days)
        $pipelineRunsForActivity = @()
        try {
            $runs = Get-AzDataFactoryV2PipelineRun `
                -DataFactoryName $df.DataFactoryName -ResourceGroupName $df.ResourceGroupName `
                -LastUpdatedAfter ((Get-Date).AddDays(-$RecentRunDays)) `
                -LastUpdatedBefore (Get-Date) -ErrorAction Stop
            foreach ($r in $runs) {
                $pipelineRunRows.Add([PSCustomObject]@{
                    Container=$df.DataFactoryName; ContainerKind='ADF'
                    PipelineName=$r.PipelineName; RunId=$r.RunId
                    RunStart=$r.RunStart; RunEnd=$r.RunEnd
                    Status=$r.Status; DurationMs=$r.DurationInMs
                })
                if ($ActivityRunDays -gt 0 -and $r.RunStart -ge (Get-Date).AddDays(-$ActivityRunDays)) {
                    $pipelineRunsForActivity += $r
                }
            }
        } catch { Write-Warning "    Pipeline runs: $_" }

        # (A) Activity runs (last $ActivityRunDays days) — drill into linked-service usage
        if ($ActivityRunDays -gt 0 -and $pipelineRunsForActivity.Count -gt 0) {
            Write-Host "    Capturing activity runs for $($pipelineRunsForActivity.Count) recent pipeline runs..." -ForegroundColor DarkGray
            foreach ($pr in $pipelineRunsForActivity) {
                try {
                    $acts = Get-AzDataFactoryV2ActivityRun `
                        -DataFactoryName $df.DataFactoryName -ResourceGroupName $df.ResourceGroupName `
                        -PipelineRunId $pr.RunId `
                        -RunStartedAfter $pr.RunStart `
                        -RunStartedBefore $(if ($pr.RunEnd) { $pr.RunEnd } else { Get-Date }) -ErrorAction Stop
                    foreach ($a in $acts) {
                        # Extract linked-service hints from Input/Output where possible
                        $lsHints = @()
                        foreach ($field in 'Input','Output') {
                            $v = $a.$field
                            if ($v) {
                                $vJson = if ($v -is [string]) { $v } else { ($v | ConvertTo-Json -Depth 4 -Compress) }
                                $matchesLs = [regex]::Matches($vJson, '"(?:linkedServiceName|referenceName)"\s*:\s*"([^"]+)"')
                                foreach ($m in $matchesLs) { $lsHints += $m.Groups[1].Value }
                            }
                        }
                        $activityRunRows.Add([PSCustomObject]@{
                            Container=$df.DataFactoryName; PipelineName=$pr.PipelineName; PipelineRunId=$pr.RunId
                            ActivityName=$a.ActivityName; ActivityType=$a.ActivityType
                            Status=$a.Status; ActivityRunStart=$a.ActivityRunStart; ActivityRunEnd=$a.ActivityRunEnd
                            DurationMs=$a.DurationInMs
                            LinkedServiceHints=(($lsHints | Select-Object -Unique) -join '; ')
                        })
                    }
                } catch { Write-Warning "      ActivityRun $($pr.RunId): $_" }
            }
        }
    }

    # ---------- Synapse Workspaces ----------
    $synWs = @(); try { $synWs = Get-AzSynapseWorkspace -ErrorAction Stop } catch {}
    foreach ($ws in $synWs) {
        Write-Host "  Synapse: $($ws.Name)" -ForegroundColor Yellow
        try {
            foreach ($ls in (Get-AzSynapseLinkedService -WorkspaceName $ws.Name -ErrorAction Stop)) {
                $type     = $ls.Properties.GetType().Name
                $connInfo = Get-ConnectionInfo -Properties $ls.Properties
                $driver   = $typeToDriver[$type]
                if ($type -eq 'OdbcLinkedService' -and $connInfo.PlainString) {
                    $parsed = Get-DriverFromConnectionString -ConnectionString $connInfo.PlainString
                    if ($parsed) { $driver = $parsed }
                }
                $ir = if ($ls.Properties.ConnectVia) { $ls.Properties.ConnectVia.ReferenceName } else { $null }
                $linkedServiceRows.Add([PSCustomObject]@{
                    Subscription=$sub.Name; ResourceGroup=$ws.ResourceGroupName
                    Container=$ws.Name; ContainerKind='Synapse'
                    LinkedServiceName=$ls.Name; LinkedServiceType=$type
                    IntegrationRuntime=$ir; IntegrationRuntimeKind=$null
                    DriverInferred=$driver
                    ConnectionStringType=$connInfo.Type
                    KvStore=$connInfo.KvStore; KvSecretName=$connInfo.KvSecretName; KvSecretVersion=$connInfo.KvSecretVersion
                    ConnectionStringRedacted=Hide-Secrets -ConnectionString $connInfo.PlainString
                })
            }
        } catch { Write-Warning "    Synapse linked services: $_" }
    }
}

# ---------- Outputs ----------
$lsCsv  = Join-Path $workDir "linked-services.csv"
$lsShir = Join-Path $workDir "shir-linked-services.csv"
$dsCsv  = Join-Path $workDir "datasets.csv"
$tgCsv  = Join-Path $workDir "triggers.csv"
$prCsv  = Join-Path $workDir "pipeline-runs-${RecentRunDays}d.csv"
$arCsv  = Join-Path $workDir "activity-runs-${ActivityRunDays}d.csv"
$sumCsv = Join-Path $workDir "driver-summary.csv"

$linkedServiceRows | Export-Csv -NoTypeInformation -Path $lsCsv
($linkedServiceRows | Where-Object IntegrationRuntimeKind -eq 'SelfHosted') | Export-Csv -NoTypeInformation -Path $lsShir
$datasetRows     | Export-Csv -NoTypeInformation -Path $dsCsv
$triggerRows     | Export-Csv -NoTypeInformation -Path $tgCsv
$pipelineRunRows | Export-Csv -NoTypeInformation -Path $prCsv
$activityRunRows | Export-Csv -NoTypeInformation -Path $arCsv

$summary = $linkedServiceRows | Where-Object { $_.DriverInferred } | Group-Object DriverInferred | ForEach-Object {
    $shirCount = ($_.Group | Where-Object IntegrationRuntimeKind -eq 'SelfHosted').Count
    [PSCustomObject]@{
        Driver               = $_.Name
        TotalLinkedServices  = $_.Count
        SHIRLinkedServices   = $shirCount
        UniqueIRs            = ($_.Group | Select-Object -ExpandProperty IntegrationRuntime -Unique).Count
        UniqueContainers     = ($_.Group | Select-Object -ExpandProperty Container -Unique).Count
    }
} | Sort-Object SHIRLinkedServices -Descending
$summary | Export-Csv -NoTypeInformation -Path $sumCsv

# ---------- Manifest ----------
$manifestFiles = Get-ChildItem -LiteralPath $workDir -File | ForEach-Object {
    [PSCustomObject]@{
        Name=$_.Name; SizeBytes=$_.Length
        Sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        LastWriteUtc=$_.LastWriteTimeUtc.ToString('o')
    }
}
$manifest = [ordered]@{
    schema='adf-discovery-manifest/v1'; scriptName='adf-linked-service-discovery.ps1'
    scriptVersion=$ScriptVersion
    runUser="$env:USERDOMAIN\$env:USERNAME"; runHost=$env:COMPUTERNAME
    psVersion="$($PSVersionTable.PSVersion)"
    startedUtc=$ts; finishedUtc=(Get-Date).ToUniversalTime().ToString('o')
    parameters=@{ RecentRunDays=$RecentRunDays; ActivityRunDays=$ActivityRunDays }
    files=$manifestFiles
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $workDir 'manifest.json') -Encoding UTF8

# ---------- Console summary ----------
Write-Host ""
Write-Host "=== SUMMARY ===" -ForegroundColor Green
$summary | Format-Table -AutoSize
$shirCount = ($linkedServiceRows | Where-Object IntegrationRuntimeKind -eq 'SelfHosted').Count
$kvCount   = ($linkedServiceRows | Where-Object ConnectionStringType -eq 'KeyVaultReference').Count
Write-Host ""
Write-Host "Total linked services:   $($linkedServiceRows.Count)" -ForegroundColor Green
Write-Host "...routed via SHIR:      $shirCount  <-- GV-228 dependency surface" -ForegroundColor Green
Write-Host "...using Key Vault refs: $kvCount" -ForegroundColor Green
Write-Host "Datasets:                $($datasetRows.Count)"
Write-Host "Triggers:                $($triggerRows.Count)  <-- proves SCHEDULED future use"
Write-Host "Pipeline runs:           $($pipelineRunRows.Count) (last $RecentRunDays days)"
Write-Host "Activity runs:           $($activityRunRows.Count) (last $ActivityRunDays days)"
Write-Host ""
Write-Host "Output dir: $workDir" -ForegroundColor Green
