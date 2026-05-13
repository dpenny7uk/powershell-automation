#requires -Version 5.1
<#
.SYNOPSIS
    Enumerates ADF linked services / IRs / triggers / datasets / pipeline runs
    across every subscription the current az login can see. Uses Azure CLI for
    authentication + the ADF REST API for queries. No Az PowerShell modules.

.DESCRIPTION
    Workaround for environments where Az PowerShell can't be installed but Azure CLI is.

    Pre-reqs:
      - Azure CLI installed (az --version)
      - az login already completed (az login --use-device-code)
      - The signed-in identity has Reader on each subscription in scope.

    Output CSVs match the schema of adf-linked-service-discovery.ps1 / parse-adf-arm-templates.ps1
    so deletion-impact-analysis.ps1 consumes them unchanged.

    Pipeline runs ARE captured (last $RecentRunDays days, default 30) - one
    advantage over the ARM template approach.

.PARAMETER SubscriptionFilter
    Optional list of subscription names or IDs to limit scope. If omitted, queries
    every subscription the current az login can see.

.PARAMETER RecentRunDays
    Pipeline-run lookback window. Default 30.

.PARAMETER OutDir
    Where to write outputs. Defaults to E:\Libcurl_Remediation\Output\adf-evidence-<ts>.

.PARAMETER SkipPipelineRuns
    Skip the pipeline-runs query (saves time across 100+ factories if you don't need run history).

.EXAMPLE
    .\query-adf-via-azcli.ps1 -SubscriptionFilter 'Group','Group Dev/Test'
#>

[CmdletBinding()]
param(
    [string[]]$SubscriptionFilter = @(),
    [int]$RecentRunDays = 30,
    [string]$OutDir,
    [switch]$SkipPipelineRuns
)

$ErrorActionPreference = 'Continue'
$ScriptVersion = '1.0.0'
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
if (-not $OutDir) { $OutDir = "E:\Libcurl_Remediation\Output\adf-evidence-$ts" }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

# ---------- Helper: short-type -> Microsoft ODBC driver mapping ----------
$shortTypeToDriver = @{
    'Odbc'                     = '<see connectionString Driver={...}>'
    'GoogleBigQuery'           = 'Microsoft Google BigQuery ODBC Driver'
    'GoogleBigQueryV2'         = 'Microsoft Google BigQuery ODBC Driver'
    'GoogleAdWords'            = 'Microsoft Google Ads ODBC Driver'
    'Hive'                     = 'Microsoft Hive ODBC Driver'
    'HBase'                    = 'Microsoft HBase ODBC Driver'
    'Spark'                    = 'Microsoft Spark ODBC Driver'
    'Impala'                   = 'Microsoft Impala ODBC Driver'
    'Presto'                   = 'Microsoft Presto ODBC Driver'
    'Phoenix'                  = 'Microsoft Phoenix ODBC Driver'
    'ServiceNow'               = 'Microsoft ServiceNow ODBC Driver'
    'ServiceNowV2'             = 'Microsoft ServiceNow ODBC Driver'
    'Salesforce'               = 'Microsoft Salesforce ODBC Driver'
    'SalesforceV2'             = 'Microsoft Salesforce ODBC Driver'
    'SalesforceMarketingCloud' = 'Microsoft Salesforce Marketing Cloud ODBC Driver'
    'SalesforceServiceCloud'   = 'Microsoft Salesforce ODBC Driver'
    'OracleServiceCloud'       = 'Microsoft Oracle Service Cloud ODBC Driver'
    'Eloqua'                   = 'Microsoft Eloqua ODBC Driver'
    'Concur'                   = 'Microsoft Concur ODBC Driver'
    'Couchbase'                = 'Microsoft Couchbase ODBC Driver'
    'Jira'                     = 'Microsoft Jira ODBC Driver'
    'Hubspot'                  = 'Microsoft Hubspot ODBC Driver'
    'Marketo'                  = 'Microsoft Marketo ODBC Driver'
    'Magento'                  = 'Microsoft Magento ODBC Driver'
    'Paypal'                   = 'Microsoft PayPal ODBC Driver'
    'QuickBooks'               = 'Microsoft Quickbooks ODBC Driver'
    'Responsys'                = 'Microsoft Responsys ODBC Driver'
    'Shopify'                  = 'Microsoft Shopify ODBC Driver'
    'Square'                   = 'Microsoft Square ODBC Driver'
    'Xero'                     = 'Microsoft Xero ODBC Driver'
    'Zoho'                     = 'Microsoft Zoho ODBC Driver'
    'AmazonRedshift'           = 'Microsoft Amazon Redshift ODBC Driver'
}

# ---------- Auth ----------
try {
    $accounts = az account list --output json 2>$null | ConvertFrom-Json
} catch {
    throw "az CLI not available or not logged in. Run: az login --use-device-code"
}
if (-not $accounts) { throw "az account list returned nothing. Run: az login --use-device-code" }

$script:token       = $null
$script:tokenExpiry = [datetime]::MinValue
function Get-MgmtToken {
    if ((Get-Date) -lt $script:tokenExpiry.AddMinutes(-5)) { return $script:token }
    $raw = az account get-access-token --resource 'https://management.azure.com/' --output json 2>$null
    if (-not $raw) { throw "Failed to get access token. Re-run az login." }
    $obj = $raw | ConvertFrom-Json
    $script:token       = $obj.accessToken
    $script:tokenExpiry = [datetime]$obj.expiresOn
    return $script:token
}

function Invoke-AzApi {
    param([string]$Uri, [string]$Method='GET', [object]$Body=$null)
    $headers = @{ Authorization = "Bearer $(Get-MgmtToken)" }
    $params  = @{ Uri=$Uri; Method=$Method; Headers=$headers; ContentType='application/json'; ErrorAction='Stop' }
    if ($Body) { $params.Body = ($Body | ConvertTo-Json -Depth 10) }
    try { return Invoke-RestMethod @params }
    catch {
        Write-Warning "  API call failed: $Uri -> $($_.Exception.Message)"
        return $null
    }
}

# Helper: walk a paginated ARM endpoint, returning all .value items
function Get-AllPaged {
    param([string]$InitialUri)
    $items = @(); $next = $InitialUri
    while ($next) {
        $resp = Invoke-AzApi -Uri $next
        if (-not $resp) { break }
        if ($resp.value) { $items += $resp.value }
        $next = $resp.nextLink
    }
    return $items
}

function Hide-Secrets { param([string]$s)
    if ([string]::IsNullOrWhiteSpace($s)) { return '' }
    $secretKey = '^\s*(PWD|PASSWORD|PASSWD|TOKEN|APIKEY|API_KEY|SECRET|CREDENTIAL|AUTHTOKEN|PRIVATEKEY)\s*='
    ($s -split ';' | ForEach-Object {
        if ($_ -match $secretKey) { ($_ -split '=',2)[0] + '=<redacted>' } else { $_ }
    }) -join ';'
}

function Get-DriverFromConnString { param([string]$s)
    if ([string]::IsNullOrWhiteSpace($s)) { return $null }
    if ($s -match 'Driver\s*=\s*\{?([^;}]+?)\}?\s*(?:;|$)') { return $matches[1].Trim() }
    return $null
}

function Get-ConnectionInfo { param($cs)
    $info = [ordered]@{ Type='None'; PlainString=$null; KvStore=$null; KvSecretName=$null; KvSecretVersion=$null }
    if ($null -eq $cs) { return $info }
    if ($cs -is [string]) { $info.Type='PlainString'; $info.PlainString=$cs; return $info }
    if ($cs.PSObject.Properties['type']) {
        switch ($cs.type) {
            'SecureString'        { $info.Type='SecureString'; $info.PlainString=$cs.value; return $info }
            'AzureKeyVaultSecret' {
                $info.Type            = 'KeyVaultReference'
                if ($cs.PSObject.Properties['store'])         { $info.KvStore = $cs.store.referenceName }
                if ($cs.PSObject.Properties['secretName'])    { $info.KvSecretName = $cs.secretName }
                if ($cs.PSObject.Properties['secretVersion']) { $info.KvSecretVersion = $cs.secretVersion }
                return $info
            }
        }
    }
    $info.Type = 'Unknown'; return $info
}

# ---------- Enumerate subscriptions ----------
$subs = $accounts | Where-Object { $_.state -eq 'Enabled' }
if ($SubscriptionFilter) {
    $subs = $subs | Where-Object { $_.name -in $SubscriptionFilter -or $_.id -in $SubscriptionFilter }
}
Write-Host "Scanning $($subs.Count) subscription(s)..." -ForegroundColor Cyan

$linkedServiceRows = New-Object System.Collections.Generic.List[object]
$datasetRows       = New-Object System.Collections.Generic.List[object]
$triggerRows       = New-Object System.Collections.Generic.List[object]
$pipelineRunRows   = New-Object System.Collections.Generic.List[object]
$factoryRows       = New-Object System.Collections.Generic.List[object]
$irKindByContainer = @{}

$apiVersion = '2018-06-01'

foreach ($sub in $subs) {
    Write-Host ""
    Write-Host "=== $($sub.name) ($($sub.id)) ===" -ForegroundColor Cyan

    $factories = Get-AllPaged -InitialUri "https://management.azure.com/subscriptions/$($sub.id)/providers/Microsoft.DataFactory/factories?api-version=$apiVersion"
    Write-Host "  Found $($factories.Count) data factories" -ForegroundColor Green

    foreach ($f in $factories) {
        $factoryName = $f.name
        $rg          = $f.id -replace '.+/resourceGroups/([^/]+)/.*','$1'
        Write-Host "    [$factoryName] ($rg)" -ForegroundColor DarkGray

        $factoryRows.Add([PSCustomObject]@{
            Subscription=$sub.name; ResourceGroup=$rg; DataFactory=$factoryName
            Location=$f.location; Id=$f.id
        })

        $base = "https://management.azure.com$($f.id)"

        # IRs first (so we can tag LS rows with kind)
        $irs = Get-AllPaged -InitialUri "$base/integrationruntimes?api-version=$apiVersion"
        foreach ($ir in $irs) {
            $irKindByContainer["$factoryName|$($ir.name)"] = $ir.properties.type
        }

        # Linked services
        $lses = Get-AllPaged -InitialUri "$base/linkedservices?api-version=$apiVersion"
        foreach ($ls in $lses) {
            $lsType = $ls.properties.type
            $cs = $null
            if ($ls.properties.typeProperties -and $ls.properties.typeProperties.PSObject.Properties['connectionString']) {
                $cs = $ls.properties.typeProperties.connectionString
            }
            $connInfo = Get-ConnectionInfo -cs $cs

            $driver = $shortTypeToDriver[$lsType]
            if ($lsType -eq 'Odbc' -and $connInfo.PlainString) {
                $parsed = Get-DriverFromConnString -s $connInfo.PlainString
                if ($parsed) { $driver = $parsed }
            }

            $irName = $null
            if ($ls.properties.connectVia) { $irName = $ls.properties.connectVia.referenceName }
            $irKind = $irKindByContainer["$factoryName|$irName"]

            $linkedServiceRows.Add([PSCustomObject]@{
                Subscription           = $sub.name
                ResourceGroup          = $rg
                Container              = $factoryName
                ContainerKind          = 'ADF'
                LinkedServiceName      = $ls.name
                LinkedServiceType      = "${lsType}LinkedService"
                IntegrationRuntime     = $irName
                IntegrationRuntimeKind = $irKind
                DriverInferred         = $driver
                ConnectionStringType   = $connInfo.Type
                KvStore                = $connInfo.KvStore
                KvSecretName           = $connInfo.KvSecretName
                KvSecretVersion        = $connInfo.KvSecretVersion
                ConnectionStringRedacted = Hide-Secrets -s $connInfo.PlainString
            })
        }

        # Datasets
        $dss = Get-AllPaged -InitialUri "$base/datasets?api-version=$apiVersion"
        foreach ($ds in $dss) {
            $linkedTo = $null
            if ($ds.properties.linkedServiceName) { $linkedTo = $ds.properties.linkedServiceName.referenceName }
            $datasetRows.Add([PSCustomObject]@{
                Container=$factoryName; ContainerKind='ADF'
                DatasetName=$ds.name
                DatasetType="$($ds.properties.type)Dataset"
                LinkedServiceName=$linkedTo
            })
        }

        # Triggers
        $trgs = Get-AllPaged -InitialUri "$base/triggers?api-version=$apiVersion"
        foreach ($t in $trgs) {
            $tp = $t.properties
            $freq = $null; $interval = $null; $start = $null
            if ($tp.typeProperties -and $tp.typeProperties.recurrence) {
                $freq     = $tp.typeProperties.recurrence.frequency
                $interval = $tp.typeProperties.recurrence.interval
                $start    = $tp.typeProperties.recurrence.startTime
            }
            $pipelines = @()
            if ($tp.pipelines) {
                foreach ($p in $tp.pipelines) {
                    if ($p.pipelineReference -and $p.pipelineReference.referenceName) { $pipelines += $p.pipelineReference.referenceName }
                }
            } elseif ($tp.pipeline -and $tp.pipeline.pipelineReference) {
                $pipelines += $tp.pipeline.pipelineReference.referenceName
            }
            $triggerRows.Add([PSCustomObject]@{
                Container=$factoryName; ContainerKind='ADF'
                TriggerName=$t.name; TriggerType=$tp.type; RuntimeState=$tp.runtimeState
                Frequency=$freq; Interval=$interval; StartTime=$start
                Pipelines=($pipelines -join '; ')
            })
        }

        # Pipeline runs - opt-in via -SkipPipelineRuns to skip
        if (-not $SkipPipelineRuns) {
            $afterIso  = (Get-Date).AddDays(-$RecentRunDays).ToUniversalTime().ToString('o')
            $beforeIso = (Get-Date).ToUniversalTime().ToString('o')
            $body = @{ lastUpdatedAfter = $afterIso; lastUpdatedBefore = $beforeIso }
            $continueToken = $null
            do {
                if ($continueToken) { $body.continuationToken = $continueToken }
                $resp = Invoke-AzApi -Method POST -Uri "$base/queryPipelineRuns?api-version=$apiVersion" -Body $body
                if ($resp -and $resp.value) {
                    foreach ($r in $resp.value) {
                        $pipelineRunRows.Add([PSCustomObject]@{
                            Subscription=$sub.name; Container=$factoryName; ContainerKind='ADF'
                            PipelineName=$r.pipelineName; RunId=$r.runId
                            RunStart=$r.runStart; RunEnd=$r.runEnd
                            Status=$r.status; DurationMs=$r.durationInMs
                        })
                    }
                }
                $continueToken = $resp.continuationToken
            } while ($continueToken)
        }
    }
}

# ---------- Write outputs ----------
$lsCsv  = Join-Path $OutDir "linked-services.csv"
$lsShir = Join-Path $OutDir "shir-linked-services.csv"
$dsCsv  = Join-Path $OutDir "datasets.csv"
$tgCsv  = Join-Path $OutDir "triggers.csv"
$prCsv  = Join-Path $OutDir "pipeline-runs-${RecentRunDays}d.csv"
$facCsv = Join-Path $OutDir "factories.csv"
$sumCsv = Join-Path $OutDir "driver-summary.csv"

$linkedServiceRows | Export-Csv -NoTypeInformation -Path $lsCsv
($linkedServiceRows | Where-Object IntegrationRuntimeKind -eq 'SelfHosted') | Export-Csv -NoTypeInformation -Path $lsShir
$datasetRows     | Export-Csv -NoTypeInformation -Path $dsCsv
$triggerRows     | Export-Csv -NoTypeInformation -Path $tgCsv
$factoryRows     | Export-Csv -NoTypeInformation -Path $facCsv
if (-not $SkipPipelineRuns) { $pipelineRunRows | Export-Csv -NoTypeInformation -Path $prCsv }

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

# Manifest
$manifestFiles = Get-ChildItem -LiteralPath $OutDir -File -Filter '*.csv' | ForEach-Object {
    [PSCustomObject]@{
        Name=$_.Name; SizeBytes=$_.Length
        Sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        LastWriteUtc=$_.LastWriteTimeUtc.ToString('o')
    }
}
$manifest = [ordered]@{
    schema='adf-azcli-manifest/v1'; scriptName='query-adf-via-azcli.ps1'
    scriptVersion=$ScriptVersion
    runUser="$env:USERDOMAIN\$env:USERNAME"; runHost=$env:COMPUTERNAME
    psVersion="$($PSVersionTable.PSVersion)"
    startedUtc=$ts; finishedUtc=(Get-Date).ToUniversalTime().ToString('o')
    parameters=@{ RecentRunDays=$RecentRunDays; SkipPipelineRuns=[bool]$SkipPipelineRuns }
    subscriptionsScanned=$subs.name
    files=$manifestFiles
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutDir 'manifest.json') -Encoding UTF8

# ---------- Console summary ----------
Write-Host ""
Write-Host "=== SUMMARY ===" -ForegroundColor Green
$summary | Format-Table -AutoSize
$shirCount = ($linkedServiceRows | Where-Object IntegrationRuntimeKind -eq 'SelfHosted').Count
$kvCount   = ($linkedServiceRows | Where-Object ConnectionStringType -eq 'KeyVaultReference').Count
Write-Host ""
Write-Host "Subscriptions scanned:  $($subs.Count)"
Write-Host "Data factories:         $($factoryRows.Count)" -ForegroundColor Green
Write-Host "Linked services:        $($linkedServiceRows.Count)" -ForegroundColor Green
Write-Host "...routed via SHIR:     $shirCount  <-- GV-228 dependency surface" -ForegroundColor Green
Write-Host "...using Key Vault refs:$kvCount"
Write-Host "Datasets:               $($datasetRows.Count)"
Write-Host "Triggers:               $($triggerRows.Count)"
if (-not $SkipPipelineRuns) {
    Write-Host "Pipeline runs ($RecentRunDays d): $($pipelineRunRows.Count)"
}
Write-Host ""
Write-Host "Output dir: $OutDir" -ForegroundColor Green
