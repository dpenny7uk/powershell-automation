#requires -Version 5.1
<#
.SYNOPSIS
    Parses ADF ARM template exports (downloaded from the Azure Portal) and produces
    the same CSVs that adf-linked-service-discovery.ps1 would have produced via Az
    PowerShell. Uses only built-in PowerShell - no Az modules, no internet, no
    PSGallery.

.DESCRIPTION
    Workaround for environments where Az PowerShell can't be installed (locked-down
    corporate laptops without working PSGallery, no Cloud Shell, etc.).

    Workflow:
      1. In Azure Portal, for each ADF you have access to:
           Open the data factory -> Author pane (pencil icon) ->
           top-left dropdown -> "ARM template" -> "Export ARM template" -> Download
      2. Save the resulting ZIP to a folder, e.g. E:\Libcurl_Remediation\Output\arm-exports\
         You can either leave the ZIPs as-is (this script will extract them) or
         unzip them yourself into named subfolders.
      3. Run this script pointing -ArmFolder at that folder.

    Output (in -OutDir):
      linked-services.csv         - every LS across every ADF, with inferred driver
                                    + IR + IR kind + connection-string type
      shir-linked-services.csv    - filtered to LSes routed via a SelfHosted IR
      datasets.csv                - datasets and their linked-service references
      triggers.csv                - trigger inventory + recurrence + pipelines
      driver-summary.csv          - per-driver counts (LS / IRs / ADFs)
      arm-source-files.csv        - which ARM files were parsed (audit trail)
      manifest.json               - file hashes

    What's lost vs adf-linked-service-discovery.ps1:
      - pipeline-runs-30d.csv: runtime telemetry, not in ARM templates
      - activity-runs-7d.csv:  ditto
    The deletion-impact-analysis script handles their absence (the PipelineRunsCsv
    parameter is optional).

.PARAMETER ArmFolder
    Folder containing ARM template ZIPs and/or already-extracted JSON files.
    The script walks recursively.

.PARAMETER OutDir
    Where to write the CSV outputs. Defaults to E:\Libcurl_Remediation\Output\adf-evidence-<ts>.

.EXAMPLE
    .\parse-adf-arm-templates.ps1 -ArmFolder 'E:\Libcurl_Remediation\Output\arm-exports'
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$ArmFolder,
    [string]$OutDir
)

$ErrorActionPreference = 'Continue'
$ScriptVersion = '1.0.0'
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
if (-not $OutDir) { $OutDir = "E:\Libcurl_Remediation\Output\adf-evidence-$ts" }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

if (-not (Test-Path -LiteralPath $ArmFolder)) { throw "ArmFolder not found: $ArmFolder" }

# ---------- ADF .NET type -> Microsoft ODBC driver mapping ----------
# ARM-template "type" values are SHORT (e.g. "Odbc", "ServiceNow") rather than the
# .NET class names (OdbcLinkedService, ServiceNowLinkedService) that the Az module returns.
$shortTypeToDriver = @{
    'Odbc'                          = '<see connectionString Driver={...}>'
    'GoogleBigQuery'                = 'Microsoft Google BigQuery ODBC Driver'
    'GoogleBigQueryV2'              = 'Microsoft Google BigQuery ODBC Driver'
    'GoogleAdWords'                 = 'Microsoft Google Ads ODBC Driver'
    'Hive'                          = 'Microsoft Hive ODBC Driver'
    'HBase'                         = 'Microsoft HBase ODBC Driver'
    'Spark'                         = 'Microsoft Spark ODBC Driver'
    'Impala'                        = 'Microsoft Impala ODBC Driver'
    'Presto'                        = 'Microsoft Presto ODBC Driver'
    'Phoenix'                       = 'Microsoft Phoenix ODBC Driver'
    'ServiceNow'                    = 'Microsoft ServiceNow ODBC Driver'
    'ServiceNowV2'                  = 'Microsoft ServiceNow ODBC Driver'
    'Salesforce'                    = 'Microsoft Salesforce ODBC Driver'
    'SalesforceV2'                  = 'Microsoft Salesforce ODBC Driver'
    'SalesforceMarketingCloud'      = 'Microsoft Salesforce Marketing Cloud ODBC Driver'
    'SalesforceServiceCloud'        = 'Microsoft Salesforce ODBC Driver'
    'OracleServiceCloud'            = 'Microsoft Oracle Service Cloud ODBC Driver'
    'Eloqua'                        = 'Microsoft Eloqua ODBC Driver'
    'Concur'                        = 'Microsoft Concur ODBC Driver'
    'Couchbase'                     = 'Microsoft Couchbase ODBC Driver'
    'Jira'                          = 'Microsoft Jira ODBC Driver'
    'Hubspot'                       = 'Microsoft Hubspot ODBC Driver'
    'Marketo'                       = 'Microsoft Marketo ODBC Driver'
    'Magento'                       = 'Microsoft Magento ODBC Driver'
    'Paypal'                        = 'Microsoft PayPal ODBC Driver'
    'QuickBooks'                    = 'Microsoft Quickbooks ODBC Driver'
    'Responsys'                     = 'Microsoft Responsys ODBC Driver'
    'Shopify'                       = 'Microsoft Shopify ODBC Driver'
    'Square'                        = 'Microsoft Square ODBC Driver'
    'Xero'                          = 'Microsoft Xero ODBC Driver'
    'Zoho'                          = 'Microsoft Zoho ODBC Driver'
    'AmazonRedshift'                = 'Microsoft Amazon Redshift ODBC Driver'
}

function Hide-Secrets { param([string]$ConnectionString)
    if ([string]::IsNullOrWhiteSpace($ConnectionString)) { return '' }
    $secretKey = '^\s*(PWD|PASSWORD|PASSWD|TOKEN|APIKEY|API_KEY|SECRET|CREDENTIAL|AUTHTOKEN|PRIVATEKEY)\s*='
    ($ConnectionString -split ';' | ForEach-Object {
        if ($_ -match $secretKey) { ($_ -split '=',2)[0] + '=<redacted>' } else { $_ }
    }) -join ';'
}

function Get-DriverFromConnectionString { param([string]$ConnectionString)
    if ([string]::IsNullOrWhiteSpace($ConnectionString)) { return $null }
    if ($ConnectionString -match 'Driver\s*=\s*\{?([^;}]+?)\}?\s*(?:;|$)') { return $matches[1].Trim() }
    return $null
}

# Resource name in ARM is "[concat(parameters('factoryName'), '/myLinkedService')]"
# or sometimes literal "factoryname/myLinkedService". Extract the resource part after the last slash.
function Get-ResourceShortName { param([string]$ArmName)
    if (-not $ArmName) { return $null }
    if ($ArmName -match "/([^/']+)'?\)?\]?\s*$") { return $matches[1] }
    return $ArmName
}

# ---------- 1. Discover ARM JSON files (extract any ZIPs first) ----------
$workExtract = Join-Path $OutDir "extracted-arm-templates"
New-Item -ItemType Directory -Path $workExtract -Force | Out-Null

$zips = Get-ChildItem -LiteralPath $ArmFolder -Recurse -Filter '*.zip' -File -ErrorAction SilentlyContinue
foreach ($z in $zips) {
    $dest = Join-Path $workExtract $z.BaseName
    if (-not (Test-Path -LiteralPath $dest)) {
        Write-Host "Extracting $($z.Name)..." -ForegroundColor DarkGray
        try { Expand-Archive -LiteralPath $z.FullName -DestinationPath $dest -Force }
        catch { Write-Warning "  Extract failed: $_" }
    }
}

# Find all JSON files: both the freshly-extracted ones AND any pre-extracted JSONs in the source folder
$jsonFiles = @()
$jsonFiles += Get-ChildItem -LiteralPath $workExtract -Recurse -Filter '*.json' -File -ErrorAction SilentlyContinue
$jsonFiles += Get-ChildItem -LiteralPath $ArmFolder -Recurse -Filter '*.json' -File -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notlike "$workExtract*" }
# Keep only arm_template.json (skip parameters.json which has no resources)
$armTemplates = $jsonFiles | Where-Object { $_.Name -like '*arm_template*' -and $_.Name -notlike '*parameters*' }
if ($armTemplates.Count -eq 0) {
    # Fallback: try every JSON file - some users might rename them
    $armTemplates = $jsonFiles | Where-Object { $_.Name -notlike '*parameters*' }
}
Write-Host "Found $($armTemplates.Count) ARM template file(s) to parse." -ForegroundColor Cyan

# ---------- 2. Parse each ARM template ----------
$linkedServiceRows = New-Object System.Collections.Generic.List[object]
$datasetRows       = New-Object System.Collections.Generic.List[object]
$triggerRows       = New-Object System.Collections.Generic.List[object]
$sourceFileRows    = New-Object System.Collections.Generic.List[object]
$irKindByContainer = @{}

foreach ($f in $armTemplates) {
    Write-Host "Parsing $($f.FullName)" -ForegroundColor DarkGray
    try {
        $arm = Get-Content -LiteralPath $f.FullName -Raw | ConvertFrom-Json -ErrorAction Stop
    } catch {
        Write-Warning "  Parse failed: $_"
        $sourceFileRows.Add([PSCustomObject]@{ File=$f.FullName; Status='ParseError'; ContainerName=$null; ResourceCount=0 })
        continue
    }
    if (-not $arm.resources) {
        Write-Warning "  No resources array - probably not an ADF ARM template."
        $sourceFileRows.Add([PSCustomObject]@{ File=$f.FullName; Status='NoResources'; ContainerName=$null; ResourceCount=0 })
        continue
    }

    # Derive container (data factory) name from parameters if present, otherwise from the first resource name
    $containerName = $null
    if ($arm.parameters -and $arm.parameters.factoryName -and $arm.parameters.factoryName.defaultValue) {
        $containerName = $arm.parameters.factoryName.defaultValue
    } else {
        $firstWithName = $arm.resources | Where-Object { $_.name } | Select-Object -First 1
        if ($firstWithName.name -match "parameters\('factoryName'\)") {
            # Couldn't resolve, leave as <unknown> - user can rename CSV rows after
            $containerName = '<unknown>'
        } elseif ($firstWithName.name -match "^([^/]+)/") {
            $containerName = $matches[1]
        } else {
            $containerName = '<unknown>'
        }
    }

    $sourceFileRows.Add([PSCustomObject]@{
        File=$f.FullName; Status='OK'; ContainerName=$containerName; ResourceCount=$arm.resources.Count
    })

    # First pass: IR kinds (linked services reference these)
    foreach ($r in $arm.resources) {
        if ($r.type -like '*integrationRuntimes') {
            $irName = Get-ResourceShortName -ArmName $r.name
            $irKind = $r.properties.type   # SelfHosted, Managed, etc.
            $irKindByContainer["$containerName|$irName"] = $irKind
        }
    }

    # Second pass: linked services, datasets, triggers
    foreach ($r in $arm.resources) {
        $rName = Get-ResourceShortName -ArmName $r.name

        if ($r.type -like '*linkedServices') {
            $lsType = $r.properties.type
            $connectionInfo = [ordered]@{
                Type=$null; PlainString=$null; KvStore=$null; KvSecretName=$null; KvSecretVersion=$null
            }
            $cs = $null
            if ($r.properties.typeProperties -and $r.properties.typeProperties.connectionString) {
                $cs = $r.properties.typeProperties.connectionString
            }
            if ($null -eq $cs) {
                $connectionInfo.Type = 'None'
            } elseif ($cs -is [string]) {
                $connectionInfo.Type = 'PlainString'
                $connectionInfo.PlainString = $cs
            } elseif ($cs.PSObject.Properties['type']) {
                switch ($cs.type) {
                    'SecureString'        { $connectionInfo.Type='SecureString'; $connectionInfo.PlainString=$cs.value }
                    'AzureKeyVaultSecret' {
                        $connectionInfo.Type = 'KeyVaultReference'
                        if ($cs.PSObject.Properties['store'])           { $connectionInfo.KvStore = $cs.store.referenceName }
                        if ($cs.PSObject.Properties['secretName'])      { $connectionInfo.KvSecretName = $cs.secretName }
                        if ($cs.PSObject.Properties['secretVersion'])   { $connectionInfo.KvSecretVersion = $cs.secretVersion }
                    }
                    default { $connectionInfo.Type = 'Unknown' }
                }
            } else {
                $connectionInfo.Type = 'Unknown'
            }

            $driver = $shortTypeToDriver[$lsType]
            if ($lsType -eq 'Odbc' -and $connectionInfo.PlainString) {
                $parsed = Get-DriverFromConnectionString -ConnectionString $connectionInfo.PlainString
                if ($parsed) { $driver = $parsed }
            }

            $irName = $null
            if ($r.properties.connectVia) { $irName = $r.properties.connectVia.referenceName }
            $irKind = $irKindByContainer["$containerName|$irName"]

            $linkedServiceRows.Add([PSCustomObject]@{
                Subscription           = ''
                ResourceGroup          = ''
                Container              = $containerName
                ContainerKind          = 'ADF'
                LinkedServiceName      = $rName
                LinkedServiceType      = "${lsType}LinkedService"   # match the .NET style the impact script expects
                IntegrationRuntime     = $irName
                IntegrationRuntimeKind = $irKind
                DriverInferred         = $driver
                ConnectionStringType   = $connectionInfo.Type
                KvStore                = $connectionInfo.KvStore
                KvSecretName           = $connectionInfo.KvSecretName
                KvSecretVersion        = $connectionInfo.KvSecretVersion
                ConnectionStringRedacted = Hide-Secrets -ConnectionString $connectionInfo.PlainString
            })
        }
        elseif ($r.type -like '*datasets') {
            $linkedTo = $null
            if ($r.properties.linkedServiceName) { $linkedTo = $r.properties.linkedServiceName.referenceName }
            $datasetRows.Add([PSCustomObject]@{
                Container=$containerName; ContainerKind='ADF'
                DatasetName=$rName
                DatasetType="$($r.properties.type)Dataset"
                LinkedServiceName=$linkedTo
            })
        }
        elseif ($r.type -like '*triggers') {
            $tp = $r.properties
            $triggerType = $tp.type
            $runtimeState = $tp.runtimeState
            $freq = $null; $interval = $null; $start = $null
            if ($tp.typeProperties -and $tp.typeProperties.recurrence) {
                $freq     = $tp.typeProperties.recurrence.frequency
                $interval = $tp.typeProperties.recurrence.interval
                $start    = $tp.typeProperties.recurrence.startTime
            }
            $pipelines = @()
            if ($tp.pipelines) {
                foreach ($p in $tp.pipelines) {
                    if ($p.pipelineReference -and $p.pipelineReference.referenceName) {
                        $pipelines += $p.pipelineReference.referenceName
                    }
                }
            } elseif ($tp.pipeline -and $tp.pipeline.pipelineReference) {
                $pipelines += $tp.pipeline.pipelineReference.referenceName
            }
            $triggerRows.Add([PSCustomObject]@{
                Container=$containerName; ContainerKind='ADF'
                TriggerName=$rName; TriggerType=$triggerType; RuntimeState=$runtimeState
                Frequency=$freq; Interval=$interval; StartTime=$start
                Pipelines=($pipelines -join '; ')
            })
        }
    }
}

# ---------- 3. Write outputs ----------
$lsCsv  = Join-Path $OutDir "linked-services.csv"
$lsShir = Join-Path $OutDir "shir-linked-services.csv"
$dsCsv  = Join-Path $OutDir "datasets.csv"
$tgCsv  = Join-Path $OutDir "triggers.csv"
$srcCsv = Join-Path $OutDir "arm-source-files.csv"
$sumCsv = Join-Path $OutDir "driver-summary.csv"

$linkedServiceRows | Export-Csv -NoTypeInformation -Path $lsCsv
($linkedServiceRows | Where-Object IntegrationRuntimeKind -eq 'SelfHosted') | Export-Csv -NoTypeInformation -Path $lsShir
$datasetRows    | Export-Csv -NoTypeInformation -Path $dsCsv
$triggerRows    | Export-Csv -NoTypeInformation -Path $tgCsv
$sourceFileRows | Export-Csv -NoTypeInformation -Path $srcCsv

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
    schema       = 'adf-arm-parse-manifest/v1'
    scriptName   = 'parse-adf-arm-templates.ps1'
    scriptVersion= $ScriptVersion
    runUser      = "$env:USERDOMAIN\$env:USERNAME"
    runHost      = $env:COMPUTERNAME
    startedUtc   = $ts
    finishedUtc  = (Get-Date).ToUniversalTime().ToString('o')
    armSource    = $ArmFolder
    armFileCount = $armTemplates.Count
    files        = $manifestFiles
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutDir 'manifest.json') -Encoding UTF8

# ---------- 4. Console summary ----------
Write-Host ""
Write-Host "=== ARM PARSE SUMMARY ===" -ForegroundColor Green
$summary | Format-Table -AutoSize
$shirCount = ($linkedServiceRows | Where-Object IntegrationRuntimeKind -eq 'SelfHosted').Count
$kvCount   = ($linkedServiceRows | Where-Object ConnectionStringType -eq 'KeyVaultReference').Count
$unknownIr = ($linkedServiceRows | Where-Object { -not $_.IntegrationRuntimeKind }).Count
Write-Host ""
Write-Host "ARM templates parsed:        $($armTemplates.Count)" -ForegroundColor Green
Write-Host "Linked services total:       $($linkedServiceRows.Count)" -ForegroundColor Green
Write-Host "...routed via SHIR:          $shirCount  <-- GV-228 dependency surface" -ForegroundColor Green
Write-Host "...with KeyVault refs:       $kvCount" -ForegroundColor Green
Write-Host "...IR kind unresolved:       $unknownIr  (likely IR defined outside this ARM export)" -ForegroundColor Yellow
Write-Host "Datasets:                    $($datasetRows.Count)"
Write-Host "Triggers:                    $($triggerRows.Count)"
Write-Host ""
Write-Host "Outputs:                     $OutDir" -ForegroundColor Green
Write-Host ""
Write-Host "Next: feed shir-linked-services.csv into deletion-impact-analysis.ps1" -ForegroundColor Cyan
Write-Host "      The -PipelineRunsCsv parameter can be omitted (ARM templates don't carry runtime telemetry)" -ForegroundColor Cyan
