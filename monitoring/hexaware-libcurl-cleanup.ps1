#requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Hard-coded file list #
$files = @(
"C:\Program Files (x86)\Microsoft SQL Server Management Studio 18\Common7\IDE\Mashup\ODBC Drivers\Simba Spark ODBC Driver\LibCurl32.DllA\libcurl.dll",
"C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\IDE\PrivateAssemblies\Business Intelligence Semantic Model\Mashup\ODBC Drivers\Simba Spark ODBC Driver\LibCurl32.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Gateway\sxs\OracleV2Unbundle\1.0.0.0\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Amazon Redshift ODBC Driver_1.5.20.1025\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Concur ODBC Driver_1.7.7.1004\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Concur ODBC Driver_1.7.8.5006\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Couchbase ODBC Driver_2.0.5.1005\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Couchbase ODBC Driver_2.2.1.1001\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Eloqua ODBC Driver_1.7.8.5009\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Google BigQuery ODBC Driver_3.0.5.1013\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Hive ODBC Driver_2.8.0.1000\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Hive ODBC Driver_2.8.1.1001\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Hubspot ODBC Driver_1.7.5.1005\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Hubspot ODBC Driver_1.7.8.1009\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Hubspot ODBC Driver_1.7.8.5010\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Impala ODBC Driver_2.8.0.1001\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Impala ODBC Driver_2.8.0.3001\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Jira ODBC Driver_1.7.7.1005\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Jira ODBC Driver_1.7.8.5007\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Magento ODBC Driver_1.6.4.1000\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Magento ODBC Driver_1.7.8.1007\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Marketo ODBC Driver_1.6.11.1006\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Marketo ODBC Driver_1.7.8.1009\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Marketo ODBC Driver_1.7.8.5009\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Oracle Service Cloud ODBC Driver_1.7.8.1004\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft PayPal ODBC Driver_1.7.7.1006\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Phoenix ODBC Driver_1.1.1.1001\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Phoenix ODBC Driver_1.2.2.1000\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Presto ODBC Driver_1.3.2.1003\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Presto ODBC Driver_1.3.2.3004\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Quickbooks ODBC Driver_1.7.7.1006\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Quickbooks ODBC Driver_1.7.8.5008\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Responsys ODBC Driver_1.7.8.1007\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Salesforce Marketing Cloud ODBC Driver_1.7.8.1008\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Salesforce Marketing Cloud ODBC Driver_1.7.8.5008\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Salesforce Marketing Cloud ODBC Driver_1.7.8.9\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Salesforce ODBC Driver_3.0.4.1006\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft ServiceNow ODBC Driver_2.1.5.1007\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Shopify ODBC Driver_1.6.34.1015\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Shopify ODBC Driver_1.7.7.1007\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Shopify ODBC Driver_1.7.8.5009\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Spark ODBC Driver_2.8.2.1014\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Spark ODBC Driver_2.8.2.3014\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Square ODBC Driver_1.7.7.1007\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Square ODBC Driver_1.7.8.5009\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Xero ODBC Driver_1.6.21.1009\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Xero ODBC Driver_1.6.34.1013\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Xero ODBC Driver_1.6.36.1014\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Xero ODBC Driver_1.7.7.1005\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Xero ODBC Driver_1.7.8.5007\lib\libcurl.dll",
"C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers\Microsoft Zoho ODBC Driver_1.7.7.1005\lib\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\New\GoogleBigQuery\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\New\Hive\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\New\Impala\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\New\Spark\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\Simba DocumentDB ODBC Driver\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\Simba QuickBooks ODBC Driver\LibCurl64.DllA\libcurl.dll",
"C:\Program Files\On-premises data gateway\m\ODBC Drivers\Simba Spark ODBC Driver\LibCurl64.DllA\libcurl.dll"
)

# Output ZIP settings #
$outDir  = "C:\Windows\Temp"
$zipName = "libcurl_collection_{0:yyyyMMdd_HHmmss}.zip" -f (Get-Date)
$zipPath = Join-Path $outDir $zipName

# Prep output folder #
if (-not (Test-Path -LiteralPath $outDir)) {
    New-Item -ItemType Directory -Path $outDir | Out-Null
}

# Create empty zip (so we can add incrementally) #
Add-Type -AssemblyName System.IO.Compression, System.IO.Compression.FileSystem
$fs = [System.IO.File]::Open($zipPath, [System.IO.FileMode]::CreateNew)
$fs.Dispose()

# Track results #
$added   = New-Object System.Collections.Generic.List[string]
$missing = New-Object System.Collections.Generic.List[string]
$failed  = New-Object System.Collections.Generic.List[string]

#Write-Host "Creating ZIP: $zipPath" -ForegroundColor Cyan

# Add each file + delete after successful add #
$zip = [System.IO.Compression.ZipFile]::Open($zipPath, [System.IO.Compression.ZipArchiveMode]::Update)
try {
    foreach ($f in $files) {
        if (-not (Test-Path -LiteralPath $f)) {
            #Write-Host "Missing: $f" -ForegroundColor Yellow
            $missing.Add($f) | Out-Null
            continue
        }

        try {
            # Make a unique entry name so duplicates don't collide
            # (many are libcurl.dll, so include part of the path)
            $safePath = ($f -replace '^[A-Za-z]:\\\\', '') -replace '\\\\', '/'
            $entryName = $safePath
            # Remove if entry already exists (re-runs)
            $existing = $zip.GetEntry($entryName)
            if ($existing) { $existing.Delete() }
            [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $zip, $f, $entryName, [System.IO.Compression.CompressionLevel]::Optimal
            ) | Out-Null
            # Verify entry exists before deleting original
            if ($zip.GetEntry($entryName) -ne $null) {
                Remove-Item -LiteralPath $f -Force
                Write-Host "Zipped + deleted: $f" -ForegroundColor Green
                $added.Add($f) | Out-Null
            }
            else {
                throw "Entry not found after add (unexpected): $entryName"
            }
        }
        catch {
            Write-Host "FAILED for '$f' -> $($_.Exception.Message)" -ForegroundColor Red
            $failed.Add($f) | Out-Null
        }
    }
}
finally {
    $zip.Dispose()
}

# Summary #
Write-Host ""
Write-Host "# Summary #" -ForegroundColor White
Write-Host ("Added & deleted : {0}" -f $added.Count)   -ForegroundColor Green
Write-Host ("Missing         : {0}" -f $missing.Count) -ForegroundColor Yellow
Write-Host ("Failed          : {0}" -f $failed.Count)  -ForegroundColor Red
Write-Host ("ZIP created at  : {0}" -f $zipPath)       -ForegroundColor Cyan

if ($missing.Count -gt 0) {
    Write-Host ""
    Write-Host "Missing files:" -ForegroundColor Yellow
    $missing | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
}

if ($failed.Count -gt 0) {
    Write-Host ""
    Write-Host "Failed files (NOT deleted):" -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
    exit 1
}
