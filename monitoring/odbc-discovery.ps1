# Run on each SHIR server. Outputs three CSVs to C:\Windows\Temp tagged with hostname.
# Run elevated (HKLM read for System DSNs).

$out = "C:\Windows\Temp"
$h   = $env:COMPUTERNAME
$ts  = Get-Date -Format "yyyyMMdd_HHmmss"

# 1. Registered ODBC drivers (64-bit + 32-bit)
$drivers64 = Get-OdbcDriver -Platform 64-bit | ForEach-Object {
    [PSCustomObject]@{
        Host        = $h
        Platform    = "64-bit"
        DriverName  = $_.Name
        DriverPath  = $_.Attribute["Driver"]
        ODBCVer     = $_.Attribute["DriverODBCVer"]
    }
}
$drivers32 = Get-OdbcDriver -Platform 32-bit | ForEach-Object {
    [PSCustomObject]@{
        Host        = $h
        Platform    = "32-bit"
        DriverName  = $_.Name
        DriverPath  = $_.Attribute["Driver"]
        ODBCVer     = $_.Attribute["DriverODBCVer"]
    }
}
($drivers64 + $drivers32) | Export-Csv -NoTypeInformation -Path (Join-Path $out "$h-$ts-drivers.csv")

# 2. System DSNs (the ones SHIR can use) - includes server/database where present
$dsns = @()
foreach ($plat in '64-bit','32-bit') {
    $dsns += Get-OdbcDsn -DsnType System -Platform $plat | ForEach-Object {
        [PSCustomObject]@{
            Host       = $h
            Platform   = $plat
            DsnName    = $_.Name
            DriverName = $_.DriverName
            Server     = $_.Attribute["Server"]
            Database   = $_.Attribute["Database"]
            Port       = $_.Attribute["Port"]
            AllAttrs   = ($_.Attribute.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join "; "
        }
    }
}
$dsns | Export-Csv -NoTypeInformation -Path (Join-Path $out "$h-$ts-system-dsns.csv")

# 3. Driver folders found under the SHIR install (what Tenable scanned)
$shirRoot = "C:\Program Files\Microsoft Integration Runtime\5.0\Shared\ODBC Drivers"
if (Test-Path -LiteralPath $shirRoot) {
    Get-ChildItem -LiteralPath $shirRoot -Directory | ForEach-Object {
        $libcurl = Get-ChildItem -LiteralPath $_.FullName -Recurse -Filter libcurl.dll -ErrorAction SilentlyContinue |
                   Select-Object -First 1
        [PSCustomObject]@{
            Host           = $h
            DriverFolder   = $_.Name
            HasLibcurl     = [bool]$libcurl
            LibcurlPath    = $libcurl.FullName
            LibcurlVersion = if ($libcurl) { (Get-Item $libcurl.FullName).VersionInfo.FileVersion } else { $null }
        }
    } | Export-Csv -NoTypeInformation -Path (Join-Path $out "$h-$ts-driver-folders.csv")
}

# 4. SHIR-registered linked services (if dmgcmd available)
$dmg = "C:\Program Files\Microsoft Integration Runtime\5.0\Shared\dmgcmd.exe"
if (Test-Path -LiteralPath $dmg) {
    & $dmg -DataStoreSetting | Out-File -Encoding utf8 (Join-Path $out "$h-$ts-shir-config.txt")
}

Write-Host "Outputs in $out (filtered by '$h-$ts-')." -ForegroundColor Green
