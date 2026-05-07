#requires -Version 5.1
<#
.SYNOPSIS
    READ-ONLY discovery of ODBC drivers / libcurl.dll / SHIR runtime state
    across SHIR, On-prem Data Gateway, Power BI Desktop, SSMS, Visual Studio/SSDT.

    *** THIS SCRIPT IS READ-ONLY. IT MODIFIES NOTHING. ***
    No file is written outside the user-specified $OutDir (defaults to C:\Windows\Temp).
    No registry write, no service stop, no driver uninstall, no DLL deletion.

.DESCRIPTION
    Designed for hand-off to a third party (e.g. Hexaware) to execute on each affected
    server. Outputs evidence to a single ZIP per host, with a manifest of file hashes
    so the receiver can confirm CSVs arrived intact (no transfer corruption).

    Captured (strongest -> weakest evidence of active driver use):
      1. Loaded modules in the running SHIR / Gateway / SSMS / PBI processes.
         libcurl.dll mapped into a live process == driver actively in use right now.
      2. libcurl.dll inventory under each product root, with FileVersion + Authenticode
         + SHA256 (proves what is installed and that it is genuine MS/Simba code).
      3. Registered ODBC drivers (with REAL DLL FileVersion).
      4. SHIR Application event log (last N days).
      5. System DSNs — INFORMATIONAL only. SHIR uses runtime connection strings.
         Empty DSN list is NOT evidence drivers are unused.

.PARAMETER OutDir
    Where intermediate CSVs and the final ZIP are written. Defaults to C:\Windows\Temp.

.PARAMETER EventDays
    How many days of SHIR Application event log to capture. Defaults to 30.

.PARAMETER NoZip
    Skip ZIP packaging. Leaves loose CSVs in $OutDir.

.NOTES
    Run elevated. The loaded-modules section requires admin to read process memory
    of services running under another account (DIAHostService etc.).
#>

[CmdletBinding()]
param(
    [string]$OutDir = "C:\Windows\Temp",
    [int]$EventDays = 30,
    [switch]$NoZip
)

$ErrorActionPreference = 'Continue'
$ScriptVersion = '2.1.0'

# ---------- Run metadata (audit / handoff verification) ----------
$h        = $env:COMPUTERNAME
$ts       = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + "Z"
$workDir  = Join-Path $OutDir "$h-$ts"
New-Item -ItemType Directory -Path $workDir -Force | Out-Null

$elevated = $false
try {
    $elevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
} catch {}

$logPath = Join-Path $workDir "discovery.log"
function Log { param([string]$Message,[string]$Level='INFO')
    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format 's'), $Level, $Message
    Add-Content -LiteralPath $logPath -Value $line
    Write-Host $line
}

Log "================================================================="
Log "odbc-discovery-v2 v$ScriptVersion starting on $h"
Log "User:    $env:USERDOMAIN\$env:USERNAME (elevated=$elevated)"
Log "PS:      $($PSVersionTable.PSVersion)"
Log "OS:      $((Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption)"
Log "OutDir:  $workDir"
if (-not $elevated) { Log "WARNING: not elevated — loaded-modules and System DSN data will be incomplete." 'WARN' }
Log "================================================================="

# ---------- 1. Discover product install roots ----------
$roots = New-Object System.Collections.Generic.List[object]
$drives = (Get-PSDrive -PSProvider FileSystem).Root

# SHIR
foreach ($d in $drives) {
    foreach ($pf in 'Program Files','Program Files (x86)') {
        $base = Join-Path $d "$pf\Microsoft Integration Runtime"
        if (Test-Path -LiteralPath $base) {
            Get-ChildItem -LiteralPath $base -Directory -ErrorAction SilentlyContinue | ForEach-Object {
                $roots.Add([PSCustomObject]@{ Product='SHIR'; Version=$_.Name; Path=$_.FullName })
            }
        }
    }
}
try {
    $shirReg = Get-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\DataTransfer\DataManagementGateway\ConfigurationManager' -ErrorAction Stop
    foreach ($prop in 'DiacmdPath','InstallPath','InstallationPath') {
        $val = $shirReg.$prop
        if ($val) {
            $candidate = if (Test-Path -LiteralPath $val -PathType Leaf) { Split-Path -Parent $val } else { $val }
            if (Test-Path -LiteralPath $candidate) {
                $roots.Add([PSCustomObject]@{ Product='SHIR'; Version="registry:$prop"; Path=$candidate })
            }
        }
    }
} catch { Log "SHIR registry lookup: $_" 'WARN' }

# On-prem Gateway
foreach ($d in $drives) {
    $p = Join-Path $d "Program Files\On-premises data gateway"
    if (Test-Path -LiteralPath $p) { $roots.Add([PSCustomObject]@{ Product='OnPremDataGateway'; Version=''; Path=$p }) }
}

# Power BI Desktop
foreach ($d in $drives) {
    foreach ($pf in 'Program Files','Program Files (x86)') {
        $p = Join-Path $d "$pf\Microsoft Power BI Desktop"
        if (Test-Path -LiteralPath $p) { $roots.Add([PSCustomObject]@{ Product='PowerBIDesktop'; Version=''; Path=$p }) }
    }
}
$pbiStore = Get-ChildItem -LiteralPath "$env:LOCALAPPDATA\Microsoft\WindowsApps" -Filter 'PBIDesktop*' -ErrorAction SilentlyContinue
if ($pbiStore) { $roots.Add([PSCustomObject]@{ Product='PowerBIDesktop-Store'; Version=''; Path=$pbiStore[0].FullName }) }

# SSMS
foreach ($d in $drives) {
    foreach ($pf in 'Program Files','Program Files (x86)') {
        $base = Join-Path $d "$pf"
        if (Test-Path -LiteralPath $base) {
            Get-ChildItem -LiteralPath $base -Directory -Filter 'Microsoft SQL Server Management Studio*' -ErrorAction SilentlyContinue | ForEach-Object {
                $verNum = ($_.Name -replace '.*Studio (\d+).*','$1')
                $roots.Add([PSCustomObject]@{ Product='SSMS'; Version=$verNum; Path=$_.FullName })
            }
        }
    }
}

# Visual Studio / SSDT
foreach ($d in $drives) {
    foreach ($pf in 'Program Files','Program Files (x86)') {
        $base = Join-Path $d "$pf"
        if (Test-Path -LiteralPath $base) {
            Get-ChildItem -LiteralPath $base -Directory -Filter 'Microsoft Visual Studio*' -ErrorAction SilentlyContinue | ForEach-Object {
                $roots.Add([PSCustomObject]@{ Product='VisualStudio/SSDT'; Version=$_.Name; Path=$_.FullName })
            }
        }
    }
}

$rootsCsv = Join-Path $workDir "$h-product-roots.csv"
$roots | Export-Csv -NoTypeInformation -Path $rootsCsv
Log "Product roots discovered: $($roots.Count)"

# ---------- 2. libcurl.dll inventory ----------
$libcurlInventory = New-Object System.Collections.Generic.List[object]
foreach ($root in $roots) {
    Log "Scanning $($root.Product) at $($root.Path)..."
    try {
        $files = Get-ChildItem -LiteralPath $root.Path -Recurse -Filter libcurl.dll -ErrorAction Stop -Force
    } catch { Log "Scan failed for $($root.Path): $_" 'WARN'; continue }
    foreach ($f in $files) {
        $sigStatus = $null; $sigSubject = $null; $hash = $null
        try { $sig = Get-AuthenticodeSignature -LiteralPath $f.FullName -ErrorAction Stop
              $sigStatus = $sig.Status; $sigSubject = $sig.SignerCertificate.Subject } catch {}
        try { $hash = (Get-FileHash -LiteralPath $f.FullName -Algorithm SHA256 -ErrorAction Stop).Hash } catch {}

        $driverFolder = if ($f.FullName -match '\\ODBC Drivers\\([^\\]+)\\') { $matches[1] } else { $null }
        $arch = if ($f.FullName -match 'LibCurl32') { '32-bit' }
                elseif ($f.FullName -match 'LibCurl64') { '64-bit' } else { 'unknown' }

        $libcurlInventory.Add([PSCustomObject]@{
            Host=$h; Product=$root.Product; ProductVersion=$root.Version; ProductRoot=$root.Path
            LibcurlPath=$f.FullName
            LibcurlFileVersion=$f.VersionInfo.FileVersion
            LibcurlProductVersion=$f.VersionInfo.ProductVersion
            Arch=$arch; DriverFolder=$driverFolder
            FileLengthBytes=$f.Length; LastWriteUtc=$f.LastWriteTimeUtc.ToString('o')
            SignatureStatus=$sigStatus; SignatureSubject=$sigSubject; Sha256=$hash
        })
    }
}
$libcurlCsv = Join-Path $workDir "$h-libcurl-inventory.csv"
$libcurlInventory | Export-Csv -NoTypeInformation -Path $libcurlCsv
Log "libcurl.dll instances found: $($libcurlInventory.Count)"

# ---------- 3. Loaded libcurl modules in running SHIR/Gateway/SSMS/PBI processes ----------
$loadedModules = New-Object System.Collections.Generic.List[object]
$processNames = @('diahost','diawp','DataExchange.Hosts','Microsoft.Mashup.Container','PBIDesktop','Ssms',
                  'Microsoft.PowerBI.EnterpriseGateway','Microsoft.PowerBI.DataMovement.Pipeline.GatewayCore')
foreach ($pn in $processNames) {
    foreach ($proc in (Get-Process -Name $pn -ErrorAction SilentlyContinue)) {
        try {
            foreach ($mod in $proc.Modules) {
                if ($mod.ModuleName -like 'libcurl*') {
                    $loadedModules.Add([PSCustomObject]@{
                        Host=$h; ProcessName=$proc.Name; ProcessId=$proc.Id
                        ModuleName=$mod.ModuleName; FilePath=$mod.FileName
                        FileVersion=$mod.FileVersionInfo.FileVersion
                    })
                }
            }
        } catch { Log "Module enum failed for $($proc.Name) PID $($proc.Id): $_" 'WARN' }
    }
}
$loadedCsv = Join-Path $workDir "$h-loaded-libcurl-MODULES.csv"
$loadedModules | Export-Csv -NoTypeInformation -Path $loadedCsv
Log "Currently-loaded libcurl modules: $($loadedModules.Count) <-- strong evidence of active use"

# ---------- 4. Registered ODBC drivers (real DLL versions) ----------
$drivers = New-Object System.Collections.Generic.List[object]
foreach ($plat in '64-bit','32-bit') {
    try {
        Get-OdbcDriver -Platform $plat -ErrorAction Stop | ForEach-Object {
            $drvDll = $_.Attribute["Driver"]
            $dllVer = $null; $dllProductVer = $null
            if ($drvDll -and (Test-Path -LiteralPath $drvDll)) {
                $vi = (Get-Item -LiteralPath $drvDll).VersionInfo
                $dllVer = $vi.FileVersion; $dllProductVer = $vi.ProductVersion
            }
            $drivers.Add([PSCustomObject]@{
                Host=$h; Platform=$plat; DriverName=$_.Name; DriverDllPath=$drvDll
                DriverDllFileVer=$dllVer; DriverDllProdVer=$dllProductVer
                ODBCApiVer=$_.Attribute["DriverODBCVer"]
            })
        }
    } catch { Log "Get-OdbcDriver -$plat failed: $_" 'WARN' }
}
$driversCsv = Join-Path $workDir "$h-registered-drivers.csv"
$drivers | Export-Csv -NoTypeInformation -Path $driversCsv
Log "Registered ODBC drivers: $($drivers.Count)"

# ---------- 5. SHIR event log ----------
try {
    $events = Get-WinEvent -FilterHashtable @{
        LogName='Application'; ProviderName='Integration Runtime'
        StartTime=(Get-Date).AddDays(-$EventDays)
    } -ErrorAction Stop |
    Select-Object @{n='Host';e={$h}}, TimeCreated, Id, LevelDisplayName,
        @{n='Message';e={($_.Message -replace '\s+',' ').Substring(0,[Math]::Min(($_.Message.Length),500))}}
    $eventsCsv = Join-Path $workDir "$h-shir-events-${EventDays}d.csv"
    $events | Export-Csv -NoTypeInformation -Path $eventsCsv
    Log "SHIR event log entries (last $EventDays days): $(@($events).Count)"
} catch { Log "SHIR event log capture: $_" 'WARN' }

# ---------- 6. System DSNs (INFORMATIONAL ONLY) ----------
$sensitiveKeys = @('PWD','PASSWORD','PASSWD','TOKEN','APIKEY','API_KEY','SECRET','CREDENTIAL','AUTHTOKEN','PRIVATEKEY')
function Hide-Sensitive { param($attributes)
    $sanitised = [ordered]@{}
    foreach ($kv in $attributes.GetEnumerator()) {
        $upper = $kv.Key.ToUpper()
        $isSecret = $false
        foreach ($s in $sensitiveKeys) { if ($upper -like "*$s*") { $isSecret = $true; break } }
        $sanitised[$kv.Key] = if ($isSecret) { '<redacted>' } else { $kv.Value }
    }
    return $sanitised
}
$dsns = New-Object System.Collections.Generic.List[object]
foreach ($plat in '64-bit','32-bit') {
    try {
        Get-OdbcDsn -DsnType System -Platform $plat -ErrorAction Stop | ForEach-Object {
            $clean = Hide-Sensitive -attributes $_.Attribute
            $dsns.Add([PSCustomObject]@{
                Host=$h; Platform=$plat; DsnName=$_.Name; DriverName=$_.DriverName
                Server=$clean['Server']; Database=$clean['Database']
                AllAttrs=($clean.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '; '
            })
        }
    } catch { Log "Get-OdbcDsn -$plat failed: $_" 'WARN' }
}
$dsnCsv = Join-Path $workDir "$h-system-dsns-INFORMATIONAL.csv"
$dsns | Export-Csv -NoTypeInformation -Path $dsnCsv
Log "System DSNs: $($dsns.Count) (informational; SHIR uses runtime connection strings)"

# ---------- 7. Run metadata (audit) ----------
$runMeta = [PSCustomObject]@{
    ScriptName    = 'odbc-discovery-v2.ps1'
    ScriptVersion = $ScriptVersion
    Host          = $h
    User          = "$env:USERDOMAIN\$env:USERNAME"
    Elevated      = $elevated
    PSVersion     = "$($PSVersionTable.PSVersion)"
    OS            = (Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption
    StartedUtc    = $ts
    FinishedUtc   = (Get-Date).ToUniversalTime().ToString('o')
}
$metaCsv = Join-Path $workDir "$h-run-metadata.csv"
$runMeta | Export-Csv -NoTypeInformation -Path $metaCsv

# ---------- 8. Manifest (tamper-evident) ----------
$manifestFiles = Get-ChildItem -LiteralPath $workDir -File -Exclude 'manifest.json' | ForEach-Object {
    [PSCustomObject]@{
        Name        = $_.Name
        SizeBytes   = $_.Length
        Sha256      = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        LastWriteUtc= $_.LastWriteTimeUtc.ToString('o')
    }
}
$manifest = [ordered]@{
    schema        = 'odbc-discovery-manifest/v1'
    scriptName    = 'odbc-discovery-v2.ps1'
    scriptVersion = $ScriptVersion
    host          = $h
    user          = "$env:USERDOMAIN\$env:USERNAME"
    elevated      = $elevated
    psVersion     = "$($PSVersionTable.PSVersion)"
    os            = (Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction SilentlyContinue).Caption
    startedUtc    = $ts
    finishedUtc   = (Get-Date).ToUniversalTime().ToString('o')
    files         = $manifestFiles
}
$manifestPath = Join-Path $workDir 'manifest.json'
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

# ---------- 9. Optional ZIP packaging ----------
$zipPath = $null
if (-not $NoZip) {
    $zipPath = Join-Path $OutDir "$h-$ts-evidence.zip"
    try {
        Compress-Archive -Path (Join-Path $workDir '*') -DestinationPath $zipPath -CompressionLevel Optimal -Force
        Log "Packaged: $zipPath"
    } catch { Log "Zip failed: $_" 'WARN' }
}

# ---------- Summary ----------
Write-Host ""
Write-Host "=== DISCOVERY COMPLETE ===" -ForegroundColor Green
Write-Host "Host:                   $h"
Write-Host "Output dir:             $workDir"
if ($zipPath) { Write-Host "Single-file evidence:   $zipPath" }
Write-Host "Manifest:               $manifestPath"
Write-Host ""
Write-Host "Send the ZIP back to the requester." -ForegroundColor Yellow
