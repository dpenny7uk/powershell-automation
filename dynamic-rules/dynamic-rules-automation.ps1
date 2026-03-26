param (
    [string]$StorageAccountName,
    [string]$StorageSASToken,
    [string]$StorageContainerName,
    [string]$DestinationPath,
    [string]$LogFile,
    [string]$Filter,
    [string]$RuleName,
    [string]$ProductFilter = "*",
    [string]$FileNameIncludePattern = "",
    [string]$FileNameExcludePattern = "",
    [switch]$DryRun
)

function Log {
    param ([string]$Message)
    $timeStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $entry = "$timeStamp - $Message"
    Write-Output $entry
    try {
        if (-not (Test-Path $LogFile)) {
            New-Item -Path $LogFile -ItemType File -Force | Out-Null
        }
        Add-Content -Path $LogFile -Value $entry
    }
    catch {
        Write-Warning "Failed to write to log file: $($_.Exception.Message)"
    }
}

function Blob_Download {
    [CmdletBinding()]
    param (
        [string]$StorageAccountName,
        [string]$StorageSASToken,
        [string]$StorageContainerName,
        [string]$DestinationPath,
        [string]$LogFile,
        [string]$Filter,
        [string]$RuleName,
        [string]$ProductFilter,
        [string]$FileNameIncludePattern,
        [string]$FileNameExcludePattern,
        [switch]$DryRun
    )

    try {
        if ($PSVersionTable.PSVersion -lt [version]"5.1") {
            Log "Error: PowerShell version 5.1 or higher is required."
            return
        }

        # PowerShell versioning to capture if Az.Storage module is available
        if (-not (Get-Module -ListAvailable -Name Az.Storage)) {
            try {
                Log "Az.Storage module not found. Installing..."
                Install-Module -Name Az.Storage -Force -Scope CurrentUser
                Log "Az.Storage module installed successfully."
            }
            catch {
                Log "Error installing Az.Storage module: $($_.Exception.Message)"
                return
            }
        }

        # Session Context
        $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $StorageSASToken

        # Capture files to be downloaded
        $Blobs = Get-AzStorageBlob -Container $StorageContainerName -Context $Context

        foreach ($Blob in $Blobs) {
            $blobName = $Blob.Name

            # REGEX to capture the correct files based on Filter path
            if ($blobName -match "^output/BE/TransactionalBordereaux/([^/]+)/([^/]+)/$Filter/(.+)$") {
                $product = $matches[1]
                $broker = $matches[2]
                $fileName = $matches[3]

                # FILTER: Product filter - skip if product doesn't match (unless wildcard)
                if ($ProductFilter -ne "*" -and $product -ne $ProductFilter) {
                    Log "Skipping $blobName - Product '$product' does not match filter '$ProductFilter'"
                    continue
                }

                # FILTER: Filename exclude - skip if filename matches exclude pattern
                if ($FileNameExcludePattern -ne "" -and $fileName -match $FileNameExcludePattern) {
                    Log "Skipping $blobName - Filename matches exclude pattern '$FileNameExcludePattern'"
                    continue
                }

                # FILTER: Filename include - skip if filename doesn't match include pattern
                if ($FileNameIncludePattern -ne "" -and $fileName -notmatch $FileNameIncludePattern) {
                    Log "Skipping $blobName - Filename does not match include pattern '$FileNameIncludePattern'"
                    continue
                }

                # Defining local cache path structure (flat for SFTP relay)
                $localDir = Join-Path -Path $DestinationPath -ChildPath "$RuleName\"
                $localPath = Join-Path -Path $localDir -ChildPath $fileName

                # Tests to confirm local path exists or creates it
                if (-not (Test-Path $localDir)) {
                    New-Item -ItemType Directory -Path $localDir -Force | Out-Null
                }

                # Test System (DryRun) allows users to test the expected outcome without making changes
                if ($DryRun) {
                    Log "[DryRun] Would download $blobName to $localPath"
                    Log "[DryRun] Would delete blob $blobName from container $StorageContainerName"
                }
                else {
                    try {
                        Get-AzStorageBlobContent -Blob $blobName -Container $StorageContainerName -Destination $localPath -Context $Context -Force
                        Log "Downloaded $blobName to $localPath"
                        Write-Host "Downloaded $blobName to $localPath"

                        # Verify download before deleting source blob
                        if ((Test-Path $localPath) -and (Get-Item $localPath).Length -eq $Blob.Length) {
                            Remove-AzStorageBlob -Blob $blobName -Container $StorageContainerName -Context $Context -Force
                            Log "Deleted blob $blobName from container $StorageContainerName"
                        }
                        else {
                            Log "WARNING: Download verification failed for $blobName - blob NOT deleted from source"
                        }
                    }
                    catch {
                        Log "ERROR: Failed to process blob $blobName - $($_.Exception.Message)"
                        continue
                    }
                }
            }
        }
    }
    catch {
        Log "ERROR: Unexpected failure $($_.Exception.Message)"
    }
}

if ($PSCommandPath -eq $MyInvocation.MyCommand.Path) {
    Blob_Download -StorageAccountName $StorageAccountName `
        -StorageSASToken $StorageSASToken `
        -StorageContainerName $StorageContainerName `
        -DestinationPath $DestinationPath `
        -LogFile $LogFile `
        -Filter $Filter `
        -RuleName $RuleName `
        -ProductFilter $ProductFilter `
        -FileNameIncludePattern $FileNameIncludePattern `
        -FileNameExcludePattern $FileNameExcludePattern `
        -DryRun:$DryRun
}