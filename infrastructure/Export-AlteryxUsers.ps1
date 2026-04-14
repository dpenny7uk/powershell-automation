# Export Alteryx Server Users

$OutputCsv    = "C:\Dev\AlteryxExport\users.csv"
$PageSize     = 100

# Authenticate
. "$PSScriptRoot\Get-AlteryxAuth.ps1"
$auth    = Get-AlteryxAuth
$headers = $auth.Headers
$BaseUrl = $auth.BaseUrl

# Get all users (paginated)
$users = [System.Collections.Generic.List[object]]::new()
$skip = 0
do {
    $response = @(Invoke-RestMethod -Uri "$BaseUrl/v3/users?view=Full&skip=$skip&take=$PageSize" -Headers $headers -Method Get)
    if ($response -and $response.Count -gt 0) {
        $users.AddRange($response)
        $skip += $PageSize
    } else { break }
} while ($response.Count -eq $PageSize)
Write-Host "Total Users: $($users.Count)"

$users | Select-Object id, firstName, lastName, email, role, active |
    Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

Write-Host "Exported to: $OutputCsv" -ForegroundColor Green