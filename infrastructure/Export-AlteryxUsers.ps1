# Export Alteryx Server Users

$BaseUrl      = "https://alteryx.contoso.com/webapi"
$TokenUrl     = "https://alteryx.contoso.com/webapi/oauth2/token"
$ClientId     = "YOUR_CLIENT_ID"
$ClientSecret = "YOUR_CLIENT_SECRET"
$OutputCsv    = "C:\Dev\AlteryxExport\users.csv"

# Authenticate
$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
}
$tokenResponse = Invoke-RestMethod -Uri $TokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
$headers = @{ "Authorization" = "Bearer $($tokenResponse.access_token)" }

# Get all users
$users = Invoke-RestMethod -Uri "$BaseUrl/v3/users?view=Full" -Headers $headers -Method Get
Write-Host "Total Users: $($users.Count)"

$users | Select-Object id, firstName, lastName, email, role, active |
    Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

Write-Host "Exported to: $OutputCsv" -ForegroundColor Green