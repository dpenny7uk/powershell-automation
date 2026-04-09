# Export Alteryx Server Schedules

$BaseUrl      = "https://alteryx.contoso.com/webapi"
$TokenUrl     = "https://alteryx.contoso.com/webapi/oauth2/token"
$ClientId     = "YOUR_CLIENT_ID"
$ClientSecret = "YOUR_CLIENT_SECRET"
$OutputCsv    = "C:\Dev\AlteryxExport\schedules.csv"

# Authenticate
$tokenBody = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
}
$tokenResponse = Invoke-RestMethod -Uri $TokenUrl -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
$headers = @{ "Authorization" = "Bearer $($tokenResponse.access_token)" }

# Get all schedules
$schedules = Invoke-RestMethod -Uri "$BaseUrl/v3/schedules" -Headers $headers -Method Get
Write-Host "Total Schedules: $($schedules.Count)"

$unique = ($schedules.workflowId | Sort-Object -Unique).Count
Write-Host "Unique workflows with schedules: $unique"

# Export
$schedules | Select-Object id, name, workflowId, ownerId, runDateTime, timeZone |
    Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8

Write-Host "Exported to: $OutputCsv" -ForegroundColor Green