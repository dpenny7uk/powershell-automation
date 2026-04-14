#Requires -Version 5.1
<#
.SYNOPSIS
    Shared authentication helper for Alteryx Server API scripts.

.DESCRIPTION
    Authenticates using OAuth2 client credentials and returns a hashtable
    containing the Authorization headers and base URL for subsequent API calls.

.EXAMPLE
    . "$PSScriptRoot\Get-AlteryxAuth.ps1"
    $auth = Get-AlteryxAuth
    Invoke-RestMethod -Uri "$($auth.BaseUrl)/v3/workflows" -Headers $auth.Headers -Method Get
#>

function Get-AlteryxAuth {
    param(
        [string]$BaseUrl      = "https://alteryx.contoso.com/webapi",
        [string]$ClientId     = "YOUR_CLIENT_ID",
        [string]$ClientSecret = "YOUR_CLIENT_SECRET"
    )

    Write-Host "Authenticating..." -ForegroundColor Cyan

    $tokenBody = @{
        grant_type    = "client_credentials"
        client_id     = $ClientId
        client_secret = $ClientSecret
    }

    try {
        $tokenResponse = Invoke-RestMethod -Uri "$BaseUrl/oauth2/token" -Method Post -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
        Write-Host "Authentication successful." -ForegroundColor Green
    }
    catch {
        Write-Error "Failed to authenticate. Check your Client ID/Secret.`n$($_.Exception.Message)"
        exit 1
    }

    return @{
        Headers = @{ "Authorization" = "Bearer $($tokenResponse.access_token)" }
        BaseUrl = $BaseUrl
    }
}
