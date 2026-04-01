[CmdletBinding()]
param(
    [string]$GraphDbBaseUrl = "http://localhost:7200",
    [string]$RepositoryId = "refractory",
    [string]$DataDir = $PSScriptRoot,
    [bool]$UseSanitizedBase = $true,
    [switch]$ClearTargetGraphs,
    [switch]$DryRun,
    [string]$Username,
    [string]$Password
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-BasicAuthHeader {
    param(
        [string]$User,
        [string]$Pass
    )
    if ([string]::IsNullOrWhiteSpace($User)) {
        return @{}
    }
    $pair = "{0}:{1}" -f $User, $Pass
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
    $token = [System.Convert]::ToBase64String($bytes)
    return @{ Authorization = "Basic $token" }
}

function Get-ContextQueryValue {
    param([string]$GraphIri)
    # RDF4J context parameter expects angle-bracketed IRI, URL-encoded.
    return [System.Uri]::EscapeDataString("<$GraphIri>")
}

$baseFile = if ($UseSanitizedBase) { "out.sanitized.ttl" } else { "out.ttl" }

$imports = @(
    [PSCustomObject]@{ File = "refractory_ontology.ttl"; Graph = "http://example.com/graph/ontology" },
    [PSCustomObject]@{ File = "refractory_kb.ttl"; Graph = "http://example.com/graph/kb" },
    [PSCustomObject]@{ File = $baseFile; Graph = "http://example.com/graph/wikidata" },
    [PSCustomObject]@{ File = "recommendation.ttl"; Graph = "http://example.com/graph/recommendation" }
)

$headers = New-BasicAuthHeader -User $Username -Pass $Password
$endpoint = "$GraphDbBaseUrl/repositories/$RepositoryId/statements"

Write-Host "GraphDB base URL : $GraphDbBaseUrl"
Write-Host "Repository ID    : $RepositoryId"
Write-Host "Data directory   : $DataDir"
Write-Host "Base file mode   : $(if ($UseSanitizedBase) { 'out.sanitized.ttl' } else { 'out.ttl' })"
Write-Host ""

foreach ($item in $imports) {
    $fullPath = Join-Path $DataDir $item.File
    if (-not (Test-Path -LiteralPath $fullPath)) {
        throw "Missing file: $fullPath"
    }

    $context = Get-ContextQueryValue -GraphIri $item.Graph
    $uri = "${endpoint}?context=$context"

    Write-Host "[IMPORT] $($item.File) -> $($item.Graph)"

    if ($DryRun) {
        Write-Host "  DryRun URI: $uri"
        continue
    }

    if ($ClearTargetGraphs) {
        Write-Host "  Clearing named graph before import..."
        Invoke-WebRequest -Method Delete -Uri $uri -Headers $headers -ErrorAction SilentlyContinue | Out-Null
    }

    $postParams = @{
        Method = "Post"
        Uri = $uri
        Headers = $headers
        InFile = $fullPath
        ContentType = "text/turtle; charset=utf-8"
    }
    Invoke-WebRequest @postParams | Out-Null

    Write-Host "  Done"
}

Write-Host ""
Write-Host "All files imported successfully."