# GraphDB TTL Import Guide

This project can be imported into GraphDB as four named graphs:

- `refractory_ontology.ttl` -> `http://example.com/graph/ontology`
- `refractory_kb.ttl` -> `http://example.com/graph/kb`
- `out.sanitized.ttl` (or `out.ttl`) -> `http://example.com/graph/wikidata`
- `recommendation.ttl` -> `http://example.com/graph/recommendation`

## 1. Create repository in GraphDB

Create a repository first (for example, ID = `refractory`) in GraphDB Workbench.

## 2. Import files (Windows PowerShell)

From this folder, run:

```powershell
./import_to_graphdb.ps1 -GraphDbBaseUrl "http://localhost:7200" -RepositoryId "refractory" -ClearTargetGraphs
```

Optional parameters:

- `-UseSanitizedBase $true` (default) uses `out.sanitized.ttl`
- `-UseSanitizedBase $false` uses `out.ttl`
- `-DryRun` only prints target URLs, does not upload
- `-Username` and `-Password` if GraphDB requires auth

## 3. Verify in SPARQL

Count triples per named graph:

```sparql
SELECT ?g (COUNT(*) AS ?n)
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
GROUP BY ?g
ORDER BY ?g
```

Check recommendation output:

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX schema: <http://schema.org/>

SELECT ?rec ?primary ?alt ?risk ?proc ?desc
WHERE {
  GRAPH <http://example.com/graph/recommendation> {
    ?rec a ex:Recommendation ;
         ex:primaryLayer ?primary ;
         schema:description ?desc .
    OPTIONAL { ?rec ex:alternativeLayer ?alt }
    OPTIONAL { ?rec ex:highlightsRisk ?risk }
    OPTIONAL { ?rec ex:recommendedProcess ?proc }
  }
}
```
