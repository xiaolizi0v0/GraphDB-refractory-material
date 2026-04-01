# Backend SPARQL Query Templates

Target GraphDB:
- Base URL: http://localhost:7200
- Repository: RefMDB

Named graph mapping:
- ontology: http://example.com/graph/ontology
- kb: http://example.com/graph/kb
- wikidata: http://example.com/graph/wikidata
- recommendation: http://example.com/graph/recommendation

Use these common prefixes:

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
```

## Q1. Count triples per named graph

```sparql
SELECT ?g (COUNT(*) AS ?n)
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
GROUP BY ?g
ORDER BY ?g
```

## Q2. Recommendation summary by condition URI

Replace `<COND_URI>` with your condition IRI, for example `http://example.com/refractory#wc-local`.

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX schema: <http://schema.org/>

SELECT ?rec ?primary ?alt ?system ?threshold ?desc
WHERE {
  GRAPH <http://example.com/graph/recommendation> {
    ?rec a ex:Recommendation ;
         ex:forCondition <COND_URI> ;
         ex:primaryLayer ?primary ;
         ex:recommendedSystem ?system ;
         ex:keyThreshold ?threshold ;
         schema:description ?desc .
    OPTIONAL { ?rec ex:alternativeLayer ?alt }
  }
}
```

## Q3. Layer details (role + recipe description)

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?rec ?layer ?role ?roleLabel ?layerLabel ?layerDesc
WHERE {
  GRAPH <http://example.com/graph/recommendation> {
    ?rec a ex:Recommendation ;
         ex:hasLayer ?layer .
  }
  GRAPH <http://example.com/graph/kb> {
    OPTIONAL { ?layer ex:layerRole ?role }
    OPTIONAL { ?role rdfs:label ?roleLabel FILTER (LANG(?roleLabel) = "zh" || LANG(?roleLabel) = "") }
    OPTIONAL { ?layer rdfs:label ?layerLabel FILTER (LANG(?layerLabel) = "zh" || LANG(?layerLabel) = "") }
    OPTIONAL { ?layer schema:description ?layerDesc FILTER (LANG(?layerDesc) = "zh" || LANG(?layerDesc) = "") }
  }
}
ORDER BY ?rec ?role ?layer
```

## Q4. Risks and processes for one recommendation

Replace `<REC_URI>` with recommendation IRI, for example `http://example.com/refractory#rec-local`.

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?risk ?riskLabel ?proc ?procLabel
WHERE {
  GRAPH <http://example.com/graph/recommendation> {
    <REC_URI> ex:highlightsRisk ?risk ;
              ex:recommendedProcess ?proc .
  }
  GRAPH <http://example.com/graph/kb> {
    OPTIONAL { ?risk rdfs:label ?riskLabel FILTER (LANG(?riskLabel) = "zh" || LANG(?riskLabel) = "") }
    OPTIONAL { ?proc rdfs:label ?procLabel FILTER (LANG(?procLabel) = "zh" || LANG(?procLabel) = "") }
  }
}
ORDER BY ?risk ?proc
```

## Q5. Triggered failures for one condition

Replace `<COND_URI>` with condition IRI.

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?fm ?fmLabel ?system
WHERE {
  GRAPH <http://example.com/graph/recommendation> {
    <COND_URI> ex:triggersFailure ?fm .
    ?fm ex:mitigatedBy ?system .
  }
  GRAPH <http://example.com/graph/kb> {
    OPTIONAL { ?fm rdfs:label ?fmLabel FILTER (LANG(?fmLabel) = "zh" || LANG(?fmLabel) = "") }
  }
}
ORDER BY ?fm ?system
```

## Q6. Get latest recommendation card (single row)

This is convenient for backend API response assembly.

```sparql
PREFIX ex: <http://example.com/refractory#>
PREFIX schema: <http://schema.org/>

SELECT ?rec ?cond ?primary ?alt ?system ?threshold ?desc
       (GROUP_CONCAT(DISTINCT STR(?risk); separator="|") AS ?risks)
       (GROUP_CONCAT(DISTINCT STR(?proc); separator="|") AS ?processes)
WHERE {
  GRAPH <http://example.com/graph/recommendation> {
    ?rec a ex:Recommendation ;
         ex:forCondition ?cond ;
         ex:primaryLayer ?primary ;
         ex:recommendedSystem ?system ;
         ex:keyThreshold ?threshold ;
         schema:description ?desc .
    OPTIONAL { ?rec ex:alternativeLayer ?alt }
    OPTIONAL { ?rec ex:highlightsRisk ?risk }
    OPTIONAL { ?rec ex:recommendedProcess ?proc }
  }
}
GROUP BY ?rec ?cond ?primary ?alt ?system ?threshold ?desc
ORDER BY DESC(?rec)
LIMIT 1
```

## HTTP calling notes (backend)

SPARQL query endpoint:
- POST http://localhost:7200/repositories/RefMDB

Example request format:
- Content-Type: application/sparql-query
- Body: raw SPARQL text

JSON results:
- Accept: application/sparql-results+json

RDF (CONSTRUCT/DESCRIBE) results:
- Accept: text/turtle
