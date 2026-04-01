"""Run decision chain: WorkCondition -> FailureMechanisms -> MaterialSystem -> ProductSpec -> Process -> Risks.

This script is a local runner so you can validate the KG decision chain before wiring it into GraphDB.
It loads:
- out.ttl (Wikidata-derived triples)
- refractory_ontology.ttl (your domain ontology)
- refractory_kb.ttl (MVP rules/data)
- a WorkCondition instance generated from JSON input
Then executes SPARQL CONSTRUCT (recommend_construct.sparql) to produce a recommendation graph.

Usage:
  python run_decision_chain.py --out-ttl rec.ttl --workcondition example_workcondition.json

You can import both out.ttl and rec.ttl into GraphDB.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Optional

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD


EX = Namespace("http://example.com/refractory#")


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def add_workcondition(g: Graph, wc: Dict[str, Any], wc_uri: Optional[str] = None) -> URIRef:
    uri = URIRef(wc_uri) if wc_uri else URIRef(EX + "wc-local")
    g.add((uri, RDF.type, EX.WorkCondition))

    def add_num(pred: URIRef, key: str) -> None:
        if key in wc and wc[key] is not None:
            g.add((uri, pred, Literal(float(wc[key]), datatype=XSD.double)))

    def add_bool(pred: URIRef, key: str) -> None:
        if key in wc and wc[key] is not None:
            g.add((uri, pred, Literal(bool(wc[key]), datatype=XSD.boolean)))

    def add_uri(pred: URIRef, key: str) -> None:
        val = wc.get(key)
        if isinstance(val, str) and val:
            g.add((uri, pred, URIRef(EX + val)))

    add_num(EX.tmax, "tmax")
    add_num(EX.slagBasicity, "slagBasicity")
    add_num(EX.thermalShockCyclesPerDay, "thermalShockCyclesPerDay")
    add_num(EX.targetCampaignHeats, "targetCampaignHeats")
    add_num(EX.maxAllowShellTemp, "maxAllowShellTemp")
    if "abrasionLevel" in wc and wc["abrasionLevel"] is not None:
        g.add((uri, EX.abrasionLevel, Literal(str(wc["abrasionLevel"]))))
    add_bool(EX.metalContact, "metalContact")

    # expects one of: Oxidizing|Reducing|CORich
    add_uri(EX.atmosphere, "atmosphere")
    add_uri(EX.hasFurnaceType, "furnaceType")
    add_uri(EX.hasZone, "zone")
    add_uri(EX.slagType, "slagType")
    add_uri(EX.operationMode, "operationMode")

    return uri


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Local runner for refractory decision chain (CONSTRUCT rules).")
    p.add_argument("--base-ttl", default="out.ttl", help="Wikidata-derived TTL (from wikipedia_to_triples.py)")
    p.add_argument("--skip-base", action="store_true", help="Skip loading --base-ttl (recommended if it contains invalid xsd:dateTime)")
    p.add_argument("--ontology-ttl", default="refractory_ontology.ttl", help="Domain ontology TTL")
    p.add_argument("--kb-ttl", default="refractory_kb.ttl", help="Domain knowledge base TTL")
    p.add_argument("--query", default="recommend_construct.sparql", help="SPARQL CONSTRUCT query")
    p.add_argument("--workcondition", default="example_workcondition.json", help="WorkCondition JSON")
    p.add_argument("--out-ttl", default="recommendation.ttl", help="Output TTL for recommendation graph")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    base = Graph()
    if not args.skip_base:
        try:
            base.parse(args.base_ttl, format="turtle")
        except Exception as e:
            # Some Wikidata exports contain invalid xsd:dateTime lexical forms like 2020-00-00.
            # These can break rdflib parsing/casting. The decision chain does not strictly require out.ttl.
            print(f"[WARN] Failed to load base TTL ({args.base_ttl}): {e}")
            print("[WARN] Continue without base TTL. Use --skip-base to silence this.")
    base.parse(args.ontology_ttl, format="turtle")
    base.parse(args.kb_ttl, format="turtle")

    wc = _load_json(args.workcondition)
    add_workcondition(base, wc)

    with open(args.query, "r", encoding="utf-8") as f:
        q = f.read()

    out = Graph()
    res = base.query(q)
    # For CONSTRUCT/DESCRIBE, rdflib returns a SPARQLResult with a .graph attribute.
    if hasattr(res, "graph") and res.graph is not None:
        out = res.graph
    else:
        # Fallback: some versions may iterate triples directly.
        try:
            for t in res:
                # t should be (s, p, o)
                if isinstance(t, tuple) and len(t) == 3:
                    out.add(t)
        except Exception:
            pass

    out.serialize(destination=args.out_ttl, format="turtle")
    if len(out) == 0:
        wc_cnt = len(list(base.triples((None, RDF.type, EX.WorkCondition))))
        print(f"[WARN] 0 triples constructed. WorkCondition instances in dataset: {wc_cnt}")

        wc_uri = URIRef(EX + "wc-local")
        print("[DEBUG] wc-local triples:")
        for _, p, o in base.triples((wc_uri, None, None)):
            print(f"  {p.n3()} {o.n3()}")

        # Minimal sanity SELECT
        q1 = """
PREFIX ex: <http://example.com/refractory#>
SELECT ?tmax ?atm ?metalContact ?basicity ?ts ?abrasion ?furnaceType ?zone ?slagType ?operationMode ?campaign ?shellTemp WHERE {
    ex:wc-local a ex:WorkCondition ;
        ex:tmax ?tmax ;
        ex:atmosphere ?atm ;
        ex:metalContact ?metalContact .
    OPTIONAL { ex:wc-local ex:slagBasicity ?basicity . }
    OPTIONAL { ex:wc-local ex:thermalShockCyclesPerDay ?ts . }
    OPTIONAL { ex:wc-local ex:abrasionLevel ?abrasion . }
    OPTIONAL { ex:wc-local ex:hasFurnaceType ?furnaceType . }
    OPTIONAL { ex:wc-local ex:hasZone ?zone . }
    OPTIONAL { ex:wc-local ex:slagType ?slagType . }
    OPTIONAL { ex:wc-local ex:operationMode ?operationMode . }
    OPTIONAL { ex:wc-local ex:targetCampaignHeats ?campaign . }
    OPTIONAL { ex:wc-local ex:maxAllowShellTemp ?shellTemp . }
}
"""
        try:
            rows = list(base.query(q1))
            print(f"[DEBUG] sanity SELECT rows={len(rows)}")
            for r in rows[:5]:
                print("  ", r)
        except Exception as e:
            print(f"[DEBUG] sanity SELECT failed: {e}")

        # Candidate selection without failure-mechanism joins
        q2 = """
PREFIX ex: <http://example.com/refractory#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?ms ?workingSpec ?minT ?maxT ?allowedAtm WHERE {
    ex:wc-local a ex:WorkCondition ;
        ex:tmax ?tmax ;
        ex:atmosphere ?atm ;
        ex:metalContact ?metalContact .
    OPTIONAL { ex:wc-local ex:hasZone ?zone . }
    OPTIONAL { ex:wc-local ex:slagType ?slagType . }

    BIND(LCASE(STR(?metalContact)) AS ?mc)
    BIND(ex:NoChoice AS ?no)
    BIND(ex:NoZone AS ?noZone)
    BIND(ex:NoSlag AS ?noSlag)
    BIND(COALESCE(?zone, ?noZone) AS ?zoneV)
    BIND(COALESCE(?slagType, ?noSlag) AS ?slagV)
    BIND(
        IF(
            ?mc = "true" && ?atm != ex:Oxidizing && ?tmax >= "1400"^^xsd:double && (?zoneV = ex:SlagLine || ?zoneV = ex:ImpactZone || ?zoneV = ?noZone),
            ex:MS_MgOC,
            IF(
                ?mc = "true" && ?atm = ex:Oxidizing,
                ex:MS_MgOSpinel,
                IF(
                    ?mc = "false" && ?slagV = ex:AcidicSlag,
                    ex:MS_HighAlumina,
                    IF(?mc = "false", ex:MS_Al2O3Spinel, ?no)
                )
            )
        ) AS ?ms
    )
    BIND(
        IF(
            ?ms = ex:MS_MgOC,
            ex:Spec_MgOC_Working,
            IF(
                ?ms = ex:MS_MgOSpinel,
                ex:Spec_MgOSpinel_Working,
                IF(
                    ?ms = ex:MS_Al2O3Spinel,
                    ex:Spec_Al2O3Spinel_Working,
                    IF(?ms = ex:MS_HighAlumina, ex:Spec_HighAlumina_Working, ?no)
                )
            )
        ) AS ?workingSpec
    )
    FILTER(?ms != ?no && ?workingSpec != ?no)

    ?ms ex:minTmax ?minT .
    OPTIONAL { ?ms ex:maxTmax ?maxT . }
    ?ms ex:allowedAtmosphere ?allowedAtm .
    FILTER(?allowedAtm = ?atm)
    FILTER(NOT EXISTS { ?ms ex:notRecommendedInAtmosphere ?atm })
    FILTER(?slagV = ?noSlag || NOT EXISTS { ?ms ex:notRecommendedForSlagType ?slagV })
}
"""
        try:
            rows = list(base.query(q2))
            print(f"[DEBUG] candidate SELECT rows={len(rows)}")
            for r in rows[:10]:
                print("  ", r)
        except Exception as e:
            print(f"[DEBUG] candidate SELECT failed: {e}")

        # Probe: ensure KB triples are present
        print("[DEBUG] KB probe triples:")
        print("  MS_MgOC allowedAtmosphere:", list(base.objects(EX.MS_MgOC, EX.allowedAtmosphere))[:5])
        print("  MS_MgOC minTmax:", list(base.objects(EX.MS_MgOC, EX.minTmax))[:5])
        print("  MS_MgOC maxTmax:", list(base.objects(EX.MS_MgOC, EX.maxTmax))[:5])

        # Probe: boolean filter in SPARQL
        qb = """
    PREFIX ex: <http://example.com/refractory#>
    SELECT ?m WHERE {
      ex:wc-local ex:metalContact ?m .
      FILTER(LCASE(STR(?m)) = "true")
    }
    """
        try:
            rows = list(base.query(qb))
            print(f"[DEBUG] bool probe rows={len(rows)}")
            for r in rows[:5]:
                print("  ", r)
        except Exception as e:
            print(f"[DEBUG] bool probe failed: {e}")

        # Probe: numeric filter in SPARQL
        qn = """
    PREFIX ex: <http://example.com/refractory#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?t WHERE {
      ex:wc-local ex:tmax ?t .
      FILTER(?t >= "1400"^^xsd:double)
    }
    """
        try:
            rows = list(base.query(qn))
            print(f"[DEBUG] numeric probe rows={len(rows)}")
            for r in rows[:5]:
                print("  ", r)
        except Exception as e:
            print(f"[DEBUG] numeric probe failed: {e}")

        # Probe: atmosphere equality in SPARQL
        qa = """
    PREFIX ex: <http://example.com/refractory#>
    SELECT ?atm ?allowed WHERE {
      ex:wc-local ex:atmosphere ?atm .
      ex:MS_MgOC ex:allowedAtmosphere ?allowed .
      FILTER(?allowed = ?atm)
    }
    """
        try:
            rows = list(base.query(qa))
            print(f"[DEBUG] atmosphere probe rows={len(rows)}")
            for r in rows[:10]:
                print("  ", r)
        except Exception as e:
            print(f"[DEBUG] atmosphere probe failed: {e}")
    print(f"[DONE] wrote {args.out_ttl} (triples={len(out)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
