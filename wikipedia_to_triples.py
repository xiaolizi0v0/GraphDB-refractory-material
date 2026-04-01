"""Wikipedia -> Wikidata -> RDF triples (TTL) exporter for GraphDB.

Why Wikidata?
- Wikipedia infobox/正文结构不稳定；Wikidata 语义结构更适合直接生成三元组。
- 通过 Wikipedia title 获取对应 Wikidata 实体 (Q-id)，再把声明(Claims)转换为 RDF。

Output:
- Turtle (.ttl) file, importable into GraphDB.

Usage examples:
  python wikipedia_to_triples.py --lang zh --titles "耐火材料" "刚玉" --out out.ttl
  python wikipedia_to_triples.py --lang en --titles "Refractory" "Alumina" --out out.ttl
  python wikipedia_to_triples.py --lang zh --titles-file input_pages.txt --out out.ttl

Notes:
- This script uses public APIs and rate-limits requests.
- Please set a meaningful --user-agent with contact info for production use.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD


WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
MEDIAWIKI_API = "https://{lang}.wikipedia.org/w/api.php"

WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
SCHEMA = Namespace("http://schema.org/")


@dataclass(frozen=True)
class PageRef:
    lang: str
    title: str

    @property
    def wikipedia_url(self) -> str:
        safe_title = self.title.replace(" ", "_")
        return f"https://{self.lang}.wikipedia.org/wiki/{safe_title}"


class WikiClient:
    def __init__(self, user_agent: str, timeout_s: int = 30, sleep_s: float = 0.2):
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})
        self._timeout_s = timeout_s
        self._sleep_s = sleep_s

    def _get(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._session.get(url, params=params, timeout=self._timeout_s)
        resp.raise_for_status()
        if self._sleep_s > 0:
            time.sleep(self._sleep_s)
        return resp.json()

    def wikipedia_title_to_qid(self, page: PageRef) -> Optional[str]:
        """Resolve Wikipedia title to Wikidata Q-id via MediaWiki API."""
        data = self._get(
            MEDIAWIKI_API.format(lang=page.lang),
            params={
                "action": "query",
                "format": "json",
                "titles": page.title,
                "prop": "pageprops|info",
                "inprop": "url",
                "redirects": 1,
            },
        )
        pages = data.get("query", {}).get("pages", {})
        for _, p in pages.items():
            props = p.get("pageprops", {}) or {}
            qid = props.get("wikibase_item")
            if isinstance(qid, str) and qid.startswith("Q"):
                return qid
        return None

    def fetch_wikidata_entity(self, qid: str) -> Dict[str, Any]:
        data = self._get(WIKIDATA_ENTITY.format(qid=qid), params={"format": "json"})
        entities = data.get("entities", {})
        ent = entities.get(qid)
        if not ent:
            raise ValueError(f"Wikidata entity not found: {qid}")
        return ent


def _best_label(entity: Dict[str, Any], lang: str) -> Optional[str]:
    labels = entity.get("labels", {}) or {}
    if lang in labels and isinstance(labels[lang], dict):
        return labels[lang].get("value")
    if "en" in labels and isinstance(labels["en"], dict):
        return labels["en"].get("value")
    return None


def _best_description(entity: Dict[str, Any], lang: str) -> Optional[str]:
    desc = entity.get("descriptions", {}) or {}
    if lang in desc and isinstance(desc[lang], dict):
        return desc[lang].get("value")
    if "en" in desc and isinstance(desc["en"], dict):
        return desc["en"].get("value")
    return None


def _parse_time_wikidata(value: str) -> Tuple[str, URIRef]:
    """Wikidata time format: +YYYY-MM-DDT00:00:00Z (may contain precision).

    Return ISO dateTime string and datatype.
    """
    # Wikidata time lexical form may contain unknown month/day as 00, e.g. +2020-00-00T00:00:00Z
    # rdflib (and some RDF engines) reject such values for xsd:dateTime.
    raw = value
    if raw.startswith("+"):
        raw = raw[1:]
    # If month/day are 00 (or otherwise invalid), keep as xsd:string.
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})T", raw)
    if not m:
        return raw, XSD.string
    mm = int(m.group(2))
    dd = int(m.group(3))
    if mm < 1 or mm > 12 or dd < 1 or dd > 31:
        return raw, XSD.string
    return raw, XSD.dateTime


def _mk_literal_from_datavalue(dv: Dict[str, Any]) -> Optional[Literal]:
    v = dv.get("value")
    dv_type = dv.get("type")

    if dv_type == "string":
        if isinstance(v, str):
            return Literal(v)
        return None

    if dv_type == "monolingualtext":
        if isinstance(v, dict) and isinstance(v.get("text"), str):
            lang = v.get("language")
            if isinstance(lang, str) and lang:
                return Literal(v["text"], lang=lang)
            return Literal(v["text"])
        return None

    if dv_type == "quantity":
        if isinstance(v, dict) and "amount" in v:
            amount = v.get("amount")
            unit = v.get("unit")
            # amount is string like "+123.4" or "-1"
            if isinstance(amount, str):
                try:
                    num = float(amount)
                except ValueError:
                    return Literal(amount)
                lit = Literal(num, datatype=XSD.double)
                # If unit is a wikidata URI, we can't attach it directly to a Literal.
                # We keep numeric literal; caller may add extra unit triples if desired.
                return lit
            return None
        return None

    if dv_type == "time":
        if isinstance(v, dict) and isinstance(v.get("time"), str):
            iso, dt = _parse_time_wikidata(v["time"])
            return Literal(iso, datatype=dt)
        return None

    if dv_type == "globecoordinate":
        if isinstance(v, dict) and "latitude" in v and "longitude" in v:
            try:
                lat = float(v["latitude"])
                lon = float(v["longitude"])
            except Exception:
                return None
            # WKT literal
            return Literal(f"POINT({lon} {lat})", datatype=URIRef("http://www.opengis.net/ont/geosparql#wktLiteral"))
        return None

    # Unsupported types: commonsMedia, math, etc.
    return None


def _mk_object_from_snak(snak: Dict[str, Any]) -> Optional[URIRef | Literal]:
    dv = snak.get("datavalue")
    if not isinstance(dv, dict):
        return None

    dv_type = dv.get("type")
    if dv_type == "wikibase-entityid":
        v = dv.get("value")
        if isinstance(v, dict):
            eid = v.get("id")
            if isinstance(eid, str) and re.fullmatch(r"[QP]\d+", eid):
                return WD[eid]
        return None

    return _mk_literal_from_datavalue(dv)


def entity_to_rdf(
    g: Graph,
    entity: Dict[str, Any],
    lang: str,
    wikipedia_url: Optional[str] = None,
    include_qualifiers: bool = False,
) -> URIRef:
    """Convert a Wikidata entity JSON to RDF triples in graph g.

    - Subject is the entity IRI: http://www.wikidata.org/entity/Qxxx
    - Predicates use direct property namespace: http://www.wikidata.org/prop/direct/Pxxx

    Qualifiers and references are optional; by default we only emit direct claims.
    """

    qid = entity.get("id")
    if not isinstance(qid, str) or not qid.startswith("Q"):
        raise ValueError("Invalid entity JSON: missing id")

    subj = WD[qid]

    label = _best_label(entity, lang)
    if label:
        g.add((subj, RDFS.label, Literal(label, lang=lang)))

    desc = _best_description(entity, lang)
    if desc:
        g.add((subj, SCHEMA.description, Literal(desc, lang=lang)))

    if wikipedia_url:
        g.add((subj, SCHEMA.sameAs, URIRef(wikipedia_url)))

    # instance of (P31) sometimes used as type; we also keep it as predicate.
    claims = entity.get("claims", {}) or {}
    if not isinstance(claims, dict):
        return subj

    for pid, claim_list in claims.items():
        if not isinstance(pid, str) or not pid.startswith("P"):
            continue
        if not isinstance(claim_list, list):
            continue

        pred = WDT[pid]
        for claim in claim_list:
            if not isinstance(claim, dict):
                continue
            mainsnak = claim.get("mainsnak")
            if not isinstance(mainsnak, dict):
                continue
            if mainsnak.get("snaktype") != "value":
                continue

            obj = _mk_object_from_snak(mainsnak)
            if obj is None:
                continue

            g.add((subj, pred, obj))

            if include_qualifiers:
                qualifiers = claim.get("qualifiers")
                if isinstance(qualifiers, dict):
                    # Minimal qualifier support: emit as blank-node reification
                    # subj --schema:statement--> _:stmt
                    stmt = URIRef(f"urn:wdstmt:{qid}:{pid}:{claim.get('id','')}")
                    g.add((subj, SCHEMA.statement, stmt))
                    g.add((stmt, RDF.subject, subj))
                    g.add((stmt, RDF.predicate, pred))
                    g.add((stmt, RDF.object, obj if isinstance(obj, URIRef) else obj))
                    for qpid, qsnaks in qualifiers.items():
                        if not (isinstance(qpid, str) and qpid.startswith("P")):
                            continue
                        if not isinstance(qsnaks, list):
                            continue
                        qpred = WDT[qpid]
                        for qsnak in qsnaks:
                            if not isinstance(qsnak, dict):
                                continue
                            if qsnak.get("snaktype") != "value":
                                continue
                            qobj = _mk_object_from_snak(qsnak)
                            if qobj is None:
                                continue
                            g.add((stmt, qpred, qobj))

    return subj


def _read_titles_file(path: str) -> List[str]:
    titles: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            titles.append(line)
    return titles


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export Wikipedia pages (via Wikidata) to RDF triples (Turtle).")
    p.add_argument("--lang", default="zh", help="Wikipedia language code, e.g. zh/en")
    p.add_argument("--titles", nargs="*", default=[], help="Wikipedia page titles")
    p.add_argument("--titles-file", default=None, help="Text file with one title per line")
    p.add_argument("--out", default="out.ttl", help="Output TTL file")
    p.add_argument(
        "--user-agent",
        default="GraphDB-Refractory-Assistant/0.1 (contact: you@example.com)",
        help="HTTP User-Agent (please include contact for production)",
    )
    p.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between requests")
    p.add_argument("--include-qualifiers", action="store_true", help="Also export qualifiers using minimal reification")
    p.add_argument("--max-pages", type=int, default=0, help="Limit number of processed pages (0 means no limit)")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    titles: List[str] = list(args.titles)
    if args.titles_file:
        titles.extend(_read_titles_file(args.titles_file))

    # de-dup while preserving order
    seen: set[str] = set()
    titles2: List[str] = []
    for t in titles:
        if t not in seen:
            seen.add(t)
            titles2.append(t)
    titles = titles2

    if not titles:
        print("No titles provided. Use --titles or --titles-file.", file=sys.stderr)
        return 2

    if args.max_pages and args.max_pages > 0:
        titles = titles[: args.max_pages]

    client = WikiClient(user_agent=args.user_agent, sleep_s=args.sleep)

    g = Graph()
    g.bind("wd", WD)
    g.bind("wdt", WDT)
    g.bind("schema", SCHEMA)
    g.bind("rdfs", RDFS)

    missing: List[str] = []

    for title in titles:
        page = PageRef(lang=args.lang, title=title)
        try:
            qid = client.wikipedia_title_to_qid(page)
        except Exception as e:
            print(f"[ERROR] Resolve QID failed: {title} ({e})", file=sys.stderr)
            missing.append(title)
            continue

        if not qid:
            print(f"[WARN] No Wikidata item found for: {title}")
            missing.append(title)
            continue

        try:
            ent = client.fetch_wikidata_entity(qid)
            entity_to_rdf(
                g,
                ent,
                lang=args.lang,
                wikipedia_url=page.wikipedia_url,
                include_qualifiers=bool(args.include_qualifiers),
            )
            print(f"[OK] {title} -> {qid}")
        except Exception as e:
            print(f"[ERROR] Export failed: {title} ({qid}) ({e})", file=sys.stderr)
            missing.append(title)

    g.serialize(destination=args.out, format="turtle")

    if missing:
        miss_path = re.sub(r"\.ttl$", "", args.out) + ".missing.json"
        with open(miss_path, "w", encoding="utf-8") as f:
            json.dump({"missing_titles": missing, "lang": args.lang}, f, ensure_ascii=False, indent=2)
        print(f"[WARN] Missing/failed titles: {len(missing)}; saved to {miss_path}")

    print(f"[DONE] Wrote: {args.out} (triples={len(g)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
