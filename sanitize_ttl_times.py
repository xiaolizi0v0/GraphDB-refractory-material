"""Sanitize invalid xsd:dateTime literals in a Turtle file.

Why:
- Wikidata time values can appear like 2020-00-00T00:00:00Z.
- If exported as "..."^^xsd:dateTime, rdflib (and some tools) may throw:
  ValueError: month must be in 1..12

This script does a *text-level* rewrite so it works even if rdflib cannot parse the file.
It converts invalid dateTime literals to xsd:string.

Usage:
  python sanitize_ttl_times.py --in out.ttl --out out.sanitized.ttl
"""

from __future__ import annotations

import argparse
import re


# Matches a Wikidata-like lexical form inside a Turtle literal with xsd:dateTime.
# Examples seen in exports:
#   "1968-00-00T00:00:00Z"^^xsd:dateTime
#   "-15000-00-00T00:00:00Z"^^xsd:dateTime
#   "+2020-12-31T00:00:00Z"^^xsd:dateTime
#   "0880-00-00T00:00:00Z"^^xsd:dateTime
#   "1909-01-01T00:00:00+00:00"^^xsd:dateTime
_DT = re.compile(
    r"\"(?P<dt>[+-]?\d{4,}-(?P<mm>\d{2})-(?P<dd>\d{2})T[^\"]*)\"\^\^xsd:dateTime"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sanitize invalid xsd:dateTime literals in Turtle")
    p.add_argument("--in", dest="inp", default="out.ttl", help="Input TTL")
    p.add_argument("--out", dest="out", default="out.sanitized.ttl", help="Output TTL")
    return p.parse_args()


def _is_valid_mm_dd(mm: int, dd: int) -> bool:
    # We intentionally keep this coarse: rdflib crashes on mm=00/13 etc.
    # Day-of-month precision (Feb 30) isn't needed for our goal of avoiding parse failures.
    return 1 <= mm <= 12 and 1 <= dd <= 31


def main() -> int:
    args = parse_args()
    with open(args.inp, "r", encoding="utf-8") as f:
        text = f.read()

    def repl(m: re.Match[str]) -> str:
        mm = int(m.group("mm"))
        dd = int(m.group("dd"))
        dt = m.group("dt")
        if _is_valid_mm_dd(mm, dd):
            return f'"{dt}"^^xsd:dateTime'
        return f'"{dt}"^^xsd:string'

    new_text, n = _DT.subn(repl, text)

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(new_text)

    print(f"[DONE] wrote {args.out} (rewrote={n})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
