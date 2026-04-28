"""One-command pipeline for refractory knowledge graph crawling.

This script wires the existing Wikipedia crawler steps together:

1. Expand a refractory-related title vocabulary from Wikipedia.
2. Export the resolved Wikidata entities to Turtle (.ttl).
3. Sanitize invalid xsd:dateTime literals for GraphDB import.

The script is intentionally thin: it reuses the repo's existing crawler/exporter
entry points so the Windows frontend can launch a single process and stream logs.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parent


def _make_output_parent(path_text: str) -> None:
    path = Path(path_text).expanduser()
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True)


def _run_stage(title: str, command: List[str]) -> None:
    print(f"[STEP] {title}", flush=True)
    print(f"[CMD] {json.dumps(command, ensure_ascii=False)}", flush=True)
    subprocess.run(command, check=True)
    print(f"[OK] {title}", flush=True)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Expand refractory Wikipedia titles and export TTL.")
    p.add_argument("--lang", default="zh", help="Wikipedia language code")
    p.add_argument("--seeds-file", default="input_pages.txt", help="Seed title file used for vocabulary expansion")
    p.add_argument("--seeds", nargs="*", default=[], help="Extra seed article titles")
    p.add_argument("--categories", nargs="*", default=["Category:耐火材料"], help="Seed categories for traversal")
    p.add_argument("--keywords", nargs="*", default=["耐火", "耐火材料", "镁砖", "高铝砖", "刚玉"], help="Search keywords for title expansion")
    p.add_argument("--depth", type=int, default=1, help="Category traversal depth")
    p.add_argument("--per-keyword", type=int, default=50, help="Max titles per keyword search")
    p.add_argument("--max-titles", type=int, default=0, help="Hard limit for total expanded titles")
    p.add_argument(
        "--relevance",
        choices=["none", "keyword", "category", "hybrid"],
        default="hybrid",
        help="Relevance filter mode used during title expansion",
    )
    p.add_argument("--max-links-per-seed", type=int, default=200, help="Max outgoing links considered per seed page")
    p.add_argument("--no-links", action="store_true", help="Disable outgoing-links expansion")
    p.add_argument("--no-categories", action="store_true", help="Disable category traversal")
    p.add_argument("--expanded-out", default="expanded_pages.txt", help="Expanded title file")
    p.add_argument("--ttl-out", default="out.ttl", help="Output TTL file")
    p.add_argument("--sanitized-out", default="out.sanitized.ttl", help="Sanitized TTL file")
    p.add_argument("--skip-sanitize", action="store_true", help="Skip xsd:dateTime sanitation step")
    p.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between requests")
    p.add_argument(
        "--user-agent",
        default="GraphDB-Refractory-CrawlPipeline/0.1 (contact: you@example.com)",
        help="HTTP User-Agent sent to Wikipedia/Wikidata",
    )
    p.add_argument("--include-qualifiers", action="store_true", help="Export Wikidata qualifiers too")
    return p.parse_args(argv)


def _maybe_add_existing_file_arg(command: List[str], flag: str, path_text: str) -> None:
    if not path_text:
        return
    path = Path(path_text).expanduser()
    if path.exists():
        command.extend([flag, str(path)])
    else:
        print(f"[WARN] {flag} skipped because the file does not exist: {path}", flush=True)


def _normalize_output_path(path_text: str) -> str:
    path = Path(path_text).expanduser()
    _make_output_parent(str(path))
    return str(path)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    expander_script = ROOT / "expand_refractory_domain_knowledge.py"
    exporter_script = ROOT / "wikipedia_to_triples.py"
    sanitizer_script = ROOT / "sanitize_ttl_times.py"

    expanded_out = _normalize_output_path(args.expanded_out)
    ttl_out = _normalize_output_path(args.ttl_out)
    sanitized_out = _normalize_output_path(args.sanitized_out)

    if not expander_script.exists():
        print(f"[ERROR] Missing script: {expander_script}", file=sys.stderr)
        return 2
    if not exporter_script.exists():
        print(f"[ERROR] Missing script: {exporter_script}", file=sys.stderr)
        return 2
    if not args.skip_sanitize and not sanitizer_script.exists():
        print(f"[ERROR] Missing script: {sanitizer_script}", file=sys.stderr)
        return 2

    expand_cmd = [
        sys.executable,
        "-u",
        str(expander_script),
        "--lang",
        args.lang,
        "--out",
        expanded_out,
        "--depth",
        str(int(args.depth)),
        "--per-keyword",
        str(int(args.per_keyword)),
        "--max-titles",
        str(int(args.max_titles)),
        "--relevance",
        args.relevance,
        "--max-links-per-seed",
        str(int(args.max_links_per_seed)),
        "--sleep",
        str(float(args.sleep)),
        "--user-agent",
        args.user_agent,
    ]

    if args.no_links:
        expand_cmd.append("--no-links")
    if args.no_categories:
        expand_cmd.append("--no-categories")

    _maybe_add_existing_file_arg(expand_cmd, "--seeds-file", args.seeds_file)
    if args.seeds:
        expand_cmd.extend(["--seeds", *args.seeds])
    if args.categories:
        expand_cmd.extend(["--categories", *args.categories])
    if args.keywords:
        expand_cmd.extend(["--keywords", *args.keywords])

    _run_stage("扩展领域知识并生成标题列表", expand_cmd)

    export_cmd = [
        sys.executable,
        "-u",
        str(exporter_script),
        "--lang",
        args.lang,
        "--titles-file",
        expanded_out,
        "--out",
        ttl_out,
        "--sleep",
        str(float(args.sleep)),
        "--user-agent",
        args.user_agent,
    ]
    if args.include_qualifiers:
        export_cmd.append("--include-qualifiers")

    _run_stage("导出 Wikidata 三元组", export_cmd)

    if args.skip_sanitize:
        print("[STEP] 跳过清洗步骤", flush=True)
        print(f"[DONE] TTL: {ttl_out}", flush=True)
        return 0

    sanitize_cmd = [
        sys.executable,
        "-u",
        str(sanitizer_script),
        "--in",
        ttl_out,
        "--out",
        sanitized_out,
    ]
    _run_stage("清洗非法 xsd:dateTime", sanitize_cmd)

    print(f"[DONE] expanded={expanded_out}", flush=True)
    print(f"[DONE] ttl={ttl_out}", flush=True)
    print(f"[DONE] sanitized={sanitized_out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))