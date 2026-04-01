"""Expand refractory-material related Wikipedia title vocabulary.

Goal
- From a small seed list of Wikipedia titles (and optionally categories / keywords),
  discover more related article titles and write them to a text file (one title per line).
- Output format matches input_pages.txt used by wikipedia_to_triples.py.

Data source
- Wikipedia (MediaWiki API) only.

Expansion strategies (enabled by default)
1) Category traversal: given Category titles, BFS over categorymembers.
2) Page link expansion: collect outgoing links from seed pages.
3) Keyword search: use Wikipedia search API for given keywords.

Usage examples
  python expand_refractory_vocab.py --lang zh --seeds-file input_pages.txt --out expanded_pages.txt
  python expand_refractory_vocab.py --lang zh --seeds "耐火材料" --categories "Category:耐火材料" --out expanded_pages.txt
  python expand_refractory_vocab.py --lang zh --keywords "耐火" "刚玉" "镁砖" --out expanded_pages.txt

Notes
- Wikipedia categories in zh are typically like "Category:耐火材料".
- Rate limit is configurable via --sleep.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import requests


MEDIAWIKI_API = "https://{lang}.wikipedia.org/w/api.php"


@dataclass(frozen=True)
class WikiTarget:
    lang: str

    @property
    def api(self) -> str:
        return MEDIAWIKI_API.format(lang=self.lang)


class MediaWikiClient:
    def __init__(self, lang: str, user_agent: str, timeout_s: int = 30, sleep_s: float = 0.2):
        self._target = WikiTarget(lang=lang)
        self._timeout_s = timeout_s
        self._sleep_s = sleep_s
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._session.get(self._target.api, params=params, timeout=self._timeout_s)
        resp.raise_for_status()
        if self._sleep_s > 0:
            time.sleep(self._sleep_s)
        return resp.json()

    def resolve_redirect(self, title: str) -> str:
        """Resolve redirects and return canonical title if possible."""
        data = self._get(
            {
                "action": "query",
                "format": "json",
                "titles": title,
                "redirects": 1,
            }
        )
        pages = data.get("query", {}).get("pages", {})
        if not isinstance(pages, dict):
            return title
        for _, p in pages.items():
            t = p.get("title")
            if isinstance(t, str) and t:
                return t
        return title

    def category_members(self, category_title: str, cmtype: str = "page") -> Iterator[str]:
        """Yield member titles in a category.

        category_title must be like 'Category:耐火材料'.
        cmtype: page|subcat|file
        """
        cont: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "action": "query",
                "format": "json",
                "list": "categorymembers",
                "cmtitle": category_title,
                "cmtype": cmtype,
                "cmlimit": "max",
            }
            if cont:
                params["cmcontinue"] = cont
            data = self._get(params)
            members = data.get("query", {}).get("categorymembers", [])
            if isinstance(members, list):
                for m in members:
                    if isinstance(m, dict):
                        t = m.get("title")
                        if isinstance(t, str) and t:
                            yield t
            cont = data.get("continue", {}).get("cmcontinue")
            if not isinstance(cont, str) or not cont:
                break

    def page_links(self, title: str) -> Iterator[str]:
        """Yield outgoing link titles from a page (main namespace only)."""
        cont: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "action": "query",
                "format": "json",
                "titles": title,
                "prop": "links",
                "plnamespace": 0,
                "pllimit": "max",
                "redirects": 1,
            }
            if cont:
                params["plcontinue"] = cont
            data = self._get(params)
            pages = data.get("query", {}).get("pages", {})
            if isinstance(pages, dict):
                for _, p in pages.items():
                    links = p.get("links", [])
                    if isinstance(links, list):
                        for lk in links:
                            if isinstance(lk, dict):
                                t = lk.get("title")
                                if isinstance(t, str) and t:
                                    yield t
            cont = data.get("continue", {}).get("plcontinue")
            if not isinstance(cont, str) or not cont:
                break

    def search_titles(self, query: str, limit: int = 50) -> Iterator[str]:
        """Yield page titles from search results."""
        cont: Optional[int] = None
        remaining = max(1, limit)
        while remaining > 0:
            batch = min(remaining, 50)
            params: Dict[str, Any] = {
                "action": "query",
                "format": "json",
                "list": "search",
                "srsearch": query,
                "srlimit": batch,
                "srnamespace": 0,
            }
            if cont is not None:
                params["sroffset"] = cont
            data = self._get(params)
            results = data.get("query", {}).get("search", [])
            if isinstance(results, list):
                for r in results:
                    if isinstance(r, dict):
                        t = r.get("title")
                        if isinstance(t, str) and t:
                            yield t
                            remaining -= 1
                            if remaining <= 0:
                                break
            cont = data.get("continue", {}).get("sroffset")
            if not isinstance(cont, int):
                break

    def page_categories_batch(self, titles: List[str]) -> Dict[str, List[str]]:
        """Fetch categories for up to ~50 titles in one API call.

        Returns mapping: canonical_title -> list of category titles (e.g. 'Category:xxx').
        """
        titles = [t for t in titles if isinstance(t, str) and t.strip()]
        if not titles:
            return {}

        data = self._get(
            {
                "action": "query",
                "format": "json",
                "titles": "|".join(titles[:50]),
                "prop": "categories",
                "cllimit": "max",
                "redirects": 1,
            }
        )

        out: Dict[str, List[str]] = {}
        pages = data.get("query", {}).get("pages", {})
        if not isinstance(pages, dict):
            return out

        for _, p in pages.items():
            if not isinstance(p, dict):
                continue
            title = p.get("title")
            if not isinstance(title, str) or not title:
                continue
            cats = p.get("categories", [])
            cat_titles: List[str] = []
            if isinstance(cats, list):
                for c in cats:
                    if isinstance(c, dict):
                        ct = c.get("title")
                        if isinstance(ct, str) and ct:
                            cat_titles.append(ct)
            out[title] = cat_titles

        return out


DEFAULT_INCLUDE_TITLE_KEYWORDS = [
    # refractory / high-temp materials
    "耐火",
    "耐火材料",
    "耐火砖",
    "耐火泥",
    "耐火浇注料",
    "不定形耐火",
    "浇注料",
    # typical systems / phases
    "刚玉",
    "尖晶石",
    "莫来石",
    "镁砂",
    "菱镁",
    "高铝",
    "硅线石",
    "锆",
    "铬",
    # common compounds / raw minerals used in refractories
    "氧化铝",
    "氧化镁",
    "氧化锆",
    "氧化铬",
    "碳化硅",
    "氮化硅",
    "石墨",
    "铝土矿",
    "白云石",
    "镁橄榄石",
    "蛇纹岩",
    "叶蜡石",
    "锆英砂",
    "铬铁矿",
    # equipment/process context (optional but useful for downstream KG)
    "高炉",
    "电弧炉",
    "转炉",
    "玻璃窑",
    "水泥窑",
    "回转窑",
    "马弗炉",
]

DEFAULT_EXCLUDE_TITLE_KEYWORDS = [
    # obvious noise sources from link expansion
    "ONE PIECE",
    "角色",
    "海賊",
    "海贼",
    "列表",
    "街道",
    "镇",
    "鄉",
    "乡",
    "区",
    "县",
    "市",
    "省",
    "大学",
]

DEFAULT_INCLUDE_CATEGORY_KEYWORDS = [
    "耐火",
    "耐火材料",
    "耐火砖",
    "冶金",
    "炼钢",
    "钢铁",
    "工业炉",
    "窑",
    "陶瓷",
    "无机化合物",
    "氧化物",
    "矿物",
    "铝",
    "镁",
    "硅酸盐",
    "锆",
    "铬",
]


class RelevanceFilter:
    def __init__(
        self,
        client: MediaWikiClient,
        include_title_keywords: List[str],
        exclude_title_keywords: List[str],
        include_category_keywords: List[str],
        use_categories: bool = True,
        batch_size: int = 40,
    ):
        self._client = client
        self._include_title_keywords = [k for k in include_title_keywords if k]
        self._exclude_title_keywords = [k for k in exclude_title_keywords if k]
        self._include_category_keywords = [k for k in include_category_keywords if k]
        self._use_categories = use_categories
        self._batch_size = max(1, batch_size)
        self._cat_cache: Dict[str, List[str]] = {}

    def _title_hit(self, title: str) -> bool:
        t = title or ""
        if any(x in t for x in self._exclude_title_keywords):
            return False
        return any(k in t for k in self._include_title_keywords)

    def _cat_hit(self, cats: List[str]) -> bool:
        if not cats:
            return False
        joined = "\n".join(cats)
        return any(k in joined for k in self._include_category_keywords)

    def warm_categories(self, titles: List[str]) -> None:
        if not self._use_categories:
            return
        need = [t for t in titles if t not in self._cat_cache]
        for i in range(0, len(need), self._batch_size):
            batch = need[i : i + self._batch_size]
            try:
                mp = self._client.page_categories_batch(batch)
            except Exception:
                mp = {}
            # cache negative results too
            for t in batch:
                self._cat_cache.setdefault(t, mp.get(t, []))
            for k, v in mp.items():
                self._cat_cache[k] = v

    def is_relevant(self, title: str) -> bool:
        if self._title_hit(title):
            return True
        if not self._use_categories:
            return False
        if title not in self._cat_cache:
            self.warm_categories([title])
        return self._cat_hit(self._cat_cache.get(title, []))


_INVALID_PREFIXES = (
    "User:",
    "用户:",
    "Wikipedia:",
    "维基百科:",
    "Help:",
    "帮助:",
    "Template:",
    "模板:",
    "File:",
    "文件:",
    "Category:",
    "分类:",
    "Talk:",
    "讨论:",
    "Portal:",
    "主题:",
)


def _is_valid_article_title(title: str) -> bool:
    if not title or not isinstance(title, str):
        return False
    if title.startswith(_INVALID_PREFIXES):
        return False
    # common maintenance pages
    if title.startswith("List of "):
        return True
    # exclude purely numeric or extremely short noise
    if len(title) < 2:
        return False
    return True


def _read_titles_file(path: str) -> List[str]:
    titles: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            titles.append(line)
    return titles


def _write_titles_file(path: str, titles: List[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("# 由 expand_refractory_vocab.py 自动生成\n")
        for t in titles:
            f.write(t)
            f.write("\n")


def expand(
    client: MediaWikiClient,
    seeds: Iterable[str],
    categories: Iterable[str],
    keywords: Iterable[str],
    depth: int,
    max_titles: int,
    per_keyword: int,
    include_links: bool,
    include_category_pages: bool,
    relevance: str,
    include_title_keywords: List[str],
    exclude_title_keywords: List[str],
    include_category_keywords: List[str],
    max_links_per_seed: int,
) -> List[str]:
    """Return expanded, de-duplicated list of article titles."""

    out: List[str] = []
    seen: Set[str] = set()

    use_categories_for_filter = relevance in ("category", "hybrid")
    use_title_for_filter = relevance in ("keyword", "hybrid")
    filt = RelevanceFilter(
        client=client,
        include_title_keywords=include_title_keywords if use_title_for_filter else [""],
        exclude_title_keywords=exclude_title_keywords,
        include_category_keywords=include_category_keywords,
        use_categories=use_categories_for_filter,
    )

    def add_title(t: str, *, force: bool = False) -> None:
        if not _is_valid_article_title(t):
            return
        if t in seen:
            return
        if not force:
            try:
                if relevance != "none" and not filt.is_relevant(t):
                    return
            except Exception:
                # If filtering fails, be conservative: drop the title.
                return
        seen.add(t)
        out.append(t)

    # 0) seeds
    seed_titles: List[str] = []
    for s in seeds:
        if not s:
            continue
        # resolve redirects to reduce duplicates
        try:
            s2 = client.resolve_redirect(s)
        except Exception:
            s2 = s
        seed_titles.append(s2)
        add_title(s2, force=True)

    # 1) keyword search
    for kw in keywords:
        kw = (kw or "").strip()
        if not kw:
            continue
        try:
            cand = list(client.search_titles(kw, limit=per_keyword))
            filt.warm_categories(cand)
            for t in cand:
                add_title(t)
                if max_titles and len(out) >= max_titles:
                    return out
        except Exception as e:
            print(f"[WARN] search failed: {kw} ({e})", file=sys.stderr)

    # 2) category BFS
    if include_category_pages:
        # Track visited categories to avoid loops
        cat_seen: Set[str] = set()
        q: deque[Tuple[str, int]] = deque()
        for cat in categories:
            cat = (cat or "").strip()
            if not cat:
                continue
            if not (cat.startswith("Category:") or cat.startswith("分类:")):
                # normalize to English namespace; zh wiki accepts both but standardize for API
                cat = "Category:" + cat
            if cat in cat_seen:
                continue
            cat_seen.add(cat)
            q.append((cat, 0))

        while q:
            cat, d = q.popleft()
            # pages
            try:
                cand = list(client.category_members(cat, cmtype="page"))
                filt.warm_categories(cand)
                for t in cand:
                    add_title(t)
                    if max_titles and len(out) >= max_titles:
                        return out
            except Exception as e:
                print(f"[WARN] category page members failed: {cat} ({e})", file=sys.stderr)

            # subcategories
            if d < depth:
                try:
                    for subcat in client.category_members(cat, cmtype="subcat"):
                        if not isinstance(subcat, str) or not subcat:
                            continue
                        if subcat in cat_seen:
                            continue
                        cat_seen.add(subcat)
                        q.append((subcat, d + 1))
                except Exception as e:
                    print(f"[WARN] category subcats failed: {cat} ({e})", file=sys.stderr)

    # 3) outgoing links expansion
    if include_links:
        for s in seed_titles:
            try:
                # Outgoing links can be very noisy; cap and filter.
                cand: List[str] = []
                for t in client.page_links(s):
                    cand.append(t)
                    if max_links_per_seed > 0 and len(cand) >= max_links_per_seed:
                        break
                filt.warm_categories(cand)
                for t in cand:
                    add_title(t)
                    if max_titles and len(out) >= max_titles:
                        return out
            except Exception as e:
                print(f"[WARN] page links failed: {s} ({e})", file=sys.stderr)

    return out


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Expand refractory-related Wikipedia vocabulary (titles list).")
    p.add_argument("--lang", default="zh", help="Wikipedia language code")
    p.add_argument("--seeds", nargs="*", default=[], help="Seed Wikipedia article titles")
    p.add_argument("--seeds-file", default=None, help="Text file (one title per line) as seeds")
    p.add_argument(
        "--categories",
        nargs="*",
        default=["Category:耐火材料"],
        help="Seed categories for traversal (default: Category:耐火材料)",
    )
    p.add_argument("--keywords", nargs="*", default=["耐火", "耐火材料", "镁砖", "高铝砖", "刚玉"], help="Search keywords")
    p.add_argument("--depth", type=int, default=1, help="Category traversal depth for subcategories")
    p.add_argument("--per-keyword", type=int, default=50, help="Max titles per keyword search")
    p.add_argument("--max-titles", type=int, default=0, help="Hard limit for total output titles (0 means unlimited)")
    p.add_argument(
        "--relevance",
        choices=["none", "keyword", "category", "hybrid"],
        default="hybrid",
        help="Relevance filter mode: none|keyword|category|hybrid (default: hybrid)",
    )
    p.add_argument(
        "--include-title-keywords",
        nargs="*",
        default=DEFAULT_INCLUDE_TITLE_KEYWORDS,
        help="Keep titles containing any of these keywords (used in keyword/hybrid mode)",
    )
    p.add_argument(
        "--exclude-title-keywords",
        nargs="*",
        default=DEFAULT_EXCLUDE_TITLE_KEYWORDS,
        help="Drop titles containing any of these keywords (always applied)",
    )
    p.add_argument(
        "--include-category-keywords",
        nargs="*",
        default=DEFAULT_INCLUDE_CATEGORY_KEYWORDS,
        help="Keep titles whose page categories contain any of these keywords (used in category/hybrid mode)",
    )
    p.add_argument(
        "--max-links-per-seed",
        type=int,
        default=200,
        help="Max outgoing links to consider per seed page (default: 200)",
    )
    p.add_argument("--no-links", action="store_true", help="Disable outgoing-links expansion")
    p.add_argument("--no-categories", action="store_true", help="Disable category traversal")
    p.add_argument("--out", default="expanded_pages.txt", help="Output file")
    p.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between requests")
    p.add_argument(
        "--user-agent",
        default="GraphDB-Refractory-VocabExpander/0.1 (contact: you@example.com)",
        help="HTTP User-Agent (please include contact for production)",
    )
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    seeds: List[str] = list(args.seeds)
    if args.seeds_file:
        seeds.extend(_read_titles_file(args.seeds_file))

    # de-dup seeds while preserving order
    seed_seen: Set[str] = set()
    seeds2: List[str] = []
    for s in seeds:
        s = (s or "").strip()
        if not s or s.startswith("#"):
            continue
        if s in seed_seen:
            continue
        seed_seen.add(s)
        seeds2.append(s)
    seeds = seeds2

    client = MediaWikiClient(lang=args.lang, user_agent=args.user_agent, sleep_s=args.sleep)

    titles = expand(
        client=client,
        seeds=seeds,
        categories=[] if args.no_categories else args.categories,
        keywords=args.keywords,
        depth=max(0, int(args.depth)),
        max_titles=max(0, int(args.max_titles)),
        per_keyword=max(1, int(args.per_keyword)),
        include_links=not bool(args.no_links),
        include_category_pages=not bool(args.no_categories),
        relevance=str(args.relevance),
        include_title_keywords=list(args.include_title_keywords or []),
        exclude_title_keywords=list(args.exclude_title_keywords or []),
        include_category_keywords=list(args.include_category_keywords or []),
        max_links_per_seed=int(args.max_links_per_seed),
    )

    # stable sort for easier diffing: keep insertion order but ensure deterministic
    # We keep insertion order because it roughly reflects relevance.

    _write_titles_file(args.out, titles)
    print(f"[DONE] wrote {args.out} (titles={len(titles)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
