from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass

import polars as pl
import requests
from lxml import etree

API_URL = "https://api.jstage.jst.go.jp/searchapi/do"

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}


class JStageAPIError(RuntimeError):
    """J-STAGE API request/parse error."""


@dataclass(frozen=True)
class FetchResult:
    df: pl.DataFrame
    total_results: int | None


def _texts_local(entry: etree._Element, xpath_expr: str) -> list[str]:
    vals = entry.xpath(xpath_expr)
    out: list[str] = []
    for v in vals:
        if isinstance(v, str):
            t = v.strip()
            if t:
                out.append(t)
        else:
            t = getattr(v, "text", None)
            if t and t.strip():
                out.append(t.strip())
    return out


def _first_local(entry: etree._Element, xpath_expr: str) -> str | None:
    vals = _texts_local(entry, xpath_expr)
    return vals[0] if vals else None


def _get_first_ns(entry: etree._Element, xpath_query: str) -> str | None:
    nodes = entry.xpath(xpath_query, namespaces=NS)
    texts = [n.text for n in nodes if getattr(n, "text", None)]
    return texts[0] if texts else None


def _pick_ja_or_first_tag_local(entry: etree._Element, tag: str) -> str | None:
    ja = _first_local(entry, f"./*[local-name()='{tag}']/*[local-name()='ja']/text()")
    if ja:
        return ja
    any_text = _first_local(entry, f"./*[local-name()='{tag}']//text()")
    return any_text


def _authors_local(entry: etree._Element) -> list[str]:
    ja = _texts_local(entry, "./*[local-name()='author']/*[local-name()='ja']/*[local-name()='name']/text()")
    if ja:
        return ja
    return _texts_local(entry, "./*[local-name()='author']/*[local-name()='name']/text()")


def fetch(
    target_word: str,
    *,
    year: int = 0,
    field: str = "article",
    max_records: int = 20000,
    sleep: float = 0.1,
    step: int = 1000,
    timeout: float = 30.0,
    session: requests.Session | None = None,
) -> FetchResult:
    """
    Fetch records from J-STAGE Search API (service=3).

    Returns: FetchResult(df, total_results)
    df: Polars DataFrame
    total_results: openSearch totalResults (if available)
    """
    if not target_word or not target_word.strip():
        raise ValueError("target_word must be a non-empty string")
    if field not in {"article", "abst", "text"}:
        raise ValueError("field must be one of {'article','abst','text'}")
    if max_records <= 0:
        raise ValueError("max_records must be > 0")
    if step <= 0:
        raise ValueError("step must be > 0")

    query = urllib.parse.quote(target_word, safe="")
    all_data: list[dict] = []

    owns_session = session is None
    if owns_session:
        session = requests.Session()

    try:
        start_idx = 1
        total_results: int | None = None

        while True:
            url = (
                f"{API_URL}?service=3&{field}={query}&pubyearfrom={int(year)}"
                f"&start={start_idx}&count={int(step)}"
            )

            try:
                r = session.get(url, timeout=timeout)
                r.raise_for_status()
            except requests.RequestException as e:
                raise JStageAPIError(f"Request failed: {e}") from e

            try:
                root = etree.fromstring(r.content)
            except Exception as e:
                raise JStageAPIError("Failed to parse XML response") from e

            if total_results is None:
                tr = root.xpath("//opensearch:totalResults", namespaces=NS)
                total_results = int(tr[0].text) if tr and tr[0].text else None

            entries = root.xpath("//atom:entry", namespaces=NS)
            if not entries:
                break

            for entry in entries:
                all_data.append(
                    {
                        "author": _authors_local(entry),
                        "article_title": _pick_ja_or_first_tag_local(entry, "article_title"),
                        "material_title": _pick_ja_or_first_tag_local(entry, "material_title"),
                        "article_link": _pick_ja_or_first_tag_local(entry, "article_link"),
                        "pubyear": _get_first_ns(entry, "atom:pubyear"),
                        "doi": _get_first_ns(entry, "prism:doi"),
                        "volume": _get_first_ns(entry, "prism:volume"),
                        "cdvols": _first_local(entry, "./*[local-name()='cdvols']/text()"),
                        "number": _get_first_ns(entry, "prism:number"),
                        "starting_page": _get_first_ns(entry, "prism:startingPage"),
                        "ending_page": _get_first_ns(entry, "prism:endingPage"),
                    }
                )
                if len(all_data) >= max_records:
                    break

            if len(all_data) >= max_records:
                break

            start_idx += step
            if total_results and start_idx > total_results:
                break

            time.sleep(float(sleep))

        df = pl.DataFrame(all_data)

        if not df.is_empty():
            df = df.with_columns(
                [
                    pl.col("author").cast(pl.List(pl.Utf8), strict=False),
                    pl.col("pubyear").cast(pl.Int32, strict=False),
                    pl.col("starting_page").cast(pl.Int32, strict=False),
                    pl.col("ending_page").cast(pl.Int32, strict=False),
                ]
            )

        return FetchResult(df=df, total_results=total_results)

    finally:
        if owns_session and session is not None:
            session.close()
