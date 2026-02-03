from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass

import polars as pl
import requests
from lxml import etree

from ._xml import NS, authors_local, get_first, pick_ja_or_first_tag_local

API_URL = "https://api.jstage.jst.go.jp/searchapi/do"
DEFAULT_STEP = 1000
ALLOWED_FIELDS = {"article", "abst", "text"}


class JStageAPIError(RuntimeError):
    """Raised when J-STAGE API request fails or returns unexpected content."""


@dataclass(frozen=True)
class FetchResult:
    df: pl.DataFrame
    total_results: int | None


def fetch(
    target_word: str,
    *,
    year: int = 1950,
    field: str = "article",
    max_records: int = 20000,
    sleep: float = 5.0,
    step: int = DEFAULT_STEP,
    timeout: float = 30.0,
    session: requests.Session | None = None,
) -> FetchResult:
    """
    Fetch records from J-STAGE Search API (service=3).

    Parameters
    ----------
    target_word : str
        Search keyword.
    year : int
        pubyearfrom
    field : {"article","abst","text"}
        Query field.
    max_records : int
        Hard cap to avoid runaway downloads.
    sleep : float
        Seconds to sleep between requests.
    step : int
        Records per request (API supports up to 1000 for service=3 in typical usage).
    timeout : float
        Request timeout in seconds.
    session : requests.Session | None
        Provide a session for connection pooling.

    Returns
    -------
    FetchResult(df, total_results)
    """
    if not target_word or not target_word.strip():
        raise ValueError("target_word must be a non-empty string")
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"field must be one of {sorted(ALLOWED_FIELDS)}")
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
                        "author": authors_local(entry),
                        "article_title": pick_ja_or_first_tag_local(entry, "article_title"),
                        "material_title": pick_ja_or_first_tag_local(entry, "material_title"),
                        "article_link": pick_ja_or_first_tag_local(entry, "article_link"),
                        "pubyear": get_first(entry, "atom:pubyear"),
                        "doi": get_first(entry, "prism:doi"),
                        "volume": get_first(entry, "prism:volume"),
                        "cdvols": entry.xpath("./*[local-name()='cdvols']/text()")[0].strip()
                        if entry.xpath("./*[local-name()='cdvols']/text()")
                        else None,
                        "number": get_first(entry, "prism:number"),
                        "starting_page": get_first(entry, "prism:startingPage"),
                        "ending_page": get_first(entry, "prism:endingPage"),
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
                    pl.when(pl.col("doi").is_not_null())
                        .then(pl.concat_str([pl.lit("https://"), pl.col("doi")]))
                        .otherwise(None)
                        .alias("url_doi")
                ]
            )

        return FetchResult(df=df, total_results=total_results)

    finally:
        if owns_session and session is not None:
            session.close()
