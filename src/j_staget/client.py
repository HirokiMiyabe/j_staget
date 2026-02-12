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

# target_word を入れる先（従来互換）
ALLOWED_FIELDS = {"article", "abst", "text", "keyword"}

# service=3 で「検索語として成立する」代表的パラメータ（ERR_012回避用）
SEARCH_PARAM_KEYS = {
    "material",
    "article",
    "author",
    "affil",
    "keyword",
    "abst",
    "text",
    "issn",
    "cdjournal",  # ここが公式
}


class JStageAPIError(RuntimeError):
    """Raised when J-STAGE API request fails or returns unexpected content."""


@dataclass(frozen=True)
class FetchResult:
    df: pl.DataFrame
    total_results: int | None


def _q(s: str) -> str:
    return urllib.parse.quote(s, safe="")


def _get_result_status(root: etree._Element) -> str | None:
    """
    <result><status> を取得。
    正常系では result 要素が無いことがあるので、その場合は None を返す。
    """
    status = root.xpath("//*[local-name()='result']/*[local-name()='status']/text()")
    if status:
        s = status[0].strip()
        return s if s else None
    return None


def _get_total_results_first(root: etree._Element) -> int | None:
    """
    <opensearch:totalResults> を取得。
    ページによって None/空になることがあるので、値があるときだけ int で返す。
    """
    # 正攻法（namespaces）
    tr = root.xpath("//opensearch:totalResults/text()", namespaces=NS)
    if tr:
        t = tr[0].strip()
        if t:
            return int(t)

    # フォールバック（prefix/NS差異対策）
    tr2 = root.xpath("//*[local-name()='totalResults']/text()")
    if tr2:
        t2 = tr2[0].strip()
        if t2:
            return int(t2)

    return None


def fetch(
    target_word: str | None = None,
    *,
    year: int = 1950,
    field: str = "article",
    max_records: int = 20000,
    sleep: float = 5.0,
    step: int = DEFAULT_STEP,
    timeout: float = 30.0,
    session: requests.Session | None = None,
    # 追加: 検索条件（指定されたものだけクエリに乗せる）
    material: str | None = None,
    author: str | None = None,
    affil: str | None = None,
    issn: str | None = None,  # ISSN
    cdjournal: str | None = None,
) -> FetchResult:
    """
    Fetch records from J-STAGE Search API (service=3).
    """
    if field not in ALLOWED_FIELDS:
        raise ValueError(f"field must be one of {sorted(ALLOWED_FIELDS)}")
    if max_records <= 0:
        raise ValueError("max_records must be > 0")
    if step <= 0:
        raise ValueError("step must be > 0")

    # まず検索条件（start/count以外）を組み立て
    base_params: dict[str, str] = {
        "service": "3",
        "pubyearfrom": str(int(year)),
    }

    # 既存互換: target_word + field
    if target_word is not None and target_word.strip():
        base_params[field] = target_word.strip()

    # 追加条件
    if material:
        base_params["material"] = material
    if author:
        base_params["author"] = author
    if affil:
        base_params["affil"] = affil
    if issn:
        base_params["issn"] = issn
    if cdjournal:
        base_params["cdjournal"] = cdjournal

    # 「検索語が何もない」状態を弾く（yearだけ等）
    if not any(k in base_params for k in SEARCH_PARAM_KEYS):
        raise ValueError(
            "At least one search parameter must be provided: "
            "target_word (with field), material, author, affil, issn, or cdjournal."
        )

    all_data: list[dict] = []

    owns_session = session is None
    if owns_session:
        session = requests.Session()

    try:
        start_idx = 1
        total_results: int | None = None

        while True:
            params = dict(base_params)
            params["start"] = str(start_idx)
            params["count"] = str(int(step))

            query_str = "&".join(f"{k}={_q(v)}" for k, v in params.items())
            url = f"{API_URL}?{query_str}"

            try:
                r = session.get(url, timeout=timeout)
                r.raise_for_status()
            except requests.RequestException as e:
                raise JStageAPIError(f"Request failed: {e}") from e

            try:
                root = etree.fromstring(r.content)
            except Exception as e:
                raise JStageAPIError("Failed to parse XML response") from e

            # ERR_001 のときは「条件不成立」なので即停止して 0 件として返す
            status = _get_result_status(root)
            if status == "ERR_001":
                return FetchResult(df=pl.DataFrame([]), total_results=0)

            # totalResults は「最初に取れた値」を固定（最後のページで None になっても上書きしない）
            if total_results is None:
                total_results = _get_total_results_first(root)

            entries = root.xpath("//atom:entry", namespaces=NS)

            # 初回ページで 0 件
            if start_idx == 1 and not entries:
                break

            # entry が空
            if not entries:
                break

            prev_len = len(all_data)

            for entry in entries:
                all_data.append(
                    {
                        "author": authors_local(entry),
                        "article_title": pick_ja_or_first_tag_local(entry, "article_title"),
                        "material_title": pick_ja_or_first_tag_local(entry, "material_title"),
                        "cdjournal": get_first(entry, "atom:cdjournal"),
                        "p_issn": get_first(entry, "prism:issn"),
                        "o_issn": get_first(entry, "prism:eIssn"),
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

            # データが増えなかった（異常系）
            if len(all_data) == prev_len:
                break

            if len(all_data) >= max_records:
                break

            start_idx += step
            if total_results is not None and start_idx > total_results:
                break

            time.sleep(float(sleep))

        df = pl.DataFrame(all_data)

        if not df.is_empty():
            df = df.with_columns(
                [
                    pl.col("p_issn").cast(pl.Utf8, strict=False),
                    pl.col("o_issn").cast(pl.Utf8, strict=False),
                    pl.col("cdjournal").cast(pl.Utf8, strict=False),
                    pl.col("author").cast(pl.List(pl.Utf8), strict=False),
                    pl.col("pubyear").cast(pl.Int32, strict=False),
                    pl.col("starting_page").cast(pl.Int32, strict=False),
                    pl.col("ending_page").cast(pl.Int32, strict=False),
                    pl.when(pl.col("doi").is_not_null())
                    .then(pl.concat_str([pl.lit("https://doi.org/"), pl.col("doi")]))
                    .otherwise(None)
                    .alias("url_doi"),
                ]
            )

        # 保険：total_results が最後まで取れなかった場合は「取得件数」を入れる（Noneのままより扱いやすい）
        if total_results is None:
            total_results = len(all_data)

        return FetchResult(df=df, total_results=total_results)

    finally:
        if owns_session and session is not None:
            session.close()
