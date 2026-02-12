from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl  # ← これを追加

from .client import fetch


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="j-staget", description="J-STAGE Search API (service=3) client")
    p.add_argument("query", help="search keyword")
    p.add_argument("--year", type=int, default=1950)
    p.add_argument("--field", choices=["article", "abst", "text"], default="article")
    p.add_argument("--max-records", type=int, default=20000)
    p.add_argument("--sleep", type=float, default=5.0)
    p.add_argument("--out", type=str, default="", help="output file path (.csv/.json/.parquet)")
    args = p.parse_args(argv)

    result = fetch(
        args.query,
        year=args.year,
        field=args.field,
        max_records=args.max_records,
        sleep=args.sleep,
    )

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        suf = out.suffix.lower()

        if suf == ".csv":
            df = result.df.with_columns(
                pl.col("author").list.join("; ").alias("author")
            )
            df.write_csv(out)

        elif suf == ".json":
            out.write_text(result.df.write_json(), encoding="utf-8")

        elif suf == ".parquet":
            result.df.write_parquet(out)

        else:
            raise SystemExit("out must end with .csv or .json or .parquet")

    else:
        # out未指定なら件数だけ表示
        print(f"rows={result.df.height} total_results={result.total_results}")

    return 0
