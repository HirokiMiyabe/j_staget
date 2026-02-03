from __future__ import annotations

import argparse
from .client import fetch


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="j-staget")
    p.add_argument("query")
    p.add_argument("--year", type=int, default=1950)
    p.add_argument("--field", default="article")
    args = p.parse_args(argv)

    res = fetch(args.query, year=args.year, field=args.field)
    print(res.df)
    return 0
