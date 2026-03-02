from __future__ import annotations

import argparse
import asyncio

from core.news.service.worker import main_async


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crypto news llm worker")
    parser.add_argument("--once", action="store_true", help="Run one llm batch and exit")
    parser.add_argument("--sources", type=str, default="", help="Ignored for llm-only mode")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    args.pull_only = False
    args.llm_only = True
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
