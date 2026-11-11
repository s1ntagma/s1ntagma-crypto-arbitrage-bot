"""CLI: ``python -m latbot.backtest <files...> [--config config.json] [--latency MS]
[--capital QUOTE] [--json]``.  Globs are expanded; exit code 2 when no records."""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
from pathlib import Path

from ..config import load_settings
from .engine import fmt_report, run_backtest


def _expand(patterns: list[str]) -> list[str]:
    out: list[str] = []
    for pat in patterns:
        matches = sorted(glob.glob(pat))
        if matches:
            out.extend(matches)
        elif Path(pat).exists():
            out.append(pat)
    return list(dict.fromkeys(out))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m latbot.backtest", description="Replay recorded order books through latbot.")
    parser.add_argument("files", nargs="+", help="JSONL / JSONL.GZ files (globs allowed)")
    parser.add_argument("--config", default="config.json", help="settings file (default config.json)")
    parser.add_argument("--latency", type=float, default=None, help="simulated API latency in ms (default: settings)")
    parser.add_argument("--capital", type=float, default=None, help="trade size in quote (default: settings.capital_quote)")
    parser.add_argument("--json", action="store_true", help="print the full report as JSON")
    parser.add_argument("-v", "--verbose", action="store_true", help="INFO logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass

    paths = _expand(args.files)
    if not paths:
        print("no input files found", file=sys.stderr)
        return 2
    settings = load_settings(args.config)
    if args.capital is not None and args.capital > 0:
        settings.update({"capital_quote": args.capital})
    report = run_backtest(paths, settings, latency_ms=args.latency)
    if not report.get("records"):
        print("no records found in the input files", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    else:
        print(fmt_report(report))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
