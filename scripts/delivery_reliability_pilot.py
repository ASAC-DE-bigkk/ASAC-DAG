#!/usr/bin/env python3
"""Render a read-only seven-day delivery reliability evidence bundle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from common.delivery_reliability.render import (  # noqa: E402
    report_from_document,
    render_csv,
    render_json,
    render_markdown,
)


RENDERERS = {
    "json": render_json,
    "csv": render_csv,
    "markdown": render_markdown,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate normalized Weather/Traffic run evidence and emit a "
            "read-only seven-day reliability report."
        )
    )
    parser.add_argument("evidence", type=Path, help="UTF-8 JSON evidence bundle")
    parser.add_argument(
        "--format",
        choices=tuple(RENDERERS),
        default="markdown",
        help="stdout format (default: markdown)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        with args.evidence.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict):
            raise ValueError("evidence document must be a JSON object")
        report = report_from_document(document)
    except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
        parser.error(str(exc))
    sys.stdout.write(RENDERERS[args.format](report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
