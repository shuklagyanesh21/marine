#!/usr/bin/env python3
"""Validate and normalize the SmORFinder input manifest."""

from __future__ import annotations

import argparse

from marine_peptides.config import load_config
from marine_peptides.orf_prediction.smorfinder import load_input_manifest, write_prepared_manifests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Validate only; do not write TSV outputs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    rows = load_input_manifest(cfg["smorfinder"]["input_tsv"])
    print(f"Validated {len(rows):,} SmORFinder input rows")
    if args.dry_run:
        return

    emitted = write_prepared_manifests(cfg, rows)
    print(f"Normalized TSV: {emitted['normalized_tsv']}")
    print(f"Status TSV:     {emitted['status_tsv']}")
    print(f"Pending TSV:    {emitted['pending_tsv']}")
    print(f"Pending rows:   {emitted['pending_rows']:,}")
    print(f"Skipped rows:   {emitted['skipped_rows']:,}")


if __name__ == "__main__":
    main()
