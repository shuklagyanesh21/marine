#!/usr/bin/env python3
"""Generate the SmORFinder input TSV from the standardized genome index.

Thin wrapper around ``build_inputs_from_genome_index``: every canonical genome
with a resolvable FASTA becomes a ``single``-mode row so the whole collection can
be enqueued at once.
"""

from __future__ import annotations

import argparse

from marine_peptides.config import load_config
from marine_peptides.orf_prediction.smorfinder import build_inputs_from_genome_index


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        help="Destination TSV (default: smorfinder.input_tsv from config.yaml)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only emit the first N resolvable genomes (useful for staged rollouts)",
    )
    parser.add_argument(
        "--representatives-only",
        action="store_true",
        help="Restrict to GTDB representative genomes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    result = build_inputs_from_genome_index(
        cfg,
        output_path=args.output,
        limit=args.limit,
        representatives_only=args.representatives_only,
    )
    print(f"Genome index: {result['index']}")
    print(f"Wrote {result['written']:,} rows to {result['output']}")
    print(f"Skipped {result['skipped']:,} genomes with no resolvable FASTA")
    if result["skipped_ids"]:
        preview = ", ".join(result["skipped_ids"][:10])
        suffix = " ..." if len(result["skipped_ids"]) > 10 else ""
        print(f"  unresolved: {preview}{suffix}")


if __name__ == "__main__":
    main()
