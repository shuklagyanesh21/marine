#!/usr/bin/env python3
"""Promote single-mode SmORFinder failures to meta mode (hybrid step).

Run this AFTER a completed single-mode pass. Any genome without a valid
``single/<id>/_SUCCESS.json`` is flipped to ``meta`` mode in the input TSV so a
subsequent ``nextflow run ... -resume`` re-processes only those genomes with the
crash-free meta path. Thin wrapper around ``promote_failures_to_meta``.
"""

from __future__ import annotations

import argparse

from marine_peptides.config import load_config
from marine_peptides.orf_prediction.smorfinder import promote_failures_to_meta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-tsv",
        help="Input TSV to rewrite (default: smorfinder.input_tsv from config.yaml)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    result = promote_failures_to_meta(cfg, input_tsv=args.input_tsv)
    print(f"Input TSV:          {result['output']}")
    print(f"Total genomes:      {result['total']:,}")
    print(f"Kept single (done): {result['kept_single']:,}")
    print(f"Promoted to meta:   {result['promoted_to_meta']:,}")
    print(f"Already meta:       {result['already_meta']:,}")
    if result["promoted_ids"]:
        preview = ", ".join(result["promoted_ids"][:10])
        suffix = " ..." if len(result["promoted_ids"]) > 10 else ""
        print(f"  promoted: {preview}{suffix}")


if __name__ == "__main__":
    main()
