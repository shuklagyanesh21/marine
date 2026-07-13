#!/usr/bin/env python3
"""Run one SmORFinder task inside an isolated work directory."""

from __future__ import annotations

import argparse
from pathlib import Path

from marine_peptides.config import load_config
from marine_peptides.orf_prediction.smorfinder import run_smorfinder_task


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-id", required=True)
    parser.add_argument("--mode", choices=("single", "meta"), required=True)
    parser.add_argument("--fasta-path", required=True, help="Project-relative FASTA path from the input TSV")
    parser.add_argument("--cpus", type=int, required=True)
    parser.add_argument(
        "--work-dir",
        default=".",
        help="Task work directory; SmORFinder outputs will be written under <work-dir>/<sample-id>",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    output_dir = run_smorfinder_task(
        cfg=cfg,
        sample_id=args.sample_id,
        mode=args.mode,
        fasta_path=args.fasta_path,
        work_dir=Path(args.work_dir),
        cpus=args.cpus,
    )
    print(f"SmORFinder output directory: {output_dir}")


if __name__ == "__main__":
    main()
