#!/usr/bin/env python3
"""Build a standardized, deduplicated genome layer from raw downloads."""

from __future__ import annotations

import argparse
import time

from marine_peptides.config import load_config
from marine_peptides.standardize.dedup import build_canonical_records
from marine_peptides.standardize.emit import emit_outputs, validate_outputs
from marine_peptides.standardize.inventory import load_inventory
from marine_peptides.standardize.sequence_hash import compute_metrics_for_paths


def log(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, help="Override config standardize.parallel_workers")
    parser.add_argument("--dry-run", action="store_true", help="Plan and validate without writing files")
    parser.add_argument("--skip-md5", action="store_true", help="Skip content hashing for faster debugging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    std_cfg = cfg["standardize"]
    workers = int(args.threads or std_cfg["parallel_workers"])
    compute_md5 = bool(std_cfg["compute_md5_seq"]) and not args.skip_md5

    started = time.monotonic()
    inventory_rows, summary = load_inventory(cfg)
    log(f"Loaded {len(inventory_rows):,} source-instance rows from manifests + disk")

    metrics_by_path = {}
    if compute_md5:
        existing_paths = {row.resolved_path for row in inventory_rows if row.status == "ok" and row.resolved_path}
        metrics_by_path = compute_metrics_for_paths(existing_paths, workers=workers, logger=log)
    else:
        log("Skipping md5_seq computation")

    canonical_records, dedup_rows, warnings = build_canonical_records(
        inventory_rows,
        metrics_by_path=metrics_by_path,
        source_order=list(std_cfg["source_priority"]),
    )
    summary["warnings"] = warnings
    elapsed_s = time.monotonic() - started

    log(f"Canonical genomes: {len(canonical_records):,}")
    log(f"Collapsed clusters: {len(dedup_rows):,}")
    log(f"Warnings: {len(warnings):,}")
    log(f"Elapsed: {elapsed_s / 60:.1f} min")

    if args.dry_run:
        return

    emitted_paths = emit_outputs(
        canonical_records=canonical_records,
        dedup_rows=dedup_rows,
        cfg=cfg,
        summary=summary,
        logger=log,
    )
    validate_outputs(
        canonical_records=canonical_records,
        dedup_rows=dedup_rows,
        emitted_paths=emitted_paths,
        inventory_count=summary["inventory_count"],
    )
    log(f"Finished in {elapsed_s / 60:.1f} min")


if __name__ == "__main__":
    main()
