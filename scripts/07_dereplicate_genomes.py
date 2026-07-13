#!/usr/bin/env python3
"""Run skani-based ANI dereplication on the standardized genome layer."""

from __future__ import annotations

import argparse
import shlex
from pathlib import Path

from marine_peptides.config import load_config, resolve_path
from marine_peptides.dereplicate.cluster import (
    cluster_records_from_edges,
    expected_cluster_path,
    load_cluster_members,
    load_skani_edges,
)
from marine_peptides.dereplicate.emit import (
    emit_cluster_tables,
    emit_representatives,
    validate_clusters,
    validate_representatives,
)
from marine_peptides.dereplicate.quality import load_checkm2_quality
from marine_peptides.dereplicate.representative import select_representatives
from marine_peptides.dereplicate.skani_runner import (
    build_checkm2_command,
    load_genome_index,
    run_skani_pipeline,
)


def log(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage",
        choices=("all", "skani", "cluster", "representatives"),
        default="all",
        help="Which dereplication stage to execute",
    )
    parser.add_argument("--threads", type=int, help="Override config dereplication.skani.threads")
    parser.add_argument("--skip-skani", action="store_true", help="Reuse an existing skani_edges.tsv")
    parser.add_argument("--dry-run", action="store_true", help="Plan work without writing outputs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    derep_cfg = cfg["dereplication"]
    thresholds = sorted(float(value) for value in derep_cfg["ani_thresholds"])
    records = load_genome_index(resolve_path(derep_cfg["input_index"]))
    threads = int(args.threads or derep_cfg["skani"]["threads"])
    log(f"Loaded {len(records):,} standardized genomes with status=ok")

    checkm2_cmd = build_checkm2_command(cfg, threads=threads)
    log(f"Documented manual CheckM2 command: {shlex.join(checkm2_cmd)}")

    if args.stage in {"all", "skani"}:
        if args.skip_skani:
            log("Skipping skani sketch + triangle because --skip-skani was requested")
        else:
            result = run_skani_pipeline(records, cfg, threads=threads, dry_run=args.dry_run, logger=log)
            if args.dry_run:
                for command in result["commands"]:
                    log(f"PLAN {shlex.join(command)}")
            else:
                log(f"Wrote skani sparse edges to {result['edges_tsv']}")

    if args.stage in {"all", "cluster"}:
        if args.dry_run:
            edges_path = resolve_path(derep_cfg["edges_tsv"])
            log(f"PLAN cluster {edges_path} into ANI thresholds {thresholds}")
        else:
            edges_path = resolve_path(derep_cfg["edges_tsv"])
            if not edges_path.exists():
                raise FileNotFoundError(
                    f"Missing skani sparse edge list: {edges_path}. Run --stage skani first or drop --skip-skani."
                )
            edges = load_skani_edges(edges_path)
            log(f"Loaded {len(edges):,} skani edges from {edges_path}")
            clusters_by_threshold = {}
            for threshold in thresholds:
                rows = cluster_records_from_edges(
                    records,
                    edges,
                    ani_threshold=threshold,
                    min_alignment_fraction=float(derep_cfg["min_alignment_fraction"]),
                )
                validate_clusters(records, rows)
                clusters_by_threshold[threshold] = rows
                log(
                    f"ANI {threshold:.0f}: {len({row.cluster_id for row in rows}):,} clusters across "
                    f"{len(rows):,} genomes"
                )
            emitted = emit_cluster_tables(cfg, clusters_by_threshold)
            for threshold, path in emitted.items():
                log(f"Wrote ANI {threshold:.0f} clusters to {path}")

    if args.stage in {"all", "representatives"}:
        quality_path = resolve_path(derep_cfg["checkm2_quality"])
        if args.dry_run:
            log(f"PLAN choose representatives from {quality_path}")
            return

        quality_by_id = load_checkm2_quality(
            quality_path,
            records,
            contamination_weight=float(derep_cfg["contamination_weight"]),
        )
        log(f"Loaded CheckM2 quality for {len(quality_by_id):,} genomes")

        representatives_by_threshold = {}
        for threshold in thresholds:
            cluster_path = expected_cluster_path(cfg, threshold)
            if not Path(cluster_path).exists():
                raise FileNotFoundError(
                    f"Missing cluster table for ANI {threshold:.0f}: {cluster_path}. Run --stage cluster first."
                )
            cluster_rows = load_cluster_members(cluster_path)
            representative_rows = select_representatives(
                cluster_rows,
                records,
                quality_by_id,
                source_order=list(derep_cfg["source_priority"]),
            )
            validate_representatives(cluster_rows, representative_rows)
            representatives_by_threshold[threshold] = representative_rows
            log(f"ANI {threshold:.0f}: selected {len(representative_rows):,} representatives")

        emitted = emit_representatives(cfg, representatives_by_threshold, records)
        for threshold, paths in emitted.items():
            log(f"Wrote ANI {threshold:.0f} representatives to {paths['table']} and {paths['directory']}")


if __name__ == "__main__":
    main()
