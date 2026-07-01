"""Emit dereplication cluster and representative outputs."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

from marine_peptides.config import resolve_path
from marine_peptides.dereplicate.cluster import ClusterMemberRow
from marine_peptides.dereplicate.representative import RepresentativeRow
from marine_peptides.dereplicate.skani_runner import GenomeRecord

CLUSTER_COLUMNS = [
    "cluster_id",
    "ani_threshold",
    "cluster_size",
    "canonical_id",
    "winning_source",
    "ncbi_accession",
    "fasta_path",
    "n_contigs",
    "total_bp",
    "is_gtdb_representative",
]

REPRESENTATIVE_COLUMNS = [
    "cluster_id",
    "ani_threshold",
    "cluster_size",
    "canonical_id",
    "winning_source",
    "ncbi_accession",
    "fasta_path",
    "completeness",
    "contamination",
    "quality_score",
    "n_contigs",
    "total_bp",
    "is_gtdb_representative",
    "selection_reason",
]


def emit_cluster_tables(
    cfg: dict[str, Any],
    clusters_by_threshold: dict[float, list[ClusterMemberRow]],
) -> dict[float, Path]:
    """Write one cluster-membership table per ANI threshold."""
    pattern = str(cfg["dereplication"]["clusters_tsv_pattern"])
    emitted: dict[float, Path] = {}
    for threshold, rows in sorted(clusters_by_threshold.items()):
        path = resolve_path(pattern.format(t=_threshold_token(threshold)))
        _write_tsv_atomic(
            path,
            [row.as_dict() for row in sorted(rows, key=lambda row: (row.cluster_id, row.canonical_id))],
            CLUSTER_COLUMNS,
        )
        emitted[threshold] = path
    return emitted


def emit_representatives(
    cfg: dict[str, Any],
    representatives_by_threshold: dict[float, list[RepresentativeRow]],
    records: list[GenomeRecord],
) -> dict[float, dict[str, Path]]:
    """Write representative tables and symlink directories."""
    tsv_pattern = str(cfg["dereplication"]["representatives_tsv_pattern"])
    dir_pattern = str(cfg["dereplication"]["representatives_dir_pattern"])
    record_by_id = {record.canonical_id: record for record in records}
    emitted: dict[float, dict[str, Path]] = {}

    for threshold, rows in sorted(representatives_by_threshold.items()):
        token = _threshold_token(threshold)
        table_path = resolve_path(tsv_pattern.format(t=token))
        dir_path = resolve_path(dir_pattern.format(t=token))
        dir_path.mkdir(parents=True, exist_ok=True)
        _clear_directory(dir_path)
        _write_tsv_atomic(
            table_path,
            [row.as_dict() for row in sorted(rows, key=lambda row: row.cluster_id)],
            REPRESENTATIVE_COLUMNS,
        )
        for row in rows:
            record = record_by_id[row.canonical_id]
            target = record.fasta_abs_path.resolve(strict=True)
            link_path = dir_path / Path(target).name
            if link_path.exists() or link_path.is_symlink():
                link_path.unlink()
            link_path.symlink_to(target)
        emitted[threshold] = {"table": table_path, "directory": dir_path}
    return emitted


def validate_clusters(records: list[GenomeRecord], cluster_rows: list[ClusterMemberRow]) -> None:
    """Ensure each genome appears exactly once in a cluster table."""
    expected_ids = {record.canonical_id for record in records}
    observed_ids = [row.canonical_id for row in cluster_rows]
    if len(observed_ids) != len(expected_ids):
        raise ValueError(
            f"Cluster table has {len(observed_ids):,} rows for {len(expected_ids):,} genomes."
        )
    if len(set(observed_ids)) != len(observed_ids):
        raise ValueError("Cluster table contains duplicate canonical_id values")
    if set(observed_ids) != expected_ids:
        missing = sorted(expected_ids.difference(observed_ids))
        extras = sorted(set(observed_ids).difference(expected_ids))
        raise ValueError(
            "Cluster membership mismatch. "
            f"Missing={len(missing)} Extra={len(extras)}"
        )


def validate_representatives(
    cluster_rows: list[ClusterMemberRow],
    representative_rows: list[RepresentativeRow],
) -> None:
    """Ensure there is exactly one representative per cluster."""
    cluster_ids = {row.cluster_id for row in cluster_rows}
    rep_cluster_ids = [row.cluster_id for row in representative_rows]
    if len(rep_cluster_ids) != len(cluster_ids):
        raise ValueError(
            f"Representative table has {len(rep_cluster_ids):,} rows for {len(cluster_ids):,} clusters."
        )
    if len(set(rep_cluster_ids)) != len(rep_cluster_ids):
        raise ValueError("Representative table contains duplicate cluster_id values")
    if set(rep_cluster_ids) != cluster_ids:
        raise ValueError("Representative table does not cover the same cluster_id set as the cluster table")


def _write_tsv_atomic(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(path)
    return path


def _clear_directory(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            raise ValueError(f"Refusing to clear unexpected directory inside {path}: {child}")
        child.unlink()


def _threshold_token(ani_threshold: float) -> str:
    return str(int(round(ani_threshold)))
