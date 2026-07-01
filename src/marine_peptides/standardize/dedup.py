"""Collapse overlapping source-instance genomes into canonical records."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from marine_peptides.config import project_root
from marine_peptides.standardize.canonical_id import make_canonical_id
from marine_peptides.standardize.inventory import InventoryRow
from marine_peptides.standardize.sequence_hash import SequenceMetrics

ASM_GENOMIC_RE = re.compile(r"_ASM.+_genomic\.fna(?:\.gz)?$")
TRUE_VALUES = {"true", "t", "1", "yes"}


@dataclass(frozen=True)
class CanonicalRecord:
    """One deduplicated genome entry in the standardized layer."""

    canonical_id: str
    winning_source: str
    ncbi_accession: str
    marref_id: str
    mardb_id: str
    gorg_id: str
    oceandna_id: str
    gomc_id: str
    raw_paths: str
    canonical_raw_path: str
    md5_seq: str
    n_contigs: int
    total_bp: int
    status: str
    is_gtdb_representative: str
    dedup_reason: str

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


@dataclass(frozen=True)
class DedupReportRow:
    """One deduplicated cluster with the reason it collapsed."""

    canonical_id: str
    cluster_size: int
    dedup_reason: str
    winning_source: str
    members: str

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, index: int) -> int:
        if self.parent[index] != index:
            self.parent[index] = self.find(self.parent[index])
        return self.parent[index]

    def union(self, left: int, right: int) -> int:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return root_left
        if self.rank[root_left] < self.rank[root_right]:
            root_left, root_right = root_right, root_left
        self.parent[root_right] = root_left
        if self.rank[root_left] == self.rank[root_right]:
            self.rank[root_left] += 1
        return root_left


def _to_relpath(value: str) -> str:
    if not value:
        return ""
    path = Path(value)
    try:
        return str(path.relative_to(project_root()))
    except ValueError:
        return str(path)


def _truthy(value: str) -> bool:
    return value.strip().lower() in TRUE_VALUES


def _status_rank(row: InventoryRow) -> int:
    ranks = {"ok": 0, "broken_symlink": 1, "missing": 2}
    return ranks.get(row.status, 99)


def _mardb_filename_rank(row: InventoryRow) -> int:
    if row.source != "mardb":
        return 1
    return 0 if ASM_GENOMIC_RE.search(Path(row.raw_path).name) else 1


def _canonical_sort_key(row: InventoryRow, source_priority: dict[str, int]) -> tuple[int, int, int, str]:
    return (
        source_priority.get(row.source, 999),
        _status_rank(row),
        _mardb_filename_rank(row),
        row.raw_path,
    )


def _union_group(
    indices: Iterable[int],
    uf: UnionFind,
    row_reasons: dict[int, set[str]],
    reason: str,
) -> None:
    group = list(indices)
    if len(group) < 2:
        return
    for index in group:
        row_reasons[index].add(reason)
    root = group[0]
    for index in group[1:]:
        root = uf.union(root, index)


def build_canonical_records(
    rows: list[InventoryRow],
    metrics_by_path: dict[str, SequenceMetrics],
    source_order: list[str],
) -> tuple[list[CanonicalRecord], list[DedupReportRow], list[str]]:
    """Collapse inventory rows into canonical records and dedup reports."""
    uf = UnionFind(len(rows))
    row_reasons: dict[int, set[str]] = defaultdict(set)
    warnings: list[str] = []
    source_priority = {source: rank for rank, source in enumerate(source_order)}

    mardb_by_accession: dict[str, list[int]] = defaultdict(list)
    accession_groups: dict[str, list[int]] = defaultdict(list)
    md5_groups: dict[str, list[int]] = defaultdict(list)

    for index, row in enumerate(rows):
        if row.source == "mardb" and row.ncbi_accession:
            mardb_by_accession[row.ncbi_accession].append(index)
        if row.ncbi_accession:
            accession_groups[row.ncbi_accession].append(index)
        metrics = metrics_by_path.get(row.resolved_path)
        if metrics and metrics.md5_seq:
            md5_groups[metrics.md5_seq].append(index)

    for accession, indices in mardb_by_accession.items():
        if len(indices) < 2:
            continue
        md5_values = {
            metrics_by_path[row.resolved_path].md5_seq
            for row in (rows[index] for index in indices)
            if row.resolved_path in metrics_by_path and metrics_by_path[row.resolved_path].md5_seq
        }
        if len(md5_values) > 1:
            warnings.append(
                f"MarDB accession {accession} has {len(indices)} files but non-matching md5_seq values; "
                "left as separate rows"
            )
            continue
        _union_group(indices, uf, row_reasons, "mardb_dual_file")

    for indices in accession_groups.values():
        _union_group(indices, uf, row_reasons, "collapsed_by_gca")

    for md5_value, indices in md5_groups.items():
        if len(indices) < 2:
            continue
        total_bp_values = {
            metrics_by_path[rows[index].resolved_path].total_bp
            for index in indices
            if rows[index].resolved_path in metrics_by_path
        }
        if len(total_bp_values) > 1:
            warnings.append(
                f"md5_seq {md5_value} appears in {len(indices)} rows with inconsistent total_bp; "
                "skipping md5 collapse"
            )
            continue
        _union_group(indices, uf, row_reasons, "collapsed_by_md5")

    clusters: dict[int, list[int]] = defaultdict(list)
    for index in range(len(rows)):
        clusters[uf.find(index)].append(index)

    canonical_records: list[CanonicalRecord] = []
    dedup_rows: list[DedupReportRow] = []

    for root, member_indices in sorted(clusters.items(), key=lambda item: min(item[1])):
        cluster_rows = [rows[index] for index in member_indices]
        cluster_rows_sorted = sorted(cluster_rows, key=lambda row: _canonical_sort_key(row, source_priority))
        winner = cluster_rows_sorted[0]
        canonical_id = make_canonical_id(
            winner.source,
            winner.source_id,
            ncbi_accession=winner.ncbi_accession,
        )

        source_ids: dict[str, set[str]] = defaultdict(set)
        raw_paths = sorted({row.raw_path for row in cluster_rows if row.raw_path})
        for row in cluster_rows:
            if row.source_id:
                source_ids[row.source].add(row.source_id)

        metrics = metrics_by_path.get(winner.resolved_path, SequenceMetrics(md5_seq="", n_contigs=0, total_bp=0))
        cluster_reasons = set().union(*(row_reasons.get(index, set()) for index in member_indices))
        reason_str = ";".join(sorted(cluster_reasons))
        rep_value = ""
        if any(_truthy(row.is_gtdb_representative) for row in cluster_rows):
            rep_value = "True"
        elif any(row.is_gtdb_representative for row in cluster_rows):
            rep_value = "False"

        canonical_records.append(
            CanonicalRecord(
                canonical_id=canonical_id,
                winning_source=winner.source,
                ncbi_accession=winner.ncbi_accession or _first_nonempty(row.ncbi_accession for row in cluster_rows),
                marref_id=_join_ids(source_ids.get("marref", set())),
                mardb_id=_join_ids(source_ids.get("mardb", set())),
                gorg_id=_join_ids(source_ids.get("gorg", set())),
                oceandna_id=_join_ids(source_ids.get("oceandna", set())),
                gomc_id=_join_ids(source_ids.get("gomc", set())),
                raw_paths=";".join(_to_relpath(path) for path in raw_paths),
                canonical_raw_path=_to_relpath(winner.raw_path),
                md5_seq=metrics.md5_seq,
                n_contigs=metrics.n_contigs,
                total_bp=metrics.total_bp,
                status=winner.status,
                is_gtdb_representative=rep_value,
                dedup_reason=reason_str,
            )
        )

        if len(member_indices) > 1:
            dedup_rows.append(
                DedupReportRow(
                    canonical_id=canonical_id,
                    cluster_size=len(member_indices),
                    dedup_reason=reason_str or "collapsed",
                    winning_source=winner.source,
                    members=";".join(
                        sorted(
                            f"{row.source}:{row.source_id or row.ncbi_accession or Path(row.raw_path).name}"
                            for row in cluster_rows
                        )
                    ),
                )
            )

    return canonical_records, dedup_rows, warnings


def _first_nonempty(values: Iterable[str]) -> str:
    for value in values:
        if value:
            return value
    return ""


def _join_ids(values: set[str]) -> str:
    return ";".join(sorted(value for value in values if value))
