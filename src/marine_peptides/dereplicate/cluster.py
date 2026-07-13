"""Parse sparse skani edges and cluster genomes by ANI."""

from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from marine_peptides.config import resolve_path
from marine_peptides.dereplicate.skani_runner import GenomeRecord


@dataclass(frozen=True)
class SkaniEdge:
    """One pairwise ANI hit emitted by ``skani triangle --sparse``."""

    ref_file: str
    query_file: str
    ani: float
    align_fraction_ref: float
    align_fraction_query: float


@dataclass(frozen=True)
class ClusterMemberRow:
    """One genome membership row in an ANI cluster."""

    cluster_id: str
    ani_threshold: float
    cluster_size: int
    canonical_id: str
    winning_source: str
    ncbi_accession: str
    fasta_path: str
    n_contigs: int
    total_bp: int
    is_gtdb_representative: str

    def as_dict(self) -> dict[str, str | int | float]:
        return asdict(self)


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, index: int) -> int:
        if self.parent[index] != index:
            self.parent[index] = self.find(self.parent[index])
        return self.parent[index]

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if self.rank[root_left] < self.rank[root_right]:
            root_left, root_right = root_right, root_left
        self.parent[root_right] = root_left
        if self.rank[root_left] == self.rank[root_right]:
            self.rank[root_left] += 1


def load_skani_edges(edges_path: str | Path) -> list[SkaniEdge]:
    """Read sparse skani output, tolerating minor header spelling variants."""
    path = Path(edges_path)
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None:
            return []
        return [
            SkaniEdge(
                ref_file=_first_value(row, "Ref_file", "ref_file", "Reference_file"),
                query_file=_first_value(row, "Query_file", "query_file"),
                ani=float(_first_value(row, "ANI", "ani")),
                align_fraction_ref=float(
                    _first_value(row, "Align_fraction_ref", "AF_ref", "align_fraction_ref")
                ),
                align_fraction_query=float(
                    _first_value(row, "Align_fraction_query", "AF_query", "align_fraction_query")
                ),
            )
            for row in reader
        ]


def cluster_records_from_edges(
    records: list[GenomeRecord],
    edges: Iterable[SkaniEdge],
    *,
    ani_threshold: float,
    min_alignment_fraction: float,
) -> list[ClusterMemberRow]:
    """Single-linkage cluster all genomes using ANI + reciprocal AF thresholds."""
    record_by_path = _build_path_index(records)
    uf = UnionFind(len(records))

    for edge in edges:
        ref_index = record_by_path.get(_normalize_path_key(edge.ref_file))
        query_index = record_by_path.get(_normalize_path_key(edge.query_file))
        if ref_index is None or query_index is None:
            continue
        if edge.ani < ani_threshold:
            continue
        if min(_normalize_fraction(edge.align_fraction_ref), _normalize_fraction(edge.align_fraction_query)) < min_alignment_fraction:
            continue
        uf.union(ref_index, query_index)

    clusters: dict[int, list[int]] = defaultdict(list)
    for index in range(len(records)):
        clusters[uf.find(index)].append(index)

    cluster_roots = sorted(
        clusters,
        key=lambda root: (
            min(records[index].canonical_id for index in clusters[root]),
            len(clusters[root]) * -1,
        ),
    )

    cluster_rows: list[ClusterMemberRow] = []
    threshold_token = _threshold_token(ani_threshold)
    for ordinal, root in enumerate(cluster_roots, start=1):
        members = sorted(
            (records[index] for index in clusters[root]),
            key=lambda record: record.canonical_id,
        )
        cluster_id = f"ani{threshold_token}_c{ordinal:06d}"
        for member in members:
            cluster_rows.append(
                ClusterMemberRow(
                    cluster_id=cluster_id,
                    ani_threshold=ani_threshold,
                    cluster_size=len(members),
                    canonical_id=member.canonical_id,
                    winning_source=member.winning_source,
                    ncbi_accession=member.ncbi_accession,
                    fasta_path=member.fasta_path,
                    n_contigs=member.n_contigs,
                    total_bp=member.total_bp,
                    is_gtdb_representative=member.is_gtdb_representative,
                )
            )
    return cluster_rows


def load_cluster_members(path: str | Path) -> list[ClusterMemberRow]:
    """Read an emitted cluster-membership table back into dataclasses."""
    with Path(path).open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return [
            ClusterMemberRow(
                cluster_id=row["cluster_id"],
                ani_threshold=float(row["ani_threshold"]),
                cluster_size=int(row["cluster_size"]),
                canonical_id=row["canonical_id"],
                winning_source=row["winning_source"],
                ncbi_accession=row["ncbi_accession"],
                fasta_path=row["fasta_path"],
                n_contigs=int(row["n_contigs"] or 0),
                total_bp=int(row["total_bp"] or 0),
                is_gtdb_representative=row["is_gtdb_representative"],
            )
            for row in reader
        ]


def expected_cluster_path(cfg: dict[str, dict[str, object]], ani_threshold: float) -> Path:
    pattern = str(cfg["dereplication"]["clusters_tsv_pattern"])
    return resolve_path(pattern.format(t=_threshold_token(ani_threshold)))


def _build_path_index(records: list[GenomeRecord]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for index, record in enumerate(records):
        fasta_abs = record.fasta_abs_path.resolve(strict=False)
        raw_abs = record.canonical_raw_abs_path.resolve(strict=False)
        try:
            derep_abs = record.derep_input_abs_path
        except FileNotFoundError:
            derep_abs = fasta_abs
        keys = {
            _normalize_path_key(str(fasta_abs)),
            _normalize_path_key(str(record.fasta_abs_path)),
            _normalize_path_key(str(raw_abs)),
            _normalize_path_key(str(record.canonical_raw_abs_path)),
            _normalize_path_key(str(derep_abs)),
            _normalize_path_key(record.fasta_path),
            _normalize_path_key(record.canonical_raw_path),
            _normalize_path_key(fasta_abs.name),
            _normalize_path_key(raw_abs.name),
            _normalize_path_key(derep_abs.name),
            _normalize_path_key(record.fasta_abs_path.name),
            _normalize_path_key(record.derep_input_file_name),
        }
        for key in keys:
            mapping[key] = index
    return mapping


def _first_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    raise KeyError(f"Missing any of the expected columns: {keys}")


def _normalize_fraction(value: float) -> float:
    return value / 100.0 if value > 1.0 else value


def _normalize_path_key(value: str) -> str:
    return str(Path(value).resolve(strict=False))


def _threshold_token(ani_threshold: float) -> str:
    return str(int(round(ani_threshold)))
