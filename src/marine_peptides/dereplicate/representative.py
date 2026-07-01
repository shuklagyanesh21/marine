"""Choose one representative genome per ANI cluster."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass

from marine_peptides.dereplicate.cluster import ClusterMemberRow
from marine_peptides.dereplicate.quality import QualityRecord
from marine_peptides.dereplicate.skani_runner import GenomeRecord


@dataclass(frozen=True)
class RepresentativeRow:
    """One selected representative for an ANI cluster."""

    cluster_id: str
    ani_threshold: float
    cluster_size: int
    canonical_id: str
    winning_source: str
    ncbi_accession: str
    fasta_path: str
    completeness: float
    contamination: float
    quality_score: float
    n_contigs: int
    total_bp: int
    is_gtdb_representative: str
    selection_reason: str

    def as_dict(self) -> dict[str, str | int | float]:
        return asdict(self)


def select_representatives(
    cluster_rows: list[ClusterMemberRow],
    records: list[GenomeRecord],
    quality_by_id: dict[str, QualityRecord],
    *,
    source_order: list[str],
) -> list[RepresentativeRow]:
    """Pick a single representative per cluster using quality-first tie-breakers."""
    record_by_id = {record.canonical_id: record for record in records}
    source_priority = {source: rank for rank, source in enumerate(source_order)}
    clusters: dict[str, list[ClusterMemberRow]] = defaultdict(list)
    for row in cluster_rows:
        clusters[row.cluster_id].append(row)

    representatives: list[RepresentativeRow] = []
    for cluster_id in sorted(clusters):
        members = clusters[cluster_id]
        winner = sorted(
            members,
            key=lambda row: _selection_key(
                row,
                record=row_and_record(record_by_id, row),
                quality_by_id=quality_by_id,
                source_priority=source_priority,
            ),
        )[0]
        quality = required_quality(quality_by_id, winner.canonical_id)
        representatives.append(
            RepresentativeRow(
                cluster_id=winner.cluster_id,
                ani_threshold=winner.ani_threshold,
                cluster_size=winner.cluster_size,
                canonical_id=winner.canonical_id,
                winning_source=winner.winning_source,
                ncbi_accession=winner.ncbi_accession,
                fasta_path=winner.fasta_path,
                completeness=quality.completeness,
                contamination=quality.contamination,
                quality_score=quality.quality_score,
                n_contigs=winner.n_contigs,
                total_bp=winner.total_bp,
                is_gtdb_representative=winner.is_gtdb_representative,
                selection_reason=(
                    "max quality_score, then fewer contigs, GTDB representative, source priority"
                ),
            )
        )
    return representatives


def required_quality(
    quality_by_id: dict[str, QualityRecord],
    canonical_id: str,
) -> QualityRecord:
    quality = quality_by_id.get(canonical_id)
    if quality is None:
        raise KeyError(
            f"Missing CheckM2 quality for {canonical_id}. Run CheckM2 on the standardized FASTA set first."
        )
    return quality


def row_and_record(record_by_id: dict[str, GenomeRecord], row: ClusterMemberRow) -> GenomeRecord:
    record = record_by_id.get(row.canonical_id)
    if record is None:
        raise KeyError(f"Cluster row references unknown canonical_id: {row.canonical_id}")
    return record


def _selection_key(
    row: ClusterMemberRow,
    *,
    record: GenomeRecord,
    quality_by_id: dict[str, QualityRecord],
    source_priority: dict[str, int],
) -> tuple[float, float, float, int, int, int, int, str]:
    quality = required_quality(quality_by_id, row.canonical_id)
    return (
        -quality.quality_score,
        -quality.completeness,
        quality.contamination,
        row.n_contigs,
        -row.total_bp,
        0 if record.gtdb_representative_bool else 1,
        source_priority.get(row.winning_source, 999),
        row.canonical_id,
    )
