"""Load CheckM2 quality estimates for representative selection."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path

from marine_peptides.dereplicate.skani_runner import GenomeRecord


@dataclass(frozen=True)
class QualityRecord:
    """CheckM2-derived genome quality metrics keyed by canonical genome id."""

    canonical_id: str
    completeness: float
    contamination: float
    quality_score: float
    total_contigs: int | None = None
    max_contig_length: int | None = None
    raw_name: str = ""

    def as_dict(self) -> dict[str, str | int | float | None]:
        return asdict(self)


def load_checkm2_quality(
    quality_report_path: str | Path,
    records: list[GenomeRecord],
    *,
    contamination_weight: float,
) -> dict[str, QualityRecord]:
    """Map CheckM2 report rows onto canonical genome ids via standardized filenames."""
    path = Path(quality_report_path)
    if not path.exists():
        raise FileNotFoundError(
            f"CheckM2 quality report not found: {path}. Run the documented `checkm2 predict` command first."
        )

    record_name_map = _build_record_name_map(records)
    quality_by_id: dict[str, QualityRecord] = {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            raw_name = (row.get("Name") or "").strip()
            canonical_id = record_name_map.get(_normalize_name(raw_name))
            if not canonical_id:
                continue
            completeness = float(row.get("Completeness") or 0.0)
            contamination = float(row.get("Contamination") or 0.0)
            quality_by_id[canonical_id] = QualityRecord(
                canonical_id=canonical_id,
                completeness=completeness,
                contamination=contamination,
                quality_score=completeness - (contamination_weight * contamination),
                total_contigs=_parse_optional_int(row.get("Total_Contigs")),
                max_contig_length=_parse_optional_int(row.get("Max_Contig_Length")),
                raw_name=raw_name,
            )
    return quality_by_id


def _build_record_name_map(records: list[GenomeRecord]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for record in records:
        fasta_name = Path(record.fasta_path).name
        for candidate in {
            record.canonical_id,
            fasta_name,
            Path(fasta_name).stem,
            _strip_known_suffixes(fasta_name),
        }:
            if candidate:
                mapping[_normalize_name(candidate)] = record.canonical_id
    return mapping


def _normalize_name(value: str) -> str:
    return _strip_known_suffixes(value.strip())


def _strip_known_suffixes(value: str) -> str:
    result = value
    for suffix in (".gz", ".fna", ".fa", ".fasta"):
        if result.endswith(suffix):
            result = result[: -len(suffix)]
    return result


def _parse_optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    return int(float(value))
