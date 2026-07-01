"""Write the standardized genome layer to disk."""

from __future__ import annotations

import csv
import os
import shutil
from pathlib import Path
from typing import Any

from marine_peptides.config import project_root, resolve_path
from marine_peptides.standardize.canonical_id import canonical_filename
from marine_peptides.standardize.dedup import CanonicalRecord, DedupReportRow

INDEX_COLUMNS = [
    "canonical_id",
    "winning_source",
    "ncbi_accession",
    "fasta_path",
    "canonical_raw_path",
    "md5_seq",
    "n_contigs",
    "total_bp",
    "status",
    "is_gtdb_representative",
    "dedup_reason",
]

ROSETTA_COLUMNS = [
    "canonical_id",
    "winning_source",
    "ncbi_accession",
    "marref_id",
    "mardb_id",
    "gorg_id",
    "oceandna_id",
    "gomc_id",
    "raw_paths",
    "canonical_raw_path",
    "md5_seq",
    "n_contigs",
    "total_bp",
    "status",
    "is_gtdb_representative",
    "dedup_reason",
]

DEDUP_COLUMNS = ["canonical_id", "cluster_size", "dedup_reason", "winning_source", "members"]


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


def _write_text_atomic(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(content)
    temp_path.replace(path)
    return path


def _safe_unlink(path: Path) -> None:
    if path.is_symlink() or path.exists():
        path.unlink()


def _clear_directory_entries(dir_path: Path) -> None:
    for child in dir_path.iterdir():
        if child.is_dir() and not child.is_symlink():
            raise ValueError(f"Refusing to clear unexpected directory inside standardized FASTA dir: {child}")
        _safe_unlink(child)


def _relpath(path: Path) -> str:
    try:
        return str(path.relative_to(project_root()))
    except ValueError:
        return str(path)


def emit_outputs(
    canonical_records: list[CanonicalRecord],
    dedup_rows: list[DedupReportRow],
    cfg: dict[str, Any],
    summary: dict[str, Any],
    logger=print,
) -> dict[str, Path]:
    """Create symlinks and write standardized TSV surfaces."""
    std_cfg = cfg["standardize"]
    out_root = resolve_path(std_cfg["out_root"])
    fasta_dir = resolve_path(std_cfg["fasta_dir"])
    rosetta_path = resolve_path(std_cfg["rosetta_tsv"])
    index_path = resolve_path(std_cfg["index_tsv"])
    dedup_path = resolve_path(std_cfg["dedup_report_tsv"])
    processed_index_path = resolve_path(std_cfg["processed_index_tsv"])
    processed_rosetta_path = resolve_path(std_cfg["processed_rosetta_tsv"])
    run_log_path = out_root / "run_log.txt"

    out_root.mkdir(parents=True, exist_ok=True)
    fasta_dir.mkdir(parents=True, exist_ok=True)
    _clear_directory_entries(fasta_dir)

    index_rows: list[dict[str, Any]] = []
    rosetta_rows: list[dict[str, Any]] = []
    emitted_links = 0

    for record in canonical_records:
        fasta_path = ""
        canonical_raw_abs = resolve_path(record.canonical_raw_path) if record.canonical_raw_path else None
        if canonical_raw_abs and record.status == "ok":
            link_name = canonical_filename(record.canonical_id, canonical_raw_abs)
            link_path = fasta_dir / link_name
            target = canonical_raw_abs.resolve(strict=True)
            _safe_unlink(link_path)
            link_path.symlink_to(target)
            fasta_path = _relpath(link_path)
            emitted_links += 1

        rosetta_row = record.as_dict()
        rosetta_rows.append(rosetta_row)
        index_rows.append(
            {
                "canonical_id": record.canonical_id,
                "winning_source": record.winning_source,
                "ncbi_accession": record.ncbi_accession,
                "fasta_path": fasta_path,
                "canonical_raw_path": record.canonical_raw_path,
                "md5_seq": record.md5_seq,
                "n_contigs": record.n_contigs,
                "total_bp": record.total_bp,
                "status": record.status,
                "is_gtdb_representative": record.is_gtdb_representative,
                "dedup_reason": record.dedup_reason,
            }
        )

    dedup_dict_rows = [row.as_dict() for row in dedup_rows]
    _write_tsv_atomic(rosetta_path, rosetta_rows, ROSETTA_COLUMNS)
    _write_tsv_atomic(index_path, index_rows, INDEX_COLUMNS)
    _write_tsv_atomic(dedup_path, dedup_dict_rows, DEDUP_COLUMNS)
    _write_tsv_atomic(processed_rosetta_path, rosetta_rows, ROSETTA_COLUMNS)
    _write_tsv_atomic(processed_index_path, index_rows, INDEX_COLUMNS)

    log_lines = [
        "Standardize raw genome layer",
        "==========================",
        f"Input inventory rows: {summary['inventory_count']:,}",
        "Input rows by source:",
    ]
    for source, count in sorted(summary["source_counts"].items()):
        log_lines.append(f"  - {source}: {count:,}")
    log_lines.append("Input rows by status:")
    for status, count in sorted(summary["status_counts"].items()):
        log_lines.append(f"  - {status}: {count:,}")
    log_lines.append("Tier 3 disk scan counts:")
    for source, count in sorted(summary["tier3_disk_counts"].items()):
        log_lines.append(f"  - {source}: {count:,}")
    log_lines.append(f"NCBI on-disk accessions: {summary['ncbi_disk_count']:,}")
    log_lines.append(f"Canonical genomes: {len(canonical_records):,}")
    log_lines.append(f"Collapsed clusters: {len(dedup_rows):,}")
    log_lines.append(f"Standardized symlinks created: {emitted_links:,}")
    for warning in summary.get("warnings", []):
        log_lines.append(f"WARNING: {warning}")
    _write_text_atomic(run_log_path, "\n".join(log_lines) + "\n")
    logger(f"Wrote standardized outputs under {out_root}")

    return {
        "out_root": out_root,
        "fasta_dir": fasta_dir,
        "rosetta_tsv": rosetta_path,
        "index_tsv": index_path,
        "dedup_report_tsv": dedup_path,
        "processed_rosetta_tsv": processed_rosetta_path,
        "processed_index_tsv": processed_index_path,
        "run_log": run_log_path,
    }


def validate_outputs(
    canonical_records: list[CanonicalRecord],
    dedup_rows: list[DedupReportRow],
    emitted_paths: dict[str, Path],
    inventory_count: int,
) -> None:
    """Sanity-check canonical rows and emitted symlink layer."""
    seen_ids: set[str] = set()
    seen_aliases: set[tuple[str, str]] = set()
    duplicate_total = 0

    for record in canonical_records:
        if record.canonical_id in seen_ids:
            raise ValueError(f"Duplicate canonical_id: {record.canonical_id}")
        seen_ids.add(record.canonical_id)

        for source, alias_values in (
            ("marref", record.marref_id),
            ("mardb", record.mardb_id),
            ("gorg", record.gorg_id),
            ("oceandna", record.oceandna_id),
            ("gomc", record.gomc_id),
        ):
            for alias in filter(None, alias_values.split(";")):
                key = (source, alias)
                if key in seen_aliases:
                    raise ValueError(f"Alias {source}:{alias} appears in multiple canonical rows")
                seen_aliases.add(key)

        if record.status == "ok":
            candidate_names = [
                canonical_filename(record.canonical_id, resolve_path(record.canonical_raw_path)),
            ]
            link_exists = any((emitted_paths["fasta_dir"] / name).exists() for name in candidate_names)
            if not link_exists:
                raise ValueError(f"Missing emitted symlink for {record.canonical_id}")

    for row in dedup_rows:
        duplicate_total += int(row.cluster_size) - 1

    if len(canonical_records) + duplicate_total != inventory_count:
        raise ValueError(
            "Row counts do not balance: "
            f"{len(canonical_records)} canonical + {duplicate_total} collapsed != {inventory_count} inventoried"
        )

    if not emitted_paths["rosetta_tsv"].exists() or not emitted_paths["index_tsv"].exists():
        raise ValueError("Expected output TSVs were not written")

    expected_links = sum(1 for record in canonical_records if record.status == "ok" and record.canonical_raw_path)
    actual_links = sum(1 for path in emitted_paths["fasta_dir"].iterdir() if path.is_symlink() or path.is_file())
    if actual_links != expected_links:
        raise ValueError(
            f"Standardized FASTA dir has {actual_links} entries but expected {expected_links}"
        )

    shutil.copyfile(emitted_paths["rosetta_tsv"], emitted_paths["processed_rosetta_tsv"])
    shutil.copyfile(emitted_paths["index_tsv"], emitted_paths["processed_index_tsv"])
