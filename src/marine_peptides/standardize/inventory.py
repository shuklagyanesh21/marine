"""Inventory raw genomes and normalize them into source-instance records."""

from __future__ import annotations

import csv
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from marine_peptides.config import resolve_path

FASTA_SUFFIXES = (".fna", ".fna.gz", ".fasta", ".fasta.gz", ".fa", ".fa.gz")


@dataclass(frozen=True)
class InventoryRow:
    """One source-instance genome record discovered from manifests + disk."""

    source: str
    source_id: str
    ncbi_accession: str
    raw_path: str
    resolved_path: str
    path_exists: bool
    is_symlink: bool
    status: str
    is_gtdb_representative: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _read_tsv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return [dict(row) for row in reader]


def _iter_fasta_files(dir_path: Path) -> list[Path]:
    if not dir_path.exists():
        return []
    return sorted(
        path
        for path in dir_path.iterdir()
        if path.is_file() and path.name.endswith(FASTA_SUFFIXES)
    )


def _strip_fasta_suffix(name: str) -> str:
    for suffix in sorted(FASTA_SUFFIXES, key=len, reverse=True):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _resolve_status(path: Path) -> tuple[str, str, bool]:
    resolved = path.resolve(strict=False)
    if path.exists():
        return "ok", str(resolved), True
    if path.is_symlink():
        return "broken_symlink", str(resolved), False
    return "missing", str(resolved), False


def scan_ncbi_dataset(ncbi_root: Path) -> dict[str, Path]:
    """Map NCBI accession -> local FASTA path by scanning accession dirs."""
    index: dict[str, Path] = {}
    if not ncbi_root.exists():
        return index

    for child in sorted(ncbi_root.iterdir()):
        if not child.is_dir() or not child.name.startswith(("GCA_", "GCF_")):
            continue
        fasta_files = [
            path
            for path in child.iterdir()
            if path.is_file() and path.name.endswith(FASTA_SUFFIXES)
        ]
        if not fasta_files:
            continue
        preferred = sorted(
            fasta_files,
            key=lambda path: (
                0 if "_genomic.fna" in path.name or "_genomic.fna.gz" in path.name else 1,
                path.name,
            ),
        )[0]
        index[child.name] = preferred
    return index


def scan_tier3_catalogs(cfg: dict[str, Any]) -> dict[str, list[Path]]:
    """Return the actual Tier 3 FASTA files discovered on disk by catalog."""
    tier3_cfg = cfg["tier3"]
    results: dict[str, list[Path]] = {}
    for catalog in ("marref", "gorg", "mardb", "oceandna", "gomc"):
        out_dir = resolve_path(tier3_cfg[catalog]["out_dir"])
        results[catalog] = _iter_fasta_files(out_dir)
    return results


def build_catalog_stem_index(cfg: dict[str, Any]) -> dict[str, dict[str, Path]]:
    """Map each Tier 3 catalog's FASTA stem to the actual on-disk file."""
    stem_index: dict[str, dict[str, Path]] = {}
    for catalog, paths in scan_tier3_catalogs(cfg).items():
        stem_index[catalog] = {_strip_fasta_suffix(path.name): path for path in paths}
    return stem_index


def load_inventory(cfg: dict[str, Any]) -> tuple[list[InventoryRow], dict[str, Any]]:
    """Build source-instance inventory rows from both manifests plus on-disk scans."""
    paths_cfg = cfg["paths"]
    tier3_cfg = cfg["tier3"]
    manifest_path = resolve_path(paths_cfg["manifest"])
    tier3_manifest_path = resolve_path(tier3_cfg["manifest"])
    ncbi_root = resolve_path(tier3_cfg["tier12_raw_root"])

    tier3_scans = scan_tier3_catalogs(cfg)
    tier3_stem_index = build_catalog_stem_index(cfg)
    ncbi_index = scan_ncbi_dataset(ncbi_root)

    inventory: list[InventoryRow] = []

    for row in _read_tsv_rows(manifest_path):
        accession = row.get("ncbi_accession", "").strip()
        local_path = ncbi_index.get(accession)
        if local_path is None:
            expected_dir = ncbi_root / accession
            expected = expected_dir / f"{accession}_genomic.fna"
            status, resolved_path, path_exists = _resolve_status(expected)
            raw_path = str(expected)
            is_symlink = expected.is_symlink()
        else:
            status, resolved_path, path_exists = _resolve_status(local_path)
            raw_path = str(local_path)
            is_symlink = local_path.is_symlink()

        inventory.append(
            InventoryRow(
                source="ncbi",
                source_id=accession,
                ncbi_accession=accession,
                raw_path=raw_path,
                resolved_path=resolved_path,
                path_exists=path_exists,
                is_symlink=is_symlink,
                status=status,
                is_gtdb_representative=row.get("is_gtdb_representative", ""),
            )
        )

    for row in _read_tsv_rows(tier3_manifest_path):
        source = row.get("catalog", "").strip()
        source_id = row.get("catalog_id", "").strip()
        ncbi_accession = row.get("ncbi_accession", "").strip()
        manifest_raw_path = row.get("local_path", "").strip()
        if manifest_raw_path:
            manifest_path_obj = Path(manifest_raw_path)
            stem = _strip_fasta_suffix(manifest_path_obj.name)
            path = tier3_stem_index.get(source, {}).get(stem, manifest_path_obj)
            raw_path = str(path)
            status, resolved_path, path_exists = _resolve_status(path)
            is_symlink = path.is_symlink()
        else:
            raw_path = ""
            status, resolved_path, path_exists = ("missing", "", False)
            is_symlink = False
        inventory.append(
            InventoryRow(
                source=source,
                source_id=source_id,
                ncbi_accession=ncbi_accession,
                raw_path=raw_path,
                resolved_path=resolved_path,
                path_exists=path_exists,
                is_symlink=is_symlink,
                status=status,
                is_gtdb_representative="",
            )
        )

    summary = {
        "source_counts": dict(Counter(row.source for row in inventory)),
        "status_counts": dict(Counter(row.status for row in inventory)),
        "ncbi_disk_count": len(ncbi_index),
        "tier3_disk_counts": {catalog: len(paths) for catalog, paths in tier3_scans.items()},
        "inventory_count": len(inventory),
    }
    return inventory, summary
