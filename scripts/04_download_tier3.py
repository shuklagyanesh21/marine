#!/usr/bin/env python3
"""Download Tier 3 curated marine catalogs."""

from __future__ import annotations

import argparse
from pathlib import Path

from marine_peptides.config import load_config, resolve_path
from marine_peptides.download.tier3.common import (
    disk_usage_gb,
    ensure_disk_budget,
    write_json,
    write_rows_tsv,
)
from marine_peptides.download.tier3.gomc import download_gomc
from marine_peptides.download.tier3.gorg import download_gorg
from marine_peptides.download.tier3.marref import download_ncbi_catalog
from marine_peptides.download.tier3.mardb import download_mardb
from marine_peptides.download.tier3.oceandna import download_oceandna

INVENTORY_FIELDS = [
    "catalog",
    "catalog_id",
    "ncbi_accession",
    "local_path",
    "source_url",
    "gtdb_taxonomy",
    "checkm_completeness",
    "checkm_contamination",
    "genome_size",
    "host",
    "isolation_source",
    "lat_lon",
    "depth",
    "tier3_evidence",
]


def _catalog_id_from_name(name: str) -> str:
    base = Path(name).name
    if base.endswith(".fna.gz"):
        return base[:-7]
    if base.endswith(".gz"):
        return base[:-3]
    return Path(base).stem


def _write_inventory(rows: list[dict[str, str]], path: str | Path) -> Path:
    return write_rows_tsv(path, rows, fieldnames=INVENTORY_FIELDS)


def _rows_for_ncbi_catalog(df, local_paths: dict[str, Path], catalog: str, evidence: str):
    rows: list[dict[str, str]] = []
    for row in df.to_dict(orient="records"):
        accession = row["ncbi_accession"]
        local = local_paths.get(accession)
        if local is None:
            continue
        rows.append(
            {
                "catalog": catalog,
                "catalog_id": row.get("id", accession),
                "ncbi_accession": accession,
                "local_path": str(local),
                "source_url": f"https://www.ncbi.nlm.nih.gov/datasets/genome/{accession}/",
                "gtdb_taxonomy": row.get("tax:gtdb_classification", ""),
                "checkm_completeness": row.get("gen:completeness", ""),
                "checkm_contamination": row.get("gen:contamination", ""),
                "genome_size": row.get("gen:length", ""),
                "host": row.get("host:species", "") or row.get("host:common_name", ""),
                "isolation_source": row.get("isol:isolation_source", ""),
                "lat_lon": row.get("isol:lat_lon", ""),
                "depth": row.get("isol:depth", ""),
                "tier3_evidence": evidence,
            }
        )
    return rows


def _rows_for_gorg(df, local_paths: dict[str, Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in df.to_dict(orient="records"):
        catalog_id = row.get("wgs_set") or row.get("accession", "")
        local = local_paths.get(catalog_id)
        if not catalog_id or local is None:
            continue
        ftp_path = row.get("set_fasta_ftp", "")
        rows.append(
            {
                "catalog": "gorg",
                "catalog_id": catalog_id,
                "ncbi_accession": "",
                "local_path": str(local),
                "source_url": ftp_path if ftp_path.startswith("http") else f"https://{ftp_path}",
                "gtdb_taxonomy": "",
                "checkm_completeness": "",
                "checkm_contamination": "",
                "genome_size": "",
                "host": "",
                "isolation_source": row.get("isolation_source", ""),
                "lat_lon": "",
                "depth": "",
                "tier3_evidence": "gorg_tropics_ena_wgs_set",
            }
        )
    return rows


def _rows_for_archive_catalog(local_paths: dict[str, Path], catalog: str, source_url: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for member_name, local_path in sorted(local_paths.items()):
        rows.append(
            {
                "catalog": catalog,
                "catalog_id": _catalog_id_from_name(member_name),
                "ncbi_accession": "",
                "local_path": str(local_path),
                "source_url": source_url,
                "gtdb_taxonomy": "",
                "checkm_completeness": "",
                "checkm_contamination": "",
                "genome_size": "",
                "host": "",
                "isolation_source": "",
                "lat_lon": "",
                "depth": "",
                "tier3_evidence": source_url,
            }
        )
    return rows


def _mark_complete(interim_dir: Path, rows: list[dict[str, str]]) -> None:
    write_json(
        interim_dir / "state.json",
        {
            "completed": True,
            "count": len(rows),
        },
    )


def run_catalog(name: str, cfg: dict, budget_path: Path, budget_gb: float) -> Path:
    tier12_root = resolve_path(cfg["tier12_raw_root"])
    cat_cfg = cfg[name]
    ensure_disk_budget(budget_path, budget_gb)

    if name == "marref":
        df, local_paths = download_ncbi_catalog(
            metadata_tsv=resolve_path(cat_cfg["metadata_tsv"]),
            accession_col=cat_cfg["accession_col"],
            catalog="marref",
            out_dir=resolve_path(cat_cfg["out_dir"]),
            interim_dir=resolve_path(cat_cfg["interim_dir"]),
            tier12_root=tier12_root,
            logger=print,
        )
        rows = _rows_for_ncbi_catalog(df, local_paths, "marref", "marref_1.8")
    elif name == "gorg":
        df, local_paths = download_gorg(
            metadata_tsv=resolve_path(cat_cfg["metadata_tsv"]),
            out_dir=resolve_path(cat_cfg["out_dir"]),
            logger=print,
        )
        rows = _rows_for_gorg(df, local_paths)
    elif name == "mardb":
        df, local_paths = download_mardb(
            metadata_tsv=resolve_path(cat_cfg["metadata_tsv"]),
            accession_col=cat_cfg["accession_col"],
            out_dir=resolve_path(cat_cfg["out_dir"]),
            interim_dir=resolve_path(cat_cfg["interim_dir"]),
            tier12_root=tier12_root,
            missing_path=resolve_path(cat_cfg["missing_accessions"]),
            logger=print,
        )
        rows = _rows_for_ncbi_catalog(df, local_paths, "mardb", "mardb_1.7")
    elif name == "oceandna":
        local_paths = download_oceandna(
            cfg=cat_cfg,
            interim_dir=resolve_path(cat_cfg["interim_dir"]),
            out_dir=resolve_path(cat_cfg["out_dir"]),
            budget_path=budget_path,
            budget_gb=budget_gb,
            logger=print,
        )
        rows = _rows_for_archive_catalog(
            local_paths,
            catalog="oceandna",
            source_url="https://figshare.com/collections/_/5564844",
        )
    elif name == "gomc":
        local_paths = download_gomc(
            cfg=cat_cfg,
            interim_dir=resolve_path(cat_cfg["interim_dir"]),
            out_dir=resolve_path(cat_cfg["out_dir"]),
            budget_path=budget_path,
            budget_gb=budget_gb,
            logger=print,
        )
        rows = _rows_for_archive_catalog(
            local_paths,
            catalog="gomc",
            source_url=cat_cfg["cngb_base"],
        )
    else:
        raise ValueError(f"Unknown catalog: {name}")

    inventory_path = resolve_path(cat_cfg["inventory_tsv"])
    _write_inventory(rows, inventory_path)
    _mark_complete(resolve_path(cat_cfg["interim_dir"]), rows)
    used_gb, free_gb, total_gb = disk_usage_gb(budget_path)
    print(
        f"{name}: wrote {len(rows):,} inventory rows -> {inventory_path} "
        f"(disk used {used_gb:.1f}/{total_gb:.1f} GB, free {free_gb:.1f} GB)"
    )
    ensure_disk_budget(budget_path, budget_gb)
    return inventory_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", choices=["marref", "gorg", "mardb", "oceandna", "gomc"])
    args = parser.parse_args()

    cfg = load_config()
    t3 = cfg["tier3"]
    budget_path = resolve_path(t3["out_root"])
    budget_gb = float(t3["disk_budget_gb"])
    order = [args.catalog] if args.catalog else list(t3["order"])

    for name in order:
        print(f"=== {name} ===")
        run_catalog(name, t3, budget_path=budget_path, budget_gb=budget_gb)


if __name__ == "__main__":
    main()
