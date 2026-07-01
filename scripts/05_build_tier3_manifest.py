#!/usr/bin/env python3
"""Build the Tier 3 manifest by scanning files on disk.

Scans data/raw/tier3/<catalog>/ directories, extracts identifiers from filenames,
enriches with metadata where available, and flags overlap with Tier 1+2.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config() -> dict:
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    with cfg_path.open() as f:
        return yaml.safe_load(f)


def resolve(rel: str) -> Path:
    return PROJECT_ROOT / rel


def extract_accession_from_filename(filename: str) -> str | None:
    """Extract GCA/GCF accession from various filename patterns."""
    m = re.match(r"(GC[AF]_\d+\.\d+)", filename)
    if m:
        return m.group(1)
    return None


def scan_catalog_dir(catalog: str, dir_path: Path) -> list[dict]:
    """Scan a catalog directory and build rows from filenames."""
    rows = []
    if not dir_path.exists():
        return rows
    for fasta in sorted(dir_path.iterdir()):
        if not fasta.name.endswith((".fna", ".fna.gz", ".fasta", ".fasta.gz", ".fa", ".fa.gz")):
            continue
        row = {
            "catalog": catalog,
            "catalog_id": "",
            "ncbi_accession": "",
            "local_path": str(fasta),
            "is_symlink": fasta.is_symlink(),
        }

        if catalog in ("marref", "mardb"):
            acc = extract_accession_from_filename(fasta.name)
            if acc:
                row["ncbi_accession"] = acc
                row["catalog_id"] = acc
        elif catalog == "gorg":
            stem = fasta.name.replace(".fna.gz", "").replace(".fna", "")
            row["catalog_id"] = stem
        elif catalog == "oceandna":
            stem = fasta.name.replace(".fna.gz", "").replace(".fna", "")
            row["catalog_id"] = stem

        rows.append(row)
    return rows


def enrich_mardb_marref(rows: list[dict], metadata_tsv: Path, accession_col: str) -> list[dict]:
    """Enrich MarRef/MarDB rows with metadata from the source TSV."""
    if not metadata_tsv.exists():
        return rows
    meta = pd.read_csv(metadata_tsv, sep="\t", dtype=str, na_filter=False)

    acc_col_clean = accession_col
    if acc_col_clean not in meta.columns:
        return rows

    def parse_acc(val: str) -> str:
        if not val:
            return ""
        parts = val.split(":")
        raw = parts[-1].strip()
        m = re.match(r"(GC[AF]_\d+\.\d+)", raw)
        return m.group(1) if m else ""

    meta["_parsed_acc"] = meta[acc_col_clean].map(parse_acc)
    meta_by_acc = meta.set_index("_parsed_acc")

    for row in rows:
        acc = row.get("ncbi_accession", "")
        if not acc or acc not in meta_by_acc.index:
            continue
        rec = meta_by_acc.loc[acc]
        if isinstance(rec, pd.DataFrame):
            rec = rec.iloc[0]

        row["catalog_id"] = rec.get("id", row["catalog_id"])
        row["gtdb_taxonomy"] = rec.get("tax:gtdb_classification", "")
        row["checkm_completeness"] = rec.get("gen:completeness", "")
        row["checkm_contamination"] = rec.get("gen:contamination", "")
        row["genome_size"] = rec.get("gen:length", "")
        row["host"] = rec.get("host:species", "")
        row["isolation_source"] = rec.get("isol:isolation_source", "")
        row["lat_lon"] = rec.get("isol:lat_lon", "")
        row["depth"] = rec.get("isol:depth", "")

    return rows


def main() -> None:
    cfg = load_config()
    t3 = cfg["tier3"]
    tier12_manifest_path = resolve(cfg["paths"]["manifest"])

    tier12_accessions: set[str] = set()
    if tier12_manifest_path.exists():
        tier12 = pd.read_csv(tier12_manifest_path, sep="\t", dtype=str, na_filter=False, usecols=["ncbi_accession"])
        tier12_accessions = set(tier12["ncbi_accession"].dropna())

    all_rows: list[dict] = []

    # --- MarRef ---
    marref_dir = resolve(t3["marref"]["out_dir"])
    marref_rows = scan_catalog_dir("marref", marref_dir)
    marref_rows = enrich_mardb_marref(
        marref_rows,
        resolve(t3["marref"]["metadata_tsv"]),
        t3["marref"]["accession_col"],
    )
    all_rows.extend(marref_rows)
    print(f"  marref: {len(marref_rows):,} genomes")

    # --- GORG ---
    gorg_dir = resolve(t3["gorg"]["out_dir"])
    gorg_rows = scan_catalog_dir("gorg", gorg_dir)
    for row in gorg_rows:
        row["isolation_source"] = "marine water"
        row["tier3_evidence"] = "gorg_tropics_ena_wgs_set"
    all_rows.extend(gorg_rows)
    print(f"  gorg: {len(gorg_rows):,} genomes")

    # --- MarDB ---
    mardb_dir = resolve(t3["mardb"]["out_dir"])
    mardb_rows = scan_catalog_dir("mardb", mardb_dir)
    mardb_rows = enrich_mardb_marref(
        mardb_rows,
        resolve(t3["mardb"]["metadata_tsv"]),
        t3["mardb"]["accession_col"],
    )
    all_rows.extend(mardb_rows)
    print(f"  mardb: {len(mardb_rows):,} genomes")

    # --- OceanDNA ---
    oceandna_dir = resolve(t3["oceandna"]["out_dir"])
    oceandna_rows = scan_catalog_dir("oceandna", oceandna_dir)
    for row in oceandna_rows:
        row["tier3_evidence"] = "oceandna_figshare"
    all_rows.extend(oceandna_rows)
    print(f"  oceandna: {len(oceandna_rows):,} genomes")

    # --- GOMC (empty but include for completeness) ---
    gomc_dir = resolve(t3["gomc"]["out_dir"])
    gomc_rows = scan_catalog_dir("gomc", gomc_dir)
    all_rows.extend(gomc_rows)
    print(f"  gomc: {len(gomc_rows):,} genomes")

    # Build DataFrame
    df = pd.DataFrame(all_rows)

    # Add is_in_tier12 flag
    df["is_in_tier12"] = df["ncbi_accession"].isin(tier12_accessions)

    # Add tier3_evidence where missing
    df.loc[(df["catalog"] == "marref") & (df.get("tier3_evidence", pd.Series(dtype=str)).eq("")), "tier3_evidence"] = "marref_1.8"
    df.loc[(df["catalog"] == "mardb") & (df.get("tier3_evidence", pd.Series(dtype=str)).eq("")), "tier3_evidence"] = "mardb_1.7"

    # Ensure all expected columns exist
    manifest_cols = [
        "catalog",
        "catalog_id",
        "ncbi_accession",
        "local_path",
        "is_symlink",
        "is_in_tier12",
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
    for col in manifest_cols:
        if col not in df.columns:
            df[col] = ""

    df = df[manifest_cols].copy()
    df = df.fillna("")
    df = df.sort_values(["catalog", "catalog_id"]).reset_index(drop=True)

    # Write
    out_path = resolve(t3["manifest"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, sep="\t", index=False)

    print(f"\nTier 3 manifest: {len(df):,} total genomes -> {out_path}")
    print(f"  Overlap with Tier 1+2: {df['is_in_tier12'].sum():,}")
    print(f"  Unique to Tier 3: {(~df['is_in_tier12']).sum():,}")


if __name__ == "__main__":
    main()
