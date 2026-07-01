"""Build the Tier 3 per-catalog manifest."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

MANIFEST_COLUMNS = [
    "catalog",
    "catalog_id",
    "ncbi_accession",
    "local_path",
    "source_url",
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


def _read_tsv(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, sep="\t", dtype=str, na_filter=False)


def build_tier3_manifest(inventory_paths: list[str | Path], tier12_manifest: str | Path) -> pd.DataFrame:
    """Combine per-catalog inventory TSVs into a single Tier 3 manifest."""
    frames = [_read_tsv(path) for path in inventory_paths]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    tier12 = _read_tsv(tier12_manifest)
    tier12_accessions = set(tier12.get("ncbi_accession", pd.Series(dtype=str)).astype(str))
    combined["is_in_tier12"] = combined.get("ncbi_accession", "").astype(str).isin(tier12_accessions)

    for col in MANIFEST_COLUMNS:
        if col not in combined.columns:
            combined[col] = ""

    combined = combined[MANIFEST_COLUMNS].copy()
    combined = combined.sort_values(["catalog", "catalog_id", "ncbi_accession"]).reset_index(drop=True)
    return combined


def write_tier3_manifest(df: pd.DataFrame, path: str | Path) -> Path:
    """Write the Tier 3 manifest TSV."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, sep="\t", index=False)
    return out
