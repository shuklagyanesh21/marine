"""GORG-Tropics Tier 3 download helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

from marine_peptides.download.tier3.common import Logger, download_file, ensure_dir, log

GORG_FIELDS = [
    "accession",
    "scientific_name",
    "sample_accession",
    "study_accession",
    "wgs_set",
    "set_fasta_ftp",
    "country",
    "collection_date",
    "location",
    "isolation_source",
]


def fetch_gorg_metadata(
    project: str,
    metadata_tsv: str | Path,
    logger: Logger = None,
    base_url: str = "https://www.ebi.ac.uk/ena/portal/api/filereport",
) -> pd.DataFrame:
    """Fetch and cache the ENA WGS-set metadata for GORG-Tropics."""
    metadata_tsv = Path(metadata_tsv)
    if metadata_tsv.exists():
        return pd.read_csv(metadata_tsv, sep="\t", dtype=str, na_filter=False)

    params = {
        "accession": project,
        "result": "wgs_set",
        "format": "tsv",
        "fields": ",".join(GORG_FIELDS),
    }
    log(f"Fetching GORG ENA filereport for {project}", logger)
    resp = requests.get(base_url, params=params, timeout=120)
    resp.raise_for_status()
    metadata_tsv.parent.mkdir(parents=True, exist_ok=True)
    metadata_tsv.write_text(resp.text)
    return pd.read_csv(metadata_tsv, sep="\t", dtype=str, na_filter=False)


def download_gorg(
    metadata_tsv: str | Path,
    out_dir: str | Path,
    logger: Logger = None,
    max_workers: int = 12,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    """Download GORG SAG FASTAs from ENA WGS-set URLs."""
    df = pd.read_csv(metadata_tsv, sep="\t", dtype=str, na_filter=False)
    out_dir = ensure_dir(out_dir)
    local_paths: dict[str, Path] = {}
    pending: list[tuple[str, str, Path]] = []

    for row in df.to_dict(orient="records"):
        catalog_id = row.get("wgs_set") or row.get("accession")
        if not catalog_id:
            continue
        ftp_path = row.get("set_fasta_ftp", "").strip()
        if not ftp_path:
            continue
        url = ftp_path if ftp_path.startswith("http") else f"https://{ftp_path}"
        dest = out_dir / f"{catalog_id}.fna.gz"
        if dest.exists():
            local_paths[catalog_id] = dest
        else:
            pending.append((catalog_id, url, dest))

    def _one(task: tuple[str, str, Path]) -> tuple[str, Path]:
        catalog_id, url, dest = task
        download_file(url, dest, logger=logger)
        return catalog_id, dest

    if pending:
        log(f"gorg: downloading {len(pending):,} remaining SAG FASTAs with {max_workers} workers", logger)
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_one, task): task[0] for task in pending}
            for future in as_completed(futures):
                catalog_id, dest = future.result()
                local_paths[catalog_id] = dest
                completed += 1
                if completed % 250 == 0:
                    log(f"gorg: downloaded {completed:,}/{len(pending):,} pending FASTAs", logger)

    log(f"gorg: {len(local_paths):,} FASTA files ready", logger)
    return df, local_paths
