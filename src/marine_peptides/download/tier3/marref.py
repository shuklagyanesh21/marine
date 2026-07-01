"""MarRef / generic NCBI-accession-backed Tier 3 catalog helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import subprocess
import zipfile
from pathlib import Path

import pandas as pd
import requests

from marine_peptides.download.tier3.common import (
    Logger,
    discover_tier12_fastas,
    ensure_dir,
    log,
    parse_prefixed_accession,
    symlink_existing_fasta,
    write_rows_tsv,
)


def load_catalog_table(metadata_tsv: str | Path, accession_col: str, catalog: str) -> pd.DataFrame:
    """Load a MarRef/MarDB-style metadata table and normalize NCBI accessions."""
    df = pd.read_csv(metadata_tsv, sep="\t", dtype=str, na_filter=False)
    df["catalog"] = catalog
    df["ncbi_accession"] = df[accession_col].map(parse_prefixed_accession)
    df = df[df["ncbi_accession"].str.startswith(("GCA_", "GCF_"))].copy()
    df = df.drop_duplicates(subset="ncbi_accession").reset_index(drop=True)
    return df


def write_accession_list(df: pd.DataFrame, path: str | Path) -> Path:
    """Write one accession per line."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(df["ncbi_accession"]) + "\n")
    return p


def write_metadata_snapshot(df: pd.DataFrame, path: str | Path) -> Path:
    """Write a compact metadata snapshot used later for the Tier 3 manifest."""
    columns = [c for c in df.columns if c]
    rows = df[columns].to_dict(orient="records")
    return write_rows_tsv(path, rows, fieldnames=columns)


def _materialize_fasta(src: Path, dest: Path) -> Path:
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        src.rename(dest)
    except OSError:
        shutil.copy2(src, dest)
    return dest


def _find_downloaded_fasta(batch_dir: Path, accession: str) -> Path | None:
    data_dir = batch_dir / "ncbi_dataset" / "data" / accession
    if not data_dir.exists():
        return None
    matches = sorted(data_dir.glob("*_genomic.fna*"))
    return matches[0] if matches else None


def _discover_existing_catalog_fastas(out_dir: Path) -> dict[str, Path]:
    """Map already materialized Tier 3 FASTAs back to assembly accessions."""
    out: dict[str, Path] = {}
    if not out_dir.exists():
        return out
    for fasta in out_dir.glob("*_genomic.fna*"):
        accession = fasta.name.split("_ASM", 1)[0]
        if accession.startswith(("GCA_", "GCF_")):
            out[accession] = fasta
    return out


def _datasets_cli() -> str | None:
    return shutil.which("datasets")


def _download_batch_via_rest(batch: list[str], batch_dir: Path, logger: Logger = None) -> bool:
    """Download an NCBI Datasets zip package directly from the REST API."""
    if (batch_dir / "ncbi_dataset" / "data").exists():
        return True
    joined = ",".join(batch)
    url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/{joined}/download"
    params = {"include_annotation_type": "GENOME_FASTA"}
    zip_path = batch_dir / "datasets.zip"
    log(f"Downloading {len(batch)} accession(s) via NCBI Datasets REST API", logger)
    with requests.get(url, params=params, stream=True, timeout=300) as resp:
        if resp.status_code != 200:
            log(f"REST download failed with status {resp.status_code}", logger)
            return False
        with zip_path.open("wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(batch_dir)
    return True


def _download_missing_with_datasets(
    accessions: list[str],
    interim_dir: Path,
    out_dir: Path,
    logger: Logger = None,
) -> tuple[dict[str, Path], list[str]]:
    """Download missing accessions via the NCBI Datasets CLI or REST API."""
    datasets = _datasets_cli()
    if not accessions:
        return {}, []

    resolved: dict[str, Path] = {}
    unresolved: list[str] = []
    batch_size = 500 if datasets is not None else 100
    ensure_dir(interim_dir)
    batches: list[tuple[int, list[str], Path]] = []

    for start in range(0, len(accessions), batch_size):
        batch = accessions[start : start + batch_size]
        batch_dir = interim_dir / f"datasets_batch_{start:06d}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        input_path = batch_dir / "accessions.txt"
        input_path.write_text("\n".join(batch) + "\n")
        batches.append((start, batch, batch_dir))

    if datasets is None:
        max_workers = 4
        log(f"Downloading {len(batches):,} REST batches with {max_workers} workers", logger)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_download_batch_via_rest, batch, batch_dir, logger): (start, batch, batch_dir)
                for start, batch, batch_dir in batches
            }
            for index, future in enumerate(as_completed(futures), start=1):
                start, batch, batch_dir = futures[future]
                ok = future.result()
                if not ok:
                    unresolved.extend(batch)
                if index % 10 == 0:
                    log(f"Completed {index:,}/{len(batches):,} REST batches", logger)
    else:
        for _, batch, batch_dir in batches:
            input_path = batch_dir / "accessions.txt"
            zip_path = batch_dir / "datasets.zip"
            cmd = [
                datasets,
                "download",
                "genome",
                "accession",
                "--inputfile",
                str(input_path),
                "--include",
                "genome",
                "--filename",
                str(zip_path),
            ]
            log(f"Running NCBI datasets for {len(batch)} accession(s)", logger)
            subprocess.run(cmd, check=False, cwd=batch_dir)
            if zip_path.exists():
                with zipfile.ZipFile(zip_path) as archive:
                    archive.extractall(batch_dir)

    copy_tasks: list[tuple[str, Path, Path, Path]] = []
    for _, batch, batch_dir in batches:
        for accession in batch:
            fasta = _find_downloaded_fasta(batch_dir, accession)
            if fasta is None:
                if accession not in unresolved:
                    unresolved.append(accession)
                continue
            dest = out_dir / fasta.name
            if dest.exists():
                resolved[accession] = dest
            else:
                copy_tasks.append((accession, fasta, dest, batch_dir))

    if copy_tasks:
        max_workers = 8
        log(f"Materializing {len(copy_tasks):,} FASTAs with {max_workers} workers", logger)

        def _copy_one(task: tuple[str, Path, Path, Path]) -> tuple[str, Path]:
            accession, fasta, dest, _ = task
            return accession, _materialize_fasta(fasta, dest)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_copy_one, task): task for task in copy_tasks}
            for index, future in enumerate(as_completed(futures), start=1):
                accession, dest = future.result()
                resolved[accession] = dest
                if index % 500 == 0:
                    log(f"Materialized {index:,}/{len(copy_tasks):,} FASTAs", logger)

    for _, batch, batch_dir in batches:
        if all(_find_downloaded_fasta(batch_dir, accession) is None for accession in batch):
            shutil.rmtree(batch_dir, ignore_errors=True)

    return resolved, unresolved


def download_ncbi_catalog(
    metadata_tsv: str | Path,
    accession_col: str,
    catalog: str,
    out_dir: str | Path,
    interim_dir: str | Path,
    tier12_root: str | Path,
    logger: Logger = None,
    missing_path: str | Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    """Download or symlink an NCBI-accession-backed Tier 3 catalog."""
    df = load_catalog_table(metadata_tsv, accession_col=accession_col, catalog=catalog)
    out_dir = ensure_dir(out_dir)
    interim_dir = ensure_dir(interim_dir)
    existing = discover_tier12_fastas(tier12_root)
    tier3_existing = _discover_existing_catalog_fastas(out_dir)
    local_paths: dict[str, Path] = {}

    missing: list[str] = []
    for accession in df["ncbi_accession"]:
        if accession in tier3_existing:
            local_paths[accession] = tier3_existing[accession]
        elif accession in existing:
            src = existing[accession]
            dest = out_dir / src.name
            local_paths[accession] = symlink_existing_fasta(src, dest)
        else:
            missing.append(accession)

    downloaded, unresolved = _download_missing_with_datasets(
        missing,
        interim_dir=interim_dir,
        out_dir=out_dir,
        logger=logger,
    )
    local_paths.update(downloaded)
    if missing_path is not None:
        p = Path(missing_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join(unresolved) + ("\n" if unresolved else ""))

    log(
        f"{catalog}: {len(local_paths):,} local FASTAs ({len(df) - len(missing):,} symlinked, "
        f"{len(downloaded):,} downloaded, {len(unresolved):,} unresolved)",
        logger,
    )
    return df, local_paths
