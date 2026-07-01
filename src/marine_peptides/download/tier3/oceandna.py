"""OceanDNA Tier 3 download helpers."""

from __future__ import annotations

from pathlib import Path

from marine_peptides.download.tier3.common import (
    Logger,
    download_file,
    ensure_dir,
    log,
    stream_extract_tar,
    verify_md5,
)

FIGSHARE_BASE = "https://ndownloader.figshare.com/files"


def figshare_url(file_id: int | str) -> str:
    """Return the direct ndownloader URL for a figshare file."""
    return f"{FIGSHARE_BASE}/{file_id}"


def download_oceandna(
    cfg: dict,
    interim_dir: str | Path,
    out_dir: str | Path,
    budget_path: str | Path,
    budget_gb: float,
    logger: Logger = None,
) -> dict[str, Path]:
    """Download, verify, and extract OceanDNA tar archives."""
    interim_dir = ensure_dir(interim_dir)
    out_dir = ensure_dir(out_dir)
    state_path = interim_dir / "state.json"
    extracted: dict[str, Path] = {}

    for label in ("reps", "nonreps"):
        entry = cfg["files"][label]
        archive_path = interim_dir / entry["name"]
        if not archive_path.exists():
            log(f"Downloading OceanDNA {label}: {entry['name']}", logger)
            download_file(figshare_url(entry["id"]), archive_path, logger=logger)
        if not verify_md5(archive_path, entry["md5"]):
            raise RuntimeError(f"MD5 mismatch for {archive_path}")
        members = stream_extract_tar(
            archive_path=archive_path,
            out_dir=out_dir,
            logger=logger,
            state_path=state_path,
            budget_path=budget_path,
            budget_gb=budget_gb,
        )
        extracted.update({k: Path(v) for k, v in members.items()})
        archive_path.unlink(missing_ok=True)

    supp = cfg["files"].get("supp")
    if supp:
        supp_path = interim_dir / supp["name"]
        if not supp_path.exists():
            log(f"Downloading OceanDNA metadata archive: {supp['name']}", logger)
            download_file(figshare_url(supp["id"]), supp_path, logger=logger)

    log(f"oceandna: {len(extracted):,} FASTA members extracted", logger)
    return extracted
