"""GOMC Tier 3 download helpers."""

from __future__ import annotations

from pathlib import Path

from marine_peptides.download.tier3.common import (
    Logger,
    download_file,
    download_file_parallel,
    ensure_dir,
    log,
    read_md5_manifest,
    stream_extract_tar,
    verify_md5,
)


def _url(base: str, name: str) -> str:
    return f"{base.rstrip('/')}/{name}"


def download_gomc(
    cfg: dict,
    interim_dir: str | Path,
    out_dir: str | Path,
    budget_path: str | Path,
    budget_gb: float,
    logger: Logger = None,
) -> dict[str, Path]:
    """Download, verify, and extract GOMC FASTA tarballs."""
    interim_dir = ensure_dir(interim_dir)
    out_dir = ensure_dir(out_dir)
    state_path = interim_dir / "state.json"

    md5_path = interim_dir / "md5.txt"
    if not md5_path.exists():
        log("Downloading GOMC md5 manifest", logger)
        download_file(_url(cfg["cngb_base"], "md5.txt"), md5_path, logger=logger)
    expected = read_md5_manifest(md5_path)

    extracted: dict[str, Path] = {}
    for name in cfg["files"]:
        if name == "md5.txt":
            continue
        archive_path = interim_dir / name
        if not archive_path.exists():
            log(f"Downloading GOMC archive: {name}", logger)
            download_file_parallel(
                _url(cfg["cngb_base"], name),
                archive_path,
                logger=logger,
                parts=24,
            )
        if name in expected and not verify_md5(archive_path, expected[name]):
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

    log(f"gomc: {len(extracted):,} FASTA members extracted", logger)
    return extracted
