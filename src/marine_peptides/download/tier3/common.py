"""Shared helpers for Tier 3 marine catalog downloads."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import shutil
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

import requests

Logger = Callable[[str], None] | None
CHUNK_SIZE = 1024 * 1024


def log(message: str, logger: Logger = None) -> None:
    """Emit a progress message when a logger is provided."""
    if logger is not None:
        logger(message)


def ensure_dir(path: str | Path) -> Path:
    """Create *path* if needed and return it as a Path."""
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def parse_prefixed_accession(value: str) -> str:
    """Extract a plain accession from values like ``insdc.gca:GCA_...``."""
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if not value:
        return ""
    if ":" in value:
        value = value.split(":", 1)[1]
    return value.strip()


def load_json(path: str | Path, default: dict | list | None = None):
    """Load JSON from disk, returning *default* when the file is missing."""
    p = Path(path)
    if not p.exists():
        return {} if default is None else default
    with p.open() as handle:
        return json.load(handle)


def write_json(path: str | Path, payload: dict | list) -> Path:
    """Write JSON with stable formatting."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return p


def compute_md5(path: str | Path) -> str:
    """Return the MD5 checksum of *path*."""
    digest = hashlib.md5()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def verify_md5(path: str | Path, expected: str) -> bool:
    """Return ``True`` when *path* matches *expected* (case-insensitive)."""
    return compute_md5(path).lower() == expected.strip().lower()


def read_md5_manifest(path: str | Path) -> dict[str, str]:
    """Parse a simple ``md5 filename`` manifest."""
    out: dict[str, str] = {}
    with Path(path).open() as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                out[parts[-1]] = parts[0]
    return out


def disk_usage_gb(path: str | Path) -> tuple[float, float, float]:
    """Return ``(used_gb, free_gb, total_gb)`` for the filesystem at *path*."""
    target = Path(path)
    while not target.exists() and target != target.parent:
        target = target.parent
    usage = shutil.disk_usage(target)
    gb = 1024**3
    return usage.used / gb, usage.free / gb, usage.total / gb


def ensure_disk_budget(path: str | Path, budget_gb: float, min_free_gb: float = 5.0) -> None:
    """Raise if the filesystem is already over budget or nearly full."""
    used_gb, free_gb, _ = disk_usage_gb(path)
    if used_gb >= budget_gb:
        raise RuntimeError(f"Disk budget exceeded: used {used_gb:.1f} GB >= {budget_gb:.1f} GB")
    if free_gb <= min_free_gb:
        raise RuntimeError(f"Low free space: only {free_gb:.1f} GB left")


def discover_tier12_fastas(ncbi_root: str | Path) -> dict[str, Path]:
    """Map Tier 1+2 NCBI accessions to their downloaded FASTA files."""
    root = Path(ncbi_root)
    out: dict[str, Path] = {}
    if not root.exists():
        return out

    for child in root.iterdir():
        if not child.is_dir():
            continue
        accession = child.name
        matches = sorted(child.glob("*_genomic.fna*"))
        if matches:
            out[accession] = matches[0]
    return out


def symlink_existing_fasta(src: str | Path, dest: str | Path) -> Path:
    """Create or replace *dest* with a symlink to *src*."""
    src_path = Path(src).resolve()
    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists() or dest_path.is_symlink():
        dest_path.unlink()
    dest_path.symlink_to(src_path)
    return dest_path


def download_file(
    url: str,
    dest: str | Path,
    logger: Logger = None,
    session: requests.Session | None = None,
    max_retries: int = 4,
    timeout: int = 120,
) -> Path:
    """Download *url* to *dest*, resuming from a partial file when possible."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    sess = session or requests.Session()

    for attempt in range(1, max_retries + 1):
        existing_bytes = dest.stat().st_size if dest.exists() else 0
        headers = {"Range": f"bytes={existing_bytes}-"} if existing_bytes else {}
        mode = "ab" if existing_bytes else "wb"
        try:
            with sess.get(url, stream=True, timeout=timeout, headers=headers) as resp:
                resp.raise_for_status()
                if existing_bytes and resp.status_code == 200:
                    # Server ignored Range; restart cleanly.
                    existing_bytes = 0
                    mode = "wb"
                with dest.open(mode) as handle:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            handle.write(chunk)
            return dest
        except requests.RequestException as exc:
            log(f"Download attempt {attempt}/{max_retries} failed for {url}: {exc}", logger)
            if attempt == max_retries:
                raise
    return dest


def download_file_parallel(
    url: str,
    dest: str | Path,
    logger: Logger = None,
    parts: int = 8,
    max_retries: int = 4,
    timeout: int = 1200,
) -> Path:
    """Download *url* with multiple HTTP range requests and merge the parts."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    head = requests.head(url, allow_redirects=True, timeout=120)
    head.raise_for_status()
    total_size = int(head.headers.get("Content-Length", "0"))
    accept_ranges = head.headers.get("Accept-Ranges", "").lower()
    if total_size <= 0 or "bytes" not in accept_ranges:
        return download_file(url, dest, logger=logger, max_retries=max_retries, timeout=timeout)

    if dest.exists() and dest.stat().st_size == total_size:
        return dest

    parts_dir = ensure_dir(dest.parent / f".{dest.name}.parts")
    chunk = (total_size + parts - 1) // parts

    def _download_one(index: int) -> Path:
        start = index * chunk
        end = min(total_size - 1, start + chunk - 1)
        if start > end:
            return parts_dir / f"{index:03d}.part"

        part_path = parts_dir / f"{index:03d}.part"
        expected_size = end - start + 1
        existing_size = part_path.stat().st_size if part_path.exists() else 0
        if existing_size == expected_size:
            return part_path
        if existing_size > expected_size:
            part_path.unlink()
            existing_size = 0

        for attempt in range(1, max_retries + 1):
            headers = {"Range": f"bytes={start + existing_size}-{end}"}
            mode = "ab" if existing_size else "wb"
            try:
                with requests.get(url, headers=headers, stream=True, timeout=timeout) as resp:
                    resp.raise_for_status()
                    if resp.status_code != 206 and expected_size != total_size:
                        raise requests.HTTPError(f"expected 206, got {resp.status_code}")
                    with part_path.open(mode) as handle:
                        for data in resp.iter_content(chunk_size=CHUNK_SIZE):
                            if data:
                                handle.write(data)
                if part_path.stat().st_size == expected_size:
                    return part_path
                existing_size = part_path.stat().st_size if part_path.exists() else 0
            except requests.RequestException as exc:
                log(f"Part {index + 1}/{parts} attempt {attempt} failed: {exc}", logger)
                existing_size = part_path.stat().st_size if part_path.exists() else 0
                if attempt == max_retries:
                    raise
        return part_path

    log(f"Starting parallel download of {dest.name} with {parts} parts", logger)
    with ThreadPoolExecutor(max_workers=parts) as pool:
        futures = {pool.submit(_download_one, idx): idx for idx in range(parts)}
        completed = 0
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed == parts or completed % 2 == 0:
                log(f"Downloaded {completed}/{parts} parts for {dest.name}", logger)

    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    with tmp_path.open("wb") as out_handle:
        for idx in range(parts):
            part_path = parts_dir / f"{idx:03d}.part"
            if not part_path.exists():
                continue
            with part_path.open("rb") as in_handle:
                shutil.copyfileobj(in_handle, out_handle, length=CHUNK_SIZE)
    tmp_path.replace(dest)
    shutil.rmtree(parts_dir, ignore_errors=True)
    return dest


def _default_member_name(member_name: str) -> str:
    """Convert archive member names into per-record ``.fna.gz`` filenames."""
    base = Path(member_name).name
    if base.endswith(".gz"):
        base = base[:-3]
    for suffix in (".fna", ".fa", ".fasta", ".fas", ".contigs"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return f"{base}.fna.gz"


def stream_extract_tar(
    archive_path: str | Path,
    out_dir: str | Path,
    logger: Logger = None,
    state_path: str | Path | None = None,
    name_transform: Callable[[str], str | None] | None = None,
    budget_path: str | Path | None = None,
    budget_gb: float | None = None,
) -> dict[str, str]:
    """Stream-extract a tar archive into one gzipped FASTA per member."""

    out_dir = ensure_dir(out_dir)
    state = load_json(state_path, default={"extracted": {}}) if state_path else {"extracted": {}}
    extracted: dict[str, str] = dict(state.get("extracted", {}))
    transform = name_transform or _default_member_name
    extracted_now = 0

    with tarfile.open(archive_path, mode="r|*") as tar:
        for member in tar:
            if not member.isfile():
                continue
            out_name = transform(member.name)
            if not out_name:
                continue

            out_path = out_dir / out_name
            if out_path.exists():
                extracted[member.name] = str(out_path)
                continue

            if budget_path is not None and budget_gb is not None:
                ensure_disk_budget(budget_path, budget_gb)

            fileobj = tar.extractfile(member)
            if fileobj is None:
                continue

            if member.name.endswith(".gz"):
                with out_path.open("wb") as handle:
                    shutil.copyfileobj(fileobj, handle, length=CHUNK_SIZE)
            else:
                with gzip.open(out_path, "wb") as handle:
                    shutil.copyfileobj(fileobj, handle, length=CHUNK_SIZE)

            extracted[member.name] = str(out_path)
            extracted_now += 1
            if state_path:
                write_json(state_path, {"extracted": extracted})
            if extracted_now % 500 == 0:
                log(f"Extracted {extracted_now:,} members from {Path(archive_path).name}", logger)

    if extracted_now:
        log(f"Finished extracting {extracted_now:,} members from {Path(archive_path).name}", logger)
    return extracted


def write_rows_tsv(path: str | Path, rows: list[dict[str, str]], fieldnames: list[str]) -> Path:
    """Write a TSV from dict rows."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    return p
