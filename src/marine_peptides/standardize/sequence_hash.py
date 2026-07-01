"""Sequence-content hashing for exact duplicate detection."""

from __future__ import annotations

import gzip
import hashlib
import os
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from multiprocessing import Pool
from pathlib import Path


@dataclass(frozen=True)
class SequenceMetrics:
    """Normalized per-genome sequence metrics."""

    md5_seq: str
    n_contigs: int
    total_bp: int
    error: str = ""

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


def _open_text(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt")
    return path.open()


def _read_fasta_sequences(path: Path) -> list[str]:
    sequences: list[str] = []
    chunks: list[str] = []
    with _open_text(path) as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith(">"):
                if chunks:
                    sequences.append("".join(chunks).upper())
                    chunks = []
                continue
            chunks.append(line)
    if chunks:
        sequences.append("".join(chunks).upper())
    return sequences


def compute_md5_seq(path: str | Path) -> SequenceMetrics:
    """Hash sorted+uppercased contigs to identify exact duplicate genomes."""
    fasta_path = Path(path)
    try:
        sequences = _read_fasta_sequences(fasta_path)
    except Exception as exc:  # pragma: no cover - exercised on real files
        return SequenceMetrics(md5_seq="", n_contigs=0, total_bp=0, error=str(exc))

    sequences.sort()
    digest = hashlib.md5()
    total_bp = 0
    for sequence in sequences:
        encoded = sequence.encode("ascii")
        digest.update(encoded)
        digest.update(b"\0")
        total_bp += len(sequence)
    return SequenceMetrics(md5_seq=digest.hexdigest(), n_contigs=len(sequences), total_bp=total_bp)


def _compute_one(path: str) -> tuple[str, SequenceMetrics]:
    return path, compute_md5_seq(path)


def compute_metrics_for_paths(
    paths: Iterable[str | Path],
    workers: int = 1,
    logger=print,
) -> dict[str, SequenceMetrics]:
    """Compute per-file sequence hashes in parallel."""
    unique_paths = sorted({str(Path(path)) for path in paths if str(path)})
    if not unique_paths:
        return {}

    workers = max(1, workers)
    logger(f"Hashing {len(unique_paths):,} genome files with {workers} worker(s)")

    metrics: dict[str, SequenceMetrics] = {}
    if workers == 1:
        for index, path in enumerate(unique_paths, start=1):
            metrics[path] = compute_md5_seq(path)
            if index % 1000 == 0:
                logger(f"  hashed {index:,}/{len(unique_paths):,}")
        return metrics

    # Keep chunks small because genome sizes vary a lot across catalogs; coarse
    # chunking leads to long straggler tails and makes progress invisible.
    chunk_size = max(1, min(16, len(unique_paths) // max(1, workers * 64)))
    with Pool(processes=min(workers, os.cpu_count() or workers)) as pool:
        for index, (path, result) in enumerate(
            pool.imap_unordered(_compute_one, unique_paths, chunksize=chunk_size),
            start=1,
        ):
            metrics[path] = result
            if index % 250 == 0:
                logger(f"  hashed {index:,}/{len(unique_paths):,}")
    return metrics
