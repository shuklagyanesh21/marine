"""Load standardized genomes and run the configured skani stages."""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from marine_peptides.config import project_root, resolve_path
from marine_peptides.standardize.inventory import FASTA_SUFFIXES


@dataclass(frozen=True)
class GenomeRecord:
    """One standardized genome eligible for ANI dereplication."""

    canonical_id: str
    winning_source: str
    ncbi_accession: str
    fasta_path: str
    canonical_raw_path: str
    md5_seq: str
    n_contigs: int
    total_bp: int
    status: str
    is_gtdb_representative: str
    dedup_reason: str

    @property
    def fasta_abs_path(self) -> Path:
        return resolve_path(self.fasta_path)

    @property
    def gtdb_representative_bool(self) -> bool:
        return self.is_gtdb_representative.strip().lower() in {"1", "true", "t", "yes"}

    @property
    def canonical_raw_abs_path(self) -> Path:
        return resolve_path(self.canonical_raw_path) if self.canonical_raw_path else self.fasta_abs_path

    @property
    def derep_input_abs_path(self) -> Path:
        for candidate in (
            self.fasta_abs_path.resolve(strict=False),
            self.canonical_raw_abs_path,
            self.fasta_abs_path,
        ):
            resolved = _resolve_existing_fasta_path(str(candidate))
            if resolved is not None:
                return resolved
        raise FileNotFoundError(
            f"Neither standardized FASTA nor canonical raw path exists for {self.canonical_id}"
        )

    @property
    def derep_input_file_name(self) -> str:
        suffix = _fasta_suffix(self.derep_input_abs_path.name) or ".fna"
        return f"{self.canonical_id}{suffix}"


def load_genome_index(index_path: str | Path) -> list[GenomeRecord]:
    """Read the standardized genome index and keep only dereplication-ready rows."""
    path = Path(index_path)
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        records = [
            GenomeRecord(
                canonical_id=row["canonical_id"],
                winning_source=row["winning_source"],
                ncbi_accession=row["ncbi_accession"],
                fasta_path=row["fasta_path"],
                canonical_raw_path=row["canonical_raw_path"],
                md5_seq=row["md5_seq"],
                n_contigs=int(row["n_contigs"] or 0),
                total_bp=int(row["total_bp"] or 0),
                status=row["status"],
                is_gtdb_representative=row["is_gtdb_representative"],
                dedup_reason=row["dedup_reason"],
            )
            for row in reader
            if (row.get("status") or "").strip() == "ok" and (row.get("fasta_path") or "").strip()
        ]
    return records


def write_genome_list(records: list[GenomeRecord], genome_list_path: str | Path) -> Path:
    """Write one absolute FASTA path per line for skani/checkm2."""
    output_path = Path(genome_list_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with temp_path.open("w") as handle:
        for record in records:
            handle.write(f"{record.derep_input_abs_path}\n")
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(output_path)
    return output_path


def build_skani_commands(cfg: dict[str, Any], threads: int | None = None) -> list[list[str]]:
    """Return the sketch + sparse triangle commands configured for this repo."""
    derep_cfg = cfg["dereplication"]
    skani_cfg = derep_cfg["skani"]
    threads_value = int(threads or skani_cfg["threads"])
    genome_list_path = resolve_path(derep_cfg["genome_list"])
    sketch_dir = resolve_path(derep_cfg["sketch_dir"])
    edges_path = resolve_path(derep_cfg["edges_tsv"])
    screen = str(skani_cfg["screen"])
    min_af = str(skani_cfg["min_af"])

    return [
        [
            "skani",
            "sketch",
            "-l",
            str(genome_list_path),
            "-o",
            str(sketch_dir),
            "-t",
            str(threads_value),
        ],
        [
            "skani",
            "triangle",
            "-l",
            str(genome_list_path),
            "-o",
            str(edges_path),
            "-t",
            str(threads_value),
            "--sparse",
            "-s",
            screen,
            "--min-af",
            min_af,
        ],
    ]


def build_checkm2_command(cfg: dict[str, Any], threads: int | None = None) -> list[str]:
    """Return the documented manual CheckM2 command for representative selection."""
    derep_cfg = cfg["dereplication"]
    threads_value = int(threads or derep_cfg["skani"]["threads"])
    fasta_dir = resolve_path(derep_cfg["work_dir"]) / "checkm2_inputs"
    quality_dir = resolve_path(derep_cfg["checkm2_quality"]).parent
    return [
        "checkm2",
        "predict",
        "--threads",
        str(threads_value),
        "--input",
        str(fasta_dir),
        "--output-directory",
        str(quality_dir),
    ]


def materialize_checkm2_inputs(records: list[GenomeRecord], cfg: dict[str, Any]) -> Path:
    """Create a stable input directory for manual CheckM2 runs."""
    output_dir = resolve_path(cfg["dereplication"]["work_dir"]) / "checkm2_inputs"
    return _materialize_checkm2_inputs(records, output_dir)


def run_skani_pipeline(
    records: list[GenomeRecord],
    cfg: dict[str, Any],
    *,
    threads: int | None = None,
    dry_run: bool = False,
    logger=print,
) -> dict[str, Any]:
    """Write the genome list and execute skani sketch + sparse triangle."""
    derep_cfg = cfg["dereplication"]
    work_dir = resolve_path(derep_cfg["work_dir"])
    sketch_dir = resolve_path(derep_cfg["sketch_dir"])
    edges_path = resolve_path(derep_cfg["edges_tsv"])
    genome_list_path = resolve_path(derep_cfg["genome_list"])
    checkm2_input_dir = work_dir / "checkm2_inputs"
    work_dir.mkdir(parents=True, exist_ok=True)
    sketch_dir.parent.mkdir(parents=True, exist_ok=True)

    commands = build_skani_commands(cfg, threads=threads)
    checkm2_cmd = build_checkm2_command(cfg, threads=threads)

    if dry_run:
        return {
            "records": len(records),
            "genome_list": genome_list_path,
            "sketch_dir": sketch_dir,
            "edges_tsv": edges_path,
            "checkm2_input_dir": checkm2_input_dir,
            "commands": commands,
            "checkm2_command": checkm2_cmd,
        }

    _reset_output_path(sketch_dir)
    _reset_output_path(edges_path)
    write_genome_list(records, genome_list_path)
    for command in commands:
        logger(f"$ {' '.join(command)}")
        try:
            subprocess.run(command, check=True, cwd=project_root())
        except FileNotFoundError as exc:
            raise RuntimeError(
                "skani is not available on PATH in this shell. Activate the marine conda environment "
                "or otherwise expose `skani` before running the skani stage."
            ) from exc

    if not edges_path.exists():
        raise FileNotFoundError(f"Expected skani output was not created: {edges_path}")

    return {
        "records": len(records),
        "genome_list": genome_list_path,
        "sketch_dir": sketch_dir,
        "edges_tsv": edges_path,
        "checkm2_input_dir": checkm2_input_dir,
        "commands": commands,
        "checkm2_command": checkm2_cmd,
    }


def _materialize_checkm2_inputs(records: list[GenomeRecord], output_dir: Path) -> Path:
    _reset_output_path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for record in records:
        target = record.derep_input_abs_path.resolve(strict=True)
        link_path = output_dir / record.derep_input_file_name
        link_path.symlink_to(target)
    return output_dir


def _reset_output_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        last_error: OSError | None = None
        for _ in range(3):
            try:
                shutil.rmtree(path)
                return
            except OSError as exc:
                last_error = exc
                time.sleep(1)
        if last_error is not None:
            raise last_error


@lru_cache(maxsize=500000)
def _resolve_existing_fasta_path(path_value: str) -> Path | None:
    path = Path(path_value)
    try:
        return path.resolve(strict=True)
    except FileNotFoundError:
        pass

    parent = path.parent
    if not parent.exists():
        return None

    stem = _strip_fasta_suffix(path.name)
    for suffix in sorted(FASTA_SUFFIXES, key=len, reverse=True):
        candidate = parent / f"{stem}{suffix}"
        if candidate.exists():
            return candidate.resolve(strict=True)
    return None


def _strip_fasta_suffix(name: str) -> str:
    for suffix in sorted(FASTA_SUFFIXES, key=len, reverse=True):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _fasta_suffix(name: str) -> str:
    for suffix in sorted(FASTA_SUFFIXES, key=len, reverse=True):
        if name.endswith(suffix):
            return suffix
    return Path(name).suffix
