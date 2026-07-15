# marine — novel peptides from marine prokaryotes

Exploratory bioinformatics project to download genomes of marine prokaryotes (including
MAGs and SAGs), predict ORFs and peptides, engineer features, and apply ML models to
discover novel peptides.

> This README is the STABLE overview. Day-to-day progress goes in
> [`docs/lab-notebook/`](docs/lab-notebook/), not here. Project conventions live in
> [`AGENTS.md`](AGENTS.md) and apply to humans and AI agents alike.

## Pipeline overview

```
download genomes/MAGs -> QC (CheckM2) -> ORF/peptide prediction -> features -> ML -> novel peptides
```

## Repository layout

| Path | Purpose |
|------|---------|
| `src/marine_peptides/` | Installable Python package: reusable, importable logic |
| `scripts/` | Thin, ordered entrypoint wrappers (`00_`, `01_`, ...) |
| `modules/` | Nextflow process modules |
| `main.nf`, `nextflow.config` | Nextflow pipeline entrypoint and config |
| `notebooks/` | Exploratory notebooks (`NN_topic_YYYYMMDD.ipynb`) |
| `config/config.yaml` | Paths, thresholds, and parameters (no hardcoding) |
| `env/environment.yml` | Conda env for bioinfo CLI tools |
| `env/smorfinder.yml` | Separate conda env for SmORFinder (Python 3.8) |
| `env/nextflow-utils.yml` | Minimal conda env for Nextflow Python helper steps |
| `pyproject.toml` | Python/ML package + dependencies (managed with `uv`) |
| `data/raw/` | Downloaded genomes/MAGs — READ-ONLY |
| `data/external/` | Reference DBs (GTDB, Pfam, AMP databases) |
| `data/interim/` | Intermediate outputs (ORFs, predicted peptides) |
| `data/processed/` | Analysis-ready, dereplicated sets |
| `results/{figures,tables}/` | Analysis outputs |
| `models/` | Trained ML artifacts/checkpoints |
| `logs/` | Run logs |
| `docs/lab-notebook/` | Dated daily entries (`YYYY-MM-DD.md`) |

`data/`, `results/`, `models/`, and `logs/` are gitignored. Reproducibility comes from
code + configs + a committed genome manifest TSV, not from committing the data itself.

## Setup

Multi-track environment (see [`AGENTS.md`](AGENTS.md) for the rationale):

### 1. Bioinfo CLI tools (conda/mamba)

```bash
mamba env create -f env/environment.yml
mamba activate marine
```

### 2. Python/ML package (uv)

```bash
uv venv --python 3.11
uv pip install -e ".[ml,dev]"
```

### 3. SmORFinder (separate conda env)

[SmORFinder](https://github.com/bhattlab/SmORFinder) pins `tensorflow==2.3.1` and
requires Python 3.8, so it cannot share the `marine` or uv environments.

```bash
conda env create -f env/smorfinder.yml
conda activate smorfinder
smorf   # one-time download of model/data files
```

Run on a genome: `smorf single myGenome.fna`. Output paths are configured in
`config/config.yaml` under `smorfinder`.

For the dedicated manifest-driven workflow, see
[`docs/smorfinder-pipeline.md`](docs/smorfinder-pipeline.md).

## Running the pipeline

```bash
nextflow run main.nf -profile conda
```

The pipeline is currently a scaffold; stages are stubbed with TODO markers and read their
parameters from `config/config.yaml`.

SmORFinder runs through its own dedicated workflow:

```bash
nextflow run smorfinder.nf -profile standard
nextflow run smorfinder.nf -profile slurm -resume
```

**Status (2026-07-14): full hybrid run finished** — 112,097 / 112,414 genomes
(99.72%) have valid outputs; 317 hard failures remain (mostly GORG SAGs). Artifacts:
`data/processed/smorfinder_run_manifest.tsv`,
`data/interim/smorfinder/{single,meta}/`. Details and how to regenerate the hard-failure
list: [`docs/smorfinder-pipeline.md`](docs/smorfinder-pipeline.md).

To rebuild the input list and re-run the hybrid strategy (only needed if the genome index or
SmORFinder cutoffs/assets change):

```bash
PYTHONPATH=src python3 scripts/12_build_smorfinder_inputs_from_index.py
nextflow run smorfinder.nf -profile slurm -resume \
  && PYTHONPATH=src python3 scripts/13_promote_smorfinder_failures.py \
  && nextflow run smorfinder.nf -profile slurm -resume
```

## Conventions

See [`AGENTS.md`](AGENTS.md). In short: reusable logic in `src/`, thin wrappers in
`scripts/`/`modules/`, `data/raw/` is read-only, no hardcoded paths, one lab-notebook entry
per working day.
