# AGENTS.md - Project conventions

Canonical rules for this project. Humans and AI agents must follow these. They exist to
keep an exploratory genomics project (thousands of FASTA files, ORF predictions, model
checkpoints) decluttered and reproducible. Read this before creating or moving files.

## Project goal

Download genomes of marine prokaryotes (including MAGs/SAGs), predict ORFs and peptides with
CLI bioinfo tools, engineer features, and apply ML models to discover novel peptides.

## Code organization (most important rule)

- Reusable, importable, testable logic lives in `src/marine_peptides/` (an installable package).
- `scripts/` and Nextflow `modules/` are THIN wrappers: they parse args/IO and call into
  `src/marine_peptides/`. They must not contain substantial logic.
- Do NOT pile everything into one giant script. If a script grows real logic, extract it into
  a function/module under `src/marine_peptides/` and call it.
- Notebooks (`notebooks/`) are for exploration only; promote anything reusable into `src/`.

## Data discipline

- `data/raw/` is READ-ONLY and immutable. Never edit or overwrite files in it.
- All transformations write to `data/interim/` (intermediate) or `data/processed/`
  (analysis-ready). Everything except `data/raw/` must be regenerable from raw + code.
- `data/external/` holds reference DBs (GTDB, Pfam, AMP databases).
- Record genome provenance in a committed manifest TSV (accession, source DB, BioSample,
  lat/lon, depth, completeness, contamination). Commit the manifest, not the genomes.

## What goes in git

- IN: code, configs, env specs, the manifest TSV, docs/notebook.
- OUT (gitignored): `data/`, `results/`, `models/`, `logs/`, `.venv/`, Nextflow `work/` and
  `.nextflow*`. Track how to regenerate data, not the data itself.

## Configuration

- No hardcoded paths or parameters. Read them from `config/config.yaml`.
- Use relative paths everywhere.

## Environments (two-track)

- Bioinfo CLIs (Prodigal/Pyrodigal, CheckM2, GTDB-Tk, MMseqs2, ncbi-datasets-cli, dRep/skani):
  conda/mamba via `env/environment.yml`.
- Python/ML package: `uv` + `pyproject.toml` (package `marine_peptides`, Python 3.11).
- Pin versions. Record any new tool in the appropriate spec.

## Naming

- Dated names use `YYYY-MM-DD` (docs) or `YYYYMMDD` (outputs); no spaces.
- Ordered scripts are numbered: `00_`, `01_`, `02_` to show pipeline order.
- Notebooks: `NN_topic_YYYYMMDD.ipynb`.

## Lab notebook

- One dated entry per working day in `docs/lab-notebook/` (e.g. `2026-06-16.md`).
- README.md is the STABLE overview; it is NOT a daily log.

## Folder map

| Path | Purpose |
|------|---------|
| `src/marine_peptides/` | reusable importable package |
| `scripts/` | thin ordered entrypoint wrappers |
| `modules/` | Nextflow process modules |
| `notebooks/` | exploratory notebooks |
| `config/` | config.yaml (paths, thresholds, params) |
| `env/` | conda environment spec |
| `data/{raw,external,interim,processed}/` | data stages (raw is read-only) |
| `results/{figures,tables}/` | analysis outputs |
| `models/` | trained ML artifacts |
| `logs/` | run logs |
| `docs/lab-notebook/` | dated daily entries |
