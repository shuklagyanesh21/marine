#!/usr/bin/env bash
# Run smorf single on every genome FASTA in the target directory.
# Usage: ./run_smorf_all.sh [input_dir] [output_root]
# Defaults assume data in ./marine_prokaryote_fna and outputs in ./smorf_output.

set -euo pipefail

INPUT_DIR="${1:-./marine_prokaryote_fna}"
OUTPUT_ROOT="${2:-./smorf_output}"
SKIP_EXISTING="${SKIP_EXISTING:-true}" # set to false to re-run and overwrite outputs

if ! command -v smorf >/dev/null 2>&1; then
  echo "Error: 'smorf' CLI not found. Activate env_marine before running this script." >&2
  exit 1
fi

if [ ! -d "$INPUT_DIR" ]; then
  echo "Error: input directory '$INPUT_DIR' does not exist." >&2
  exit 1
fi

mkdir -p "$OUTPUT_ROOT"

mapfile -t GENOMES < <(find "$INPUT_DIR" -maxdepth 1 -type f \( -name "*.fna" -o -name "*.fna.gz" -o -name "*.fa" -o -name "*.fa.gz" \) | sort)

if [ "${#GENOMES[@]}" -eq 0 ]; then
  echo "No FASTA files (*.fna/.fna.gz/.fa/.fa.gz) found in '$INPUT_DIR'." >&2
  exit 1
fi

TOTAL=${#GENOMES[@]}
COUNT=0
declare -a FAILED=()
for GENOME in "${GENOMES[@]}"; do
  COUNT=$((COUNT + 1))
  BASENAME=$(basename "$GENOME")
  OUTDIR="$OUTPUT_ROOT/$BASENAME"

  if [ -d "$OUTDIR" ] && [ "$SKIP_EXISTING" = "true" ]; then
    echo "[$COUNT/$TOTAL] Skipping $BASENAME (output exists: $OUTDIR)"
    continue
  fi

  if [ -d "$OUTDIR" ]; then
    echo "[$COUNT/$TOTAL] Removing existing output directory: $OUTDIR"
    rm -rf "$OUTDIR"
  fi

  echo "[$COUNT/$TOTAL] Running smorf on $BASENAME -> $OUTDIR"
  if smorf single "$GENOME" -o "$OUTDIR"; then
    continue
  fi

  EXIT_CODE=$?
  echo "[$COUNT/$TOTAL] ERROR: smorf failed for $BASENAME (exit code $EXIT_CODE)." >&2
  if [ -d "$OUTDIR" ]; then
    echo "[$COUNT/$TOTAL] Removing incomplete output directory: $OUTDIR" >&2
    rm -rf "$OUTDIR"
  fi
  FAILED+=("$BASENAME")
done

if [ "${#FAILED[@]}" -eq 0 ]; then
  echo "Completed smorf runs for $TOTAL genome(s). Outputs stored under '$OUTPUT_ROOT'."
else
  echo "Completed smorf runs for $TOTAL genome(s) with ${#FAILED[@]} failure(s). See list below:" >&2
  printf '  - %s\n' "${FAILED[@]}" >&2
  echo "Outputs stored under '$OUTPUT_ROOT'. Rerun the script after resolving the errors to resume." >&2
fi

