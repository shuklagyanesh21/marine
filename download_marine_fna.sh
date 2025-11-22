#!/usr/bin/env bash
# Download all assemblies matching a query and extract *_genomic.fna files.
# Usage: ./download_marine_all.sh [query] [batch_size]
# Example: ./download_marine_all.sh "marine" 50
set -euo pipefail

QUERY="${1:-marine}"
BATCH_SIZE="${2:-50}"         # how many accessions per datasets download
UID_CHUNK_SIZE=500            # UIDs per esummary call (keep reasonable)
TMPDIR="${TMPDIR:-./tmp_datasets}"
OUTDIR="${OUTDIR:-./marine_fna}"
KEEP_ZIP="${KEEP_ZIP:-false}" # set to true to keep .zip bundles
MAX_RETRIES="${MAX_RETRIES:-3}"
SLEEP_BETWEEN_DOWNLOADS="${SLEEP_BETWEEN_DOWNLOADS:-1}"  # seconds

mkdir -p "$TMPDIR" "$OUTDIR"
MANIFEST="$TMPDIR/manifest.csv"
FAILED="$TMPDIR/failed_batches.txt"

# checks
command -v datasets >/dev/null 2>&1 || { echo "ERROR: 'datasets' CLI not found in PATH."; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl not found."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq not found."; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "ERROR: unzip not found."; exit 1; }

echo "Query: $QUERY"
echo "Output dir: $OUTDIR"
echo "Batch size: $BATCH_SIZE"
echo "TMP dir: $TMPDIR"

# quick disk check
echo "Disk free at target:"
df -h . | sed -n '2p' || true

# 1) get all assembly UIDs
QENC="${QUERY// /+}"
ESearchURL="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=assembly&term=${QENC}&retmax=200000&retmode=json"
echo "Fetching assembly UIDs..."
curl -s "$ESearchURL" | jq -r '.esearchresult.idlist[]' > "$TMPDIR/assembly_uids.txt"
UID_TOTAL=$(wc -l < "$TMPDIR/assembly_uids.txt" | tr -d ' ')
echo "Found $UID_TOTAL assembly UIDs."

if [ "$UID_TOTAL" -eq 0 ]; then
  echo "No results for query: $QUERY"; exit 0
fi

# 2) convert UIDs -> Assembly accessions using esummary in chunks
echo "Converting UIDs -> Assembly accessions (esummary)..."
split -l "$UID_CHUNK_SIZE" -d -a 5 "$TMPDIR/assembly_uids.txt" "$TMPDIR/uid_chunk_"
> "$TMPDIR/accessions.raw"
for uc in "$TMPDIR"/uid_chunk_*; do
  IDLIST=$(paste -s -d, "$uc")
  ESummaryURL="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=assembly&id=${IDLIST}&retmode=json"
  curl -s "$ESummaryURL" \
    | jq -r '.result | to_entries[] | select(.key!="uids") | (.value.AssemblyAccession // .value.assemblyaccession) // empty' \
    >> "$TMPDIR/accessions.raw"
  sleep 0.34
done
sort -u "$TMPDIR/accessions.raw" > "$TMPDIR/accessions.txt"
TOTAL_ACC=$(wc -l < "$TMPDIR/accessions.txt" | tr -d ' ')
echo "Found $TOTAL_ACC unique Assembly accessions."

# 3) produce batches for datasets download
split -l "$BATCH_SIZE" -d -a 5 "$TMPDIR/accessions.txt" "$TMPDIR/chunk_"
BATCH_FILES=( "$TMPDIR"/chunk_* )
echo "Number of dataset batches: ${#BATCH_FILES[@]}"

# manifest header (append if exists)
if [ ! -f "$MANIFEST" ]; then
  echo "accession,zipfile,extracted_filename,size_bytes" > "$MANIFEST"
fi
> "$FAILED"

i=0
for chunk in "${BATCH_FILES[@]}"; do
  i=$((i+1))
  batch_label=$(printf "%04d" "$i")
  zipfile="$TMPDIR/marine_batch_${batch_label}.zip"

  # Skip batch if nothing to do (all accessions already have extracted files)
  SKIP_BATCH=true
  while read -r acc; do
    # if any accession not present in OUTDIR, we should process the batch
    pattern="$OUTDIR/${acc}_*genomic.fna"
    if ! ls $pattern >/dev/null 2>&1; then
      SKIP_BATCH=false
      break
    fi
  done < "$chunk"

  if [ "$SKIP_BATCH" = "true" ]; then
    echo "Batch $batch_label: all accessions already extracted, skipping."
    continue
  fi

  ACCESSIONS=$(paste -s -d ' ' "$chunk")
  attempt=0
  success=false
  while [ "$attempt" -lt "$MAX_RETRIES" ]; do
    attempt=$((attempt+1))
    echo "Batch $batch_label (attempt $attempt): downloading $(wc -l < "$chunk" | tr -d ' ') accessions -> $zipfile"
    if datasets download genome accession $ACCESSIONS --filename "$zipfile"; then
      success=true
      break
    else
      echo "datasets download failed for batch $batch_label (attempt $attempt). Retrying after sleep..."
      sleep 5
    fi
  done

  if [ "$success" != "true" ]; then
    echo "Batch $batch_label failed after $MAX_RETRIES attempts. Recording for retry."
    echo "$chunk" >> "$FAILED"
    continue
  fi

  # extract genomic FASTA files
  echo "Batch $batch_label: extracting genomic FASTA files..."
  # try gz first, then plain
  if unzip -j -o "$zipfile" "ncbi_dataset/data/*/*_genomic.fna.gz" -d "$OUTDIR" >/dev/null 2>&1; then
    echo "Extracted gz FASTA files to $OUTDIR"
  fi
  if unzip -j -o "$zipfile" "ncbi_dataset/data/*/*_genomic.fna" -d "$OUTDIR" >/dev/null 2>&1; then
    echo "Extracted FASTA files to $OUTDIR"
  fi

  # record manifest entries for any newly added files
  # For each file in OUTDIR that matches accession prefixes in this chunk, append to manifest
  while read -r acc; do
    for f in "$OUTDIR"/${acc}_*genomic.fna*; do
      [ -f "$f" ] || continue
      fname=$(basename "$f")
      fsize=$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f" 2>/dev/null || echo 0)
      # do not duplicate manifest rows: check if accession,filename already present
      if ! grep -Fq "${acc},${zipfile},${fname}," "$MANIFEST"; then
        echo "${acc},${zipfile},${fname},${fsize}" >> "$MANIFEST"
      fi
    done
  done < "$chunk"

  # optionally remove zip to save space
  if [ "$KEEP_ZIP" = "false" ]; then
    rm -f "$zipfile"
  fi

  sleep "$SLEEP_BETWEEN_DOWNLOADS"
done

echo "All batches processed."
if [ -s "$FAILED" ]; then
  echo "Some batches failed. See: $FAILED"
else
  echo "No failed batches."
fi

echo "Manifest: $MANIFEST"
echo "FASTAs (in $OUTDIR):"
ls -lh "$OUTDIR" | sed -n '1,200p' || true

# optionally gunzip remaining gz files (uncomment if you want uncompressed .fna)
# gunzip -f "$OUTDIR"/*.fna.gz 2>/dev/null || true

echo "Done."
