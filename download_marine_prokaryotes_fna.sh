#!/usr/bin/env bash
# Download prokaryote assemblies matching a query (default: marine) and extract *_genomic.fna files.
# Usage: ./download_marine_prokaryotes_fna.sh [query] [batch_size]
# Example: ./download_marine_prokaryotes_fna.sh "marine AND (txid2[Organism:exp] OR txid2157[Organism:exp])" 50
set -euo pipefail

# Default: marine + Bacteria OR Archaea (safe to pass as one argument with quotes)
QUERY="${1:-marine AND (txid2[Organism:exp] OR txid2157[Organism:exp])}"
BATCH_SIZE="${2:-50}"         # how many accessions per datasets download (start small: 10)
UID_CHUNK_SIZE=500            # UIDs per esummary call
TMPDIR="${TMPDIR:-./tmp_datasets}"
OUTDIR="${OUTDIR:-./marine_prokaryote_fna}"
KEEP_ZIP="${KEEP_ZIP:-false}" # set to true to keep .zip bundles
MAX_RETRIES="${MAX_RETRIES:-3}"
SLEEP_BETWEEN_DOWNLOADS="${SLEEP_BETWEEN_DOWNLOADS:-1}"

mkdir -p "$TMPDIR" "$OUTDIR"
MANIFEST="$TMPDIR/manifest.csv"
FAILED="$TMPDIR/failed_batches.txt"

# checks
for cmd in datasets curl jq unzip; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: required command '$cmd' not found."; exit 1; }
done

echo "Query: $QUERY"
echo "Output dir: $OUTDIR"
echo "Batch size: $BATCH_SIZE"
echo "TMP dir: $TMPDIR"

# 1) safer URL-encoded ESearch (handles parentheses/brackets)
ES_URL="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
echo "Fetching assembly UIDs..."
curl -s --get \
  --data-urlencode "term=${QUERY}" \
  --data "db=assembly&retmode=json&retmax=200000" \
  "$ES_URL" \
  | jq -r '.esearchresult.idlist[]' > "$TMPDIR/assembly_uids.txt"

if [ ! -s "$TMPDIR/assembly_uids.txt" ]; then
  echo "Error: no assembly UIDs returned for query: $QUERY"
  echo "Inspect connectivity or the query. Try running:"
  echo "curl -s --get --data-urlencode \"term=${QUERY}\" --data \"db=assembly&retmode=json&retmax=200000\" \"$ES_URL\" | jq ."
  exit 1
fi
UID_TOTAL=$(wc -l < "$TMPDIR/assembly_uids.txt" | tr -d ' ')
echo "Found $UID_TOTAL assembly UIDs."

# 2) convert UIDs -> Assembly accessions using esummary in chunks
echo "Converting UIDs -> Assembly accessions..."
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

if [ "$TOTAL_ACC" -eq 0 ]; then
  echo "No assembly accessions retrieved; aborting."
  exit 1
fi

# 3) split into batches and download
split -l "$BATCH_SIZE" -d -a 5 "$TMPDIR/accessions.txt" "$TMPDIR/chunk_"
BATCH_FILES=( "$TMPDIR"/chunk_* )
echo "Number of dataset batches: ${#BATCH_FILES[@]}"

# manifest header
if [ ! -f "$MANIFEST" ]; then
  echo "accession,zipfile,extracted_filename,size_bytes" > "$MANIFEST"
fi
> "$FAILED"

i=0
for chunk in "${BATCH_FILES[@]}"; do
  i=$((i+1))
  batch_label=$(printf "%04d" "$i")
  zipfile="$TMPDIR/marine_prok_batch_${batch_label}.zip"

  # quick skip check: if all accessions already extracted, skip
  SKIP_BATCH=true
  while read -r acc; do
    if ! ls "$OUTDIR"/${acc}_*genomic.fna* >/dev/null 2>&1; then
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

  echo "Batch $batch_label: extracting genomic FASTA files..."
  # try gz first, then plain
  unzip -j -o "$zipfile" "ncbi_dataset/data/*/*_genomic.fna.gz" -d "$OUTDIR" >/dev/null 2>&1 || true
  unzip -j -o "$zipfile" "ncbi_dataset/data/*/*_genomic.fna" -d "$OUTDIR" >/dev/null 2>&1 || true

  # record manifest entries
  while read -r acc; do
    for f in "$OUTDIR"/${acc}_*genomic.fna*; do
      [ -f "$f" ] || continue
      fname=$(basename "$f")
      fsize=$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f" 2>/dev/null || echo 0)
      if ! grep -Fq "${acc},${zipfile},${fname}," "$MANIFEST"; then
        echo "${acc},${zipfile},${fname},${fsize}" >> "$MANIFEST"
      fi
    done
  done < "$chunk"

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

# Optional: uncompress gz files (uncomment if you want uncompressed .fna)
# gunzip -f "$OUTDIR"/*.fna.gz 2>/dev/null || true

echo "Done."
