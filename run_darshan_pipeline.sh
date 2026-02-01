#!/usr/bin/env bash
set -e


ROOT_DIR="$(pwd)"
DARSHAN_TO_CSV="darshan_to_csv.py"
CSV_TO_AGG="darshan_csv_to_file_aggregate.py"


for dir in "$ROOT_DIR"/*/; do
    [ -d "$dir" ] || continue

    echo "===================================="
    echo "Processing folder: $dir"
    echo "===================================="

    cd "$dir"

    
    mkdir -p per_file_analysis

    # Extract
    for tarfile in *.tar.gz; do
        [ -f "$tarfile" ] || continue
        echo "Extracting $tarfile"
        tar -xzf "$tarfile"
    done

    
    find . -maxdepth 1 -name "*.darshan" | while read -r darshan_file; do
        base=$(basename "$darshan_file" .darshan)

        echo "  → Processing $base.darshan"

        # darshan to csv
        
        python3 "$ROOT_DIR/$DARSHAN_TO_CSV" "$darshan_file" || echo "⚠ Skipped $darshan_file due to error"


        if [[ ! -f "$base.csv" ]]; then
            echo "⚠ CSV not generated for $darshan_file, skipping aggregation"
            continue
        fi
        # csv to per-file aggregate
        python3 "$ROOT_DIR/$CSV_TO_AGG" "$base.csv"

        
        mv "${base}_per_file.csv" "per_file_analysis/${base}.csv"

        
        rm -f "$base.csv"
    done

    cd "$ROOT_DIR"
done

echo "===================================="
echo "DONE. Aggregated CSVs are inside each day folder."
echo "===================================="
