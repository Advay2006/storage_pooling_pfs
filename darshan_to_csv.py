import darshan
import pandas as pd
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 darshan_to_csv.py <file.darshan>")
        sys.exit(1)

    darshan_file = sys.argv[1]
    out_csv = Path(darshan_file).with_suffix(".csv")

    print(f"Parsing: {darshan_file}")
    print(f"PyDarshan version: {darshan.__version__}")

    
    report = darshan.DarshanReport(darshan_file, read_all=False)

    # load
    report.read_all_generic_records()

    all_frames = []

    for module, rec in report.records.items():
        print(f"Processing module: {module}")

        try:
            dfs = rec.to_df()
        except Exception as e:
            print(f"  [SKIPPED] {module}: {e}")
            continue

        
        if isinstance(dfs, dict):
            for dtype, df in dfs.items():
                df = df.copy()
                df["module"] = module
                df["record_type"] = dtype
                all_frames.append(df)
        else:
            df = dfs.copy()
            df["module"] = module
            df["record_type"] = "records"
            all_frames.append(df)

    if not all_frames:
        print("ERROR: no data extracted")
        sys.exit(2)

    final_df = pd.concat(all_frames, ignore_index=True)
    final_df.to_csv(out_csv, index=False)

    print(f"\n Written CSV: {out_csv}")
    print(f"Rows: {len(final_df)} | Columns: {len(final_df.columns)}")

if __name__ == "__main__":
    main()