import darshan
import pandas as pd
from pathlib import Path

# =============================
# Helper to safely convert module to dataframe
# =============================
def safe_to_df(rec):
    """
    Safely converts a Darshan record to a dataframe.
    Returns None if the module cannot be converted.
    """
    try:
        return rec.to_df()
    except Exception as e:
        print(f"  [SKIPPED] cannot convert module to df: {e}")
        return None

# =============================
# Main parser
# =============================
def main():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python3 parser.py <file.darshan>")
        return

    darshan_file = sys.argv[1]
    report = darshan.DarshanReport(darshan_file, read_all=True)

    print(f"Parsing Darshan log: {darshan_file}")
    print(f"Darshan version: {darshan.__version__}")

    # Iterate over all modules
    for module, rec in report.records.items():
        print(f"\n=== Module: {module} ===")

        dfs = safe_to_df(rec)
        if dfs is None:
            continue

        # Check if module returned multiple dfs (counters, fcounters, etc.)
        if isinstance(dfs, dict):
            for k, df in dfs.items():
                print(f"\n  [{k}] columns:")
                print(list(df.columns))
                
        else:
            print("  columns:")
            print(list(dfs.columns))

    # Optional: output summary of modules present
    print("\nModules present in log:")
    print(list(report.records.keys()))

if __name__ == "__main__":
    main()
