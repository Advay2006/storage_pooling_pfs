import darshan
import pandas as pd
from pathlib import Path

def safe_to_df(rec):
    try:
        return rec.to_df()
    except Exception as e:
        print(f"[SKIPPED] cannot convert to df: {e}")
        return None

def main():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python3 parser.py <file.darshan>")
        return

    darshan_file = sys.argv[1]

    print(f"Parsing Darshan log: {darshan_file}")
    print(f"Darshan version: {darshan.__version__}")

    # create report without eager loading
    report = darshan.DarshanReport(darshan_file, read_all=False)

    # load all generic records for all modules
    report.read_all_generic_records()

    # inspect modules available
    print("\nModules present in log:")
    print(list(report.modules.keys()))

    # try to convert POSIX if present
    if "POSIX" in report.modules:
        # read generic records for POSIX if not already populated
        data = report.records.get("POSIX")
        if data is None:
            report.read_generic_records("POSIX", dtype="counters")
            data = report.records.get("POSIX")

        df_dict = safe_to_df(data)
        if df_dict:
            # print counters table if available
            if isinstance(df_dict, dict) and "counters" in df_dict:
                print("\nPOSIX counters:")
                print(df_dict["counters"].head())
            else:
                print("\nPOSIX records df columns:")
                print(df_dict.columns.tolist())
    else:
        print("No POSIX module in this log.")

if __name__ == "__main__":
    main()
