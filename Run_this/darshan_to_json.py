import darshan
import pandas as pd
from darshan.cli import summary as cli_summary

import json
import sys
from pathlib import Path
import fnmatch


def safe_sum(df, col):
    """Return sum of column if it exists, else 0"""
    if col in df.columns:
        return int(df[col].sum())
    return 0


def extract_counters_with_wildcards(df, counters):
    """
    Extract counters from df.
    Supports wildcards like MPIIO_SIZE_READ_AGG_*
    """
    data = {}

    for c in counters:
        if "*" in c:
            matched = [col for col in df.columns if fnmatch.fnmatch(col, c)]
            data[c] = int(df[matched].sum().sum()) if matched else 0
        else:
            data[c] = safe_sum(df, c)

    return data


def extract_module_counters(report, module, counters):
    """Extract and sum selected counters from a module"""
    if module not in report.records:
        return {}

    dfs = report.records[module].to_df()

    # PyDarshan returns dict of dataframes
    if isinstance(dfs, dict):
        df = dfs.get("counters")
        if df is None:
            return {}
    else:
        df = dfs

    return extract_counters_with_wildcards(df, counters)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 darshan_to_json.py <file.darshan>")
        sys.exit(1)

    darshan_file = sys.argv[1]
    report = darshan.DarshanReport(darshan_file, read_all=True)

    # ============================
    # Counters to extract
    # ============================

    target_counter_posix = 'POSIX_OPENS'
    target_counter_mpiio = 'MPIIO_INDEP_OPENS'
    target_counter_stdio = 'STDIO_OPENS'

    darshan_counters_posix = [
        'POSIX_OPENS', 'POSIX_READS', 'POSIX_WRITES',
        'POSIX_BYTES_READ', 'POSIX_BYTES_WRITTEN',
        'POSIX_CONSEC_READS', 'POSIX_CONSEC_WRITES',
        'POSIX_SEQ_READS', 'POSIX_SEQ_WRITES',
        'POSIX_MEM_NOT_ALIGNED', 'POSIX_MEM_ALIGNMENT',
        'POSIX_FILE_NOT_ALIGNED', 'POSIX_FILE_ALIGNMENT',
        'POSIX_SIZE_READ_0_100', 'POSIX_SIZE_READ_100_1K',
        'POSIX_SIZE_READ_1K_10K', 'POSIX_SIZE_READ_10K_100K',
        'POSIX_SIZE_READ_100K_1M', 'POSIX_SIZE_READ_1M_4M',
        'POSIX_SIZE_READ_4M_10M', 'POSIX_SIZE_READ_10M_100M',
        'POSIX_SIZE_READ_100M_1G', 'POSIX_SIZE_READ_1G_PLUS',
        'POSIX_SIZE_WRITE_0_100', 'POSIX_SIZE_WRITE_100_1K',
        'POSIX_SIZE_WRITE_1K_10K', 'POSIX_SIZE_WRITE_10K_100K',
        'POSIX_SIZE_WRITE_100K_1M', 'POSIX_SIZE_WRITE_1M_4M',
        'POSIX_SIZE_WRITE_4M_10M', 'POSIX_SIZE_WRITE_10M_100M',
        'POSIX_SIZE_WRITE_100M_1G', 'POSIX_SIZE_WRITE_1G_PLUS',
        'POSIX_F_OPEN_START_TIMESTAMP', 'POSIX_F_READ_START_TIMESTAMP',
        'POSIX_F_WRITE_START_TIMESTAMP', 'POSIX_F_CLOSE_START_TIMESTAMP',
        'POSIX_F_OPEN_END_TIMESTAMP', 'POSIX_F_READ_END_TIMESTAMP',
        'POSIX_F_WRITE_END_TIMESTAMP', 'POSIX_F_CLOSE_END_TIMESTAMP'
    ]

    darshan_counters_mpiio = [
        'MPIIO_INDEP_OPENS',
        'MPIIO_COLL_OPENS',
        'MPIIO_INDEP_READS',
        'MPIIO_INDEP_WRITES',
        'MPIIO_COLL_READS',
        'MPIIO_COLL_WRITES',
        'MPIIO_SPLIT_READS',
        'MPIIO_SPLIT_WRITES',
        'MPIIO_NB_READS',
        'MPIIO_NB_WRITES',
        'MPIIO_BYTES_READ',
        'MPIIO_BYTES_WRITTEN',
        'MPIIO_RW_SWITCHES',
        'MPIIO_SIZE_READ_AGG_*',
        'MPIIO_SIZE_WRITE_AGG_*',
        'MPIIO_F_READ_TIME',
        'MPIIO_F_WRITE_TIME',
        'MPIIO_F_META_TIME'
    ]

    darshan_counters_stdio = [
        'STDIO_OPENS',
        'STDIO_READS',
        'STDIO_WRITES',
        'STDIO_BYTES_WRITTEN',
        'STDIO_BYTES_READ',
        'STDIO_F_WRITE_TIME',
        'STDIO_F_READ_TIME',
        'STDIO_F_*_START_TIMESTAMP',
        'STDIO_F_*_END_TIMESTAMP'
    ]

    # ============================
    # Extract data
    # ============================

    job_info = {
        "jobid": report.metadata.get("jobid"),
        "uid": report.metadata.get("uid"),
        "nprocs": report.metadata.get("nprocs"),
        "exe": report.metadata.get("exe"),
        "start_time": report.metadata.get("start_time"),
        "end_time": report.metadata.get("end_time"),
        "run_time": report.metadata.get("run_time"),
    }

    # from darshan job_stats CSV
    job = pd.read_csv("job.csv").iloc[0].to_dict()

    output = {
        "file": Path(darshan_file).name,
        "job": job,
        "modules_present": list(report.records.keys()),
        "posix": extract_module_counters(report, "POSIX", darshan_counters_posix),
        "mpiio": extract_module_counters(report, "MPI-IO", darshan_counters_mpiio),
        "stdio": extract_module_counters(report, "STDIO", darshan_counters_stdio),
    }

    # ============================
    # Write JSON
    # ============================

    out_file = Path(darshan_file).with_suffix(".json")
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[OK] Written: {out_file}")


if __name__ == "__main__":
    main()
