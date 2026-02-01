import sys
import pandas as pd
from pathlib import Path
import fnmatch



target_counter_posix = "POSIX_OPENS"
target_counter_mpiio = "MPIIO_INDEP_OPENS"
target_counter_stdio = "STDIO_OPENS"

darshan_counters_posix = [
    'POSIX_OPENS',
    'POSIX_READS',
    'POSIX_WRITES',
    'POSIX_BYTES_READ',
    'POSIX_BYTES_WRITTEN',
    'POSIX_CONSEC_READS',
    'POSIX_CONSEC_WRITES',
    'POSIX_SEQ_READS',
    'POSIX_SEQ_WRITES',
    'POSIX_MEM_NOT_ALIGNED',
    'POSIX_MEM_ALIGNMENT',
    'POSIX_FILE_NOT_ALIGNED',
    'POSIX_FILE_ALIGNMENT',
    'POSIX_SIZE_READ_0_100',
    'POSIX_SIZE_READ_100_1K',
    'POSIX_SIZE_READ_1K_10K',
    'POSIX_SIZE_READ_10K_100K',
    'POSIX_SIZE_READ_100K_1M',
    'POSIX_SIZE_READ_1M_4M',
    'POSIX_SIZE_READ_4M_10M',
    'POSIX_SIZE_READ_10M_100M',
    'POSIX_SIZE_READ_100M_1G',
    'POSIX_SIZE_READ_1G_PLUS',
    'POSIX_SIZE_WRITE_0_100',
    'POSIX_SIZE_WRITE_100_1K',
    'POSIX_SIZE_WRITE_1K_10K',
    'POSIX_SIZE_WRITE_10K_100K',
    'POSIX_SIZE_WRITE_100K_1M',
    'POSIX_SIZE_WRITE_1M_4M',
    'POSIX_SIZE_WRITE_4M_10M',
    'POSIX_SIZE_WRITE_10M_100M',
    'POSIX_SIZE_WRITE_100M_1G',
    'POSIX_SIZE_WRITE_1G_PLUS',
    'POSIX_F_OPEN_START_TIMESTAMP',
    'POSIX_F_READ_START_TIMESTAMP',
    'POSIX_F_WRITE_START_TIMESTAMP',
    'POSIX_F_CLOSE_START_TIMESTAMP',
    'POSIX_F_OPEN_END_TIMESTAMP',
    'POSIX_F_READ_END_TIMESTAMP',
    'POSIX_F_WRITE_END_TIMESTAMP',
    'POSIX_F_CLOSE_END_TIMESTAMP',
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
    'MPIIO_F_META_TIME',
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
    'STDIO_F_*_END_TIMESTAMP',
]

ALL_COUNTERS = (
    darshan_counters_posix +
    darshan_counters_mpiio +
    darshan_counters_stdio
)


def expand_counters(patterns, columns):
    expanded = set()
    for p in patterns:
        if "*" in p:
            expanded |= {c for c in columns if fnmatch.fnmatch(c, p)}
        else:
            expanded.add(p)
    return sorted(expanded)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 darshan_csv_to_file_aggregate.py input.csv")
        return

    inp = Path(sys.argv[1])
    out = inp.with_name(inp.stem + "_per_file.csv")

    df = pd.read_csv(inp)

    
    group_cols = [c for c in df.columns if c in (
        "file_id", "file_name", "module"
    )]

    if not group_cols:
        raise RuntimeError("CSV does not contain file identifiers")

    # expand wildcards
    wanted_counters = expand_counters(ALL_COUNTERS, df.columns)

    # needed counters
    for c in wanted_counters:
        if c not in df.columns:
            df[c] = 0

    # Aggregate per file
    agg = (
        df.groupby(group_cols)[wanted_counters]
          .sum()
          .reset_index()
    )

    agg.to_csv(out, index=False)

    print(f" Written: {out}")
    print(f"Rows: {len(agg)} | Columns: {len(agg.columns)}")

if __name__ == "__main__":
    main()
