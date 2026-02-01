import darshan
import darshan.backend.full_backend as backend

filename = "eggzample.darshan"

# Open the log manually to avoid the high-level constructor crash
log_handle = backend.darshan_log_open(filename)

if log_handle:
    print("Successfully opened log handle.")
    try:
        # Instead of the Report object, try to fetch the job info directly
        job_info = backend.darshan_log_get_job(log_handle)
        print(f"Job ID: {job_info.get('jobid')}")
        
        # Now try to wrap it in a report WITHOUT reading all
        report = darshan.DarshanReport(filename, read_all=False)
        
        # Manually load ONLY the POSIX module (most common)
        # Avoid loading 'exe' or 'mounts' which are likely where the hashes are breaking things
        if 'POSIX' in report.modules:
            report.mod_read_all_records('POSIX')
            df = report.records['POSIX'].to_df()
            print("Successfully extracted POSIX data to DataFrame!")
            print(df.head())
            
    finally:
        backend.darshan_log_close(log_handle)