import argparse, logging
import questdb_client
from log_scraper import LogScraper

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--database', required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--dry-run", action='store_true', default=False)

    args = parser.parse_args()
    qdb_client = questdb_client.from_cmd(args)
    suspended_tables = qdb_client.search_suspended()
    print("\nFound suspended tables:")
    for table in suspended_tables:
        print(f"- {table}")

    log_scraper = LogScraper()
    with open(args.log_file, 'r') as f:
        lines = f.readlines()
    corrupted_wals = log_scraper.find_corrupted_wals(lines)
    print("\nFound corrupted WALs:")
    for cw in corrupted_wals:
        print(f"- {cw}")

    filtered = list(filter(lambda cw: cw.table_name in suspended_tables, corrupted_wals))
    print("\n\nProcessing corrupted WALs:")
    for corrupted_wal in filtered:
        print(f"- processing {corrupted_wal.table_name}")
        writer_txn = qdb_client.find_writer_txn(corrupted_wal.table_name)
        print(f"  - found writer txn = {writer_txn}")
        corrupted_txn = qdb_client.find_corrupted_txn(
            corrupted_wal.table_name,
            writer_txn,
            corrupted_wal.wal_id,
            corrupted_wal.segment_id
        )
        print(f"  - found corrupted txn = {corrupted_txn}")
        if not args.dry_run:
            print(f"  - resuming WAL in {corrupted_wal.table_name}")
            qdb_client.resume_wal(corrupted_wal.table_name, corrupted_txn)
