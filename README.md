# QuestDB (WAL) Table Resumer

## Overview

Command line utility written in python that is meant to:
1. Connect to `questdb` instance
2. Search for suspended WAL tables
3. Search for WAL/segment ids of suspended tables from provided log file
4. Search for corrupted transaction for each suspended tables
5. Resume WAL by skipping corrupted transation

Essentially, it does what's described in [Diagnosing corrupted WAL transactions](https://questdb.io/docs/reference/sql/alter-table-resume-wal/#diagnosing-corrupted-wal-transactions) but in automated way.

## Running
1. Install dependencies from `requirements.txt`, otherwise `Pipfile` can be utilised along with `pipenv`
2. Fetch logs from your `questdb` instance, for `questdb` running in docker container: `docker logs questdb > logs/qdb.log`
3. Make sure your `questdb` instance has PGWire enabled and that it accepts connections on `8812` port (or other if configured differently)
4. Execute with `--dry-run` flag:
    ```bash
    python main.py \
        --host <host> \
        --port <port> \
        --username <username> \
        --password <password> \
        --database questdb \
        --log-file logs/qdb.log \
        --dry-run
    ```
5. Examine if there are no errors when connecting to `questdb` or parsin log file.
6. Run again without `--dry-run` flag
7. Enjoy resumed WAL table :)

### Disclaimer
This utility was written to handle suspended WAL tables that have been suspended because of power failures on device hosting `questdb`.
As pointed out in [#4829](https://github.com/questdb/questdb/issues/4829) issue, skipping corrupted WAL transactions might not always work in case of serious storage corruption.
