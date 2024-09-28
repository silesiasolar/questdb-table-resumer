import psycopg2, logging

def from_cmd(args):
    return QuestDBClient(args.host, args.port, args.username, args.password, args.database)

class QuestDBClient:

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def search_suspended(self) -> [str]:
        query = "SELECT name FROM wal_tables WHERE suspended = true"
        rows = self.__execute(query)
        return [row[0] for row in rows]

    def find_writer_txn(self, table_name) -> int:
        query = f"SELECT writerTxn FROM wal_tables() WHERE name = '{table_name}'"
        rows = self.__execute(query)
        return rows[0][0]

    def find_corrupted_txn(self, table_name, writer_txn, wal_id, segment_id) -> int:
        query = f"""
            SELECT max(sequencertxn)
            FROM wal_transactions('{table_name}')
            WHERE sequencertxn > {writer_txn}
                AND walId = {wal_id}
                AND segmentId = {segment_id}
        """
        rows = self.__execute(query)
        return rows[0][0]

    def resume_wal(self, table_name, corrupted_txn):
        new_txn = corrupted_txn + 1
        self.__execute(f"ALTER TABLE {table_name} RESUME WAL FROM TXN {new_txn}", False)


    def __execute(self, query, fetch=True):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall() if fetch else None
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            return None