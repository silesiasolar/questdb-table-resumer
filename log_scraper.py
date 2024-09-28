import re
from operator import attrgetter

from dateutil import parser
from itertools import groupby

class CorruptedWal:

    def __init__(self, time, table_name, wal_id, segment_id):
        self.time = parser.isoparse(time)
        self.table_name = table_name
        self.wal_id = wal_id
        self.segment_id = segment_id

    def __repr__(self):
        return f"<CorruptedWal time={self.time.isoformat()}, table={self.table_name}, wal_id={self.wal_id}, segment_id={self.segment_id}>"


class LogScraper:
    pattern = re.compile(
        r"""^(?P<time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\sC\s.*ApplyWal2TableJob.*table=(?P<table_name>[\w]+)(~\d+)?.*?\/wal(?P<wal_id>\d+)\/(?P<segment_id>\d+).*?"""
    )
    def __parse_line(self, line: str) -> CorruptedWal | None:
        match = self.pattern.match(line)
        if match:
            return CorruptedWal(match.group("time"), match.group("table_name"), match.group("wal_id"), match.group("segment_id"))
        return None

    def __find_all_corrupted_wals(self, lines: [str]) -> [CorruptedWal]:
        corrupted_wals = []
        for line in lines:
            parsed_entry = self.__parse_line(line)
            if parsed_entry:
                corrupted_wals.append(parsed_entry)
        return corrupted_wals

    def find_corrupted_wals(self, lines: [str]) -> [CorruptedWal]:
        corrupted = self.__find_all_corrupted_wals(lines)
        corrupted.sort(key=attrgetter("table_name"))
        result = {}
        for table_name, group in groupby(corrupted, key=attrgetter("table_name")):
            result[table_name] = max(group, key=attrgetter("time"))
        return list(result.values())

