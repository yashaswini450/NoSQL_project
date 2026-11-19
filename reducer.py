"""Q1 reducer: Hadoop supplies rows sorted by the date/status key."""
import sys
from itertools import groupby

rows = (line.rstrip('\n').split('\t') for line in sys.stdin)
for key, group in groupby(rows, key=lambda row: row[0]):
    requests = total_bytes = 0
    for _, count, size in group:
        requests += int(count)
        total_bytes += int(size)
    date, status = key.split('|')
    print(date, status, requests, total_bytes, sep='\t')
