"""Introductory Q1 mapper: emit a date/status key, count and bytes."""
import re
import sys
from datetime import datetime

LOG = re.compile(r'^\S+[ \t]+\S+[ \t]+\S+[ \t]+\[([^\]]+)\][ \t]+"(.*)"[ \t]+([0-9]{3})(?:[ \t]+([0-9]+|-))?[ \t]*$')
REQUEST = re.compile(r'^[A-Za-z]+[ \t]+.+?(?:[ \t]+HTTP/[0-9]+\.[0-9]+)?$')

for line in sys.stdin:
    try:
        raw = line.rstrip('\r\n')
        match = LOG.fullmatch(raw)
        if not raw.isascii() or match is None or not REQUEST.fullmatch(match[2].strip(' \t')):
            raise ValueError('Malformed record')
        date = datetime.strptime(match[1], '%d/%b/%Y:%H:%M:%S %z').date().isoformat()
        size = 0 if match[4] in (None, '-') else int(match[4])
        if size > 2**63 - 1:
            raise ValueError('Byte count too large')
        print(f'{date}|{match[3]}\t1\t{size}')
    except ValueError:
        print('reporter:counter:Records,Malformed,1', file=sys.stderr)
