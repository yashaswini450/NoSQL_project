"""
parser.py — Canonical 2-stage NASA HTTP log parser (Member 1).

This file is the SINGLE SOURCE OF TRUTH for parsing rules. Every pipeline must
implement exactly these rules:
  * MongoDB pipeline  -> imports parse_line() directly (Python driver side)
  * MapReduce mapper  -> imports parse_line() directly (Hadoop Streaming, Python)
  * Pig  (Phase 2)    -> same regex ported to REGEX_EXTRACT_ALL + same rules
  * Hive (Phase 2)    -> same regex ported to RegexSerDe / regexp_extract

Raw NASA line (Common Log Format):
  199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
  |-- host --| |ident/user| |----- timestamp ----| |------ request ---------| |st| |bytes|

STAGE 1 (line regex): split the line into host, timestamp, request, status, bytes.
STAGE 2 (request tokenizer): split the quoted request into method, path, protocol.

A line that fails either stage is MALFORMED: counted, never silently dropped.
"""

import re
from datetime import datetime

# ---------------------------------------------------------------------------
# STAGE 1: whole-line regex
#   group 1  host        : first run of non-space chars
#   (skip)   ident, user : always "- -" in NASA logs, but we accept any \S+
#   group 2  timestamp   : everything inside [ ]
#   group 3  request     : everything between the FIRST " and the LAST " that is
#                          followed by status+bytes. Greedy (.*) so a stray quote
#                          or extra spaces inside the URL do not break the match.
#   group 4  status      : exactly 3 digits
#   group 5  bytes       : digits OR "-"
# Anchored with ^ and $ so a truncated/garbled line cannot partially match.
# ---------------------------------------------------------------------------
LINE_RE = re.compile(
    r'^(\S+) \S+ \S+ \[([^\]]+)\] "(.*)" (\d{3}) (\d+|-)\s*$'
)

# Protocol token, e.g. HTTP/1.0 or HTTP/1.1 (case-insensitive for safety)
PROTO_RE = re.compile(r'^HTTP/\d+\.\d+$', re.IGNORECASE)

# Method must be an alphabetic token (GET, HEAD, POST ...)
METHOD_RE = re.compile(r'^[A-Za-z]+$')

# NASA timestamp: 01/Jul/1995:00:00:01 -0400
TS_FORMAT = "%d/%b/%Y:%H:%M:%S"

# Month names hard-coded so parsing never depends on the machine's locale
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


def _parse_timestamp(raw_ts):
    """
    '01/Jul/1995:00:00:01 -0400' -> datetime(1995, 7, 1, 0, 0, 1)

    TIMEZONE RULE (equivalence-critical): we keep the server's LOCAL time
    exactly as written and IGNORE the offset. We never convert to UTC.
    Reason: Hive/Pig/Mongo each convert time zones differently (Hive uses the
    cluster's timezone). Converting would shift requests near midnight into a
    different log_date/log_hour on different engines. Every NASA line is -0400
    anyway, so local time is consistent across the whole dataset.
    """
    parts = raw_ts.split()
    if len(parts) != 2:
        return None
    dt_part, _offset = parts
    try:
        day, mon, rest = dt_part.split("/", 2)        # '01', 'Jul', '1995:00:00:01'
        year, hh, mm, ss = rest.split(":")
        return datetime(int(year), _MONTHS[mon], int(day),
                        int(hh), int(mm), int(ss))
    except (ValueError, KeyError):
        return None


def _tokenize_request(request):
    """
    STAGE 2: split the request string into (method, resource_path, protocol).

    Cases handled:
      'GET /a.html HTTP/1.0'            -> ('GET', '/a.html', 'HTTP/1.0')  normal
      'GET /a.html'                     -> ('GET', '/a.html', '')           HTTP/0.9 style (2 tokens)
      'GET /a b.html HTTP/1.0'          -> ('GET', '/a b.html', 'HTTP/1.0') space inside URL
      'GET /a b.html'                   -> ('GET', '/a b.html', '')         space + no protocol
      ''  or  'GET'  or  '/a.html'      -> None  (malformed: no method+path)
    """
    tokens = request.split()                 # split on ANY whitespace run
    if len(tokens) < 2:
        return None                          # empty request or single token
    method = tokens[0]
    if not METHOD_RE.match(method):
        return None                          # garbage where method should be
    if len(tokens) >= 3 and PROTO_RE.match(tokens[-1]):
        protocol = tokens[-1]
        path_tokens = tokens[1:-1]
    else:
        protocol = ""                        # HTTP/0.9: version omitted
        path_tokens = tokens[1:]
    # Re-join with single spaces so the path is identical in every engine
    resource_path = " ".join(path_tokens)
    return method.upper(), resource_path, protocol


def parse_line(line):
    """
    Parse one raw log line.
    Returns a dict with the 9 mandatory fields, or None if MALFORMED.
    The caller (pipeline) is responsible for incrementing malformed_count on None.
    """
    m = LINE_RE.match(line.rstrip("\r\n"))
    if not m:
        return None                                       # stage 1 failed
    host, raw_ts, request, status, raw_bytes = m.groups()

    ts = _parse_timestamp(raw_ts)
    if ts is None:
        return None                                       # bad timestamp

    req = _tokenize_request(request)
    if req is None:
        return None                                       # stage 2 failed
    method, path, protocol = req

    return {
        "host": host,
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),    # local time, ISO text
        "log_date": ts.strftime("%Y-%m-%d"),
        "log_hour": ts.hour,                              # int 0..23
        "http_method": method,
        "resource_path": path,
        "protocol_version": protocol,
        "status_code": int(status),
        "bytes_transferred": 0 if raw_bytes == "-" else int(raw_bytes),  # '-' -> 0
    }


class ParseStats:
    """Small helper so every pipeline counts the same way."""

    def __init__(self):
        self.total = 0
        self.valid = 0
        self.malformed = 0

    def record(self, parsed):
        self.total += 1
        if parsed is None:
            self.malformed += 1
        else:
            self.valid += 1

    def __repr__(self):
        return f"ParseStats(total={self.total}, valid={self.valid}, malformed={self.malformed})"


if __name__ == "__main__":
    # Quick manual check:  python parser.py < some_log_file
    import sys
    stats = ParseStats()
    for raw in sys.stdin:
        p = parse_line(raw)
        stats.record(p)
        if p is None:
            print("MALFORMED:", raw.rstrip())
    print(stats)
