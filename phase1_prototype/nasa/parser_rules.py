"""Versioned parsing rules. Used inside Hadoop mappers, never to preprocess input.

MongoDB implements this contract using native aggregation expressions.
"""
import datetime as dt
import re

VERSION = 'nasa-common-v1'
ENVELOPE = r'^(\S+)[ \t]+\S+[ \t]+\S+[ \t]+\[([^\]]+)\][ \t]+"(.*)"[ \t]+([0-9]{3})(?:[ \t]+([^ \t]+))?[ \t]*$'
REQUEST = r'^([A-Za-z]+)[ \t]+(.+?)(?:[ \t]+(HTTP/[0-9]+\.[0-9]+))?$'
STAMP = r'^([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):([0-9]{2}):([0-5][0-9]):([0-5][0-9]) ([+-][0-9]{4})$'
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
_env, _req, _stamp = map(re.compile, (ENVELOPE, REQUEST, STAMP))
MAX_LONG = 2**63 - 1


def parse(raw, batch_id):
    raw = raw.rstrip('\n')
    if raw.endswith('\r'):
        raw = raw[:-1]

    def bad(reason):
        return {'valid': False, 'batch_id': batch_id, 'reason': reason, 'raw_line': raw[:300]}

    envelope = _env.fullmatch(raw) if raw.isascii() else None
    if not envelope:
        return bad('invalid_envelope')
    host, timestamp, request, status, byte_text = envelope.groups()
    stamp = _stamp.fullmatch(timestamp)
    if not stamp:
        return bad('invalid_timestamp')
    day, month, year, hour, minute, second, offset = stamp.groups()
    try:
        month_num = MONTHS.index(month.lower()) + 1
        oh, om = int(offset[1:3]), int(offset[3:5])
        if oh > 23 or om > 59:
            raise ValueError('timezone')
        # Validate calendar and time, but preserve the log's local date/hour.
        local = dt.datetime(int(year), month_num, int(day), int(hour), int(minute), int(second))
    except ValueError:
        return bad('invalid_timestamp')
    req = _req.fullmatch(request.strip(' \t'))
    if not req:
        return bad('invalid_request')
    method, path, protocol = req.groups()
    if byte_text in (None, '-'):
        byte_count = 0
    elif re.fullmatch(r'[0-9]+', byte_text) and len(byte_text.lstrip('0')) <= 19:
        byte_count = int(byte_text.lstrip('0') or '0')
        if byte_count > MAX_LONG:
            return bad('invalid_bytes')
    else:
        return bad('invalid_bytes')
    return dict(valid=True, batch_id=batch_id, host=host, timestamp=timestamp,
                log_date=f'{local.year:04d}-{local.month:02d}-{local.day:02d}', log_hour=int(hour),
                http_method=method, resource_path=path, protocol_version=protocol,
                status_code=int(status), bytes_transferred=byte_count)
