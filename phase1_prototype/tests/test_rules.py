"""Synthetic unit fixtures only. Not official benchmark inputs."""
from pathlib import Path
import tempfile
import unittest
from nasa.parser_rules import parse
from nasa.raw_input import RawBatches


def line(host='h', path='/', status=200, size='10', stamp='01/Jul/1995:00:00:01 -0400', request=None):
    request = request if request is not None else f'GET {path} HTTP/1.0'
    return f'{host} - - [{stamp}] "{request}" {status}' + ('' if size is None else ' ' + size)


class ParsingTests(unittest.TestCase):
    def test_nine_fields_and_local_date(self):
        r = parse(line(path='/A?q=X', stamp='01/Jul/1995:23:59:59 -0400'), 1)
        self.assertTrue(r['valid'])
        self.assertEqual((r['log_date'], r['log_hour'], r['resource_path']), ('1995-07-01', 23, '/A?q=X'))
        self.assertEqual(r['protocol_version'], 'HTTP/1.0')
        self.assertEqual(parse(line(stamp='01/Jul/0001:00:00:00 -0400'), 1)['log_date'], '0001-07-01')

    def test_bytes(self):
        for size in (None, '-', '0', '0000'):
            self.assertEqual(parse(line(size=size), 1)['bytes_transferred'], 0)
        self.assertEqual(parse(line(size=str(2**63-1)), 1)['bytes_transferred'], 2**63-1)
        for size in ('-1', '+1', '1.5', 'abc', str(2**63)):
            self.assertEqual(parse(line(size=size), 1)['reason'], 'invalid_bytes')

    def test_protocol_optional(self):
        self.assertIsNone(parse(line(request='GET /old'), 1)['protocol_version'])
        self.assertEqual(parse(line(request='GET /has space HTTP/1.0'), 1)['resource_path'], '/has space')

    def test_malformed(self):
        for raw in ('', 'garbage', 'é - - [x] "GET /" 200 1', line(request='-')):
            self.assertFalse(parse(raw, 1)['valid'])
        for stamp in ('30/Feb/1995:01:00:00 -0400', '01/Foo/1995:00:00:00 -0400',
                      '01/Jul/1995:24:00:00 -0400', '01/Jul/1995:00:00:60 -0400',
                      '01/Jul/0000:00:00:00 -0400', '01/Jul/1995:00:00:00 +2460'):
            self.assertEqual(parse(line(stamp=stamp), 1)['reason'], 'invalid_timestamp')


class BatchTests(unittest.TestCase):
    def test_blank_lines_partial_and_file_boundary(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root/'a').write_bytes(b'first\n\nunterminated')
            (root/'b').write_bytes(b'next\nlast\n')
            framed = RawBatches([root/'a', root/'b'], 3, root/'spool')
            batches = list(framed)
            self.assertEqual([n for _, n, _ in batches], [3, 2])
            self.assertEqual(batches[0][2].read_bytes(), b'first\n\nunterminated\n')
            self.assertEqual(batches[1][2].read_bytes(), b'next\nlast\n')
            self.assertEqual([s['records'] for s in framed.sources], [3, 2])

    def test_exact_multiple_and_empty(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root/'a').write_bytes(b'a\nb\nc\nd\n')
            self.assertEqual([n for _, n, _ in RawBatches([root/'a'], 2, root/'spool')], [2, 2])
            (root/'empty').write_bytes(b'')
            self.assertEqual(list(RawBatches([root/'empty'], 2, root/'empty-spool')), [])


if __name__ == '__main__':
    unittest.main()
