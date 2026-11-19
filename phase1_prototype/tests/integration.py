"""Real engine/SQL integration. Synthetic fixtures are explicitly labelled in SQL.

Run from project directory: bash run.sh tests/integration.py
"""
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nasa.controller import run
from nasa.mongo_engine import MongoEngine
from nasa.parser_rules import parse
from nasa.raw_input import state_root
from reporter import get_report
from verify_equivalence import compare
from tests.test_rules import line


def reference(raw_lines):
    """Small independent in-memory oracle, used ONLY in tests, never in a pipeline."""
    q1, resources, hourly = {}, {}, {}
    for raw in raw_lines:
        r = parse(raw, 1)
        if not r['valid']:
            continue
        d = q1.setdefault((r['log_date'], r['status_code']), [0, 0])
        d[0] += 1
        d[1] += r['bytes_transferred']
        res = resources.setdefault(r['resource_path'], [0, 0, set()])
        res[0] += 1
        res[1] += r['bytes_transferred']
        res[2].add(r['host'])
        hour = hourly.setdefault((r['log_date'], r['log_hour']), [0, 0, set()])
        hour[0] += 1
        if 400 <= r['status_code'] <= 599:
            hour[1] += 1
            hour[2].add(r['host'])
    return {'q1': [dict(log_date=d, status_code=s, request_count=n, total_bytes=b) for (d, s), (n, b) in sorted(q1.items())],
            'q2': [dict(rank=i, resource_path=p, request_count=v[0], total_bytes=v[1], distinct_host_count=len(v[2]))
                   for i, (p, v) in enumerate(sorted(resources.items(), key=lambda x: (-x[1][0], x[0]))[:20], 1)],
            'q3': [dict(log_date=d, log_hour=h, total_request_count=v[0], error_request_count=v[1], error_rate=v[1]/v[0], distinct_error_hosts=len(v[2]))
                   for (d, h), v in sorted(hourly.items())]}


def assert_oracle(run_id, oracle):
    actual = get_report(run_id)['results']
    for q in oracle:
        assert len(actual[q]) == len(oracle[q]), q
        for a, e in zip(actual[q], oracle[q]):
            for key in e:
                if key == 'error_rate':
                    assert abs(float(a[key])-e[key]) < 1e-9
                elif key == 'log_date':
                    assert str(a[key]) == e[key]
                else:
                    assert a[key] == e[key], (q, key, a, e)


def parser_equivalence(root):
    samples = [line(), line(size=None), line(size='-'), line(request='GET /old'), line(request='-'), '', 'broken',
               line(path='/Case?A=B'), line(size=str(2**63-1)), line(size=str(2**63)), line(size='+1'), line(size='abc'),
               line(stamp='30/Feb/1995:00:00:00 -0400'), line(stamp='29/Feb/1996:00:00:00 -0400'),
               line(stamp='01/Jul/1995:24:00:00 -0400'), line(stamp='01/Jul/1995:00:00:00 +2359'),
               line(stamp='01/Jul/1995:00:00:00 +2460'), line(stamp='01/Jul/0000:00:00:00 -0400'),
               line(stamp='01/XYZ/1995:00:00:00 -0400'), line(request='GET /a space HTTP/1.1'), line(host='é'),
               line(stamp='01/Jul/0001:00:00:00 -0400')]
    engine = MongoEngine(uuid.uuid4(), root/'parser-check')
    engine.initialize()
    spool = root/'parser-check.log'
    spool.write_text('\n'.join(samples)+'\n')
    # Only run native parsing here, avoiding SUM overflow for the int64 boundary fixture.
    from nasa.mongo_engine import parsing_pipeline
    engine.raw.insert_many([{'_id': i, 'batch_id': 1, 'raw_line': raw} for i, raw in enumerate(samples)])
    list(engine.raw.aggregate(parsing_pipeline(engine.parsed.name), allowDiskUse=True))
    actual = list(engine.parsed.find().sort('_id', 1))
    for raw, a in zip(samples, actual):
        expected = parse(raw, 1)
        for key in expected:
            assert a.get(key) == expected[key], (raw, key, a, expected)
    engine.close(True)
    return len(samples)


def main():
    root = state_root()/'integration-fixtures'
    root.mkdir(parents=True, exist_ok=True)
    parser_checks = parser_equivalence(root)
    # /winner ranks below 20 in both local halves but becomes first globally.
    first = [line(host='same', path='/winner', status=404)]*3
    second = [line(host='same', path='/winner', status=500, size='-')]*3
    for n in range(21):
        first.extend([line(host='h1', path=f'/first-{n:02d}')]*4)
        second.extend([line(host='h2', path=f'/second-{n:02d}')]*4)
    first.extend(['broken', '', 'broken\rstill one LF record', line(path='/crlf') + '\r'])
    second.extend([line(path='/zero', stamp='01/Jul/1995:01:00:00 -0400', size=None), line(request='GET /old')])
    path = root/'synthetic.log'
    raw = first + second
    path.write_text('\n'.join(raw)+'\n')
    b = len(first)
    mongo = run('mongodb', [path], b, state_root(), test_input=True)
    mr = run('mapreduce', [path], b, state_root(), test_input=True)
    equivalence = compare(mongo, mr)
    assert_oracle(mongo, reference(raw))
    assert_oracle(mr, reference(raw))
    assert get_report(mr)['results']['q2'][0]['resource_path'] == '/winner'
    assert get_report(mr)['results']['q2'][0]['distinct_host_count'] == 1
    assert get_report(mr)['results']['q3'][1]['error_rate'] == 0
    other_mongo = run('mongodb', [path], 100, state_root(), test_input=True)
    invariance = compare(mongo, other_mongo, allow_different_batch_size=True)
    empty = root/'empty.log'
    empty.write_bytes(b'')
    empty_run = run('mongodb', [empty], 2, state_root(), test_input=True)
    assert get_report(empty_run)['run']['batch_count'] == 0
    assert get_report(empty_run)['run']['average_batch_size'] == 0
    summary = {'status': 'PASS', 'native_parser_cases': parser_checks, 'fixture_equivalence': equivalence,
               'batch_size_invariance': invariance, 'oracle': 'all Q1/Q2/Q3 rows matched', 'empty_run': empty_run}
    Path('sample_outputs/integration_checks.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
