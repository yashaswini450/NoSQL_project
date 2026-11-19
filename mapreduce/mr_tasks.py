"""Executable Hadoop Streaming mapper/reducer tasks (stdlib only).

parse -> canonical outcomes; query-map -> pair-reduce -> query-reduce.
These programs run as Hadoop tasks, including the parsing and exact distinct logic.
"""
import argparse
from itertools import groupby
import json
import sys
from parser_rules import parse


def encoded(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=True)


def emit(key, value):
    sys.stdout.write(encoded(key) + '\t' + encoded(value) + '\n')


def empty_stats():
    return {'input': 0, 'valid': 0, 'malformed': 0, 'reasons': {}, 'examples': []}


def merge_stats(target, value):
    for key in ('input', 'valid', 'malformed'):
        target[key] += value[key]
    for key, count in value['reasons'].items():
        target['reasons'][key] = target['reasons'].get(key, 0) + count
    target['examples'] = (target['examples'] + value['examples'])[:5]


def parse_map(batch_id):
    for raw in sys.stdin.buffer:
        sys.stdout.write(encoded(parse(raw.decode('utf-8', errors='replace'), batch_id)) + '\n')


def query_map():
    stats = empty_stats()
    for line in sys.stdin:
        r = json.loads(line)
        stats['input'] += 1
        if not r['valid']:
            stats['malformed'] += 1
            stats['reasons'][r['reason']] = stats['reasons'].get(r['reason'], 0) + 1
            if len(stats['examples']) < 5:
                stats['examples'].append({'reason': r['reason'], 'raw_line': r['raw_line']})
            continue
        stats['valid'] += 1
        emit(['q1', r['log_date'], r['status_code']], [1, r['bytes_transferred']])
        emit(['q2', r['resource_path'], r['host']], [1, r['bytes_transferred']])
        emit(['q3', r['log_date'], r['log_hour'], r['host']], [1, int(400 <= r['status_code'] <= 599)])
    emit(['stats'], stats)


def grouped_input():
    records = (line.rstrip('\n').split('\t', 1) for line in sys.stdin)
    for raw_key, values in groupby(records, key=lambda item: item[0]):
        yield json.loads(raw_key), (json.loads(item[1]) for item in values)


def pair_reduce():
    for key, values in grouped_input():
        tag = key[0]
        if tag == 'stats':
            stats = empty_stats()
            for v in values:
                merge_stats(stats, v)
            emit(key, stats)
            continue
        a = b = 0
        for n, value in values:
            a += n
            b += value
        if tag == 'q1':
            emit(key, [a, b])
        elif tag == 'q2':
            # One output per (resource,host), hence an exact distinct host increment of 1.
            emit(key[:2], [a, b, 1])
        elif tag == 'q3':
            emit(key[:3], [a, b, int(b > 0)])


def query_reduce():
    top = []  # At most 21 entries, independent of the number of hosts/resources.
    for key, values in grouped_input():
        tag = key[0]
        if tag == 'stats':
            stats = empty_stats()
            for v in values:
                merge_stats(stats, v)
            emit('stats', stats)
            continue
        total = [0] * (2 if tag == 'q1' else 3)
        for v in values:
            for i, count in enumerate(v):
                total[i] += count
        if tag == 'q1':
            emit('q1', dict(log_date=key[1], status_code=key[2], request_count=total[0], total_bytes=total[1]))
        elif tag == 'q2':
            top.append(dict(resource_path=key[1], request_count=total[0], total_bytes=total[1], distinct_host_count=total[2]))
            top.sort(key=lambda r: (-r['request_count'], r['resource_path']))
            del top[20:]
        elif tag == 'q3':
            emit('q3', dict(log_date=key[1], log_hour=key[2], total_request_count=total[0], error_request_count=total[1],
                            distinct_error_hosts=total[2], error_rate=total[1] / total[0]))
    for rank, row in enumerate(top, 1):
        row['rank'] = rank
        emit('q2', row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('task', choices=['parse', 'query-map', 'pair-reduce', 'query-reduce'])
    parser.add_argument('--batch-id', type=int)
    args = parser.parse_args()
    if args.task == 'parse':
        parse_map(args.batch_id)
    elif args.task == 'query-map':
        query_map()
    elif args.task == 'pair-reduce':
        pair_reduce()
    else:
        query_reduce()
