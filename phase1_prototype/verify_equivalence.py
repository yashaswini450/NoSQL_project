"""Read stored SQL results and compare two complete runs, including every batch."""
import argparse
import json
from pathlib import Path
from reporter import get_report


def compare(left_id, right_id, allow_different_batch_size=False):
    left, right = get_report(left_id), get_report(right_id)
    a, b = left['run'], right['run']
    for field in ['sources', 'parser_version', 'total_records', 'valid_records', 'malformed_records']:
        if a[field] != b[field]:
            raise AssertionError(f'Run metadata differs: {field}')
    same_b = a['configured_batch_size'] == b['configured_batch_size']
    if not same_b and not allow_different_batch_size:
        raise AssertionError('Use the same batch size for a fair engine comparison')

    def equal_results(x, y, label):
        for query in ('q1', 'q2', 'q3'):
            if len(x[query]) != len(y[query]):
                raise AssertionError(f'{label}/{query}: row count differs')
            for n, (row_x, row_y) in enumerate(zip(x[query], y[query])):
                for key in row_x:
                    if key == 'error_rate':
                        equal = abs(row_x[key] - row_y[key]) <= 1e-9
                    else:
                        equal = row_x[key] == row_y[key]
                    if not equal:
                        raise AssertionError(f'{label}/{query} row {n}, {key}: {row_x[key]} != {row_y[key]}')

    equal_results(left['results'], right['results'], 'FINAL')
    if same_b:
        if len(left['batches']) != len(right['batches']):
            raise AssertionError('Batch count differs')
        for x, y in zip(left['batches'], right['batches']):
            for field in ('batch_id', 'actual_batch_size', 'valid_records', 'malformed_records', 'malformed_reasons'):
                if x[field] != y[field]:
                    raise AssertionError(f'Batch {x["batch_id"]}: {field} differs')
            batch_id = x['batch_id']
            equal_results(get_report(left_id, 'BATCH', batch_id)['results'], get_report(right_id, 'BATCH', batch_id)['results'], f'BATCH {batch_id}')
    return {'status': 'PASS', 'left_run': str(a['run_id']), 'right_run': str(b['run_id']),
            'input_records': a['total_records'], 'valid_records': a['valid_records'], 'malformed_records': a['malformed_records'],
            'batch_size': a['configured_batch_size'] if same_b else 'different (final-result invariance check)',
            'batches_compared': a['batch_count'] if same_b else 0,
            'final_rows_compared': {q: len(rows) for q, rows in left['results'].items()},
            'integer_comparison': 'exact', 'error_rate_absolute_tolerance': 1e-9}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--left', required=True)
    parser.add_argument('--right', required=True)
    parser.add_argument('--output')
    args = parser.parse_args()
    result = compare(args.left, args.right)
    text = json.dumps(result, indent=2) + '\n'
    print(text)
    if args.output:
        Path(args.output).write_text(text)
