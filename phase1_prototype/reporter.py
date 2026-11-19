"""Separate SQL-only report reader. Never reads logs, MongoDB or Hadoop output."""
import argparse
import json
from pathlib import Path
from nasa.store import connect, TABLES

TITLES = {'q1': 'Q1 — Daily Traffic Summary', 'q2': 'Q2 — Top Requested Resources', 'q3': 'Q3 — Hourly Error Analysis'}


def get_report(run_id=None, scope='FINAL', batch_id=None):
    with connect() as conn:
        if run_id:
            run = conn.execute('SELECT * FROM runs WHERE run_id=%s', (run_id,)).fetchone()
        else:
            run = conn.execute("SELECT * FROM runs WHERE status='SUCCESS' ORDER BY (engine_versions->>'dataset'='official NASA raw logs') DESC NULLS LAST, finished_at DESC LIMIT 1").fetchone()
        if not run:
            raise ValueError('No matching run. Execute a pipeline first.')
        if run['status'] != 'SUCCESS':
            raise ValueError(f'Run {run["run_id"]} is {run["status"]}: {run["error"]}')
        batches = conn.execute('SELECT * FROM batches WHERE run_id=%s ORDER BY batch_id', (run['run_id'],)).fetchall()
        selected = run['batch_count'] if batch_id is None else batch_id
        if scope == 'BATCH' and (selected < 1 or selected > run['batch_count']):
            raise ValueError('Choose a non-empty batch in this run')
        if scope == 'FINAL' and selected != run['batch_count']:
            raise ValueError('FINAL covers all batches and is associated with the final batch ID')
        results = {}
        for key, (table, columns) in TABLES.items():
            order = 'rank' if key == 'q2' else ('log_date,status_code' if key == 'q1' else 'log_date,log_hour')
            results[key] = conn.execute(f'SELECT {",".join(columns)} FROM {table} WHERE run_id=%s AND scope=%s AND batch_id=%s ORDER BY {order}',
                                        (run['run_id'], scope, selected)).fetchall()
        return {'run': run, 'batches': batches, 'scope': scope, 'batch_id': selected, 'results': results}


def render(report, row_limit=10):
    r = report['run']
    lines = [f'NASA web log analytics — {r["pipeline"].upper()}', f'Run: {r["run_id"]} | {r["status"]}',
             f'Dataset: {r["engine_versions"].get("dataset", "unknown")}',
             f'Executed: {r["started_at"]} to {r["finished_at"]}',
             f'ETL runtime through final SQL commit: {r["runtime_seconds"]} seconds',
             f'Configured batch size: {r["configured_batch_size"]:,} input records',
             f'Input: {r["total_records"]:,} | valid: {r["valid_records"]:,} | malformed: {r["malformed_records"]:,}',
             f'Non-empty batches: {r["batch_count"]} | average batch size: {r["average_batch_size"]}',
             f'Result scope: {report["scope"]}; batch ID: {report["batch_id"]}' +
             (f' (covers batches 1–{r["batch_count"]})' if report['scope'] == 'FINAL' and r['batch_count'] else ''), '',
             'Batch ID | Actual size | Valid | Malformed | Engine/query/load preparation seconds']
    for b in report['batches']:
        lines.append(f'{b["batch_id"]} | {b["actual_batch_size"]} | {b["valid_records"]} | {b["malformed_records"]} | {b["elapsed_seconds"]}')
        if b['malformed_records']:
            lines.append('  Malformed reasons: ' + json.dumps(b['malformed_reasons'], sort_keys=True))
    for key, rows in report['results'].items():
        lines += ['', TITLES[key] + f' ({len(rows)} rows)', ' | '.join(TABLES[key][1])]
        shown = rows if key == 'q2' or row_limit == 0 else rows[:row_limit]
        lines += [' | '.join(str(row[c]) for c in TABLES[key][1]) for row in shown]
        if len(shown) < len(rows):
            lines.append(f'... {len(rows)-len(shown)} additional rows in SQL; use --rows 0 or --json for every row.')
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id')
    parser.add_argument('--scope', choices=['BATCH', 'FINAL'], default='FINAL')
    parser.add_argument('--batch-id', type=int)
    parser.add_argument('--rows', type=int, default=10, help='Q1/Q3 display limit; 0 prints all rows')
    parser.add_argument('--json', dest='json_path', help='export complete SQL report to this file')
    parser.add_argument('--text', help='also save the displayed text')
    args = parser.parse_args()
    report = get_report(args.run_id, args.scope, args.batch_id)
    text = render(report, args.rows)
    print(text)
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(report, default=str, indent=2) + '\n', encoding='utf-8')
    if args.text:
        Path(args.text).write_text(text, encoding='utf-8')


if __name__ == '__main__':
    main()
