"""WSL presentation menu. Reports read live PostgreSQL, never the exported reports."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

from pymongo import MongoClient
from nasa.controller import run
from nasa.raw_input import FILES, state_root
from nasa.store import connect, TABLES
from reporter import get_report, render
from verify_equivalence import compare

BASE = Path(__file__).resolve().parent


def official_ids():
    return json.loads((BASE / 'sample_outputs/official_run_ids.json').read_text())


def check():
    root = state_root()
    with MongoClient('mongodb://127.0.0.1:27018', serverSelectionTimeoutMS=5000) as mongo:
        print('MongoDB:', mongo.server_info()['version'], 'on 127.0.0.1:27018')
    with connect() as conn:
        print('PostgreSQL:', conn.execute('SHOW server_version').fetchone()['server_version'])
    java = root / 'java/bin/java'
    version = subprocess.check_output([str(java), '-cp', str(root / 'hadoop/*'), 'org.apache.hadoop.util.VersionInfo'],
                                      text=True, stderr=subprocess.STDOUT).splitlines()[0]
    print(version + ' (Streaming LocalJobRunner)')
    for name in FILES:
        path = root / 'data' / name
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f'Official input missing: {path}. Run bash run.sh main.py download')
        print(f'Raw file ready: {name} ({path.stat().st_size:,} bytes)')
    for pipeline, run_id in official_ids().items():
        report = get_report(run_id)
        print(f'{pipeline}: completed official SQL run {run_id}, {report["run"]["total_records"]:,} input records')
    print('READY: services, raw files and completed SQL results are available.\n')


def report(pipeline):
    print(render(get_report(official_ids()[pipeline]), row_limit=3))


def equivalence():
    ids = official_ids()
    print('Comparing every batch and final query row from live PostgreSQL...')
    print(json.dumps(compare(ids['mongodb'], ids['mapreduce']), indent=2))


def sql_view():
    ids = list(official_ids().values())
    with connect() as conn:
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name").fetchall()
        print('PostgreSQL reporting tables: ' + ', '.join(r['table_name'] for r in tables))
        print('\nSELECT pipeline, status, total_records, malformed_records, batch_count, runtime_seconds FROM runs ...')
        rows = conn.execute('SELECT run_id,pipeline,status,total_records,malformed_records,batch_count,runtime_seconds FROM runs WHERE run_id IN (%s,%s) ORDER BY pipeline', ids).fetchall()
        for row in rows:
            print(' | '.join(f'{k}={v}' for k, v in row.items()))
        print('\nFinal stored aggregate row counts:')
        for key, (table, _) in TABLES.items():
            rows = conn.execute(f"SELECT pipeline,count(*) AS rows FROM {table} WHERE run_id IN (%s,%s) AND scope='FINAL' GROUP BY pipeline ORDER BY pipeline", ids).fetchall()
            print(f'{key}: ' + ' | '.join(f'{row["pipeline"]}={row["rows"]}' for row in rows))
        print('\nTop three stored MapReduce resources (SELECT only):')
        rows = conn.execute("SELECT rank,resource_path,request_count,total_bytes,distinct_host_count FROM q2_top_resources WHERE run_id=%s AND scope='FINAL' ORDER BY rank LIMIT 3", (official_ids()['mapreduce'],)).fetchall()
        for row in rows:
            print(' | '.join(str(v) for v in row.values()))


def jobs():
    run_id = official_ids()['mapreduce']
    staging = state_root() / 'staging' / run_id
    logs = sorted(staging.glob('*.log'))
    if not logs:
        raise ValueError(f'Native job logs are not installed here: {staging}. The portable sample logs are in sample_outputs/.')
    print(f'Actual native Hadoop logs: {staging}')
    for log in logs:
        content = log.read_text(errors='replace')
        ids = sorted(set(re.findall(r'job_local\d+_\d+', content)))
        status = 'SUCCESS' if 'completed successfully' in content else 'CHECK LOG'
        print(f'{log.stem}: {status} | {", ".join(ids)}')
    print('\nLast job: native Hadoop counters')
    print('\n'.join(logs[-1].read_text(errors='replace').splitlines()[-38:]))


def live(pipeline, month, batch_size):
    root = state_root()
    names = FILES if month == 'both' else [FILES[0 if month == 'jul' else 1]]
    print(f'Fresh {pipeline} ETL: {", ".join(names)}; B={batch_size:,}.')
    print('Complete official raw files; the execution may take several minutes. Job progress appears below.', flush=True)
    run_id = run(pipeline, [root / 'data' / name for name in names], batch_size, root)
    state = root / 'demo'
    state.mkdir(exist_ok=True)
    path = state / 'latest_run.json'
    path.write_text(json.dumps({'run_id': run_id, 'pipeline': pipeline, 'month': month, 'batch_size': batch_size}, indent=2) + '\n')
    print('\nReading this new run back through the separate SQL reporting module:\n')
    print(render(get_report(run_id), row_limit=3))


def latest():
    path = state_root() / 'demo/latest_run.json'
    if not path.exists():
        raise ValueError('No fresh demo run yet. Use live-mapreduce or live-mongodb; the full-data reports are menu options 2 and 3.')
    print(render(get_report(json.loads(path.read_text())['run_id']), row_limit=3))


def tour():
    print('NASA web log ETL — Phase 1\n')
    print('Raw NASA lines -> one selected native engine -> Q1/Q2/Q3 -> PostgreSQL -> separate report\n')
    print('1. Runtime readiness')
    check()
    print('2. Completed MapReduce run, read from SQL')
    report('mapreduce')
    print('3. PostgreSQL storage')
    sql_view()
    print('\n4. MongoDB / MapReduce equivalence')
    equivalence()
    print('\nTour complete. This displayed previously completed runs from SQL.')
    print('For fresh execution: bash demo.sh live-mapreduce --month aug --batch-size 500000')


def menu():
    actions = {
        '1': tour, '2': lambda: report('mapreduce'), '3': lambda: report('mongodb'),
        '4': equivalence, '5': sql_view, '6': jobs,
        '7': lambda: live('mapreduce', 'aug', 500000),
        '8': lambda: live('mongodb', 'aug', 500000), '9': latest, '10': check,
    }
    while True:
        print('\nNASA web logs / Member 3 demo\n'
              '1  Guided tour of completed official runs\n'
              '2  Full-data MapReduce report from SQL\n'
              '3  Full-data MongoDB report from SQL\n'
              '4  Verify equivalence from SQL\n'
              '5  Show SQL tables and stored results\n'
              '6  Show actual native Hadoop jobs and counters\n'
              '7  New MapReduce run: complete August file\n'
              '8  New MongoDB run: complete August file\n'
              '9  Report the latest new demo run\n'
              '10 Check readiness\n'
              '0  Exit')
        try:
            choice = input('Choose: ').strip()
        except EOFError:
            return
        if choice == '0':
            return
        action = actions.get(choice)
        if action is None:
            print('Choose one of the numbered options.')
            continue
        try:
            action()
        except KeyboardInterrupt:
            print('\nInterrupted. A started ETL run is marked failed; choose another action when ready.')
        except Exception as error:
            print(f'ERROR: {error}', file=sys.stderr)
        try:
            input('\nPress Enter to return to the menu...')
        except EOFError:
            return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', nargs='?', default='menu', choices=['menu', 'check', 'tour', 'report-mr', 'report-mongo', 'compare', 'sql', 'jobs', 'live-mapreduce', 'live-mongodb', 'latest'])
    parser.add_argument('--month', choices=['jul', 'aug', 'both'], default='aug')
    parser.add_argument('--batch-size', type=int, default=500000)
    args = parser.parse_args()
    actions = {'menu': menu, 'check': check, 'tour': tour, 'report-mr': lambda: report('mapreduce'),
               'report-mongo': lambda: report('mongodb'), 'compare': equivalence, 'sql': sql_view,
               'jobs': jobs, 'latest': latest,
               'live-mapreduce': lambda: live('mapreduce', args.month, args.batch_size),
               'live-mongodb': lambda: live('mongodb', args.month, args.batch_size)}
    actions[args.action]()


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        sys.exit(1)
