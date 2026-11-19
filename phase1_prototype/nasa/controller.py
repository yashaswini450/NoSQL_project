import json
from pathlib import Path
import time
import uuid
from .parser_rules import VERSION
from .raw_input import RawBatches, BASE_URL
from .store import Store


def validate_result(result, expected_count):
    s = result['stats']
    if s['input'] != expected_count or s['valid'] + s['malformed'] != expected_count:
        raise RuntimeError(f'Accounting mismatch: {s}, expected {expected_count}')
    if sum(s['reasons'].values()) != s['malformed']:
        raise RuntimeError('Malformed reason counts do not reconcile')
    if sum(r['request_count'] for r in result['q1']) != s['valid']:
        raise RuntimeError('Q1 request counts do not reconcile with valid records')
    if sum(r['total_request_count'] for r in result['q3']) != s['valid']:
        raise RuntimeError('Q3 request counts do not reconcile with valid records')
    if len(result['q2']) > 20:
        raise RuntimeError('Q2 returned more than 20 resources')


def run(pipeline, paths, batch_size, state, test_input=False):
    if pipeline not in ('mongodb', 'mapreduce'):
        raise NotImplementedError(f'{pipeline.title()} is planned for Phase 2; this prototype implements MongoDB and MapReduce.')
    if batch_size <= 0:
        raise ValueError('Batch size must be greater than zero')
    paths = [Path(path).resolve() for path in paths]
    if not paths or any(not p.is_file() for p in paths):
        raise ValueError('Input missing. Run: bash run.sh main.py download')
    expected_sources = []
    if not test_input:
        for path in paths:
            manifest = json.loads((path.parent / 'manifest.json').read_text())
            matches = [m for m in manifest['files'] if m['name'] == path.name and m['url'] == BASE_URL + path.name + '.gz']
            if len(matches) != 1:
                raise ValueError('Only the official unmodified NASA files downloaded by this tool are allowed')
            expected_sources.append(matches[0])
    run_id = uuid.uuid4()
    staging = Path(state) / 'staging' / str(run_id)
    if pipeline == 'mongodb':
        from .mongo_engine import MongoEngine
        engine = MongoEngine(run_id, staging)
    else:
        from .mapreduce_engine import MapReduceEngine
        engine = MapReduceEngine(run_id, staging)
    engine.versions['dataset'] = 'synthetic integration fixture' if test_input else 'official NASA raw logs'
    store = Store()
    store.start(run_id, pipeline, batch_size, VERSION, engine.versions)
    batches = RawBatches(paths, batch_size, staging / 'raw')
    print(f'RUN_ID={run_id}\nPipeline: {pipeline} | B={batch_size} | parser={VERSION}', flush=True)
    success, total, batch_id = False, 0, 0
    try:
        # Includes engine setup, raw framing, parsing, jobs, queries and SQL loads.
        # Downloads, service startup, connection checks, schema setup and report rendering are outside.
        start = time.perf_counter()
        engine.initialize()
        for batch_id, count, spool in batches:
            batch_start = time.perf_counter()
            result = engine.batch(batch_id, count, spool)
            validate_result(result, count)
            store.batch(run_id, pipeline, batch_id, result, time.perf_counter() - batch_start)
            total += count
            print(f'Batch {batch_id}: input={count:,}, valid={result["stats"]["valid"]:,}, malformed={result["stats"]["malformed"]:,}', flush=True)
            spool.unlink()  # Only this run's raw transport copy; original downloaded files are untouched.
        if expected_sources:
            for actual, expected in zip(batches.sources, expected_sources):
                if actual['raw_sha256'] != expected['raw_sha256'] or actual['raw_bytes'] != expected['raw_bytes']:
                    raise RuntimeError(f'Official raw file changed: {actual["name"]}; download it again')
        result = engine.final() if batch_id else {'q1': [], 'q2': [], 'q3': [],
                  'stats': {'input': 0, 'valid': 0, 'malformed': 0, 'reasons': {}, 'examples': []}}
        validate_result(result, total)
        store.finish_results(run_id, pipeline, batch_id, result, batches.sources)
        duration = time.perf_counter() - start  # AFTER successful final SQL commit.
        store.success(run_id, duration)
        success = True
        print(f'SUCCESS {run_id} | {total:,} records | {batch_id} batches | {duration:.3f} seconds', flush=True)
        return str(run_id)
    except BaseException as error:
        store.failed(run_id, error)
        raise
    finally:
        store.close()
        engine.close(success)
