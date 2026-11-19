"""Run both complete official inputs sequentially and save genuine SQL reports."""
import json
from pathlib import Path
from nasa.controller import run
from nasa.raw_input import state_root, FILES
from reporter import get_report, render
from verify_equivalence import compare


def main():
    root = state_root()
    output = Path(__file__).resolve().parent/'sample_outputs'
    paths = [root/'data'/name for name in FILES]
    run_ids = {}
    for pipeline in ('mongodb', 'mapreduce'):
        run_id = run(pipeline, paths, 500000, root)
        run_ids[pipeline] = run_id
        report = get_report(run_id)
        (output/f'{pipeline}_official_report.json').write_text(json.dumps(report, default=str, indent=2)+'\n')
        (output/f'{pipeline}_official_report.txt').write_text(render(report), encoding='utf-8')
        (output/'official_run_ids.json').write_text(json.dumps(run_ids, indent=2)+'\n')
    result = compare(run_ids['mongodb'], run_ids['mapreduce'])
    (output/'official_equivalence.json').write_text(json.dumps(result, indent=2)+'\n')
    (output/'data_manifest.json').write_text((root/'data/manifest.json').read_text())
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
