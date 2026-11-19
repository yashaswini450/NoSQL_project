"""One selection interface and controller for the Phase 1 prototype."""
import argparse
import sys
from nasa.raw_input import download, state_root, FILES
from nasa.controller import run


def main():
    parser = argparse.ArgumentParser(description='NASA log ETL: choose one backend, run ETL, store results in PostgreSQL.')
    commands = parser.add_subparsers(dest='command')
    dl = commands.add_parser('download', help='download and decompress only the official raw NASA logs')
    dl.add_argument('--data-dir')
    execute = commands.add_parser('run', help='run one selected pipeline')
    execute.add_argument('--pipeline', required=True, choices=['pig', 'mapreduce', 'mongodb', 'hive'])
    execute.add_argument('--batch-size', type=int, default=500000)
    execute.add_argument('--input', nargs='+', help='official raw file paths; default: July then August')
    args = parser.parse_args()
    root = state_root()
    if args.command == 'download':
        download(args.data_dir or root / 'data')
    else:
        if args.command is None:
            print('NASA web log analytics\n1. Pig (Phase 2)\n2. MapReduce\n3. MongoDB\n4. Hive (Phase 2)')
            pipeline = {'1': 'pig', '2': 'mapreduce', '3': 'mongodb', '4': 'hive'}[input('Select one pipeline [1-4]: ').strip()]
            batch_size = int(input('Batch size [500000]: ').strip() or '500000')
            paths = [root / 'data' / name for name in FILES]
        else:
            pipeline, batch_size = args.pipeline, args.batch_size
            paths = args.input or [root / 'data' / name for name in FILES]
        run_id = run(pipeline, paths, batch_size, root)
        print(f'Generate the separate SQL report: bash run.sh reporter.py --run-id {run_id}')


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        sys.exit(1)
