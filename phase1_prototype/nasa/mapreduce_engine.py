"""Genuine Hadoop Streaming jobs using Hadoop's single-machine LocalJobRunner."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from .raw_input import state_root


class MapReduceEngine:
    def __init__(self, run_id, staging):
        root = state_root()
        self.java = Path(os.environ.get('JAVA_HOME', root / 'java')) / 'bin/java'
        self.hadoop = Path(os.environ.get('HADOOP_HOME', root / 'hadoop'))
        self.classpath = str(self.hadoop / '*')
        self.staging = Path(staging).resolve()
        self.code = Path(__file__).resolve().parent
        self.inputs = []
        self.jobs = 0
        self.run_id = str(run_id)
        version = subprocess.check_output([str(self.java), '-cp', self.classpath, 'org.apache.hadoop.util.VersionInfo'], text=True, stderr=subprocess.STDOUT)
        self.versions = {'hadoop': version.splitlines()[0], 'java': 'Temurin 11', 'mode': 'Hadoop Streaming LocalJobRunner; one reducer'}

    def initialize(self):
        self.staging.mkdir(parents=True, exist_ok=True)

    def job(self, label, inputs, mapper, reducer=None):
        self.jobs += 1
        output = self.staging / f'{self.jobs:03d}-{label}'
        temp = self.staging / 'hadoop-tmp'
        temp.mkdir(exist_ok=True)
        command = [str(self.java), '-Xmx1024m', '-Dorg.slf4j.simpleLogger.defaultLogLevel=info',
                   '-cp', self.classpath, 'org.apache.hadoop.streaming.HadoopStreaming',
                   '-D', 'mapreduce.framework.name=local', '-D', 'fs.defaultFS=file:///',
                   # Explicit LF framing matches the binary raw reader; CRLF is stripped by the parser.
                   '-D', 'textinputformat.record.delimiter=\n',
                   '-D', f'hadoop.tmp.dir={temp}', '-D', f'mapreduce.cluster.local.dir={temp / "local"}',
                   '-D', 'mapreduce.task.io.sort.mb=128',
                   '-D', 'mapreduce.local.map.tasks.maximum=1',
                   '-D', 'mapreduce.input.fileinputformat.split.maxsize=536870912',
                   '-D', f'mapreduce.job.name=NASA-{self.run_id}-{label}',
                   '-D', f'mapreduce.job.reduces={1 if reducer else 0}',
                   '-files', ','.join((self.code / name).as_uri() for name in ('mr_tasks.py', 'parser_rules.py'))]
        for path in inputs:
            command += ['-input', str(path)]
        command += ['-output', str(output), '-mapper', mapper,
                    '-reducer', reducer or 'NONE', '-cmdenv', 'PYTHONIOENCODING=utf-8']
        log = self.staging / f'{self.jobs:03d}-{label}.log'
        (self.staging / f'{self.jobs:03d}-{label}.command.json').write_text(json.dumps(command, indent=2) + '\n')
        print(f'  Hadoop job {self.jobs}: {label} (log: {log})', flush=True)
        with log.open('w') as stream:
            completed = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT)
        if completed.returncode:
            tail = '\n'.join(log.read_text(errors='replace').splitlines()[-25:])
            raise RuntimeError(f'Hadoop job {label} failed. Log: {log}\n{tail}')
        if not (output / '_SUCCESS').exists():
            raise RuntimeError(f'Hadoop did not write _SUCCESS: {output}')
        return output

    @staticmethod
    def task(name, suffix=''):
        return shlex.quote(sys.executable) + ' mr_tasks.py ' + name + suffix

    def batch(self, batch_id, count, spool):
        parsed = self.job(f'batch-{batch_id}-parse', [spool], self.task('parse', f' --batch-id {batch_id}'))
        self.inputs.append(parsed)
        return self.query_jobs(f'batch-{batch_id}', [parsed])

    def query_jobs(self, label, inputs):
        pairs = self.job(label + '-pairs', inputs, self.task('query-map'), self.task('pair-reduce'))
        final = self.job(label + '-queries', [pairs], 'cat', self.task('query-reduce'))
        result = {'q1': [], 'q2': [], 'q3': [], 'stats': None}
        for part in sorted(final.glob('part-*')):
            with part.open() as source:
                for line in source:
                    key, value = line.split('\t', 1)
                    key, value = json.loads(key), json.loads(value)
                    if key == 'stats':
                        if result['stats'] is not None:
                            raise RuntimeError('Expected one global statistics row')
                        result['stats'] = value
                    else:
                        result[key].append(value)
        # Row order for persistence/display only, all data values and rank were computed by Hadoop.
        result['q1'].sort(key=lambda r: (r['log_date'], r['status_code']))
        result['q2'].sort(key=lambda r: r['rank'])
        result['q3'].sort(key=lambda r: (r['log_date'], r['log_hour']))
        if result['stats'] is None:
            raise RuntimeError('Missing native Hadoop statistics')
        return result

    def final(self):
        # Re-read ALL native parsed outcomes. Never add batch-local distinct counts or top-20 lists.
        return self.query_jobs('final', self.inputs)

    def close(self, success):
        pass  # Preserve job commands, counters and native _SUCCESS markers for review.
