"""Save authentic Hadoop job IDs/counters and two full logs for the official run."""
import json
from pathlib import Path
import re
from nasa.raw_input import state_root


def collect():
    base = Path(__file__).resolve().parent
    ids = json.loads((base/'sample_outputs/official_run_ids.json').read_text())
    staging = state_root()/'staging'/ids['mapreduce']
    logs = sorted(staging.glob('*.log'))
    summary = {'run_id': ids['mapreduce'], 'execution_mode': 'Hadoop Streaming LocalJobRunner', 'jobs': []}
    for log in logs:
        text = log.read_text(errors='replace')
        counters = {}
        for name in ('Map input records', 'Map output records', 'Reduce input groups', 'Reduce input records', 'Reduce output records', 'Spilled Records'):
            matches = re.findall(re.escape(name)+r'=(\d+)', text)
            if matches:
                counters[name] = int(matches[-1])
        summary['jobs'].append({'stage': log.stem, 'job_ids': sorted(set(re.findall(r'job_local\d+_\d+', text))),
                                'successful': 'completed successfully' in text,
                                'counters': counters})
    assert len(logs) == 23 and all(job['successful'] for job in summary['jobs']), 'Expected 3*7+2 completed native jobs'
    for log in (logs[0], logs[-1]):
        (base/'sample_outputs'/f'hadoop_{log.stem}.txt').write_text(log.read_text(errors='replace'))
    (base/'sample_outputs/hadoop_job_counters.json').write_text(json.dumps(summary, indent=2)+'\n')
    print('Saved genuine counters for', len(logs), 'successful Hadoop jobs')


if __name__ == '__main__':
    collect()
