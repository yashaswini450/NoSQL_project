"""Verify the portable SQL export in a new temporary database, then remove only that database."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
import psycopg
from psycopg import sql
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nasa.store import DSN
from reporter import get_report


def main():
    base = Path(__file__).resolve().parents[1]
    ids = json.loads((base/'sample_outputs/official_run_ids.json').read_text())
    expected = {key: get_report(value) for key, value in ids.items()}
    dbname = 'nasa_restore_check_' + uuid.uuid4().hex
    original_dsn = os.environ.get('NASA_SQL_DSN', DSN)
    # This test uses the default private demo server, never an external configured server.
    if original_dsn != DSN:
        raise ValueError('Restore check requires the default private demo server')
    admin_dsn = 'host=127.0.0.1 port=55432 dbname=postgres user=nasa password=nasa_local_demo'
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))
        try:
            subprocess.run(['/usr/lib/postgresql/16/bin/psql', '-h', '127.0.0.1', '-p', '55432', '-U', 'nasa', '-d', dbname,
                            '-v', 'ON_ERROR_STOP=1', '-f', str(base/'sample_outputs/restore_official_reports.sql')],
                           check=True, env={**os.environ, 'PGPASSWORD': 'nasa_local_demo'})
            os.environ['NASA_SQL_DSN'] = original_dsn.replace('dbname=nasa_reporting', f'dbname={dbname}')
            for key, run_id in ids.items():
                assert get_report(run_id) == expected[key], f'{key}: restored report differs'
            summary = {'status': 'PASS', 'check': 'Both complete official SQL reports round-tripped through the portable COPY script',
                       'run_ids': ids}
            (base/'sample_outputs/restore_check.json').write_text(json.dumps(summary, indent=2)+'\n')
            print(json.dumps(summary, indent=2))
        finally:
            os.environ['NASA_SQL_DSN'] = original_dsn
            admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(dbname)))


if __name__ == '__main__':
    main()
