"""Typed SQL persistence only. No SQL replacement for the engine's analytics."""
import os
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

TABLES = {
    'q1': ('q1_daily_traffic', ['log_date', 'status_code', 'request_count', 'total_bytes']),
    'q2': ('q2_top_resources', ['rank', 'resource_path', 'request_count', 'total_bytes', 'distinct_host_count']),
    'q3': ('q3_hourly_errors', ['log_date', 'log_hour', 'error_request_count', 'total_request_count', 'error_rate', 'distinct_error_hosts']),
}
DSN = 'host=127.0.0.1 port=55432 dbname=nasa_reporting user=nasa password=nasa_local_demo'


def connect():
    return psycopg.connect(os.environ.get('NASA_SQL_DSN', DSN), row_factory=dict_row)


class Store:
    def __init__(self):
        self.conn = connect()
        self.conn.execute(Path(__file__).resolve().parent.parent.joinpath('schema.sql').read_text())
        self.conn.commit()

    def start(self, run_id, pipeline, batch_size, version, engines):
        self.conn.execute('INSERT INTO runs(run_id,pipeline,status,parser_version,configured_batch_size,engine_versions) VALUES (%s,%s,\'RUNNING\',%s,%s,%s)',
                          (run_id, pipeline, version, batch_size, Jsonb(engines)))
        self.conn.commit()

    def results(self, run_id, pipeline, scope, batch_id, result):
        for key, (table, columns) in TABLES.items():
            names = ['run_id', 'pipeline', 'scope', 'batch_id'] + columns
            values = [(run_id, pipeline, scope, batch_id, *[r[c] for c in columns]) for r in result[key]]
            if values:
                with self.conn.cursor() as cursor:
                    cursor.executemany(f'INSERT INTO {table} ({",".join(names)}) VALUES ({",".join(["%s"]*len(names))})', values)

    def batch(self, run_id, pipeline, batch_id, result, elapsed):
        s = result['stats']
        self.conn.execute('INSERT INTO batches(run_id,batch_id,actual_batch_size,valid_records,malformed_records,malformed_reasons,malformed_examples,elapsed_seconds) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                          (run_id, batch_id, s['input'], s['valid'], s['malformed'], Jsonb(s['reasons']), Jsonb(s['examples']), elapsed))
        self.results(run_id, pipeline, 'BATCH', batch_id, result)
        self.conn.commit()

    def finish_results(self, run_id, pipeline, batch_id, result, sources):
        if batch_id:
            self.results(run_id, pipeline, 'FINAL', batch_id, result)
        s = result['stats']
        self.conn.execute('UPDATE runs SET total_records=%s,valid_records=%s,malformed_records=%s,batch_count=%s,average_batch_size=%s,sources=%s WHERE run_id=%s',
                          (s['input'], s['valid'], s['malformed'], batch_id, s['input']/batch_id if batch_id else 0, Jsonb(sources), run_id))
        self.conn.commit()  # Timer ends only AFTER this final aggregate commit succeeds.

    def success(self, run_id, seconds):
        self.conn.execute("UPDATE runs SET status='SUCCESS',runtime_seconds=%s,finished_at=clock_timestamp() WHERE run_id=%s", (seconds, run_id))
        self.conn.commit()

    def failed(self, run_id, error):
        self.conn.rollback()
        self.conn.execute("UPDATE runs SET status='FAILED',error=%s,finished_at=clock_timestamp() WHERE run_id=%s", (str(error)[-5000:], run_id))
        self.conn.commit()

    def close(self):
        self.conn.close()
