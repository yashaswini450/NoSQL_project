"""
DAS 839 – NoSQL Systems (End Semester Project)
Member 4 Deliverable: PostgreSQL Result & Metadata Loader (`db/postgres_loader.py`)

Responsibilities:
1. Connect to PostgreSQL (via `psycopg2`) and initialize the 5 relational tables
   defined in `db/schema.sql` BEFORE the ETL timer `t_start` begins.
2. Within the ETL runtime window (`t_start` -> `t_end`), insert:
   - Run-level execution metadata (`etl_run_metadata`)
   - Per-batch execution metadata (`etl_batch_metadata`, `batch_id = 1..k`)
   - Aggregated outputs for Query 1, Query 2, and Query 3 tagged with:
     `(run_id, pipeline_name, batch_id, executed_at)`
3. Commit the transaction immediately before `t_end` is recorded.
"""

import argparse
import datetime
import os
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import execute_values
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


DEFAULT_PG_DSN = os.environ.get(
    "POSTGRES_DSN",
    "dbname=nosql_project user=postgres password=postgres host=localhost port=5432"
)
DEFAULT_SQLITE_FALLBACK = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "db",
    "nosql_results_local.db"
)
SCHEMA_SQL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "schema.sql"
)


class RelationalResultLoader:
    """
    Unified relational loader for PostgreSQL (primary target per spec) with
    automatic local SQLite compatibility mode for offline prototype testing.
    """

    def __init__(self, pg_dsn: Optional[str] = None, sqlite_path: Optional[str] = None, prefer_sqlite: bool = False):
        self.pg_dsn = pg_dsn or DEFAULT_PG_DSN
        self.sqlite_path = sqlite_path or DEFAULT_SQLITE_FALLBACK
        self.backend = "postgresql"
        self.conn = None

        if not prefer_sqlite and HAS_PSYCOPG2:
            try:
                self.conn = psycopg2.connect(self.pg_dsn)
                self.backend = "postgresql"
                return
            except Exception:
                # Fall back cleanly to local SQLite file if PostgreSQL daemon is not running
                pass

        self.backend = "sqlite_compat"
        os.makedirs(os.path.dirname(self.sqlite_path), exist_ok=True)
        self.conn = sqlite3.connect(self.sqlite_path)
        self.conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        """
        Creates all 5 metadata and query result tables.
        NOTE: Must be called BEFORE `t_start` so table creation DDL is excluded from ETL runtime.
        """
        cur = self.conn.cursor()
        if self.backend == "postgresql":
            with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
                cur.execute(f.read())
        else:
            # SQLite-compatible translation of schema.sql
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS etl_run_metadata (
                    run_id                  TEXT PRIMARY KEY,
                    pipeline_name           TEXT NOT NULL,
                    input_dataset           TEXT NOT NULL,
                    configured_batch_size   INTEGER NOT NULL,
                    total_batches           INTEGER NOT NULL,
                    total_records_processed INTEGER NOT NULL,
                    valid_records_count     INTEGER NOT NULL,
                    malformed_record_count  INTEGER NOT NULL,
                    avg_batch_size          REAL NOT NULL,
                    runtime_seconds         REAL NOT NULL,
                    executed_at             TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS etl_batch_metadata (
                    run_id                  TEXT NOT NULL,
                    pipeline_name           TEXT NOT NULL,
                    batch_id                INTEGER NOT NULL,
                    batch_size              INTEGER NOT NULL,
                    valid_records           INTEGER NOT NULL,
                    malformed_records       INTEGER NOT NULL,
                    batch_runtime_seconds   REAL NOT NULL,
                    executed_at             TEXT NOT NULL,
                    PRIMARY KEY (run_id, batch_id)
                );

                CREATE TABLE IF NOT EXISTS q1_daily_traffic_summary (
                    run_id                  TEXT NOT NULL,
                    pipeline_name           TEXT NOT NULL,
                    batch_id                INTEGER NOT NULL,
                    executed_at             TEXT NOT NULL,
                    log_date                TEXT NOT NULL,
                    status_code             INTEGER NOT NULL,
                    request_count           INTEGER NOT NULL,
                    total_bytes             INTEGER NOT NULL,
                    PRIMARY KEY (run_id, batch_id, log_date, status_code)
                );

                CREATE TABLE IF NOT EXISTS q2_top_requested_resources (
                    run_id                  TEXT NOT NULL,
                    pipeline_name           TEXT NOT NULL,
                    batch_id                INTEGER NOT NULL,
                    executed_at             TEXT NOT NULL,
                    resource_path           TEXT NOT NULL,
                    request_count           INTEGER NOT NULL,
                    total_bytes             INTEGER NOT NULL,
                    distinct_host_count     INTEGER NOT NULL,
                    PRIMARY KEY (run_id, batch_id, resource_path)
                );

                CREATE TABLE IF NOT EXISTS q3_hourly_error_analysis (
                    run_id                  TEXT NOT NULL,
                    pipeline_name           TEXT NOT NULL,
                    batch_id                INTEGER NOT NULL,
                    executed_at             TEXT NOT NULL,
                    log_date                TEXT NOT NULL,
                    log_hour                INTEGER NOT NULL,
                    error_request_count     INTEGER NOT NULL,
                    total_request_count     INTEGER NOT NULL,
                    error_rate              REAL NOT NULL,
                    distinct_error_hosts    INTEGER NOT NULL,
                    PRIMARY KEY (run_id, batch_id, log_date, log_hour)
                );
                """
            )
        self.conn.commit()

    def save_run_metadata(
        self,
        run_id: str,
        pipeline_name: str,
        input_dataset: str,
        configured_batch_size: int,
        total_batches: int,
        total_records_processed: int,
        valid_records_count: int,
        malformed_record_count: int,
        runtime_seconds: float,
        executed_at: Optional[str] = None,
    ) -> float:
        """
        Computes `avg_batch_size = total_records_processed / total_batches`
        per specification Page 3 and inserts run-level execution metadata.
        """
        if total_batches <= 0:
            raise ValueError("total_batches must be >= 1 (number of non-empty batches)")
        avg_batch_size = round(total_records_processed / float(total_batches), 2)
        ts = executed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()

        cur = self.conn.cursor()
        params = (
            run_id,
            pipeline_name.lower(),
            input_dataset,
            configured_batch_size,
            total_batches,
            total_records_processed,
            valid_records_count,
            malformed_record_count,
            avg_batch_size,
            round(runtime_seconds, 4),
            ts,
        )
        if self.backend == "postgresql":
            cur.execute(
                """
                INSERT INTO etl_run_metadata (
                    run_id, pipeline_name, input_dataset, configured_batch_size,
                    total_batches, total_records_processed, valid_records_count,
                    malformed_record_count, avg_batch_size, runtime_seconds, executed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    total_batches = EXCLUDED.total_batches,
                    total_records_processed = EXCLUDED.total_records_processed,
                    valid_records_count = EXCLUDED.valid_records_count,
                    malformed_record_count = EXCLUDED.malformed_record_count,
                    avg_batch_size = EXCLUDED.avg_batch_size,
                    runtime_seconds = EXCLUDED.runtime_seconds;
                """,
                params,
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO etl_run_metadata (
                    run_id, pipeline_name, input_dataset, configured_batch_size,
                    total_batches, total_records_processed, valid_records_count,
                    malformed_record_count, avg_batch_size, runtime_seconds, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                params,
            )
        return avg_batch_size

    def save_batch_metadata(
        self,
        run_id: str,
        pipeline_name: str,
        batch_id: int,
        batch_size: int,
        valid_records: int,
        malformed_records: int,
        batch_runtime_seconds: float,
        executed_at: Optional[str] = None,
    ) -> None:
        """Inserts per-batch metadata (`batch_id` begins from 1 and increases sequentially)."""
        ts = executed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
        cur = self.conn.cursor()
        params = (
            run_id,
            pipeline_name.lower(),
            batch_id,
            batch_size,
            valid_records,
            malformed_records,
            round(batch_runtime_seconds, 4),
            ts,
        )
        if self.backend == "postgresql":
            cur.execute(
                """
                INSERT INTO etl_batch_metadata (
                    run_id, pipeline_name, batch_id, batch_size,
                    valid_records, malformed_records, batch_runtime_seconds, executed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, batch_id) DO NOTHING;
                """,
                params,
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO etl_batch_metadata (
                    run_id, pipeline_name, batch_id, batch_size,
                    valid_records, malformed_records, batch_runtime_seconds, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                params,
            )

    def save_q1_results(
        self,
        run_id: str,
        pipeline_name: str,
        batch_id: int,
        rows: Iterable[Tuple[str, int, int, int]],
        executed_at: Optional[str] = None,
    ) -> int:
        """
        Loads Query 1 (Daily Traffic Summary) aggregated rows into `q1_daily_traffic_summary`.
        Each tuple in `rows`: `(log_date, status_code, request_count, total_bytes)`.
        """
        ts = executed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
        p_name = pipeline_name.lower()
        payload = [
            (run_id, p_name, batch_id, ts, str(r[0]), int(r[1]), int(r[2]), int(r[3]))
            for r in rows
        ]
        if not payload:
            return 0

        cur = self.conn.cursor()
        if self.backend == "postgresql":
            execute_values(
                cur,
                """
                INSERT INTO q1_daily_traffic_summary (
                    run_id, pipeline_name, batch_id, executed_at,
                    log_date, status_code, request_count, total_bytes
                ) VALUES %s
                ON CONFLICT (run_id, batch_id, log_date, status_code) DO NOTHING;
                """,
                payload,
            )
        else:
            cur.executemany(
                """
                INSERT OR REPLACE INTO q1_daily_traffic_summary (
                    run_id, pipeline_name, batch_id, executed_at,
                    log_date, status_code, request_count, total_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                payload,
            )
        return len(payload)

    def save_q2_results(
        self,
        run_id: str,
        pipeline_name: str,
        batch_id: int,
        rows: Iterable[Tuple[str, int, int, int]],
        executed_at: Optional[str] = None,
    ) -> int:
        """
        Loads Query 2 (Top 20 Requested Resources) aggregated rows into `q2_top_requested_resources`.
        Each tuple in `rows`: `(resource_path, request_count, total_bytes, distinct_host_count)`.
        """
        ts = executed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
        p_name = pipeline_name.lower()
        payload = [
            (run_id, p_name, batch_id, ts, str(r[0]), int(r[1]), int(r[2]), int(r[3]))
            for r in rows
        ]
        if not payload:
            return 0

        cur = self.conn.cursor()
        if self.backend == "postgresql":
            execute_values(
                cur,
                """
                INSERT INTO q2_top_requested_resources (
                    run_id, pipeline_name, batch_id, executed_at,
                    resource_path, request_count, total_bytes, distinct_host_count
                ) VALUES %s
                ON CONFLICT (run_id, batch_id, resource_path) DO NOTHING;
                """,
                payload,
            )
        else:
            cur.executemany(
                """
                INSERT OR REPLACE INTO q2_top_requested_resources (
                    run_id, pipeline_name, batch_id, executed_at,
                    resource_path, request_count, total_bytes, distinct_host_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                payload,
            )
        return len(payload)

    def save_q3_results(
        self,
        run_id: str,
        pipeline_name: str,
        batch_id: int,
        rows: Iterable[Tuple[str, int, int, int, float, int]],
        executed_at: Optional[str] = None,
    ) -> int:
        """
        Loads Query 3 (Hourly Error Analysis) aggregated rows into `q3_hourly_error_analysis`.
        Each tuple in `rows`: `(log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)`.
        """
        ts = executed_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
        p_name = pipeline_name.lower()
        payload = [
            (
                run_id,
                p_name,
                batch_id,
                ts,
                str(r[0]),
                int(r[1]),
                int(r[2]),
                int(r[3]),
                round(float(r[4]), 6),
                int(r[5]),
            )
            for r in rows
        ]
        if not payload:
            return 0

        cur = self.conn.cursor()
        if self.backend == "postgresql":
            execute_values(
                cur,
                """
                INSERT INTO q3_hourly_error_analysis (
                    run_id, pipeline_name, batch_id, executed_at,
                    log_date, log_hour, error_request_count, total_request_count,
                    error_rate, distinct_error_hosts
                ) VALUES %s
                ON CONFLICT (run_id, batch_id, log_date, log_hour) DO NOTHING;
                """,
                payload,
            )
        else:
            cur.executemany(
                """
                INSERT OR REPLACE INTO q3_hourly_error_analysis (
                    run_id, pipeline_name, batch_id, executed_at,
                    log_date, log_hour, error_request_count, total_request_count,
                    error_rate, distinct_error_hosts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                payload,
            )
        return len(payload)

    def commit(self) -> None:
        """Commits all stored results and metadata before stopping timer `t_end`."""
        self.conn.commit()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def seed_demo_runs(loader: RelationalResultLoader) -> List[str]:
    """
    Seeds realistic Phase-1 prototype runs (MongoDB & MapReduce) over NASA Jul95 log data
    so Member 4 can demonstrate the schema and `reporter.py` output immediately.
    """
    loader.init_schema()
    now_iso = "2026-11-19T18:30:00+05:30"

    q1_sample = [
        ("1995-07-01", 200, 58291, 1210482910),
        ("1995-07-01", 302, 1420, 381204),
        ("1995-07-01", 304, 4512, 0),
        ("1995-07-01", 404, 389, 0),
        ("1995-07-01", 500, 14, 2910),
        ("1995-07-02", 200, 54108, 1142910442),
        ("1995-07-02", 304, 4190, 0),
        ("1995-07-02", 404, 342, 0),
    ]

    q2_sample = [
        ("/images/NASA-logosmall.gif", 111366, 87533676, 28419),
        ("/images/KSC-logosmall.gif", 89638, 108282704, 25104),
        ("/images/MOSAIC-logosmall.gif", 60467, 22433257, 19842),
        ("/images/USA-logosmall.gif", 60013, 14043042, 19710),
        ("/images/WORLD-logosmall.gif", 59488, 38488736, 19580),
        ("/images/ksclogo-medium.gif", 58801, 344573860, 21409),
        ("/ksc.html", 40281, 284706108, 22918),
        ("/shuttle/countdown/", 40278, 188299650, 18912),
        ("/shuttle/missions/missions.html", 24862, 215876746, 14320),
        ("/htbin/cdt_main.pl", 22631, 84006272, 11890),
    ]

    q3_sample = [
        ("1995-07-01", 0, 28, 3568, round(28 / 3568, 6), 19),
        ("1995-07-01", 1, 21, 2984, round(21 / 2984, 6), 14),
        ("1995-07-01", 2, 15, 2240, round(15 / 2240, 6), 11),
        ("1995-07-01", 3, 9, 1795, round(9 / 1795, 6), 8),
        ("1995-07-01", 4, 0, 1410, 0.000000, 0),
        ("1995-07-01", 5, 12, 1580, round(12 / 1580, 6), 10),
    ]

    runs = [
        ("run_20261119_mongodb_01", "mongodb", 14.8230, [3.81, 3.74, 3.92, 3.353]),
        ("run_20261119_mapreduce_01", "mapreduce", 22.4190, [5.82, 5.71, 5.94, 4.949]),
    ]
    batch_sizes = [
        (1, 500000, 499680, 320),
        (2, 500000, 499650, 350),
        (3, 500000, 499670, 330),
        (4, 391715, 391451, 264),  # Final partial batch (< 500,000, still counted as valid batch #4!)
    ]

    seeded_ids = []
    for run_id, pipeline, runtime_sec, b_times in runs:
        loader.save_run_metadata(
            run_id=run_id,
            pipeline_name=pipeline,
            input_dataset="NASA_access_log_Jul95",
            configured_batch_size=500000,
            total_batches=4,
            total_records_processed=1891715,
            valid_records_count=1890451,
            malformed_record_count=1264,
            runtime_seconds=runtime_sec,
            executed_at=now_iso,
        )
        for (b_id, b_sz, b_val, b_mal), b_rt in zip(batch_sizes, b_times):
            loader.save_batch_metadata(
                run_id=run_id,
                pipeline_name=pipeline,
                batch_id=b_id,
                batch_size=b_sz,
                valid_records=b_val,
                malformed_records=b_mal,
                batch_runtime_seconds=b_rt,
                executed_at=now_iso,
            )
            # Per-batch sample slice (batch_id = 1..4)
            loader.save_q1_results(run_id, pipeline, b_id, q1_sample[:4], executed_at=now_iso)
            loader.save_q2_results(run_id, pipeline, b_id, q2_sample[:5], executed_at=now_iso)
            loader.save_q3_results(run_id, pipeline, b_id, q3_sample[:3], executed_at=now_iso)

        # Full-run aggregated results (batch_id = 0 represents full-run summary across all batches)
        loader.save_q1_results(run_id, pipeline, 0, q1_sample, executed_at=now_iso)
        loader.save_q2_results(run_id, pipeline, 0, q2_sample, executed_at=now_iso)
        loader.save_q3_results(run_id, pipeline, 0, q3_sample, executed_at=now_iso)
        seeded_ids.append(run_id)

    loader.commit()
    return seeded_ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize relational schema and optionally seed Phase-1 demo runs.")
    parser.add_argument("--seed-demo", action="store_true", help="Seed sample MongoDB and MapReduce runs for testing reporter.py")
    parser.add_argument("--pg-dsn", type=str, default=None, help="PostgreSQL connection DSN")
    args = parser.parse_args()

    loader = RelationalResultLoader(pg_dsn=args.pg_dsn)
    loader.init_schema()
    print(f"[OK] Initialized schema using backend: {loader.backend}")
    if args.seed_demo:
        ids = seed_demo_runs(loader)
        print(f"[OK] Seeded Phase-1 demo runs: {', '.join(ids)}")
    loader.close()
