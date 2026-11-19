#!/usr/bin/env python3
"""
DAS 839 – NoSQL Systems (End Semester Project)
Member 4 Deliverable: Decoupled Reporting Module (`reporter.py`)

Specification Requirements (Page 2 & Page 3):
- Reads stored results from PostgreSQL AFTER the ETL run has finished
  (report rendering time is strictly EXCLUDED from the ETL runtime).
- Displays execution metadata of the run:
    * Selected pipeline (`mongodb`, `mapreduce`, `pig`, `hive`)
    * Run ID & Execution Timestamp
    * Query name
    * Runtime (seconds)
    * Batch ID (`1..k`) & Batch Size (`records processed per batch`)
    * Average Batch Size (`total_records_processed / number_of_non_empty_batches`)
    * Malformed-record count
- Displays the formatted results of:
    * Query 1: Daily Traffic Summary (`log_date, status_code, request_count, total_bytes`)
    * Query 2: Top Requested Resources (`resource_path, request_count, total_bytes, distinct_host_count`)
    * Query 3: Hourly Error Analysis (`log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts`)
"""

import argparse
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

from db.postgres_loader import RelationalResultLoader


def _fetchall_dicts(loader: RelationalResultLoader, sql_pg: str, sql_sqlite: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    cur = loader.conn.cursor()
    query = sql_pg if loader.backend == "postgresql" else sql_sqlite
    cur.execute(query, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _format_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    str_rows = [[str(cell) for cell in r] for r in rows]
    widths = [len(h) for h in headers]
    for r in str_rows:
        for idx, cell in enumerate(r):
            widths[idx] = max(widths[idx], len(cell))

    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    head_line = "| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)) + " |"
    lines = [sep, head_line, sep]
    for r in str_rows:
        lines.append("| " + " | ".join(r[i].ljust(widths[i]) for i in range(len(headers))) + " |")
    lines.append(sep)
    return "\n".join(lines)


def list_runs(loader: RelationalResultLoader) -> List[Dict[str, Any]]:
    sql = """
        SELECT run_id, pipeline_name, input_dataset, configured_batch_size,
               total_batches, total_records_processed, valid_records_count,
               malformed_record_count, avg_batch_size, runtime_seconds, executed_at
        FROM etl_run_metadata
        ORDER BY executed_at DESC, run_id DESC;
    """
    return _fetchall_dicts(loader, sql, sql)


def display_run_report(
    loader: RelationalResultLoader,
    run_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    batch_id: int = 0,
    limit_rows: int = 20,
) -> None:
    runs = list_runs(loader)
    if not runs:
        print("No ETL runs found in the database. Run `python3 -m db.postgres_loader --seed-demo` or an ETL pipeline first.")
        return

    selected_run = None
    if run_id:
        for r in runs:
            if r["run_id"] == run_id:
                selected_run = r
                break
        if not selected_run:
            print(f"[ERROR] Run ID '{run_id}' not found in etl_run_metadata.")
            return
    elif pipeline_name:
        for r in runs:
            if r["pipeline_name"].lower() == pipeline_name.lower():
                selected_run = r
                break
        if not selected_run:
            print(f"[ERROR] No runs found for pipeline '{pipeline_name}'.")
            return
    else:
        selected_run = runs[0]

    target_run_id = selected_run["run_id"]
    p_name = selected_run["pipeline_name"].upper()

    # Fetch per-batch metadata rows (batch_id = 1..k)
    batches = _fetchall_dicts(
        loader,
        """
        SELECT batch_id, batch_size, valid_records, malformed_records, batch_runtime_seconds, executed_at
        FROM etl_batch_metadata
        WHERE run_id = %s
        ORDER BY batch_id ASC;
        """,
        """
        SELECT batch_id, batch_size, valid_records, malformed_records, batch_runtime_seconds, executed_at
        FROM etl_batch_metadata
        WHERE run_id = ?
        ORDER BY batch_id ASC;
        """,
        (target_run_id,),
    )

    batch_scope_str = (
        f"ALL BATCHES (1..{selected_run['total_batches']})"
        if batch_id == 0
        else f"Batch #{batch_id}"
    )

    print("=" * 106)
    print(f" MULTI-PIPELINE WEB SERVER LOG ANALYTICS — EXECUTION & QUERY REPORT")
    print("=" * 106)
    print(f" Selected Pipeline       : {p_name}")
    print(f" Run Identifier (run_id) : {target_run_id}")
    print(f" Source Dataset          : {selected_run['input_dataset']}")
    print(f" Execution Timestamp     : {selected_run['executed_at']}")
    print(f" Total ETL Runtime       : {float(selected_run['runtime_seconds']):.4f} seconds  (t_start -> PostgreSQL COMMIT)")
    print("-" * 106)
    print(f" Configured Batch Size   : {int(selected_run['configured_batch_size']):,} records")
    print(f" Non-Empty Batches (k)   : {int(selected_run['total_batches'])} batches (Batch IDs 1..{selected_run['total_batches']})")
    print(f" Total Records Processed : {int(selected_run['total_records_processed']):,} records")
    print(f" Valid Parsed Records    : {int(selected_run['valid_records_count']):,} records")
    print(f" Malformed Record Count  : {int(selected_run['malformed_record_count']):,} records  (Tracked & Reported)")
    print(f" Average Batch Size      : {float(selected_run['avg_batch_size']):,.2f} records/batch  (= {int(selected_run['total_records_processed']):,} / {int(selected_run['total_batches'])})")
    print("=" * 106)

    if batches:
        print("\n[Per-Batch Execution Breakdown (Batch IDs 1..k)]")
        b_headers = ["batch_id", "batch_size", "valid_records", "malformed_records", "batch_runtime_sec"]
        b_rows = [
            [
                b["batch_id"],
                f"{int(b['batch_size']):,}",
                f"{int(b['valid_records']):,}",
                f"{int(b['malformed_records']):,}",
                f"{float(b['batch_runtime_seconds']):.4f}",
            ]
            for b in batches
        ]
        print(_format_table(b_headers, b_rows))

    # Determine selected batch size display for query header
    selected_batch_size_display = (
        f"{int(selected_run['total_records_processed']):,} (across {selected_run['total_batches']} batches)"
        if batch_id == 0
        else next(
            (f"{int(b['batch_size']):,}" for b in batches if int(b["batch_id"]) == batch_id),
            str(selected_run["configured_batch_size"]),
        )
    )

    # -------------------------------------------------------------------------
    # QUERY 1: Daily Traffic Summary
    # Output columns: log_date, status_code, request_count, total_bytes
    # -------------------------------------------------------------------------
    q1_rows = _fetchall_dicts(
        loader,
        """
        SELECT log_date, status_code, request_count, total_bytes
        FROM q1_daily_traffic_summary
        WHERE run_id = %s AND batch_id = %s
        ORDER BY log_date ASC, status_code ASC
        LIMIT %s;
        """,
        """
        SELECT log_date, status_code, request_count, total_bytes
        FROM q1_daily_traffic_summary
        WHERE run_id = ? AND batch_id = ?
        ORDER BY log_date ASC, status_code ASC
        LIMIT ?;
        """,
        (target_run_id, batch_id, limit_rows),
    )
    print("\n" + "-" * 106)
    print(
        f" Query Name: Query 1 - Daily Traffic Summary  |  Pipeline: {p_name}  |  "
        f"Runtime: {float(selected_run['runtime_seconds']):.4f}s\n"
        f" Batch ID  : {batch_scope_str}  |  Batch Size: {selected_batch_size_display}  |  "
        f"Avg Batch Size: {float(selected_run['avg_batch_size']):,.2f}"
    )
    print("-" * 106)
    print(
        _format_table(
            ["log_date", "status_code", "request_count", "total_bytes"],
            [[r["log_date"], r["status_code"], r["request_count"], r["total_bytes"]] for r in q1_rows],
        )
    )

    # -------------------------------------------------------------------------
    # QUERY 2: Top Requested Resources (Top 20)
    # Output columns: resource_path, request_count, total_bytes, distinct_host_count
    # -------------------------------------------------------------------------
    q2_rows = _fetchall_dicts(
        loader,
        """
        SELECT resource_path, request_count, total_bytes, distinct_host_count
        FROM q2_top_requested_resources
        WHERE run_id = %s AND batch_id = %s
        ORDER BY request_count DESC, resource_path ASC
        LIMIT 20;
        """,
        """
        SELECT resource_path, request_count, total_bytes, distinct_host_count
        FROM q2_top_requested_resources
        WHERE run_id = ? AND batch_id = ?
        ORDER BY request_count DESC, resource_path ASC
        LIMIT 20;
        """,
        (target_run_id, batch_id),
    )
    print("\n" + "-" * 106)
    print(
        f" Query Name: Query 2 - Top Requested Resources (Top 20)  |  Pipeline: {p_name}  |  "
        f"Runtime: {float(selected_run['runtime_seconds']):.4f}s\n"
        f" Batch ID  : {batch_scope_str}  |  Batch Size: {selected_batch_size_display}  |  "
        f"Avg Batch Size: {float(selected_run['avg_batch_size']):,.2f}"
    )
    print("-" * 106)
    print(
        _format_table(
            ["resource_path", "request_count", "total_bytes", "distinct_host_count"],
            [
                [r["resource_path"], r["request_count"], r["total_bytes"], r["distinct_host_count"]]
                for r in q2_rows
            ],
        )
    )

    # -------------------------------------------------------------------------
    # QUERY 3: Hourly Error Analysis (Status Codes 400-599)
    # Output columns: log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
    # -------------------------------------------------------------------------
    q3_rows = _fetchall_dicts(
        loader,
        """
        SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
        FROM q3_hourly_error_analysis
        WHERE run_id = %s AND batch_id = %s
        ORDER BY log_date ASC, log_hour ASC
        LIMIT %s;
        """,
        """
        SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts
        FROM q3_hourly_error_analysis
        WHERE run_id = ? AND batch_id = ?
        ORDER BY log_date ASC, log_hour ASC
        LIMIT ?;
        """,
        (target_run_id, batch_id, limit_rows),
    )
    print("\n" + "-" * 106)
    print(
        f" Query Name: Query 3 - Hourly Error Analysis  |  Pipeline: {p_name}  |  "
        f"Runtime: {float(selected_run['runtime_seconds']):.4f}s\n"
        f" Batch ID  : {batch_scope_str}  |  Batch Size: {selected_batch_size_display}  |  "
        f"Avg Batch Size: {float(selected_run['avg_batch_size']):,.2f}"
    )
    print("-" * 106)
    print(
        _format_table(
            [
                "log_date",
                "log_hour",
                "error_request_count",
                "total_request_count",
                "error_rate",
                "distinct_error_hosts",
            ],
            [
                [
                    r["log_date"],
                    f"{int(r['log_hour']):02d}",
                    r["error_request_count"],
                    r["total_request_count"],
                    f"{float(r['error_rate']):.6f}",
                    r["distinct_error_hosts"],
                ]
                for r in q3_rows
            ],
        )
    )
    print("=" * 106)


def compare_two_runs(loader: RelationalResultLoader, run_a: str, run_b: str) -> bool:
    """
    Compares Q1, Q2, and Q3 results stored in PostgreSQL between two runs
    (e.g., MongoDB vs. MapReduce) to verify cross-pipeline semantic equivalence.
    """
    print("=" * 106)
    print(f" CROSS-PIPELINE EQUIVALENCE CHECK: '{run_a}' vs. '{run_b}'")
    print("=" * 106)
    checks = [
        ("Query 1 (q1_daily_traffic_summary)", "q1_daily_traffic_summary", "log_date, status_code, request_count, total_bytes"),
        ("Query 2 (q2_top_requested_resources)", "q2_top_requested_resources", "resource_path, request_count, total_bytes, distinct_host_count"),
        ("Query 3 (q3_hourly_error_analysis)", "q3_hourly_error_analysis", "log_date, log_hour, error_request_count, total_request_count, ROUND(error_rate, 6), distinct_error_hosts"),
    ]
    all_ok = True
    for label, table, cols in checks:
        ph = "%s" if loader.backend == "postgresql" else "?"
        sql_diff = f"""
            SELECT {cols} FROM {table} WHERE run_id = {ph} AND batch_id = 0
            EXCEPT
            SELECT {cols} FROM {table} WHERE run_id = {ph} AND batch_id = 0;
        """
        diff_ab = _fetchall_dicts(loader, sql_diff, sql_diff, (run_a, run_b))
        diff_ba = _fetchall_dicts(loader, sql_diff, sql_diff, (run_b, run_a))
        total_diffs = len(diff_ab) + len(diff_ba)
        status = "PASS (0 row differences)" if total_diffs == 0 else f"FAIL ({total_diffs} mismatched rows)"
        if total_diffs > 0:
            all_ok = False
        print(f" - {label.ljust(40)} : {status}")
    print("=" * 106)
    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Decoupled Reporting Module for Multi-Pipeline Web Server Log Analytics")
    parser.add_argument("--run-id", type=str, default=None, help="Specific run_id to display")
    parser.add_argument("--pipeline", type=str, default=None, choices=["mongodb", "mapreduce", "pig", "hive"], help="Display latest run for selected pipeline")
    parser.add_argument("--batch-id", type=int, default=0, help="Batch ID (1..k) to display, or 0 for full-run aggregation (default: 0)")
    parser.add_argument("--list-runs", action="store_true", help="List all recorded runs in etl_run_metadata")
    parser.add_argument("--compare", nargs=2, metavar=("RUN_ID_1", "RUN_ID_2"), help="Compare two runs in PostgreSQL for exact query output equivalence")
    parser.add_argument("--pg-dsn", type=str, default=None, help="PostgreSQL connection DSN")
    args = parser.parse_args()

    loader = RelationalResultLoader(pg_dsn=args.pg_dsn)
    loader.init_schema()

    if args.list_runs:
        runs = list_runs(loader)
        headers = ["run_id", "pipeline", "batches", "records", "malformed", "avg_batch_size", "runtime_s", "executed_at"]
        rows = [
            [
                r["run_id"],
                r["pipeline_name"],
                r["total_batches"],
                f"{int(r['total_records_processed']):,}",
                r["malformed_record_count"],
                f"{float(r['avg_batch_size']):,.2f}",
                f"{float(r['runtime_seconds']):.4f}",
                r["executed_at"],
            ]
            for r in runs
        ]
        print(_format_table(headers, rows))
    elif args.compare:
        ok = compare_two_runs(loader, args.compare[0], args.compare[1])
        loader.close()
        sys.exit(0 if ok else 1)
    else:
        display_run_report(loader, run_id=args.run_id, pipeline_name=args.pipeline, batch_id=args.batch_id)

    loader.close()


if __name__ == "__main__":
    main()
