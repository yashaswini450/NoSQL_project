"""
main.py — Unified CLI controller (Member 1).

Usage:
  python main.py --pipeline mongodb   --batch-size 100000 --input NASA_access_log_Jul95
  python main.py --pipeline mapreduce --batch-size 100000 --input NASA_access_log_Jul95

Flow:
  1. parse CLI args, pick the engine from the registry (Strategy pattern)
  2. setup engine + DB connection             <- NOT timed (setup)
  3. t_start = perf_counter()                 <- right before opening the log file
  4. for each batch 1..k: engine.process_batch(batch_id, raw_lines)
  5. engine.finalize() -> Q1, Q2, Q3 results
  6. loader writes results + batch/run metadata, COMMIT
  7. t_end = perf_counter()                   <- right after COMMIT returns
  8. runtime = t_end - t_start, then saved to etl_run_metadata (small UPDATE
     after the timer; it can't be inside the window it is measuring)
  reporter.py is run separately afterwards   <- NOT timed (spec)
"""

import argparse
import importlib
import sys
import time
import uuid
from datetime import datetime

from batcher import iter_batches, BatchStats

# Registry: CLI name -> "module:ClassName". Adding Pig/Hive in Phase 2 =
# adding one line here + one class. main.py itself doesn't change.
PIPELINES = {
    "mongodb":   "pipelines.mongodb_pipeline:MongoDBPipeline",
    "mapreduce": "pipelines.mapreduce_pipeline:MapReducePipeline",
    "pig":       "pipelines.pig_pipeline:PigPipeline",          # Phase 2
    "hive":      "pipelines.hive_pipeline:HivePipeline",        # Phase 2
}


def load_pipeline(name):
    module_name, class_name = PIPELINES[name].split(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        sys.exit(f"[error] pipeline '{name}' is not implemented yet ({e})")
    return getattr(module, class_name)()


def build_arg_parser():
    ap = argparse.ArgumentParser(description="Multi-pipeline ETL for NASA HTTP logs")
    ap.add_argument("--pipeline", required=True, choices=sorted(PIPELINES),
                    help="exactly one execution engine")
    ap.add_argument("--batch-size", type=int, required=True,
                    help="number of raw log lines per batch (same B for all engines)")
    ap.add_argument("--input", required=True, nargs="+",
                    help="raw (decompressed) NASA log file(s)")
    ap.add_argument("--dsn", default="dbname=nasa_etl user=postgres host=localhost",
                    help="PostgreSQL connection string")
    ap.add_argument("--dry-run", action="store_true",
                    help="skip PostgreSQL (testing only; runtime then excludes the load)")
    return ap


def run(args, pipeline=None, loader=None):
    if args.batch_size <= 0:
        sys.exit("[error] --batch-size must be > 0")

    pipeline = pipeline or load_pipeline(args.pipeline)
    run_id = uuid.uuid4().hex[:12]
    executed_at = datetime.now()

    if loader is None and not args.dry_run:
        from db.postgres_loader import PostgresLoader      # Member 4
        loader = PostgresLoader(args.dsn)

    # ---- setup: outside the timed window ---------------------------------
    pipeline.setup(run_id)

    stats = BatchStats()
    batch_meta = []          # one row per batch -> etl_batch_metadata
    malformed_total = 0

    # ===== TIMED WINDOW STARTS: just before the input file is opened =======
    t_start = time.perf_counter()

    # Files are chained: Jul95 + Aug95 = one run, batch ids 1..k
    for batch in iter_batches(args.input, args.batch_size):
        bad = pipeline.process_batch(batch.batch_id, batch.lines)
        stats.record(batch)
        malformed_total += bad
        batch_meta.append({"batch_id": batch.batch_id,
                           "batch_size": batch.size,
                           "malformed_count": bad})

    results = pipeline.finalize()

    if loader is not None:
        loader.write_run(                                   # one transaction
            run_id=run_id,
            pipeline_name=args.pipeline,
            executed_at=executed_at,
            batch_size=args.batch_size,
            avg_batch_size=stats.average_batch_size,
            num_batches=stats.num_batches,
            total_records=stats.total_records,
            malformed_record_count=malformed_total,
            batch_meta=batch_meta,
            results=results,
        )
        loader.commit()

    t_end = time.perf_counter()
    # ===== TIMED WINDOW ENDS: right after COMMIT ===========================

    runtime = t_end - t_start
    if loader is not None:
        loader.save_runtime(run_id, runtime)               # tiny UPDATE, untimed
        loader.commit()
    pipeline.teardown()

    print(f"run_id={run_id} pipeline={args.pipeline} batches={stats.num_batches} "
          f"records={stats.total_records} malformed={malformed_total} "
          f"avg_batch_size={stats.average_batch_size:.2f} runtime={runtime:.3f}s")
    print(f"Now run:  python reporter.py --run-id {run_id}")
    return run_id, runtime, stats, malformed_total, results


if __name__ == "__main__":
    run(build_arg_parser().parse_args())
