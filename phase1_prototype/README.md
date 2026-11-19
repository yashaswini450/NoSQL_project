# NASA web log analytics — Phase 1

This prototype implements **MongoDB and genuine Hadoop MapReduce**, with PostgreSQL result storage and a separate SQL reporting script. The controller offers Pig, MapReduce, MongoDB and Hive; Pig and Hive explicitly report that they are planned for Phase 2.

The official July and August 1995 NASA logs are downloaded directly from the Internet Traffic Archive. Downloading and decompression happen before timing. No cleaned CSV/JSON input is used.

## Open the demo on this computer

For the WSL presentation menu, double-click `Open-WSL-Demo.cmd` or follow [WSL_DEMO.md](WSL_DEMO.md). In Ubuntu, run `bash demo.sh` from this directory. `bash demo.sh tour` gives a noninteractive rehearsal using the completed results stored in PostgreSQL.

Open PowerShell in this folder. The runtime and official files have already been installed in Ubuntu-24.04 through WSL.

You can also double-click `Open-Report.cmd` to see the saved SQL report or `Run-MapReduce.cmd` to start a new full MapReduce run. Keep the terminal open until it finishes. The default report selects the latest successful official-data run, so integration fixtures do not replace your presentation report.

```powershell
# Start the local databases and show the most recent successful SQL report.
.\Start-Demo.ps1 -Action report

# Execute a new complete run using exactly one backend.
.\Start-Demo.ps1 -Action mapreduce -BatchSize 500000
.\Start-Demo.ps1 -Action mongodb -BatchSize 500000

# Show a particular saved run; IDs are in sample_outputs/official_run_ids.json.
.\Start-Demo.ps1 -Action report -RunId 'paste-run-id-here'
```

If PowerShell blocks local scripts, use `powershell -ExecutionPolicy Bypass -File .\Start-Demo.ps1 -Action report`. This applies to that process only.

The complete runs take time. Start one before presenting, explain the jobs while it runs, and use the existing SQL report to demonstrate reporting. The saved `.json` files contain every query row; text reports show all top-20 rows and the first ten rows of the longer queries. A saved report is evidence of a completed run, not a substitute for demonstrating execution.

The original black-and-white Member 3 board is included in `presentation/`.

## Install again, if needed

Requires Windows with **WSL Ubuntu-24.04**, internet access and several GB of free space. PostgreSQL 16 is installed with apt if absent. First-time apt installation may ask for the Ubuntu sudo password. MongoDB Community 8.0.38, Temurin JRE 11.0.33 and Apache Hadoop 3.3.6 client/Streaming jars are downloaded and verified by the setup script.

```powershell
.\Start-Demo.ps1 -Action setup
.\Start-Demo.ps1 -Action download
```

Runtime state is isolated in `~/.local/share/nasa-phase1` **inside Ubuntu**, including databases, the venv, downloaded raw files and per-run Hadoop job logs. Source code stays in this folder. The ZIP contains source and reports; dependencies and large NASA files are fetched by setup/download, not bundled.

The setup uses Hadoop's actual `LocalJobRunner`, shuffle/sort and Python Streaming tasks. It does not start HDFS or YARN. These are single-machine prototype results, not a cluster scalability benchmark. MongoDB and PostgreSQL bind to loopback at ports 27018 and 55432. The included PostgreSQL credentials are for this private local demo; do not expose these services to a network.

Recorded timings are sequential observations on this computer, with shared filesystem caches and normal background activity. Use repeated controlled trials before drawing performance conclusions.

## Commands inside Ubuntu

From this project directory:

```bash
bash setup.sh
bash run.sh main.py download
bash run.sh main.py                  # interactive selection menu
bash run.sh main.py run --pipeline mapreduce --batch-size 500000
bash run.sh main.py run --pipeline mongodb --batch-size 500000
bash run.sh reporter.py --run-id UUID --rows 0
bash run.sh reporter.py --run-id UUID --scope BATCH --batch-id 1
bash run.sh reporter.py --run-id UUID --json all_results.json
bash run.sh verify_equivalence.py --left MONGO_UUID --right MR_UUID
bash run.sh -m unittest discover -s tests -v
bash run.sh tests/integration.py
bash run.sh benchmark.py             # both full official inputs, both backends sequentially
bash services.sh stop
```

For a shorter live run, select **one complete official raw file** using `--input "$HOME/.local/share/nasa-phase1/data/NASA_access_log_Aug95"`. Use the same file(s), order and batch size when comparing engines. Do not create a trimmed or cleaned demo input. Synthetic inputs are used only by the explicitly labelled integration tests.

Environment overrides: `NASA_STATE_DIR`, `NASA_MONGO_URI`, `NASA_SQL_DSN`, `JAVA_HOME`, `HADOOP_HOME`. The defaults work on this computer. `services.sh` manages only the default private demo services; use external service management if overriding database connections.

## What happens in a run

```text
CLI chooses one backend + batch size
  -> controller assigns run ID and starts timer
  -> raw-line batching, without field extraction
  -> selected backend parses and validates the raw lines
  -> native Q1/Q2/Q3 for each batch and for the complete run
  -> common loader inserts finished aggregates into PostgreSQL
  -> final SQL commit, stop timer
  -> separate reporter reads SQL
```

The Python controller performs transport, orchestration, reconciliation checks and SQL insertion. It does not compute production query answers. Any typed JSON created by Hadoop is internal to its own native jobs, after its mapper parses raw input. MongoDB receives raw strings and performs extraction and queries inside aggregation pipelines.

**MongoDB:** native `$regexFind`, `$convert`, `$dateFromString`, `$group`, `$sort`, `$limit`, `$merge`. Exact hosts use two grouping levels, avoiding unbounded host arrays. `nasa/mongo_engine.py` contains the implementation.

**MapReduce:** three real Hadoop Streaming jobs per nonempty batch:

1. A map-only job parses raw lines into valid or malformed outcomes.
2. A grouping job counts `(date,status)`, `(resource,host)` and `(date,hour,host)`.
3. A reduction job finishes resource/hour groups and selects the global top 20, with one final reducer.

Two additional jobs calculate complete-run answers from **all native parsed outcomes**. They do not add batch-local distinct counts or merge only batch-local top-20 lists. Source is `nasa/mr_tasks.py` and `nasa/mapreduce_engine.py`. Each invocation, job log, counters and `_SUCCESS` marker is retained under the run's staging directory in Ubuntu.

## Shared parsing and query rules

Parser contract: `nasa-common-v1`. These choices clarify the assignment's unspecified edge cases and are implemented identically in both engines.

- Extract `host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred`.
- Validate the timestamp's calendar, clock, English month and numeric UTC offset. Preserve its original text and derive **recorded local date/hour**. A missing protocol becomes SQL/JSON null.
- Preserve host case and the complete request target, including case, escapes, query strings and internal spaces. No DNS lookup, URL decoding, lowercasing or deduplication.
- Missing bytes and `-` become 0. Other byte tokens must be nonnegative decimal integers fitting signed 64-bit storage.
- Invalid envelopes, timestamps, requests or bytes are counted once as malformed. They count toward batch size and are excluded from all three queries. Reason totals and up to five examples per batch are stored in SQL.
- **Q1:** group date/status, count requests, sum bytes.
- **Q2:** global top 20 by request count descending, then resource path ascending using ASCII/binary order; include bytes and exact distinct hosts.
- **Q3:** group local date/hour; errors are statuses 400–599; rate is errors/all valid requests in that hour; include exact distinct error hosts. Keep hours with traffic and zero errors.
- No record deduplication. Repeated input lines remain repeated requests.

## Batches, SQL and timing

`B` is **input lines**, including malformed/blank records. Batch IDs begin at 1, continue across file boundaries, and include a final partial batch. The average is total input / nonempty batches. Empty input gives zero batches and average 0 in tests. A file without a trailing newline still contributes its last record; raw transport adds only the separator needed to preserve that record boundary.

`schema.sql` defines five typed PostgreSQL tables:

| Table | Contents |
|---|---|
| `runs` | Pipeline, status, parser version, source SHA-256 hashes, engine versions, runtime and record/batch totals |
| `batches` | Positive batch ID, actual size, valid/malformed counts, reasons and examples |
| `q1_daily_traffic` | Q1 columns plus run/pipeline/scope/batch/time metadata |
| `q2_top_resources` | Q2 columns and rank plus the same metadata |
| `q3_hourly_errors` | Q3 columns plus the same metadata |

`scope=BATCH` identifies one batch's answers. `scope=FINAL` identifies complete-run answers covering batches 1–K and uses the last contributing batch ID K. There is no invented batch 0. Final distinct counts are recomputed across all records.

Runtime includes raw reads/framing, native staging, parsing, query jobs, result transport and every aggregate SQL commit. The stopwatch starts immediately before timed engine staging and the first input read, and stops **after the final result commit succeeds**. It excludes installation, download/decompression, service startup, connection checks, schema setup and separate report rendering. A small following metadata transaction records the measured duration and marks the run successful. Failed runs remain marked `FAILED`; their partial results are not presented as a successful report.

The per-batch `elapsed_seconds` covers the native batch work and preparation for its SQL insert. It is diagnostic; the separately measured complete-run runtime is the required comparison metric.

## Correctness evidence

`tests/test_rules.py` checks parser boundaries and raw batching. `tests/integration.py` runs **real MongoDB, Hadoop and PostgreSQL** on labelled synthetic fixtures, compares their parsed decisions and query outputs, checks an independent fixture oracle, verifies batch-size invariance, repeated hosts across batches, zero-error hours, an empty run and a global winner outside both local top-20 lists.

`verify_equivalence.py` reads only SQL. It compares source hashes/order, parser version, input/valid/malformed totals, each batch's counts/reasons and every Q1/Q2/Q3 row. Keys, integer counts and bytes must match exactly; error rates allow absolute difference at most `1e-9`. IDs, timestamps and runtimes differ by design.

Complete official-data evidence is saved in `sample_outputs/`. Reports include actual engine versions, hashes, counts, runtimes, full query rows and run IDs. These are observed results from this machine.

`sample_outputs/restore_official_reports.sql` restores the two completed official runs into a fresh reporting database. After setup, import it **once into an empty database** inside Ubuntu:

```bash
PGPASSWORD=nasa_local_demo /usr/lib/postgresql/16/bin/psql -h 127.0.0.1 -p 55432 -U nasa -d nasa_reporting -v ON_ERROR_STOP=1 -f sample_outputs/restore_official_reports.sql
```

This restores historical results with their original IDs and timestamps. It does not execute a new ETL run or claim a new runtime. Do not import it into this computer's existing populated demo database; those runs are already present. Re-importing duplicate IDs fails and rolls back the transaction.

## Your presentation part (Member 3)

Use `MEMBER3_DEMO.md` to explain the MapReduce stages and show execution, SQL reporting and equivalence. The existing black-and-white board remains a useful architecture drawing. The new prototype supplies the working demonstration that a board alone cannot provide.

Phase 1 requires the proposed architecture and a working prototype with one or two pipelines. This package implements two. Pig, Hive, a final four-way comparison, the compact final report and the team video belong to the remaining Phase 2 work. Follow any separate upload instructions from your instructor.

Sources: the supplied project statement (pages 1–3), [official NASA dataset](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html), [Hadoop Streaming documentation](https://hadoop.apache.org/docs/r3.3.6/hadoop-streaming/HadoopStreaming.html), [MongoDB aggregation documentation](https://www.mongodb.com/docs/manual/aggregation/).
