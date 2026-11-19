# DAS 839 – NoSQL Systems: Phase-1 Team Work Division (4 Members)

**Project:** Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics  
**Phase-1 Deliverables Required by Specification (Page 3):**
1. **System Architecture & Design Discussion (using the 6 Handwritten Discussion Boards):**
   - Overall design of the coherent multi-pipeline tool
   - NASA HTTP log parsing strategy (9 mandatory fields + edge cases)
   - End-to-end ETL workflow
   - Sequential batching approach & strict runtime measurement window
   - PostgreSQL relational reporting database schema & decoupled reporting module
   - Plan for ensuring semantic equivalence across all 4 pipelines (`MongoDB`, `MapReduce`, `Pig`, `Hive`)
2. **Working Prototype Demonstration:**
   - End-to-end prototype with **2 execution pipelines implemented (`MongoDB` & `MapReduce`)**, PostgreSQL loading, and standalone reporting script.

---

## 1. Quick Overview: 4-Member Ownership Matrix

| Team Member | Role Title | Discussion Boards to Present (Viva) | Prototype Coding Deliverables |
| :--- | :--- | :--- | :--- |
| **Member 1** | **Core Orchestration, Parsing & Batching Lead** | **Board 2** (Parsing Strategy & Edge Cases) & **Board 3** (Batching Approach & Runtime Boundary) | `main.py` (CLI Controller), `batcher.py` (Sequential Batch Slicer `1..k`), `parser.py` (2-stage Regex & Malformed Counter) |
| **Member 2** | **MongoDB Pipeline Lead (Prototype Pipeline #1)** | **Board 1** (Overall System Architecture & Pluggable Engine Design) + **Board 4 (Left Half: MongoDB ETL)** | `pipelines/mongodb_pipeline.py` (BSON batch ingestion + Native Aggregation Pipelines for Q1, Q2, Q3) |
| **Member 3** | **MapReduce Pipeline Lead (Prototype Pipeline #2)** | **Board 4 (Right Half: MapReduce ETL)** & **Board 6** (Cross-Pipeline Equivalence Plan + Pig/Hive Phase-2 Roadmap) | `pipelines/mapreduce_pipeline.py` (`mapper.py` & `reducer.py` for Q1, Q2, Q3 + `verify_equivalence.py` test script) |
| **Member 4** | **PostgreSQL Sink & Reporting Module Lead** | **Board 5** (PostgreSQL Relational Schema & Decoupled Reporting Module) + **Live Demo Driver** | `db/schema.sql`, `db/postgres_loader.py` (`psycopg2` result & metadata writer), `reporter.py` (Decoupled CLI Report Renderer) |

---

## 2. Detailed Responsibilities by Team Member

### 👤 Member 1: Core Orchestration, Parsing & Batching Lead

#### A. Prototype Implementation Tasks
1. **Unified CLI & Runtime Timer Hook (`main.py`):**
   - Build the unified entry point accepting `--pipeline {mongodb, mapreduce, pig, hive}`, `--batch-size B`, and `--input <log_path>`.
   - Start high-resolution timer `t_start = time.perf_counter()` immediately when opening the raw NASA log file.
   - Stop timer `t_end = time.perf_counter()` immediately after the final PostgreSQL `COMMIT` (`runtime = t_end - t_start`).
2. **Sequential Batch Slicer (`batcher.py`):**
   - Read raw ASCII log lines sequentially in batches of size $B$, assigning `batch_id = 1, 2, ..., k`.
   - Ensure the final partial batch ($< B$ records) is counted as a valid non-empty batch.
   - Compute exact average batch size:
     $$\text{average\_batch\_size} = \frac{\text{total\_records\_processed}}{\text{number\_of\_non\_empty\_batches}}$$
3. **Canonical 2-Stage NASA Log Parser (`parser.py`):**
   - Extract the 9 mandatory fields: `host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, and `bytes_transferred`.
   - Convert missing bytes (`'-'`) to `0`.
   - Handle 2-token HTTP/0.9 requests (where `protocol_version` is omitted) and extra internal spaces in URLs.
   - Route unparseable lines to `malformed_record_count` without silently dropping them.

#### B. Phase-1 Discussion & Board Presentation ([board_2.png](./phase1_boards/board_2.png) & [board_3.png](./phase1_boards/board_3.png))
- Walk through the anatomy of a raw NASA log line and explain why naive `.split(' ')` fails on real NASA traces.
- Explain the 2-stage regex + request tokenizer and how `'-'` bytes vs. truly malformed records are distinguished.
- Present the batch slicing mechanics (`batch_id = 1..k`), worked `average_batch_size` calculation, and what is strictly included vs. excluded in the runtime window.

---

### 👤 Member 2: MongoDB Pipeline Lead (Prototype Pipeline #1)

#### A. Prototype Implementation Tasks
1. **MongoDB Batch Ingestion (`pipelines/mongodb_pipeline.py`):**
   - Parse raw batch records into structured BSON documents tagged with `(run_id, batch_id)` and bulk-insert into `db.nasa_logs_staging` via `insert_many()`.
2. **Native MongoDB Aggregation Pipelines for Mandatory Queries:**
   - **Query 1 (Daily Traffic Summary):** `$match` $\rightarrow$ `$group` by `(log_date, status_code)` computing `$sum: 1` (`request_count`) and `$sum: "$bytes_transferred"` (`total_bytes`) $\rightarrow$ `$sort` by `(log_date, status_code)`.
   - **Query 2 (Top Requested Resources):** `$match` $\rightarrow$ `$group` by `resource_path` computing `request_count`, `total_bytes`, and `$addToSet: "$host"` $\rightarrow$ `$project` with `distinct_host_count: {$size: "$hosts_set"}` $\rightarrow$ `$sort: {request_count: -1, resource_path: 1}` $\rightarrow$ `$limit: 20`.
   - **Query 3 (Hourly Error Analysis):** `$match` $\rightarrow$ `$group` by `(log_date, log_hour)` computing `total_request_count`, conditional `error_request_count` for `status_code` in `[400, 599]`, conditional `$addToSet` for `distinct_error_hosts`, and `$round` for `error_rate` to 6 decimal places.

#### B. Phase-1 Discussion & Board Presentation ([board_1.png](./phase1_boards/board_1.png) & Left Half of [board_4.png](./phase1_boards/board_4.png))
- Present the overall end-to-end system architecture and explain how the Strategy Pattern (`BaseETLPipeline`) makes all 4 execution backends pluggable under one coherent tool.
- Walk through the MongoDB document schema and explain how `$addToSet` + `$size` and conditional `$group` expressions solve Q1, Q2, and Q3 natively inside MongoDB.

---

### 👤 Member 3: MapReduce Pipeline & Cross-Engine Equivalence Lead (Prototype Pipeline #2)

#### A. Prototype Implementation Tasks
1. **MapReduce Engine (`pipelines/mapreduce_pipeline.py`, `mapper.py`, `reducer.py`):**
   - **Mapper (`mapper.py`):** Read raw log lines from `stdin`, apply the exact 2-stage regex rules, emit `MALFORMED\t1` on parse failure, and emit query-tagged key-value records (`Q1`, `Q2`, `Q3`) for valid lines.
   - **Reducer (`reducer.py`):**
     - **Q1:** Sum `(request_count, total_bytes)` per `(log_date, status_code)`.
     - **Q2:** Accumulate `request_count`, `total_bytes`, and `len(set(hosts))` per `resource_path`, then select the global Top 20 ordered by `(-request_count, resource_path)`.
     - **Q3:** Accumulate `error_request_count` (`400 <= status_code <= 599`), `total_request_count`, `error_rate = round(err / total, 6)`, and `len(set(error_hosts))` per `(log_date, log_hour)`.
2. **Cross-Pipeline Equivalence Test Harness (`verify_equivalence.py`):**
   - Run a 10,000-line golden sample slice of `NASA_access_log_Jul95` through both MongoDB and MapReduce with identical batch size $B$ and run SQL `EXCEPT` queries on PostgreSQL result tables to verify zero discrepancy.

#### B. Phase-1 Discussion & Board Presentation (Right Half of [board_4.png](./phase1_boards/board_4.png) & [board_6.png](./phase1_boards/board_6.png))
- Explain the Mapper, Combiner optimization, and Reducer data flow for Q1, Q2, and Q3 in MapReduce.
- Explain how non-additive distinct host counts (`distinct_host_count` in Q2 and `distinct_error_hosts` in Q3) are handled accurately across batches.
- Present the **Cross-Pipeline Semantic Equivalence Matrix** across MongoDB, MapReduce, Apache Pig, and Apache Hive (timezone invariance, `'-'` $\rightarrow 0$ bytes rule, deterministic tie-breaking `ORDER BY request_count DESC, resource_path ASC`, and zero-error hour buckets).

---

### 👤 Member 4: PostgreSQL Storage, Reporting Module & Live Demo Lead

#### A. Prototype Implementation Tasks
1. **PostgreSQL Relational Schema (`db/schema.sql`):**
   - Author the DDL for `etl_run_metadata`, `etl_batch_metadata`, `q1_daily_traffic_summary`, `q2_top_requested_resources`, and `q3_hourly_error_analysis`.
   - Ensure every stored query result includes `pipeline_name`, `run_id`, `batch_id`, and `executed_at`.
2. **PostgreSQL Result & Metadata Loader (`db/postgres_loader.py`):**
   - Insert per-batch and run-level aggregated results and metadata into PostgreSQL within a transaction and commit before `t_end`.
3. **Decoupled Reporting Script (`reporter.py`):**
   - Read stored results from PostgreSQL *after* the ETL run finishes (excluded from runtime).
   - Display formatted tables for Q1, Q2, and Q3 alongside the mandatory run metadata: `pipeline_name`, `query_name`, `runtime`, `batch_id`, `batch_size`, `avg_batch_size`, and `malformed_record_count`.

#### B. Phase-1 Discussion & Board Presentation ([board_5.png](./phase1_boards/board_5.png) & Live Demo Execution)
- Present the PostgreSQL ER/Table schema and explain why dedicated result tables per query were chosen over a generic untyped table (strict SQL types, zero NULL padding, fast indexing on `(run_id, batch_id)`, and easy SQL `EXCEPT` validation).
- Drive the live terminal demonstration during the Phase-1 review showing pipeline selection, batch execution, PostgreSQL table population, and `reporter.py` output.
