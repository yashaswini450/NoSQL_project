# DAS 839 – NoSQL Systems: Phase-1 Architecture & Discussion Board Blueprint
**Project Title:** Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics  
**Dataset:** Official NASA Kennedy Space Center HTTP Web Server Logs (`NASA_access_log_Jul95`, `NASA_access_log_Aug95`)  
**Phase-1 Prototype Pipelines:** MongoDB Aggregation Engine & MapReduce (Hadoop Streaming / Python)  
**Phase-2 Planned Pipelines:** Apache Pig & Apache Hive  
**Target Relational Reporting Database:** PostgreSQL  

---

## How to Use This Document
1. **Ready-to-Submit Scanned Handwritten Discussion Board PDF:**  
   See [`Phase1_Handwritten_Discussion_Board.pdf`](./Phase1_Handwritten_Discussion_Board.pdf) (and individual high-res page scans in [`phase1_boards/`](./phase1_boards/)).
2. **Physical Whiteboard / Paper Transcription Blueprint & Live Viva Guide:**  
   If you also want to draw these 6 boards by hand on physical A4 sheets or a whiteboard during the live Phase-1 discussion with your professor, follow the 6 board templates and talking points below.

---

## Board 1: Overall System Architecture & Unified Orchestrator

### 1.1 Architecture Diagram (Whiteboard Layout)
```text
                        |<------------------- STRICT RUNTIME WINDOW (t_start to t_end) ------------------->|
                        |                                                                                  |
+--------------------+  |  +---------------------------+        +---------------------------------------+  |  +-----------------------+
| (1) RAW NASA LOGS  |  |  | (2) UNIFIED CONTROLLER    |        | (3) PLUGGABLE EXECUTION BACKENDS      |  |  | (4) PostgreSQL RDBMS  |
|                    |  |  |     (main.py / CLI)       |        |     (Exactly 1 selected per run)      |  |  |                       |
| - Jul95 (1.89M)    |  |  |                           |        |                                       |  |  | - etl_run_metadata    |
| - Aug95 (1.57M)    |  |  | $ python main.py \        |  --->  | [A] MongoDB Pipeline    (Phase-1 OK)  | --> | - etl_batch_metadata  |
| - Raw ASCII lines  | --> |   --pipeline {mongo|mr|   |        | [B] MapReduce Pipeline  (Phase-1 OK)  | --> | - q1_daily_traffic    |
| - gunzip allowed   |  |  |               pig|hive} \ |  --->  | [C] Apache Pig Pipeline (Phase-2)     | --> | - q2_top_resources    |
| - NO pre-cleaning! |  |  |   --batch-size 500000     |        | [D] Apache Hive Pipeline(Phase-2)     |  |  | - q3_hourly_errors    |
| - NO CSV/JSON prep |  |  |                           |        |                                       |  |  +-----------------------+
+--------------------+  |  | - Starts t_start          |        +---------------------------------------+  |              |
                        |  | - Slices batches 1..k     |                                                   |              | (After t_end)
                        |  | - Stops t_end on COMMIT   |                                                   |              v
                        |  +---------------------------+                                                   |  +-----------------------+
                                                                                                              | (5) REPORTING MODULE  |
                                                                                                              |     (reporter.py)     |
                                                                                                              | - Excluded from timer |
                                                                                                              | - Prints Q1, Q2, Q3 + |
                                                                                                              |   run/batch metadata  |
                                                                                                              +-----------------------+
```

### 1.2 Key Design Thought Process to Highlight in Phase-1 Discussion
* **Why a Strategy-Pattern Unified Controller (`BaseETLPipeline`)?**
  * *Initial thought (rejected):* Write four separate standalone scripts (`run_mongo.py`, `run_mr.sh`, `run_pig.sh`, `run_hive.sh`).
  * *Why rejected:* The specification states that the system must be *"a comparative systems prototype and not merely a collection of independent scripts."*
  * *Final design:* A single CLI orchestrator (`main.py`) handles timer boundaries ($t_{\text{start}} \rightarrow t_{\text{end}}$), sequential batch slicing (`batch_id = 1..k`), and PostgreSQL result/metadata loading, while delegating core parsing and Q1/Q2/Q3 execution to the chosen backend engine (`MongoPipeline`, `MapReducePipeline`, `PigPipeline`, `HivePipeline`).
* **Genuine Execution Guarantee:**
  * No pandas, SQLite, or pre-converted CSVs are used. Every pipeline parses raw log lines and performs grouping, filtering, distinct host counting, and top-20 sorting inside its native engine.

---

## Board 2: NASA HTTP Log Parsing Strategy & Edge-Case Rules

### 2.1 Anatomy of a Raw NASA Log Record
```text
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
|__________| | |  |________________________|   |___|_______________|________| |_| |__|
     |       | |              |                  |         |            |      |    |
  1. host    | |         2. timestamp            5.        6.           7.     8.   9.
  (STRING)   | |  +---> 3. log_date: 1995-07-01  http_   resource_   protocol_ status bytes_
             | |  +---> 4. log_hour: 0 (00..23)  method  path        version   _code  transferred
          (ignored)                              (STR)   (STR)       (STR)     (INT)  (BIGINT: '-' -> 0)
```

### 2.2 Two-Stage Deterministic Parsing Algorithm
1. **Why naive whitespace splitting fails:**
   * Some NASA log lines omit `protocol_version` (HTTP/0.9 simple requests like `"GET /shuttle/countdown/"`).
   * Some request strings contain trailing spaces or internal spaces inside the quoted request.
2. **Stage 1 — Outer Envelope Regex:**
   ```regex
   ^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(.*)"\s+(\d{3})\s+(\S+)\s*$
   ```
   * Group 1: `host`
   * Group 2: `timestamp` $\rightarrow$ parsed with format `dd/Mon/yyyy:HH:mm:ss -0400` in server-local time (`-0400`) to yield `log_date` (`YYYY-MM-DD`) and `log_hour` (`0..23`).
   * Group 3: `raw_request`
   * Group 4: `status_code` (3-digit integer)
   * Group 5: `raw_bytes`
3. **Stage 2 — Inner Request String & Bytes Cleaning:**
   * If `raw_bytes == "-"` or is empty $\rightarrow$ `bytes_transferred = 0` (**valid record**, not malformed!).
   * Split trimmed `raw_request`:
     * 3 tokens (`METHOD PATH PROTO`): `http_method = tokens[0]`, `resource_path = tokens[1]`, `protocol_version = tokens[2]`.
     * 2 tokens (`METHOD PATH`, HTTP/0.9): `http_method = tokens[0]`, `resource_path = tokens[1]`, `protocol_version = ""`.
     * $>3$ tokens (extra spaces in path): if last token starts with `HTTP/`, pop `protocol_version` from right, pop `http_method` from left, and join remaining middle tokens as `resource_path`.
4. **Malformed Record Rule (Never Silently Drop!):**
   * If a line fails Stage 1 (e.g., truncated line, missing timestamp brackets, unclosed quote, non-integer status code) or has an empty `resource_path` in Stage 2:
     * Increment `malformed_record_count += 1` for that batch and run.
     * Exclude the malformed record from Q1, Q2, and Q3 aggregations.
     * Store `malformed_record_count` in PostgreSQL metadata and report it in `reporter.py`.

---

## Board 3: Batching Approach, Average Batch Math & Runtime Boundary

### 3.1 Sequential Batch Slicing (`batch_id = 1, 2, ..., k`)
* Given configured `batch_size` $= B$ (number of raw input log lines per batch) and total input records $N$:
  * `batch_id = 1`: records $1 \dots B$
  * `batch_id = 2`: records $B+1 \dots 2B$
  * $\dots$
  * `batch_id = k` (Final tail batch): records $(k-1)B + 1 \dots N$, with size $r = N - (k-1)B \le B$.
* **Partial Final Batch Rule:** Even if $r < B$, `batch_id = k` is counted as a valid non-empty batch.
* **Mandatory Average Batch Size Formula:**
  $$\text{average\_batch\_size} = \frac{\text{total\_records\_processed}}{\text{number\_of\_non\_empty\_batches } (k)}$$
  * *Worked Example (`NASA_access_log_Jul95`, $N = 1,891,715$, $B = 500,000$):*
    * Batches: $500,000 + 500,000 + 500,000 + 391,715 \implies k = 4$ non-empty batches.
    * $\text{average\_batch\_size} = \frac{1,891,715}{4} = 472,928.75$.

### 3.2 Exact Runtime Measurement Window
$$\text{Runtime} = t_{\text{end}} - t_{\text{start}}$$
* **Excluded before $t_{\text{start}}$:** Downloading `.gz` files, `gunzip` decompression, software installation, initial DDL table creation.
* **Included ($t_{\text{start}} \rightarrow t_{\text{end}}$):** Opening raw log file $\rightarrow$ batch slicing $\rightarrow$ parsing & malformed counting $\rightarrow$ executing Q1, Q2, Q3 in selected backend $\rightarrow$ writing results & metadata into PostgreSQL (`COMMIT`).
* **Excluded after $t_{\text{end}}$:** Running `reporter.py` to query PostgreSQL and render tables.

### 3.3 Handling Non-Additive Distinct Host Counts Across Batches
* *Thought Process Note:* While `request_count` and `total_bytes` in Q1 are additive across batches, `distinct_host_count` (Q2) and `distinct_error_hosts` (Q3) are **non-additive** (a host visiting in Batch 1 and Batch 2 must not be counted twice in the full run).
* Therefore, each pipeline processes batches $1 \dots k$ sequentially (recording per-batch metrics and results tagged with `batch_id = 1..k`) and executes the run-level aggregation over the ingested staging collection / batch directories inside the chosen engine before $t_{\text{end}}$.

---

## Board 4: ETL Workflow & Phase-1 Working Prototype Pipelines (MongoDB & MapReduce)

### 4.1 Pipeline 1: MongoDB Aggregation Engine
1. **Batch Load:** Raw batch lines are parsed into BSON documents (`{run_id, batch_id, host, log_date, log_hour, http_method, resource_path, protocol_version, status_code, bytes_transferred}`) and bulk-inserted into `db.nasa_logs_staging`.
2. **Query 1 (`q1_daily_traffic_summary`):**
   ```javascript
   db.nasa_logs_staging.aggregate([
     { $match: { run_id: RUN_ID } },
     { $group: {
         _id: { log_date: "$log_date", status_code: "$status_code" },
         request_count: { $sum: 1 },
         total_bytes: { $sum: "$bytes_transferred" }
     }},
     { $project: {
         _id: 0,
         log_date: "$_id.log_date",
         status_code: "$_id.status_code",
         request_count: 1,
         total_bytes: 1
     }},
     { $sort: { log_date: 1, status_code: 1 } }
   ])
   ```
3. **Query 2 (`q2_top_requested_resources`):**
   ```javascript
   db.nasa_logs_staging.aggregate([
     { $match: { run_id: RUN_ID } },
     { $group: {
         _id: "$resource_path",
         request_count: { $sum: 1 },
         total_bytes: { $sum: "$bytes_transferred" },
         hosts_set: { $addToSet: "$host" }
     }},
     { $project: {
         _id: 0,
         resource_path: "$_id",
         request_count: 1,
         total_bytes: 1,
         distinct_host_count: { $size: "$hosts_set" }
     }},
     { $sort: { request_count: -1, resource_path: 1 } },
     { $limit: 20 }
   ])
   ```
4. **Query 3 (`q3_hourly_error_analysis`):**
   ```javascript
   db.nasa_logs_staging.aggregate([
     { $match: { run_id: RUN_ID } },
     { $group: {
         _id: { log_date: "$log_date", log_hour: "$log_hour" },
         total_request_count: { $sum: 1 },
         error_request_count: {
           $sum: { $cond: [{ $and: [{ $gte: ["$status_code", 400] }, { $lte: ["$status_code", 599] }] }, 1, 0] }
         },
         error_hosts_set: {
           $addToSet: {
             $cond: [{ $and: [{ $gte: ["$status_code", 400] }, { $lte: ["$status_code", 599] }] }, "$host", "$$REMOVE"]
           }
         }
     }},
     { $project: {
         _id: 0,
         log_date: "$_id.log_date",
         log_hour: "$_id.log_hour",
         error_request_count: 1,
         total_request_count: 1,
         error_rate: { $round: [{ $divide: ["$error_request_count", "$total_request_count"] }, 6] },
         distinct_error_hosts: { $size: "$error_hosts_set" }
     }},
     { $sort: { log_date: 1, log_hour: 1 } }
   ])
   ```

### 4.2 Pipeline 2: MapReduce (Hadoop Streaming / Python MapReduce)
* **Mapper (`mapper.py`):** Reads raw log lines from `stdin`, runs the 2-stage regex parser, emits `MALFORMED\t1` on parse error, and emits query-tagged key-value records for valid lines:
  * **Q1 Emission:** `Q1\t{log_date}\t{status_code}\t1\t{bytes_transferred}`
  * **Q2 Emission:** `Q2\t{resource_path}\t1\t{bytes_transferred}\t{host}`
  * **Q3 Emission:** `Q3\t{log_date}\t{log_hour}\t1\t{is_error}\t{error_host}` (where `is_error = 1` iff $400 \le \text{status\_code} \le 599$)
* **Reducer (`reducer.py`):**
  * **Q1 Reduction:** Sums `request_count` and `total_bytes` per `(log_date, status_code)`.
  * **Q2 Reduction:** Accumulates `request_count`, `total_bytes`, and `len(set(hosts))` per `resource_path`, then emits the global Top 20 sorted by `(-request_count, resource_path)`.
  * **Q3 Reduction:** Accumulates `error_request_count`, `total_request_count`, computes `error_rate = round(error_request_count / total_request_count, 6)`, and counts `distinct_error_hosts = len(set(error_hosts))` per `(log_date, log_hour)`.

---

## Board 5: PostgreSQL Relational Schema & Reporting Module

### 5.1 Complete PostgreSQL DDL Schema
```sql
-- 1. Run-Level Execution Metadata Table
CREATE TABLE IF NOT EXISTS etl_run_metadata (
    run_id                  VARCHAR(64) PRIMARY KEY,
    pipeline_name           VARCHAR(32) NOT NULL,  -- 'mongodb', 'mapreduce', 'pig', 'hive'
    input_dataset           VARCHAR(255) NOT NULL,
    configured_batch_size   INTEGER NOT NULL,
    total_batches           INTEGER NOT NULL,
    total_records_processed BIGINT NOT NULL,
    valid_records_count     BIGINT NOT NULL,
    malformed_record_count  BIGINT NOT NULL,
    avg_batch_size          NUMERIC(14, 2) NOT NULL,
    runtime_seconds         NUMERIC(12, 4) NOT NULL,
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Batch-Level Execution Metadata Table (Batch IDs 1..k)
CREATE TABLE IF NOT EXISTS etl_batch_metadata (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,      -- Sequential: 1, 2, ..., k
    batch_size              INTEGER NOT NULL,      -- Actual records in this batch
    valid_records           INTEGER NOT NULL,
    malformed_records       INTEGER NOT NULL,
    batch_runtime_seconds   NUMERIC(12, 4) NOT NULL,
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, batch_id)
);

-- 3. Query 1 Result Table: Daily Traffic Summary
CREATE TABLE IF NOT EXISTS q1_daily_traffic_summary (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,      -- 1..k for per-batch, 0 for full-run summary
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    log_date                DATE NOT NULL,
    status_code             INTEGER NOT NULL,
    request_count           BIGINT NOT NULL,
    total_bytes             BIGINT NOT NULL
);

-- 4. Query 2 Result Table: Top Requested Resources (Top 20)
CREATE TABLE IF NOT EXISTS q2_top_requested_resources (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resource_path           TEXT NOT NULL,
    request_count           BIGINT NOT NULL,
    total_bytes             BIGINT NOT NULL,
    distinct_host_count     BIGINT NOT NULL
);

-- 5. Query 3 Result Table: Hourly Error Analysis
CREATE TABLE IF NOT EXISTS q3_hourly_error_analysis (
    run_id                  VARCHAR(64) NOT NULL REFERENCES etl_run_metadata(run_id) ON DELETE CASCADE,
    pipeline_name           VARCHAR(32) NOT NULL,
    batch_id                INTEGER NOT NULL,
    executed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    log_date                DATE NOT NULL,
    log_hour                SMALLINT NOT NULL,     -- 0..23
    error_request_count     BIGINT NOT NULL,
    total_request_count     BIGINT NOT NULL,
    error_rate              NUMERIC(10, 6) NOT NULL,
    distinct_error_hosts    BIGINT NOT NULL
);
```

---

## Board 6: Plan for Ensuring Equivalence Across All 4 Pipelines

| Equivalence Invariant | Exact Rule Enforced Across MongoDB, MapReduce, Pig, and Hive |
| :--- | :--- |
| **1. Identical Raw Input** | Raw decompressed `NASA_access_log_Jul95` & `Aug95` files read directly by each pipeline without external pre-cleaning. |
| **2. Timestamp & Timezone** | Local server offset (`-0400`) preserved directly from `[dd/Mon/yyyy:HH:mm:ss -0400]` $\rightarrow$ `log_date = YYYY-MM-DD`, `log_hour = 0..23` (no UTC conversion shift). |
| **3. Missing Bytes & Malformed Lines** | `bytes == '-'` mapped to `0`. Any record failing the 2-stage parser increments `malformed_record_count` and is excluded from Q1–Q3. |
| **4. Q2 Deterministic Tie-Breaking** | Top 20 sorted by `request_count DESC, resource_path ASC` so any ties produce identical row sets across all 4 engines. |
| **5. Q3 Zero-Error Hour Buckets** | Every `(log_date, log_hour)` bucket with traffic is emitted even when `error_request_count = 0` (`error_rate = 0.000000`, `distinct_error_hosts = 0`). |
| **6. Automated SQL `EXCEPT` Check** | `verify_equivalence.py` runs all implemented pipelines on a 10,000-line golden sample and executes SQL `EXCEPT` queries across PostgreSQL result tables to assert 0 row differences. |
