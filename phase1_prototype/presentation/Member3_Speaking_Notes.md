# Member 3 - MapReduce

Use the single-page board from left to right. A working MongoDB and Hadoop MapReduce prototype is now included in `nasa_phase1/`; use its `MEMBER3_DEMO.md` for the live demonstration and its saved verification results for the observed counts and runtimes.

## What to say - about 2 to 3 minutes

My part is the MapReduce pipeline and checking whether its answers match MongoDB. The controller selects one pipeline and provides the raw NASA logs, batch size and run information. Our MapReduce path processes those raw lines through genuine Hadoop Streaming jobs using the local runner.

Starting on the left, the mapper reads each log line and extracts the required fields, including the host, timestamp, method, resource, status and bytes. Missing bytes or a dash become zero. A malformed line is counted with a reason and left out of the analytical queries. It still counts towards the input batch size.

Next, Hadoop shuffles and sorts the mapper output so records with the same query key reach the appropriate reducer. The reduce stages calculate our three queries.

For Q1, the key is date and status code. We count requests and add the transferred bytes.

For Q2, we calculate requests, bytes and distinct hosts for each resource. We then select the global top 20 by request count. We cannot just combine each batch's top 20, because a resource outside the local lists could still belong in the final answer.

For Q3, we group by date and hour. Status codes from 400 to 599 are errors. The denominator is every valid request in that hour, including successful requests. We also count distinct hosts making errors and retain hours that have traffic but no errors.

The reduce block represents several jobs where needed. For example, exact host deduplication and final top-20 selection can require additional grouping stages. A host occurring in two batches must count only once in the final group.

We pass the finished aggregates and record counts to Member 4's SQL loader. PostgreSQL stores them with the pipeline, run, batch and execution metadata. The separate report reads these saved tables.

Finally, our comparison script reads the stored SQL query values for MongoDB and MapReduce using the same raw files, batch size and parsing rules. Counts and bytes must match exactly. Run IDs, timestamps and execution times are excluded from result equality. This checks that switching the engine preserves the same workload and answers.

## Likely questions

**Why use Hadoop Streaming if the mapper and reducer are Python scripts?**  
Hadoop actually launches the tasks, shuffles their key-value output and runs the reducers. The scripts implement the task logic. A standalone Python loop is not this MapReduce design.

**Is the diagram a single Hadoop job?**  
No. It shows logical stages. The implementation can use multiple jobs for parsing, query aggregation, exact distinct counts and final ranking.

**How will distinct hosts be counted correctly?**  
For Q2, group by `(resource, host)`, then by resource. For Q3, group by `(date, hour, host)`, then by date and hour; count a host only when its error count is positive. Use all valid records for the final run-level answers.

**Why cannot you add the distinct-host counts from each batch?**  
Suppose host A requests `/a` in two batches. There are two requests, but only one distinct host for that resource. Adding the batch distinct counts would incorrectly give two.

**How is the global top 20 obtained?**  
First calculate complete request totals for every resource. A final Hadoop ranking stage selects 20 by `request_count DESC, resource_path ASC` using consistent binary/ASCII ordering. Do not discard resources using per-batch top-20 lists before the final calculation.

**How are malformed records handled?**  
Each malformed input record is counted once in its batch. It is excluded from Q1-Q3 but included in input totals. Check `input = valid + malformed`.

**How will you compare floating-point error rates?**  
Check keys, counts and bytes exactly, and allow an absolute rate difference of at most `1e-9`. Both pipelines must use `error_request_count / total_request_count`. This tolerance is our proposed comparison rule, not an extra assignment requirement.

**What is included in runtime?**  
From the first raw input read through the final SQL result commit, including raw staging, native jobs and loading. Download, installation and separate report rendering are excluded. Member 1 owns the common timer; Member 4 confirms the final commit.

## Quick reference

- Required parsed fields: `host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred`.
- Q1 output: `log_date, status_code, request_count, total_bytes`.
- Q2 output: `resource_path, request_count, total_bytes, distinct_host_count`.
- Q3 output: `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts`.
- Use recorded log-local date/hour, the same resource/host identity rules, and the same missing-value and malformed-record decisions as MongoDB.
- Compare batch outputs and whole-run outputs. Final answers should remain unchanged when batch size changes.
- This board connects to Member 1's controller/batching and Member 4's loader/report. It covers Member 3's MapReduce and equivalence responsibilities without changing the team's division.

Based on the supplied project statement, pages 1-3, and [Apache Hadoop Streaming documentation](https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html).
