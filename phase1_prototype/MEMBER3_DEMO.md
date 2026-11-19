# Member 3: MapReduce and equivalence

Start with the black-and-white MapReduce board. Then open a terminal in this folder.

There is also a second board in `presentation/Member3_Demo_Flow.pdf`: a handwritten-style overview from the raw logs through Hadoop, PostgreSQL and reporting. The matching `.drawio` file is editable, and the PNG is a presentation preview. Both boards are digital drawings.

For explaining the queries, use `presentation/Member3_Three_Queries.pdf`. It follows the same five illustrative records through Q1, Q2 and Q3, showing byte sums, exact distinct hosts and the error-rate denominator. These small values explain the logic; the actual benchmark and SQL reports use the complete official NASA files. Editable `.drawio` and PNG versions are included.

The verified full runs processed 3,461,613 input records in seven batches; both counted 24 malformed records and produced identical query results. The measured full run took about 5.5 minutes for MongoDB and 10.7 minutes for MapReduce on this computer. Start the MapReduce run before your speaking slot, or use one complete official file for a shorter live demonstration. Keep these single-machine timings separate from general performance claims.

**Say:** “My part is the MapReduce pipeline and checking that it gives the same answers as MongoDB. Hadoop launches the Python tasks and performs the shuffle, sort and reduce stages. We are demonstrating Hadoop's local runner on one machine.”

Start a complete official-data run:

```powershell
.\Start-Demo.ps1 -Action mapreduce -BatchSize 500000
```

The terminal prints a run ID and each native job's log path. A batch has a parse job, a grouping job and a final query reduction job. Complete-run answers use two more jobs over all parsed outcomes. Job logs contain Hadoop counters and job IDs.

While it runs, explain:

“The mapper reads raw NASA lines and extracts the nine required fields. Missing bytes or a dash become zero. Malformed lines are counted once and excluded from queries, but remain part of the input batch size.

“For Q1, we group date and status, then count requests and sum bytes.

“For Q2, we first group resource and host. We then group resource to get exact distinct hosts, counts and bytes. The final reducer selects the global top 20. We must retain all resources before final ranking: merging batch top-20 lists can lose the true winner.

“For Q3, we group date, hour and host, then date and hour. Statuses 400 to 599 are errors. The denominator includes every valid request in the hour. A host with errors counts once, even if it appears repeatedly or in multiple batches. Hours with no errors stay in the output.

“The loader writes finished aggregates into PostgreSQL with run, pipeline, batch and execution metadata. The timer ends after the final SQL commit.”

Show a completed report:

```powershell
.\Start-Demo.ps1 -Action report
```

Point to the pipeline name, genuine runtime, input/valid/malformed totals, requested and actual batch sizes, average, and the three query outputs. The report is a separate script reading SQL; it does not query Hadoop or MongoDB.

For equivalence, open `sample_outputs/official_equivalence.json` and the corresponding run IDs. To repeat the SQL comparison, enter Ubuntu with `wsl -d Ubuntu-24.04`, change to this folder's `/mnt/c/...` path, and run:

```bash
bash run.sh verify_equivalence.py --left MONGO_RUN_ID --right MAPREDUCE_RUN_ID
```

**Say:** “We compare all batch and final query values using the same source hashes, parser rules and batch size. Counts and bytes match exactly. Error rates allow a difference of at most one billionth. IDs, timestamps and runtimes are not expected to match.”

If asked about scope: MongoDB and MapReduce are implemented for Phase 1. Pig and Hive are explicitly planned for Phase 2. The command-line interface can select all four names, and unimplemented choices return a clear message instead of using another backend.

If asked about testing: the synthetic correctness fixture includes a resource ranked below 20 in each batch that becomes first globally, the same host in different batches, missing bytes, malformed lines and a zero-error hour. It is labelled as test input and is separate from the official NASA runs.
