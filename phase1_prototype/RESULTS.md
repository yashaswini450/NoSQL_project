# Verified Phase 1 results

Executed on this Windows computer through Ubuntu-24.04/WSL on 19 November 2026. Both runs used the complete official July file followed by the complete official August file, parser `nasa-common-v1`, and batch size **500,000 input records**.

| Measurement | MongoDB | Hadoop MapReduce |
|---|---:|---:|
| Input records | 3,461,613 | 3,461,613 |
| Valid records | 3,461,589 | 3,461,589 |
| Malformed records | 24 | 24 |
| Nonempty batches | 7 | 7 |
| Final batch size | 461,613 | 461,613 |
| Average batch size | 494,516.142857 | 494,516.142857 |
| Measured ETL seconds through final SQL commit | 330.846687 | 643.300682 |
| Final Q1 rows | 288 | 288 |
| Final Q2 rows | 20 | 20 |
| Final Q3 rows | 1,369 | 1,369 |

MongoDB run: `112f5738-e752-4bc6-a7a9-2105280d9d93`  
MapReduce run: `f9f70fa9-4c74-47be-9706-e6c47e9e670d`

**Equivalence: PASS.** All seven batches and every final Q1/Q2/Q3 row matched. Keys, counts and bytes were compared exactly; error-rate absolute tolerance was `1e-9`. These timings are observations from one sequential execution on this machine. Shared caches, WSL scheduling and background activity prevent a general performance ranking from this single trial.

Hadoop ran **23 genuine Streaming jobs** through its LocalJobRunner: three per batch and two for the complete run. `sample_outputs/hadoop_job_counters.json` contains the actual job IDs and counters. Two complete native logs are included as text. This is a single-machine demonstration.

Other completed checks:

- Six unit tests passed, including missing/dash/invalid/overflow bytes, optional protocol, timestamp validation, blank lines, exact and partial batches, empty input and file boundaries without a final newline.
- Twenty-two native MongoDB parsing cases matched the Hadoop mapper's parsing contract, including the signed-64-bit boundary and a zero-padded early year.
- Real MongoDB, Hadoop and PostgreSQL integration passed on a labelled 180-record synthetic fixture, including CRLF, an embedded carriage return, three malformed records, repeated hosts across batches, a zero-error hour and a global winner absent from both local top-20 lists.
- All synthetic query rows matched an independent fixture oracle. Changing the MongoDB batch size preserved the final answers.
- The portable SQL export was loaded into a fresh temporary database. Both restored official reports matched the original SQL reports exactly. The temporary test database was then removed.
- The Windows PowerShell launcher successfully started the private services and displayed the completed official MapReduce report, even after newer synthetic test runs existed.

Evidence files are under `sample_outputs/`: complete JSON reports, readable text reports, source hashes, equivalence results, integration checks, restore checks, native Hadoop logs/counters and the portable PostgreSQL COPY script.

The prototype implements MongoDB and MapReduce. Pig and Hive remain Phase 2 work.
