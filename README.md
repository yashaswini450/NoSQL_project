# NASA Web Log Analytics

The runnable Phase 1 implementation is in [phase1_prototype](phase1_prototype/README.md).

For the prepared WSL presentation, see [WSL demo instructions](phase1_prototype/WSL_DEMO.md). The demo menu shows real SQL results, checks both backends and can execute a fresh official-data run.

It provides MongoDB and genuine Hadoop Streaming MapReduce pipelines, a common CLI/controller, PostgreSQL result tables, separate SQL reporting and equivalence checks. Pig and Hive are planned for Phase 2.

On Windows with WSL Ubuntu-24.04, open PowerShell in `phase1_prototype`:

```powershell
.\Start-Demo.ps1 -Action setup
.\Start-Demo.ps1 -Action download
.\Start-Demo.ps1 -Action mapreduce -BatchSize 500000
.\Start-Demo.ps1 -Action report
```

See [verified results](phase1_prototype/RESULTS.md) for the completed runs on all 3,461,613 official NASA input records, [Member 3's demo guide](phase1_prototype/MEMBER3_DEMO.md) for the presentation, and [the implementation README](phase1_prototype/README.md) for architecture, parsing, SQL schema, tests and setup details.
