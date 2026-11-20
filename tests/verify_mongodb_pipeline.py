"""
tests/verify_mongodb_pipeline.py: correctness check for the MongoDB pipeline (Member 2).

Checks, against a running mongod:
  1. PARSING EQUIVALENCE: every line parsed inside MongoDB ($regexFind ...)
     gives exactly the same 9 fields as parser.parse_line() (Member 1's
     canonical parser), and the same lines are marked malformed.
  2. QUERY EQUIVALENCE: Q1, Q2, Q3 from MongoDB aggregation == a plain-Python
     reference computed from parser.parse_line() (test oracle only, it is
     never used by the pipeline).
  3. BATCHING: batch ids 1..k, last batch may be short, malformed per batch.

Usage:
  python tests/verify_mongodb_pipeline.py                         # built-in edge cases
  python tests/verify_mongodb_pipeline.py --input NASA_access_log_Jul95 --batch-size 100000
  python tests/verify_mongodb_pipeline.py --input NASA_access_log_Jul95 --limit 10000 --batch-size 3000
"""

import argparse
import os
import sys
import tempfile
import time
from collections import defaultdict
from itertools import islice

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from batcher import iter_batches, BatchStats            # noqa: E402
from parser import parse_line                           # noqa: E402
from pipelines.mongodb_pipeline import MongoDBPipeline  # noqa: E402

EDGE_CASES = [
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
    'burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 304 -',
    'jbiagioni.npt.nuwc.navy.mil - - [01/Jul/1995:23:59:59 -0400] "GET /images/NASA-logosmall.gif" 200 786',
    'remote27.compusmart.ab.ca - - [01/Jul/1995:10:44:35 -0400] "GET /shuttle/missions/sts-71/sts-71-day-04 highlights.html HTTP/1.0" 404 -',
    'a - - [01/Jul/1995:10:44:35 -0400] "GET  /two   spaces.html  " 404 -',
    'klothos.crl.research.digital.com - - [10/Jul/1995:16:45:50 -0400] "" 400 -',
    'host1 - - [10/Jul/1995:16:45:50 -0400] "/index.html" 400 -',
    'host2 - - [10/Jul/1995:16:45:50 -0400] "GET /a"b.html HTTP/1.0" 404 -',
    'host3 - - [10/Jul/1995:16:45:50 -0400] "GET /a.html HTTP/1.0" 200',
    'host4 - - [32/Jul/1995:16:45:50 -0400] "GET /a.html HTTP/1.0" 200 10',
    'host4b - - [29/Feb/1995:16:45:50 -0400] "GET /a.html HTTP/1.0" 200 10',
    'host4c - - [29/Feb/1996:16:45:50 -0400] "GET /a.html HTTP/1.0" 200 10',
    'host4d - - [01/Jul/1995:24:00:00 -0400] "GET /a.html HTTP/1.0" 200 10',
    'host4e - - [01/jul/1995:10:00:00 -0400] "GET /a.html HTTP/1.0" 200 10',
    'host5 - - [01/Aug/1995:00:00:01 -0400] "HEAD / HTTP/1.0" 200 0\r',
    'host6 - - [01/Aug/1995:05:00:01 -0400] "post /cgi-bin/x http/1.0" 500 12',
    'host7 - - [01/Aug/1995:05:00:01 -0400] "GET /x HTTP/1.0" 503 12   ',
    'host8 - - [01/Aug/1995:05:00:01 -0400] "G3T /x HTTP/1.0" 200 12',
    'host9 - - [01/Aug/1995:05:00:01 -0400] "GET HTTP/1.0" 200 12',
    'host10 - - [01/Aug/1995:05:00:01 -0400] "GET /caf\xe9\xa0x.html HTTP/1.0" 200 12',
    '',
    'garbage line without structure',
]

FIELDS = ["host", "timestamp", "log_date", "log_hour", "http_method", "resource_path",
          "protocol_version", "status_code", "bytes_transferred"]


def reference_queries(parsed_records):
    """Plain-Python Q1/Q2/Q3 (oracle). parsed_records = iterable of parse_line dicts."""
    q1 = defaultdict(lambda: [0, 0])
    q2 = defaultdict(lambda: [0, 0, set()])
    q3 = defaultdict(lambda: [0, 0, set()])
    for r in parsed_records:
        a = q1[(r["log_date"], r["status_code"])]
        a[0] += 1
        a[1] += r["bytes_transferred"]
        b = q2[r["resource_path"]]
        b[0] += 1
        b[1] += r["bytes_transferred"]
        b[2].add(r["host"])
        c = q3[(r["log_date"], r["log_hour"])]
        c[1] += 1
        if 400 <= r["status_code"] <= 599:
            c[0] += 1
            c[2].add(r["host"])
    out1 = [dict(log_date=d, status_code=s, request_count=n, total_bytes=b)
            for (d, s), (n, b) in sorted(q1.items())]
    top = sorted(q2.items(), key=lambda kv: (-kv[1][0], kv[0]))[:20]
    out2 = [dict(resource_path=p, request_count=n, total_bytes=b, distinct_host_count=len(h))
            for p, (n, b, h) in top]
    out3 = [dict(log_date=d, log_hour=h, error_request_count=e, total_request_count=t,
                 error_rate=e / t, distinct_error_hosts=len(hs))
            for (d, h), (e, t, hs) in sorted(q3.items())]
    return {"q1": out1, "q2": out2, "q3": out3}


def compare_rows(name, got, want):
    if got == want:
        print(f"  [PASS] {name}: {len(got)} rows identical")
        return True
    print(f"  [FAIL] {name}: got {len(got)} rows, expected {len(want)}")
    for i, (g, w) in enumerate(zip(got, want)):
        if g != w:
            print(f"     first diff at row {i}:\n       mongo: {g}\n       ref:   {w}")
            break
    return False


def run(paths, batch_size, show=False):
    ok = True
    pipe = MongoDBPipeline()
    pipe.setup("verify_" + str(int(time.time())))

    stats = BatchStats()
    ref_parsed = []                  # (batch_id, line_no) -> parse_line result
    t0 = time.perf_counter()
    malformed_total = 0
    for batch in iter_batches(paths, batch_size):
        bad = pipe.process_batch(batch.batch_id, batch.lines)
        ref = [parse_line(ln) for ln in batch.lines]
        ref_bad = sum(r is None for r in ref)
        if bad != ref_bad:
            ok = False
            print(f"  [FAIL] batch {batch.batch_id}: malformed mongo={bad} parser.py={ref_bad}")
        stats.record(batch)
        malformed_total += bad
        ref_parsed.append((batch.batch_id, ref))
    results = pipe.finalize()
    elapsed = time.perf_counter() - t0
    print(f"MongoDB ETL: batches={stats.num_batches} sizes(first/last)={stats.sizes[:1]}/{stats.sizes[-1:]} "
          f"records={stats.total_records} malformed={malformed_total} "
          f"avg_batch_size={stats.average_batch_size:.2f} time={elapsed:.2f}s")

    # 1. field-by-field parse equivalence
    coll = pipe.db[pipe.PARSED]
    mismatches = 0
    for batch_id, ref in ref_parsed:
        docs = coll.find({"run_id": pipe.run_id, "batch_id": batch_id}).sort("line_no", 1)
        for doc, r in zip(docs, ref):
            mine = {f: doc[f] for f in FIELDS} if doc["valid"] else None
            if mine != r:
                mismatches += 1
                if mismatches <= 5:
                    print(f"  [FAIL] parse diff batch {batch_id} line {doc['line_no']}:\n"
                          f"     mongo:  {mine}\n     parser: {r}")
    total_docs = coll.count_documents({"run_id": pipe.run_id})
    if mismatches or total_docs != stats.total_records:
        ok = False
        print(f"  [FAIL] parsing: {mismatches} mismatching lines, {total_docs} docs for "
              f"{stats.total_records} input lines")
    else:
        print(f"  [PASS] parsing: all {total_docs} lines identical to parser.py "
              f"(incl. {malformed_total} malformed)")

    # 2. query equivalence
    want = reference_queries(r for _, ref in ref_parsed for r in ref if r is not None)
    for q in ("q1", "q2", "q3"):
        ok &= compare_rows(q.upper(), results[q], want[q])

    if show:
        for q in ("q1", "q2", "q3"):
            print(f"\n{q.upper()} (first 25 rows)")
            for row in results[q][:25]:
                print("  ", row)
    pipe.teardown()
    print("\nRESULT:", "ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", nargs="+", help="raw NASA log file(s); default = built-in edge cases")
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--limit", type=int, help="only use the first N lines")
    ap.add_argument("--show", action="store_true", help="print query results")
    args = ap.parse_args()
    if args.input and not args.limit:
        ok = run(args.input, args.batch_size, show=args.show)
    else:
        if args.input:      # first N raw lines, copied byte-for-byte (test only)
            lines = islice((ln for p in args.input
                            for ln in open(p, encoding="latin-1", newline="\n")), args.limit)
            text = "".join(lines)
        else:
            text = "\n".join(EDGE_CASES) + "\n"
        fd, path = tempfile.mkstemp(suffix=".log")
        with os.fdopen(fd, "w", encoding="latin-1", newline="") as f:
            f.write(text)
        try:
            ok = run([path], args.batch_size, show=args.show)
        finally:
            os.remove(path)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
