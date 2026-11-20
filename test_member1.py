import os
import sys, os, tempfile, types
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parser import parse_line, ParseStats
from batcher import iter_batches, BatchStats
import main

cases = {
 "normal": '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
 "bytes_dash": 'burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 304 -',
 "http09": 'jbiagioni.npt.nuwc.navy.mil - - [01/Jul/1995:23:59:59 -0400] "GET /images/NASA-logosmall.gif" 200 786',
 "space_in_url": 'pipe1.nyc.pipeline.com - - [01/Jul/1995:00:22:43 -0400] "GET /shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg HTTP/1.0" 200 946425',
 "space_in_url2": 'remote27.compusmart.ab.ca - - [01/Jul/1995:10:44:35 -0400] "GET /shuttle/missions/sts-71/sts-71-day-04 highlights.html HTTP/1.0" 404 -',
 "empty_request": 'klothos.crl.research.digital.com - - [10/Jul/1995:16:45:50 -0400] "" 400 -',
 "one_token": 'host1 - - [10/Jul/1995:16:45:50 -0400] "/index.html" 400 -',
 "quote_in_url": 'host2 - - [10/Jul/1995:16:45:50 -0400] "GET /a"b.html HTTP/1.0" 404 -',
 "truncated": 'host3 - - [10/Jul/1995:16:45:50 -0400] "GET /a.html HTTP/1.0" 200',
 "bad_date": 'host4 - - [32/Jul/1995:16:45:50 -0400] "GET /a.html HTTP/1.0" 200 10',
 "crlf": 'host5 - - [01/Aug/1995:00:00:01 -0400] "HEAD / HTTP/1.0" 200 0\r\n',
 "post": 'host6 - - [01/Aug/1995:05:00:01 -0400] "POST /cgi-bin/x HTTP/1.0" 500 12',
}
for k, v in cases.items():
    print(f"{k:15s}", parse_line(v))

# batching
lines = [cases["normal"]] * 23
fd, p = tempfile.mkstemp(); os.write(fd, ("\n".join(lines) + "\n").encode()); os.close(fd)
st = BatchStats()
for b in iter_batches(p, 10): st.record(b); print("batch", b.batch_id, b.size)
print("total", st.total_records, "batches", st.num_batches, "avg", st.average_batch_size)
assert st.sizes == [10, 10, 3] and abs(st.average_batch_size - 23/3) < 1e-9
st2 = BatchStats(); [st2.record(b) for b in iter_batches(p, 23)]; assert st2.sizes == [23]

# main with fake pipeline
from pipelines.base import BaseETLPipeline
class Fake(BaseETLPipeline):
    def __init__(s): s.seen=[]
    def process_batch(s, bid, raw):
        ps = ParseStats(); [ps.record(parse_line(l)) for l in raw]; s.seen.append(bid); return ps.malformed
    def finalize(s): return {"q1": [], "q2": [], "q3": []}
fd, p2 = tempfile.mkstemp(); os.write(fd, ("\n".join(cases.values()).replace("\r\n","\r") + "\n").encode()); os.close(fd)
args = main.build_arg_parser().parse_args(["--pipeline","mongodb","--batch-size","5","--input",p2,p,"--dry-run"])
f = Fake(); out = main.run(args, pipeline=f)
print("batch ids", f.seen)
