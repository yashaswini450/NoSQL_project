"""
pipelines/mongodb_pipeline.py: MongoDB execution engine (Member 2).

All ETL work is done inside MongoDB. Python only moves raw lines in and
result rows out.

Per batch (inside the timed window, called by main.py):
  E  insert_many() the RAW lines of the batch into  nasa_logs_staging
       {run_id, batch_id, line_no, raw}     <- no parsing in Python
  T  aggregation pipeline on the staging batch:
       $rtrim -> $regexFind (stage 1: whole line)
              -> timestamp regex + calendar check
              -> $regexFindAll tokenizer (stage 2: method / path / protocol)
       $merge -> nasa_logs_parsed   (valid docs AND malformed docs, flag `valid`)
     malformed_count = countDocuments({run_id, batch_id, valid: false})
     staging docs of this batch are deleted (they are transient)

finalize() (after the last batch, over ALL batches of the run):
  Q1  $group (log_date, status_code)            -> $sum 1, $sum bytes
  Q2  $group (resource_path, host) -> $group resource_path
        -> distinct_host_count = number of (resource, host) groups
        -> $sort {request_count: -1, resource_path: 1} -> $limit 20
  Q3  $group (log_date, log_hour, host) -> $group (log_date, log_hour)
        -> error_request_count  = requests with 400 <= status <= 599
        -> distinct_error_hosts = hosts with at least one error
        -> error_rate = errors / total  (unrounded double; PostgreSQL
           NUMERIC(10,6) does the single rounding step for every engine)

Why two-level $group instead of $addToSet + $size?  $addToSet builds one
array per group: /images/NASA-logosmall.gif has ~100k distinct hosts and a
growing array per group can hit the 100 MB $group memory limit. The two-level
group is exact, spills to disk (allowDiskUse) and is the same plan the
MapReduce pipeline uses (key = resource+host, then key = resource), which
makes the equivalence argument easy.

Parsing rules are a 1:1 translation of parser.py (Member 1, single source of
truth). tests/test_mongodb_pipeline.py checks this line by line against
parser.parse_line() and checks Q1-Q3 against a pure-Python reference.

Config (env vars): MONGO_URI (default mongodb://localhost:27017),
                   MONGO_DB  (default nasa_etl)
"""

import os

from pymongo import ASCENDING, MongoClient

from pipelines.base import BaseETLPipeline

# ---------------------------------------------------------------------------
# Regex building blocks (PCRE inside MongoDB).
#
# parser.py runs Python `re` on str, where \s and str.split() treat these
# characters as whitespace: \t \n \v \f \r \x1c-\x1f space \x85 \xa0.
# MongoDB's PCRE \s only knows the ASCII ones, so we spell the class out to
# get IDENTICAL behaviour on odd bytes (batcher.py decodes as latin-1).
# ---------------------------------------------------------------------------
_WS_CHARS = r"\t\n\x0b\x0c\r\x1c-\x1f \x{85}\x{a0}"
WS = "[" + _WS_CHARS + "]"
NWS = "[^" + _WS_CHARS + "]"

# Stage 1, same as parser.LINE_RE:  ^(\S+) \S+ \S+ \[([^\]]+)\] "(.*)" (\d{3}) (\d+|-)\s*$
# ( `.` -> [^\n] : identical to Python's `.`, independent of PCRE newline mode)
LINE_REGEX = (
    "^(" + NWS + "+) " + NWS + "+ " + NWS + "+ "
    r"\[([^\]]+)\] "
    r'"([^\n]*)" '
    r"([0-9]{3}) ([0-9]+|-)" + WS + "*$"
)

# Timestamp, same as parser._parse_timestamp(): exactly two whitespace-separated
# parts, the first being DD/Mon/YYYY:HH:MM:SS. The offset (-0400) is ignored:
# local server time is kept, never converted to UTC (equivalence rule).
TS_REGEX = (
    "^" + WS + "*"
    r"([0-9]+)/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/"
    r"([0-9]+):([0-9]+):([0-9]+):([0-9]+)"
    + WS + "+" + NWS + "+" + WS + "*$"
)
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

TOKEN_REGEX = NWS + "+"                     # request.split()
METHOD_REGEX = r"^[A-Za-z]+$"               # parser.METHOD_RE
PROTO_REGEX = r"^HTTP/[0-9]+\.[0-9]+$"      # parser.PROTO_RE (re.IGNORECASE -> "i")


# ---------------------------------------------------------------------------
# Small aggregation-expression helpers
# ---------------------------------------------------------------------------
def _cap(match_var, i):
    """i-th capture group of a $regexFind result."""
    return {"$arrayElemAt": [f"$${match_var}.captures", i]}


def _to_int(expr):
    return {"$convert": {"input": expr, "to": "int", "onError": None, "onNull": None}}


def _pad2(expr):
    s = {"$concat": ["0", {"$toString": expr}]}
    return {"$substrCP": [s, {"$subtract": [{"$strLenCP": s}, 2]}, 2]}


def build_parse_stages(run_id, batch_id):
    """Aggregation stages that turn {raw} staging docs into parsed docs."""
    line = {"$rtrim": {"input": "$raw", "chars": "\r\n"}}   # line.rstrip("\r\n")

    stage1 = {"$regexFind": {"input": line, "regex": LINE_REGEX}}

    return [
        {"$match": {"run_id": run_id, "batch_id": batch_id}},

        # ---- STAGE 1: whole-line regex --------------------------------------
        {"$set": {"_m": stage1}},
        {"$set": {
            "_host":    {"$arrayElemAt": ["$_m.captures", 0]},
            "_ts":      {"$arrayElemAt": ["$_m.captures", 1]},
            "_req":     {"$arrayElemAt": ["$_m.captures", 2]},
            "_status":  {"$arrayElemAt": ["$_m.captures", 3]},
            "_bytes":   {"$arrayElemAt": ["$_m.captures", 4]},
        }},

        # ---- timestamp: regex + numeric fields ------------------------------
        {"$set": {"_t": {"$cond": [
            {"$eq": ["$_m", None]}, None,
            {"$regexFind": {"input": "$_ts", "regex": TS_REGEX}}]}}},
        {"$set": {"_tp": {"$cond": [{"$eq": ["$_t", None]}, None, {
            "d":   _to_int({"$arrayElemAt": ["$_t.captures", 0]}),
            "mon": {"$add": [{"$indexOfArray": [MONTHS, {"$arrayElemAt": ["$_t.captures", 1]}]}, 1]},
            "y":   _to_int({"$arrayElemAt": ["$_t.captures", 2]}),
            "H":   _to_int({"$arrayElemAt": ["$_t.captures", 3]}),
            "M":   _to_int({"$arrayElemAt": ["$_t.captures", 4]}),
            "S":   _to_int({"$arrayElemAt": ["$_t.captures", 5]}),
        }]}}},
        # calendar validation, same as Python's datetime(...) constructor
        {"$set": {"_ts_ok": {"$let": {
            "vars": {"p": "$_tp"},
            "in": {"$and": [
                {"$ne": ["$$p", None]},
                {"$ne": ["$$p.y", None]}, {"$ne": ["$$p.d", None]},
                {"$ne": ["$$p.H", None]}, {"$ne": ["$$p.M", None]}, {"$ne": ["$$p.S", None]},
                {"$gte": ["$$p.y", 1]}, {"$lte": ["$$p.y", 9999]},
                {"$gte": ["$$p.H", 0]}, {"$lte": ["$$p.H", 23]},
                {"$gte": ["$$p.M", 0]}, {"$lte": ["$$p.M", 59]},
                {"$gte": ["$$p.S", 0]}, {"$lte": ["$$p.S", 59]},
                {"$gte": ["$$p.d", 1]},
                {"$lte": ["$$p.d", {"$arrayElemAt": [
                    [31,
                     {"$cond": [{"$or": [
                         {"$and": [{"$eq": [{"$mod": ["$$p.y", 4]}, 0]},
                                   {"$ne": [{"$mod": ["$$p.y", 100]}, 0]}]},
                         {"$eq": [{"$mod": ["$$p.y", 400]}, 0]}]}, 29, 28]},
                     31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
                    {"$subtract": ["$$p.mon", 1]}]}]},
            ]}}}}},

        # ---- STAGE 2: request tokenizer --------------------------------------
        {"$set": {"_tok": {"$cond": [
            "$_ts_ok",
            {"$map": {"input": {"$regexFindAll": {"input": "$_req", "regex": TOKEN_REGEX}},
                      "as": "t", "in": "$$t.match"}},
            []]}}},
        {"$set": {"_n": {"$size": "$_tok"}}},
        {"$set": {"_has_proto": {"$and": [
            {"$gte": ["$_n", 3]},
            {"$regexMatch": {"input": {"$arrayElemAt": ["$_tok", -1]},
                             "regex": PROTO_REGEX, "options": "i"}}]}}},
        {"$set": {"_req_ok": {"$and": [
            {"$gte": ["$_n", 2]},
            {"$regexMatch": {"input": {"$arrayElemAt": ["$_tok", 0]},
                             "regex": METHOD_REGEX}}]}}},
        {"$set": {"_bytes_l": {"$cond": [
            {"$eq": ["$_bytes", "-"]}, {"$toLong": 0},                 # '-' -> 0
            {"$convert": {"input": "$_bytes", "to": "long", "onError": None, "onNull": None}}]}}},
        {"$set": {"valid": {"$and": ["$_ts_ok", "$_req_ok", {"$ne": ["$_bytes_l", None]}]}}},

        # ---- final document shape ---------------------------------------------
        {"$replaceWith": {"$cond": [
            "$valid",
            {
                "_id": "$_id", "run_id": "$run_id", "batch_id": "$batch_id",
                "line_no": "$line_no", "valid": True,
                "host": "$_host",
                "timestamp": {"$concat": [
                    {"$toString": "$_tp.y"}, "-", _pad2("$_tp.mon"), "-", _pad2("$_tp.d"), " ",
                    _pad2("$_tp.H"), ":", _pad2("$_tp.M"), ":", _pad2("$_tp.S")]},
                "log_date": {"$concat": [
                    {"$toString": "$_tp.y"}, "-", _pad2("$_tp.mon"), "-", _pad2("$_tp.d")]},
                "log_hour": "$_tp.H",
                "http_method": {"$toUpper": {"$arrayElemAt": ["$_tok", 0]}},
                "resource_path": {"$reduce": {
                    "input": {"$slice": ["$_tok", 1,
                                         {"$subtract": ["$_n", {"$cond": ["$_has_proto", 2, 1]}]}]},
                    "initialValue": None,
                    "in": {"$cond": [{"$eq": ["$$value", None]}, "$$this",
                                     {"$concat": ["$$value", " ", "$$this"]}]}}},
                "protocol_version": {"$cond": ["$_has_proto", {"$arrayElemAt": ["$_tok", -1]}, ""]},
                "status_code": _to_int("$_status"),
                "bytes_transferred": "$_bytes_l",
            },
            {   # malformed: counted, kept with its raw text, never silently dropped
                "_id": "$_id", "run_id": "$run_id", "batch_id": "$batch_id",
                "line_no": "$line_no", "valid": False, "raw": "$raw",
            },
        ]}},
    ]


# ---------------------------------------------------------------------------
# Query pipelines (Q1, Q2, Q3), shared by finalize() and the tests
# ---------------------------------------------------------------------------
def q1_pipeline(run_id):
    return [
        {"$match": {"run_id": run_id, "valid": True}},
        {"$group": {"_id": {"log_date": "$log_date", "status_code": "$status_code"},
                    "request_count": {"$sum": 1},
                    "total_bytes": {"$sum": "$bytes_transferred"}}},
        {"$project": {"_id": 0, "log_date": "$_id.log_date", "status_code": "$_id.status_code",
                      "request_count": 1, "total_bytes": 1}},
        {"$sort": {"log_date": 1, "status_code": 1}},
    ]


def q2_pipeline(run_id, top_n=20):
    return [
        {"$match": {"run_id": run_id, "valid": True}},
        {"$group": {"_id": {"r": "$resource_path", "h": "$host"},
                    "n": {"$sum": 1}, "b": {"$sum": "$bytes_transferred"}}},
        {"$group": {"_id": "$_id.r",
                    "request_count": {"$sum": "$n"},
                    "total_bytes": {"$sum": "$b"},
                    "distinct_host_count": {"$sum": 1}}},      # one doc per distinct host
        {"$sort": {"request_count": -1, "_id": 1}},           # deterministic tie-break
        {"$limit": top_n},
        {"$project": {"_id": 0, "resource_path": "$_id", "request_count": 1,
                      "total_bytes": 1, "distinct_host_count": 1}},
    ]


def q3_pipeline(run_id):
    is_error = {"$and": [{"$gte": ["$status_code", 400]}, {"$lte": ["$status_code", 599]}]}
    return [
        {"$match": {"run_id": run_id, "valid": True}},
        {"$group": {"_id": {"d": "$log_date", "h": "$log_hour", "host": "$host"},
                    "total": {"$sum": 1},
                    "errors": {"$sum": {"$cond": [is_error, 1, 0]}}}},
        {"$group": {"_id": {"d": "$_id.d", "h": "$_id.h"},
                    "total_request_count": {"$sum": "$total"},
                    "error_request_count": {"$sum": "$errors"},
                    "distinct_error_hosts": {"$sum": {"$cond": [{"$gt": ["$errors", 0]}, 1, 0]}}}},
        {"$project": {"_id": 0, "log_date": "$_id.d", "log_hour": "$_id.h",
                      "error_request_count": 1, "total_request_count": 1,
                      "error_rate": {"$divide": ["$error_request_count", "$total_request_count"]},
                      "distinct_error_hosts": 1}},
        {"$sort": {"log_date": 1, "log_hour": 1}},
    ]


# Column order required by the spec (used to build ordered row dicts)
Q1_COLS = ["log_date", "status_code", "request_count", "total_bytes"]
Q2_COLS = ["resource_path", "request_count", "total_bytes", "distinct_host_count"]
Q3_COLS = ["log_date", "log_hour", "error_request_count", "total_request_count",
           "error_rate", "distinct_error_hosts"]


def _rows(cursor, cols):
    return [{c: doc[c] for c in cols} for doc in cursor]


class MongoDBPipeline(BaseETLPipeline):
    name = "mongodb"

    STAGING = "nasa_logs_staging"
    PARSED = "nasa_logs_parsed"

    def __init__(self, uri=None, db_name=None):
        self.uri = uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.environ.get("MONGO_DB", "nasa_etl")
        self.client = None
        self.db = None
        self.batch_malformed = {}

    # -- setup: NOT timed ------------------------------------------------------
    def setup(self, run_id):
        super().setup(run_id)
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
        self.client.admin.command("ping")             # fail fast if mongod is down
        self.db = self.client[self.db_name]
        # keep only the current run (previous runs' results live in PostgreSQL)
        self.db[self.STAGING].delete_many({})
        self.db[self.PARSED].delete_many({})
        self.db[self.STAGING].create_index([("run_id", ASCENDING), ("batch_id", ASCENDING)])
        self.db[self.PARSED].create_index([("run_id", ASCENDING), ("valid", ASCENDING),
                                           ("batch_id", ASCENDING)])

    # -- per batch: timed --------------------------------------------------------
    def process_batch(self, batch_id, raw_lines):
        staging = self.db[self.STAGING]
        # E: raw lines only, untouched (no Python parsing)
        staging.insert_many(
            [{"run_id": self.run_id, "batch_id": batch_id, "line_no": i, "raw": line}
             for i, line in enumerate(raw_lines)],
            ordered=False)
        # T: parse inside MongoDB, write to the parsed collection
        staging.aggregate(
            build_parse_stages(self.run_id, batch_id) + [
                {"$merge": {"into": self.PARSED, "whenMatched": "replace",
                            "whenNotMatched": "insert"}}],
            allowDiskUse=True)
        staging.delete_many({"run_id": self.run_id, "batch_id": batch_id})
        bad = self.db[self.PARSED].count_documents(
            {"run_id": self.run_id, "valid": False, "batch_id": batch_id})
        self.batch_malformed[batch_id] = bad
        return bad

    # -- after last batch: timed -----------------------------------------------
    def finalize(self):
        parsed = self.db[self.PARSED]
        return {
            "q1": _rows(parsed.aggregate(q1_pipeline(self.run_id), allowDiskUse=True), Q1_COLS),
            "q2": _rows(parsed.aggregate(q2_pipeline(self.run_id), allowDiskUse=True), Q2_COLS),
            "q3": _rows(parsed.aggregate(q3_pipeline(self.run_id), allowDiskUse=True), Q3_COLS),
        }

    def teardown(self):
        if self.client is not None:
            self.client.close()
            self.client = None
