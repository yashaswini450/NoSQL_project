"""Native MongoDB raw-log parsing and all three aggregation queries."""
import os
from pymongo import MongoClient
from .parser_rules import ENVELOPE, REQUEST, STAMP, MONTHS


def cap(field, i):
    return {'$ifNull': [{'$arrayElemAt': [{'$ifNull': [f'${field}.captures', []]}, i]}, None]}


def to_int(expr):
    return {'$convert': {'input': expr, 'to': 'int', 'onError': None, 'onNull': None}}


def parsing_pipeline(destination):
    """Every extraction, validation and transformation below runs on mongod."""
    pipeline = [
        {'$set': {'env': {'$regexFind': {'input': '$raw_line', 'regex': ENVELOPE}},
                  'ascii': {'$regexMatch': {'input': '$raw_line', 'regex': r'^[\x00-\x7f]*$'}}}},
        {'$set': {'host': cap('env', 0), 'timestamp': cap('env', 1),
                  'request': cap('env', 2), 'status_text': cap('env', 3), 'byte_text': cap('env', 4)}},
        {'$set': {'ts': {'$regexFind': {'input': {'$ifNull': ['$timestamp', '']}, 'regex': STAMP}},
                  'rq': {'$regexFind': {'input': {'$trim': {'input': {'$ifNull': ['$request', '']}, 'chars': ' \t'}}, 'regex': REQUEST}}}},
        {'$set': {'day': cap('ts', 0), 'month_text': cap('ts', 1), 'year': cap('ts', 2),
                  'hour': cap('ts', 3), 'minute': cap('ts', 4), 'second': cap('ts', 5), 'offset': cap('ts', 6),
                  'http_method': cap('rq', 0), 'resource_path': cap('rq', 1), 'protocol_version': cap('rq', 2)}},
        {'$set': {'month_index': {'$indexOfArray': [MONTHS, {'$toLower': {'$ifNull': ['$month_text', '']}}]},
                  'bytes_transferred': {'$cond': [{'$in': ['$byte_text', [None, '-']]}, 0,
                      {'$convert': {'input': '$byte_text', 'to': 'long', 'onError': None, 'onNull': None}}]}}},
        {'$set': {'month': {'$cond': [{'$gte': ['$month_index', 0]},
                  {'$arrayElemAt': [['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], '$month_index']}, '00']}}},
        {'$set': {'log_date': {'$concat': ['$year', '-', '$month', '-', '$day']},
                  'log_hour': to_int('$hour'),
                  'status_code': to_int('$status_text'),
                  'offset_hour': to_int({'$substrCP': [{'$ifNull': ['$offset', '']}, 1, 2]}),
                  'offset_minute': to_int({'$substrCP': [{'$ifNull': ['$offset', '']}, 3, 2]})}},
        {'$set': {'date_check': {'$dateFromString': {
                  'dateString': {'$concat': ['$log_date', 'T', '$hour', ':', '$minute', ':', '$second']},
                  'format': '%Y-%m-%dT%H:%M:%S', 'onError': None, 'onNull': None}}}},
        {'$set': {'reason': {'$switch': {'branches': [
            {'case': {'$or': [{'$eq': ['$env', None]}, {'$eq': ['$ascii', False]}]}, 'then': 'invalid_envelope'},
            {'case': {'$or': [{'$eq': ['$ts', None]}, {'$eq': ['$date_check', None]},
                             {'$lt': [to_int('$year'), 1]}, {'$gt': ['$log_hour', 23]},
                             {'$gt': ['$offset_hour', 23]}, {'$gt': ['$offset_minute', 59]}]}, 'then': 'invalid_timestamp'},
            {'case': {'$eq': ['$rq', None]}, 'then': 'invalid_request'},
            {'case': {'$or': [{'$eq': ['$bytes_transferred', None]},
                             {'$not': [{'$or': [{'$in': ['$byte_text', [None, '-']]},
                                {'$regexMatch': {'input': {'$ifNull': ['$byte_text', '']}, 'regex': r'^[0-9]+$'}}]}]}]},
             'then': 'invalid_bytes'}], 'default': None}}}},
        {'$set': {'valid': {'$eq': ['$reason', None]}}},
        {'$project': {'_id': 1, 'batch_id': 1, 'valid': 1, 'reason': 1,
                      **{field: {'$cond': ['$valid', f'${field}', '$$REMOVE']} for field in
                         ['host', 'timestamp', 'log_date', 'log_hour', 'http_method', 'resource_path', 'protocol_version', 'status_code', 'bytes_transferred']},
                      'raw_line': {'$cond': ['$valid', '$$REMOVE', {'$substrCP': ['$raw_line', 0, 300]}]}}},
        {'$merge': {'into': destination, 'on': '_id', 'whenMatched': 'fail', 'whenNotMatched': 'insert'}},
    ]
    return pipeline


class MongoEngine:
    def __init__(self, run_id, staging):
        self.client = MongoClient(os.environ.get('NASA_MONGO_URI', 'mongodb://127.0.0.1:27018'), serverSelectionTimeoutMS=5000)
        self.versions = {'mongodb': self.client.server_info()['version'], 'mode': 'native aggregation'}
        self.db = self.client.nasa_phase1
        suffix = str(run_id).replace('-', '')
        self.raw, self.parsed = self.db['raw_' + suffix], self.db['parsed_' + suffix]
        self.record_id = 0

    def initialize(self):
        self.parsed.create_index([('batch_id', 1), ('valid', 1)])

    def batch(self, batch_id, count, spool):
        docs = []
        with spool.open('rb') as source:
            for line in source:
                self.record_id += 1
                raw = line.decode('utf-8', errors='replace').removesuffix('\n').removesuffix('\r')
                docs.append({'_id': self.record_id, 'batch_id': batch_id, 'raw_line': raw})
                if len(docs) == 10000:
                    self.raw.insert_many(docs, ordered=False)
                    docs.clear()
        if docs:
            self.raw.insert_many(docs, ordered=False)
        list(self.raw.aggregate(parsing_pipeline(self.parsed.name), allowDiskUse=True))
        self.raw.delete_many({})
        return self.queries(batch_id)

    def aggregate(self, pipeline):
        return list(self.parsed.aggregate(pipeline, allowDiskUse=True, collation={'locale': 'simple'}))

    def queries(self, batch_id=None):
        match = {'batch_id': batch_id} if batch_id is not None else {}
        match_valid = {**match, 'valid': True}
        q1 = self.aggregate([
            {'$match': match_valid},
            {'$group': {'_id': {'d': '$log_date', 's': '$status_code'}, 'request_count': {'$sum': 1}, 'total_bytes': {'$sum': '$bytes_transferred'}}},
            {'$project': {'_id': 0, 'log_date': '$_id.d', 'status_code': '$_id.s', 'request_count': 1, 'total_bytes': 1}},
            {'$sort': {'log_date': 1, 'status_code': 1}},
        ])
        # Group resource + host first: exact counts without an unbounded addToSet array.
        q2 = self.aggregate([
            {'$match': match_valid},
            {'$group': {'_id': {'p': '$resource_path', 'h': '$host'}, 'n': {'$sum': 1}, 'b': {'$sum': '$bytes_transferred'}}},
            {'$group': {'_id': '$_id.p', 'request_count': {'$sum': '$n'}, 'total_bytes': {'$sum': '$b'}, 'distinct_host_count': {'$sum': 1}}},
            {'$sort': {'request_count': -1, '_id': 1}}, {'$limit': 20},
            {'$project': {'_id': 0, 'resource_path': '$_id', 'request_count': 1, 'total_bytes': 1, 'distinct_host_count': 1}},
        ])
        for rank, row in enumerate(q2, 1):  # Display rank only; MongoDB already computed and ordered the top 20.
            row['rank'] = rank
        q3 = self.aggregate([
            {'$match': match_valid},
            {'$group': {'_id': {'d': '$log_date', 't': '$log_hour', 'h': '$host'},
                        'n': {'$sum': 1}, 'e': {'$sum': {'$cond': [{'$and': [{'$gte': ['$status_code', 400]}, {'$lte': ['$status_code', 599]}]}, 1, 0]}}}},
            {'$group': {'_id': {'d': '$_id.d', 't': '$_id.t'}, 'total_request_count': {'$sum': '$n'},
                        'error_request_count': {'$sum': '$e'}, 'distinct_error_hosts': {'$sum': {'$cond': [{'$gt': ['$e', 0]}, 1, 0]}}}},
            {'$project': {'_id': 0, 'log_date': '$_id.d', 'log_hour': '$_id.t', 'total_request_count': 1,
                          'error_request_count': 1, 'distinct_error_hosts': 1, 'error_rate': {'$divide': ['$error_request_count', '$total_request_count']}}},
            {'$sort': {'log_date': 1, 'log_hour': 1}},
        ])
        counts = self.aggregate([{'$match': match}, {'$group': {'_id': {'v': '$valid', 'r': '$reason'}, 'n': {'$sum': 1}}}])
        stats = {'input': 0, 'valid': 0, 'malformed': 0, 'reasons': {}, 'examples': []}
        for c in counts:
            stats['input'] += c['n']
            if c['_id']['v']:
                stats['valid'] += c['n']
            else:
                stats['malformed'] += c['n']
                stats['reasons'][c['_id']['r']] = c['n']
        stats['examples'] = self.aggregate([{'$match': {**match, 'valid': False}}, {'$sort': {'_id': 1}}, {'$limit': 5},
                                           {'$project': {'_id': 0, 'reason': 1, 'raw_line': 1}}])
        return {'q1': q1, 'q2': q2, 'q3': q3, 'stats': stats}

    def final(self):
        return self.queries()

    def close(self, success):
        # Results and counts remain in SQL; retain staging only on failure for diagnosis.
        if success:
            self.raw.drop()
            self.parsed.drop()
        self.client.close()
