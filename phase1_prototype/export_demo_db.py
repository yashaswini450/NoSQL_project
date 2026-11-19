"""Export only the two verified official runs as a portable PostgreSQL COPY script."""
import json
from pathlib import Path
from psycopg import sql
from nasa.store import connect, TABLES


def export():
    base = Path(__file__).resolve().parent
    ids = list(json.loads((base/'sample_outputs/official_run_ids.json').read_text()).values())
    destination = base/'sample_outputs/restore_official_reports.sql'
    with connect() as conn, destination.open('wb') as dest:
        dest.write(b'-- Genuine completed official NASA runs; generated from PostgreSQL.\n-- Import once into an empty nasa_reporting database with psql -v ON_ERROR_STOP=1 -f this_file.sql\nBEGIN;\n')
        dest.write((base/'schema.sql').read_bytes())
        dest.write(b'\n')
        for table in ['runs', 'batches', *[v[0] for v in TABLES.values()]]:
            query = sql.SQL('COPY (SELECT * FROM {} WHERE run_id IN ({})) TO STDOUT').format(
                sql.Identifier(table), sql.SQL(',').join(sql.Literal(v) for v in ids))
            dest.write(f'COPY {table} FROM stdin;\n'.encode())
            with conn.cursor().copy(query) as copy:
                for block in copy:
                    dest.write(block)
            dest.write(b'\\.\n')
        dest.write(b'COMMIT;\n')
    print(destination)


if __name__ == '__main__':
    export()
