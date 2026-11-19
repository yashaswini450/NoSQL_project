"""Raw transport only: download, decompress and frame lines. No field parsing."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import urllib.request

FILES = ['NASA_access_log_Jul95', 'NASA_access_log_Aug95']
BASE_URL = 'https://ita.ee.lbl.gov/traces/'


def state_root():
    return Path(os.environ.get('NASA_STATE_DIR', Path.home() / '.local/share/nasa-phase1'))


def download(data_dir):
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest = {'source': BASE_URL, 'files': []}
    for name in FILES:
        archive, raw = data_dir / (name + '.gz'), data_dir / name
        if not archive.exists():
            print('Downloading', BASE_URL + name + '.gz', flush=True)
            with urllib.request.urlopen(BASE_URL + name + '.gz', timeout=180) as source, archive.with_suffix('.gz.part').open('wb') as dest:
                shutil.copyfileobj(source, dest)
            archive.with_suffix('.gz.part').replace(archive)
        # Always verify the gzip stream and produce the raw file without editing any records.
        digest = hashlib.sha256()
        with gzip.open(archive, 'rb') as source, raw.with_suffix('.part').open('wb') as dest:
            while chunk := source.read(1024 * 1024):
                dest.write(chunk)
                digest.update(chunk)
        raw.with_suffix('.part').replace(raw)
        manifest['files'].append({'name': name, 'url': BASE_URL + name + '.gz',
                                  'raw_bytes': raw.stat().st_size, 'raw_sha256': digest.hexdigest()})
        print('Ready:', raw, flush=True)
    (data_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


class RawBatches:
    """Sequential raw lines, including malformed/blank lines. B spans file boundaries.

    A spool only joins unchanged records into engine input files. It never removes,
    filters or parses a record. A missing final LF gets a separator so two files'
    last/first records cannot be merged by Hadoop's line reader.
    """
    def __init__(self, paths, batch_size, staging):
        if batch_size < 1:
            raise ValueError('batch size must be positive')
        self.paths, self.batch_size, self.staging = list(map(Path, paths)), batch_size, Path(staging)
        self.sources = []

    def __iter__(self):
        self.staging.mkdir(parents=True, exist_ok=True)
        batch_id, count, dest = 1, 0, None
        try:
            for path in self.paths:
                digest, records = hashlib.sha256(), 0
                with path.open('rb') as source:
                    for line in source:
                        if dest is None:
                            spool = self.staging / f'batch-{batch_id:06d}.log'
                            dest = spool.open('wb')
                        digest.update(line)
                        records += 1
                        dest.write(line)
                        if not line.endswith(b'\n'):
                            dest.write(b'\n')
                        count += 1
                        if count == self.batch_size:
                            dest.close()
                            dest = None
                            yield batch_id, count, spool
                            batch_id += 1
                            count = 0
                self.sources.append({'name': path.name, 'raw_sha256': digest.hexdigest(),
                                     'records': records, 'raw_bytes': path.stat().st_size})
            if count:
                dest.close()
                dest = None
                yield batch_id, count, spool
        finally:
            if dest is not None:
                dest.close()
