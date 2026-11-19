"""
batcher.py — Sequential batch slicer (Member 1).

Batch size B = number of RAW INPUT LOG LINES in one batch (spec definition).
  * batch_id starts at 1 and increases by 1: 1, 2, ..., k
  * last batch may have < B lines and still counts as a valid batch
  * every line counts as a record, including lines that later turn out malformed
    (a malformed line was still read and processed, so it belongs to a batch)
  * average_batch_size = total_records_processed / number_of_non_empty_batches

Example: 1,891,715 lines with B = 100,000
  k = ceil(1,891,715 / 100,000) = 19 batches
  batches 1..18 have 100,000 lines, batch 19 has 91,715 lines
  avg = 1,891,715 / 19 = 99,563.95 (NOT 100,000)
"""

from dataclasses import dataclass, field
from typing import Iterator, List


@dataclass
class Batch:
    batch_id: int
    lines: List[str]

    @property
    def size(self):
        return len(self.lines)


@dataclass
class BatchStats:
    total_records: int = 0
    num_batches: int = 0                         # non-empty batches only
    sizes: List[int] = field(default_factory=list)

    def record(self, batch):
        self.total_records += batch.size
        self.num_batches += 1
        self.sizes.append(batch.size)

    @property
    def average_batch_size(self):
        if self.num_batches == 0:
            return 0.0                           # empty input: avoid divide-by-zero
        return self.total_records / self.num_batches


def _iter_lines(paths):
    """Chain all input files into ONE continuous stream of lines."""
    for path in paths:
        with open(path, "r", encoding="latin-1", newline="\n") as f:
            for line in f:
                yield line


def iter_batches(paths, batch_size) -> Iterator[Batch]:
    """
    Stream the file(s) line by line and yield Batch objects.
    paths may be one path or a list (Jul95 + Aug95). Files are chained, so a
    batch can span the file boundary and ONLY the very last batch of the run
    can be short (otherwise Jul95's tail would be a 2nd partial batch).
    Never loads the whole file into memory (Jul95 is ~200 MB uncompressed);
    at most B lines are held at a time.

    encoding='latin-1': maps every byte 0x00-0xFF to a character, so a stray
    non-ASCII byte in the NASA trace can never crash the reader. Such lines
    simply flow to the parser, which marks them malformed if they don't match.
    newline='\n': ONLY '\n' ends a line, so a stray '\r' inside a line can't
    split it into two records (line count = what `wc -l` reports).
    """
    if batch_size <= 0:
        raise ValueError("batch size must be a positive integer")

    if isinstance(paths, str):
        paths = [paths]
    batch_id = 1
    buf = []
    for line in _iter_lines(paths):
        buf.append(line)
        if len(buf) == batch_size:
            yield Batch(batch_id, buf)
            batch_id += 1
            buf = []
    if buf:                                      # final partial batch (< B lines)
        yield Batch(batch_id, buf)
    # if buf is empty here, no empty trailing batch is produced
