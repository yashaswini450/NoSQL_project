"""
pipelines/base.py — Strategy-pattern contract every engine implements.
(Agree the final version with Member 2, who owns Board 1. main.py only
depends on these method names.)
"""

from abc import ABC, abstractmethod


class BaseETLPipeline(ABC):
    name = "base"

    def setup(self, run_id):
        """Connect to the engine and clear staging for this run_id.
        Called BEFORE the runtime timer starts (setup is not ETL work)."""
        self.run_id = run_id

    @abstractmethod
    def process_batch(self, batch_id, raw_lines):
        """Parse + transform + aggregate one batch of RAW lines inside the engine.
        Parsing must happen here (in the engine), not in main.py.
        Must return the number of malformed lines in this batch."""

    @abstractmethod
    def finalize(self):
        """Produce final Q1/Q2/Q3 results across all batches.
        Returns dict: {"q1": [...rows], "q2": [...rows], "q3": [...rows]}"""

    def teardown(self):
        """Close connections. Called after the timer stops."""
