from io import TextIOWrapper
import os
from pathlib import Path
from contextlib import contextmanager


LOG = "log.txt"


class Logger:
    """Ease of use tool to read and append to log files"""

    def __init__(self, path: Path):
        self.path = path
        self.log_file: TextIOWrapper | None = None

    def get_log(self) -> list[str]:
        """Return all lines in the log file without '\n' line terminator"""
        try:
            with open(self.path / LOG, mode="r", encoding="utf-8") as log_file:
                return [line[:-1] for line in log_file.readlines()]
        except FileNotFoundError:
            return []

    @contextmanager
    def get_log_file(self):
        """Return a handler in append mode of the log file"""
        with open(self.path / LOG, mode="a", encoding="utf-8") as log_file:
            yield log_file

    def clear(self):
        """Clear all records from the log"""
        try:
            os.remove(self.path / LOG)
        except Exception:
            pass
