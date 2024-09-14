"""UniversalFile target sink class, which handles writing streams."""

from __future__ import annotations
import csv
from functools import cached_property
from pathlib import Path
from abc import ABCMeta, abstractmethod
import sys
import typing as t

from singer_sdk.sinks import BatchSink
from target_universal_file.filesystem import FileSystemManager

class UniversalFileSink(BatchSink, metaclass=ABCMeta):

    max_size = sys.maxsize  # All records in one batch.

    @cached_property
    def filesystem_manager(self) -> FileSystemManager:
        return FileSystemManager.create_for_stream(stream=self)

    @property
    @abstractmethod
    def file_name(self) -> str:
        pass

    def full_path(self) -> Path:
        return Path(self.config["filesystem"]["path"], self.file_name)

    @abstractmethod
    def create_writer(self, f: t.IO):
        pass

    @abstractmethod
    def write_begin(self, writer) -> None:
        pass

    @abstractmethod
    def write_record(self, writer, record: dict) -> None:
        pass

    @abstractmethod
    def write_end(self, writer) -> None:
        pass

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        with self.filesystem_manager.filesystem.transaction:
            with open(self.full_path, "wt") as f:
                writer = self.create_writer(f)
                self.write_begin(writer)
                for record in context["records"]:
                    self.write_record(writer, record)
                self.write_end(writer)


class CSVSink(UniversalFileSink):
    """UniversalFile target sink class."""

    @property
    def file_name(self):
        return f"{self.stream_name}.csv"

    def create_writer(self, f: t.IO) -> csv.DictWriter:
        return csv.DictWriter(f, fieldnames=self.schema.keys())

    def write_begin(self, writer: csv.DictWriter) -> None:
        writer.writeheader()

    def write_record(self, writer: csv.DictWriter, record: dict) -> None:
        writer.writerow(record)

    def write_end(self, writer: csv.DictWriter) -> None:
        pass