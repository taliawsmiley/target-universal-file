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
from target_universal_file.writer import Writer, CSVWriter, JSONLWriter

class UniversalFileSink(BatchSink, metaclass=ABCMeta):

    max_size = sys.maxsize  # All records in one batch.

    @cached_property
    def filesystem_manager(self) -> FileSystemManager:
        return FileSystemManager.create_for_stream(stream=self)

    @property
    @abstractmethod
    def file_name(self) -> str:
        pass

    @property
    def full_path(self) -> Path:
        return Path(self.config["filesystem"]["path"], self.file_name)

    @abstractmethod
    def create_writer(self, f: t.IO) -> Writer:
        pass

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        with self.filesystem_manager.filesystem.transaction:
            with open(self.full_path, "wt") as f:
                writer = self.create_writer(f)
                writer.write_begin()
                for record in context["records"]:
                    writer.write_record(record)
                writer.write_end()


class CSVSink(UniversalFileSink):

    @property
    def file_name(self):
        return f"{self.stream_name}.csv"

    def create_writer(self, f: t.IO) -> CSVWriter:
        return CSVWriter(f=f, fieldnames=self.schema["properties"].keys())


class JSONLSink(UniversalFileSink):

    @property
    def file_name(self):
        return f"{self.stream_name}.jsonl"

    def create_writer(self, f: t.IO) -> JSONLWriter:
        return JSONLWriter(f=f)