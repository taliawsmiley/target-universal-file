"""UniversalFile target sink class, which handles writing streams."""

from __future__ import annotations
from functools import cached_property
from pathlib import Path
from abc import ABCMeta, abstractmethod
import sys
import typing as t

from singer_sdk.sinks import BatchSink
import target_universal_file.filesystem as tuf_fs
import target_universal_file.writer as tuf_w

class UniversalFileSink(BatchSink, metaclass=ABCMeta):

    max_size = sys.maxsize  # All records in one batch.

    @cached_property
    def filesystem_manager(self) -> tuf_fs.FileSystemManager:
        return tuf_fs.FileSystemManager.create_for_sink(stream=self)

    @property
    @abstractmethod
    def file_name(self) -> str:
        pass

    @property
    def full_path(self) -> Path:
        return Path(self.config["filesystem"]["path"], self.file_name)

    @property
    @abstractmethod
    def writer(self) -> type[tuf_w.Writer]:
        pass

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        with self.filesystem_manager.filesystem.transaction:
            with self.writer.create_for_sink(stream=self) as w:
                w: tuf_w.Writer
                w.write_records(context["records"])


class CSVSink(UniversalFileSink):

    writer = tuf_w.CSVWriter

    @property
    def file_name(self):
        return f"{self.stream_name}.csv"


class JSONLSink(UniversalFileSink):

    writer = tuf_w.JSONLWriter

    @property
    def file_name(self):
        return f"{self.stream_name}.jsonl"


class ParquetSink(UniversalFileSink):

    writer = tuf_w.ParquetWriter

    @property
    def file_name(self):
        return f"{self.stream_name}.parquet"
