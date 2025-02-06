"""UniversalFile target sink class, which handles writing streams."""

from __future__ import annotations

import sys
from abc import ABCMeta, abstractmethod
from functools import cached_property
from pathlib import Path

from singer_sdk.sinks import BatchSink

import target_universal_file.filesystem as tuf_fs
import target_universal_file.writer as tuf_w


class UniversalFileSink(BatchSink):

    max_size = sys.maxsize  # All records in one batch.

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.file_type = self.config["format"]["type"]
        self.protocol = self.config["filesystem"]["protocol"]

    @property
    def filesystem_manager_type(self) -> type[tuf_fs.BaseFileSystemManager]:
        return tuf_fs.FileSystemManagerRegistryMeta.registry[self.protocol]

    @property
    def writer_type(self) -> type[tuf_w.BaseWriter]:
        return tuf_w.WriterRegistryMeta.registry[self.file_type]

    @property
    def file_name(self) -> str:
        return f"{self.stream_name}.{self.file_type}"

    @property
    def full_path(self) -> Path:
        return Path(self.config["filesystem"]["path"], self.file_name)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        filesystem_manager = self.filesystem_manager_type(stream=self)
        with (
            filesystem_manager.filesystem.transaction,
            self.writer_type.create_for_sink(stream=self) as w,
        ):
            w.write_records(context["records"])
