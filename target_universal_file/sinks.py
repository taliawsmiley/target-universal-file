"""UniversalFile target sink class, which handles writing streams."""

from __future__ import annotations

from functools import cached_property
import sys
from pathlib import Path

from singer_sdk.sinks import BatchSink

import target_universal_file.filesystem as tuf_fs
import target_universal_file.writer as tuf_w


class UniversalFileSink(BatchSink):

    max_size = sys.maxsize  # All records in one batch.

    @property
    def filesystem_manager_type(self) -> type[tuf_fs.BaseFileSystemManager]:
        return tuf_fs.FileSystemManagerRegistry.get(self.config["protocol"])

    @cached_property
    def filesystem_manager(self) -> tuf_fs.BaseFileSystemManager:
        return self.filesystem_manager_type(stream=self)

    @property
    def writer_type(self) -> type[tuf_w.BaseWriter]:
        return tuf_w.WriterRegistry.get(self.config["file_type"])

    @property
    def file_name(self) -> str:
        return self.config["file_name_format"].format(
            stream_name=self.stream_name,
            file_type=self.config["file_type"],
        )

    @property
    def full_path(self) -> Path:
        return Path(self.config["path"], self.file_name)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        with (
            self.filesystem_manager.filesystem.transaction,
            self.writer_type.create_for_sink(stream=self) as w,
        ):
            w.write_records(context["records"])
