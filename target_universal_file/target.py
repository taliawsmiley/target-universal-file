"""UniversalFile target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

import target_universal_file.filesystem as tuf_fs
import target_universal_file.sinks as tuf_s
import target_universal_file.writer as tuf_w


class TargetUniversalFile(Target):
    """Target for UniversalFile."""

    name = "target-universal-file"
    default_sink_class = tuf_s.UniversalFileSink

    config_jsonschema = th.PropertiesList(
        th.Property(
            "protocol",
            th.StringType,
            required=True,
            allowed_values=tuf_fs.FileSystemManagerRegistry.protocols(),
        ),
        th.Property(
            "file_type",
            th.StringType,
            required=True,
            allowed_values=tuf_w.WriterRegistry.file_types(),
        ),
        th.Property(
            "path",
            th.StringType,
            required=True,
        ),
        th.Property(
            "file_name_format",
            th.StringType,
            required=True,
            default="{stream_name}.{file_type}",
        ),
        th.Property(
            "protocol_options",
            th.ObjectType(
                additional_properties=True,
            ),
            required=False,
        ),
        th.Property(
            "file_type_options",
            th.ObjectType(
                additional_properties=True,
            ),
            required=False,
        ),
    ).to_dict()


if __name__ == "__main__":
    TargetUniversalFile.cli()
