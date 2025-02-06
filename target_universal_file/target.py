"""UniversalFile target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

import target_universal_file.sinks as tuf_s
import target_universal_file.filesystem as tuf_fs
import target_universal_file.writer as tuf_w


class TargetUniversalFile(Target):
    """Target for UniversalFile."""

    name = "target-universal-file"
    default_sink_class = tuf_s.UniversalFileSink

    config_jsonschema = th.PropertiesList(
        th.Property(
            "filesystem",
            th.ObjectType(
                th.Property(
                    "protocol",
                    th.StringType,
                    required=True,
                    allowed_values=list(tuf_fs.BaseFileSystemManager.registry.keys()),
                ),
                th.Property(
                    "path",
                    th.StringType,
                    required=True,
                ),
            ),
            required=True,
        ),
        th.Property(
            "format",
            th.ObjectType(
                th.Property(
                    "type",
                    th.StringType,
                    required=True,
                    allowed_values=list(tuf_w.WriterRegistryMeta.registry.keys()),
                ),
            ),
            required=True,
        ),
    ).to_dict()


if __name__ == "__main__":
    TargetUniversalFile.cli()
