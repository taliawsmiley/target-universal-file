"""UniversalFile target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_universal_file.sinks import (
    CSVSink,
)


class TargetUniversalFile(Target):
    """Sample target for UniversalFile."""

    name = "target-universal-file"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "filesystem",
            th.ObjectType(
                th.Property(
                    "protocol",
                    th.StringType,
                    required=True,
                ),
                th.Property(
                    "path",
                    th.StringType,
                    required=True,
                ),
            )
        )
    ).to_dict()

    default_sink_class = CSVSink


if __name__ == "__main__":
    TargetUniversalFile.cli()
