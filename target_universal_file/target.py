"""UniversalFile target class."""

from __future__ import annotations

from singer_sdk import Sink, typing as th
from singer_sdk.target_base import Target

from target_universal_file.sinks import (
    CSVSink,
    JSONLSink
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
                ),
            ),
            required=True,
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> type[TargetUniversalFile]:

        file_type = self.config["format"]["type"]

        if file_type == "csv":
            return CSVSink
        if file_type == "jsonl":
            return JSONLSink


if __name__ == "__main__":
    TargetUniversalFile.cli()
