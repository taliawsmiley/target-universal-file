"""UniversalFile target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

import target_universal_file.sinks as tuf_s


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

    def get_sink_class(
        self, stream_name: str  # noqa: ARG002
    ) -> type[TargetUniversalFile]:

        file_type = self.config["format"]["type"]

        if file_type == "csv":
            return tuf_s.CSVSink
        if file_type == "jsonl":
            return tuf_s.JSONLSink
        if file_type == "parquet":
            return tuf_s.ParquetSink
        error_msg = f"File type '{file_type}' not recognized."
        raise RuntimeError(error_msg)


if __name__ == "__main__":
    TargetUniversalFile.cli()
