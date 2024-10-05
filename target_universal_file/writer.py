from __future__ import annotations

import csv
import json
import typing as t
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

if t.TYPE_CHECKING:
    import target_universal_file.sinks as tuf_s


class Writer(metaclass=ABCMeta):

    open_mode = "wt"

    def __init__(self, stream: tuf_s.UniversalFileSink) -> None:
        self.config = stream.config
        self.schema = stream.schema
        self.logger = stream.logger
        self.file = Path(stream.full_path).open(self.open_mode)  # noqa: SIM115

    def cleanup(self) -> None:
        self.file.close()

    @classmethod
    @contextmanager
    def create_for_sink(
        cls: Writer, stream: tuf_s.UniversalFileSink
    ) -> t.Generator[Writer, None, None]:
        writer = cls(stream)

        try:
            writer.write_begin()
            yield writer
            writer.write_end()
        finally:
            writer.cleanup()

    def write_records(self, records: t.Iterable[dict]) -> None:
        for record in records:
            self.write_record(record)

    def write_begin(self) -> None:  # noqa: B027
        pass

    @abstractmethod
    def write_record(self, record: dict) -> None:
        pass

    def write_end(self) -> None:  # noqa: B027
        pass


class CSVWriter(Writer):

    def __init__(self, stream: tuf_s.UniversalFileSink) -> None:
        super().__init__(stream)
        self.csv_dict_writer = csv.DictWriter(
            f=self.file, fieldnames=self.schema["properties"].keys()
        )

    def write_begin(self) -> None:
        self.csv_dict_writer.writeheader()

    def write_record(self, record: dict) -> None:
        self.csv_dict_writer.writerow(record)


class JSONLWriter(Writer):

    def write_record(self, record: dict) -> None:
        json.dump(record, self.file)
        self.file.write("\n")


class ParquetWriter(Writer):

    open_mode = "wb"

    def __init__(self, stream: tuf_s.UniversalFileSink) -> None:
        super().__init__(stream)
        self.records = []

    def write_record(self, record: dict) -> None:
        self.records.append(record)

    def write_end(self) -> None:
        table = pa.Table.from_pylist(self.records, schema=self._parquet_schema)
        pq.write_table(table, self.file)

    def _parquet_type(self, property_dict: dict) -> pa.DataType:
        simple_types = {
            "integer": pa.int64(),
            "number": pa.float64(),
            "string": pa.string(),
            "boolean": pa.bool_(),
        }
        jsonschema_type = property_dict.get("type")
        if jsonschema_type in simple_types:
            return simple_types[jsonschema_type]
        if jsonschema_type == "array":
            items = property_dict.get("items")
            if len(items) != 1:
                error_msg = "arrays must contain values of exactly one type."
                raise RuntimeError(error_msg)
            return pa.array(self._parquet_type(self._parquet_type({"type": items[0]})))
        if jsonschema_type == "object":
            error_msg = "objects for parquet haven't been implemented yet."
            raise NotImplementedError(error_msg)
        if jsonschema_type is None:
            error_msg = f"jsonschema type not found for property: {property_dict}"
            raise ValueError(error_msg)
        error_msg = (
            f"jsonschema type of {jsonschema_type} not recognized for property: "
            f"{property_dict}"
        )
        raise ValueError(error_msg)

    @property
    def _parquet_schema(self) -> pa.Schema:
        fields = []
        for field_name, property_dict in self.schema.get("properties", {}).items():
            field_type = self._parquet_type(property_dict=property_dict)
            field = pa.field(field_name, field_type)
            fields.append(field)
        return pa.schema(fields)
