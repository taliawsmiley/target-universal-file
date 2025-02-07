from __future__ import annotations

import csv
import simplejson as json
import typing as t
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import decimal

import pyarrow as pa
import pyarrow.parquet as pq

if t.TYPE_CHECKING:
    import target_universal_file.sinks as tuf_s


class WriterRegistry(ABCMeta):

    _registry: t.ClassVar[dict[str, type[BaseWriter]]] = {}

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        dct: dict[str, t.Any],
    ) -> type[BaseWriter]:
        new_cls = super().__new__(cls, name, bases, dct)
        if new_cls.__name__ == "BaseWriter":
            return new_cls
        if new_cls.file_type not in cls._registry:
            cls._registry[new_cls.file_type] = new_cls
        return new_cls

    @classmethod
    def file_types(cls) -> list[str]:
        return list(cls._registry.keys())

    @classmethod
    def get(cls, file_type: str) -> type[BaseWriter]:
        if file_type not in cls._registry:
            error_msg = f"Writer for file type {file_type} not found."
            raise ValueError(error_msg)
        return cls._registry[file_type]


class BaseWriter(metaclass=WriterRegistry):

    open_mode = "wt"
    file_type: str

    def __init__(self, stream: tuf_s.UniversalFileSink) -> None:
        self.config = stream.config
        self.schema = stream.schema
        self.logger = stream.logger
        self.file = stream.filesystem_manager.filesystem.open(
            stream.full_path, self.open_mode
        )

    def write_records(self, records: t.Iterable[dict]) -> None:
        for record in records:
            self.write_record(record)

    @abstractmethod
    def write_record(self, record: dict) -> None:
        error_msg = "write_record must be implemented by a subclass."
        raise NotImplementedError(error_msg)

    @classmethod
    @contextmanager
    def create_for_sink(
        cls: type[BaseWriter], stream: tuf_s.UniversalFileSink
    ) -> t.Generator[BaseWriter, None, None]:
        writer = cls(stream)

        try:
            writer.write_begin()
            yield writer
            writer.write_end()
        finally:
            writer.cleanup()

    def write_begin(self) -> None:
        pass

    def write_end(self) -> None:
        pass

    def cleanup(self) -> None:
        self.file.close()


class CSVWriter(BaseWriter):

    file_type = "csv"

    def __init__(self, stream: tuf_s.UniversalFileSink) -> None:
        super().__init__(stream)
        properties: dict = self.schema["properties"]
        self.csv_dict_writer = csv.DictWriter(f=self.file, fieldnames=properties.keys())

    def write_begin(self) -> None:
        self.csv_dict_writer.writeheader()

    def write_record(self, record: dict) -> None:
        self.csv_dict_writer.writerow(record)

class JSONLWriter(BaseWriter):

    file_type = "jsonl"

    def write_record(self, record: dict) -> None:
        json.dump(record, self.file)
        self.file.write("\n")


class ParquetWriter(BaseWriter):

    open_mode = "wb"
    file_type = "parquet"

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
            if items is None or len(items) != 1:
                error_msg = "arrays must contain values of exactly one type."
                raise RuntimeError(error_msg)
            return pa.list_(self._parquet_type(items))
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
