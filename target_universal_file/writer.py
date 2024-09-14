from abc import ABCMeta, abstractmethod
import csv
import json
import typing as t

class Writer(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, f: t.IO, **kwargs) -> None:
        pass

    @abstractmethod
    def write_begin(self, writer) -> None:
        pass

    @abstractmethod
    def write_record(self, writer, record: dict) -> None:
        pass

    @abstractmethod
    def write_end(self, writer) -> None:
        pass

class CSVWriter(Writer):

    def __init__(self, f: t.IO, **kwargs):
        self.csv_dict_writer = csv.DictWriter(f=f, **kwargs)

    def write_begin(self) -> None:
        self.csv_dict_writer.writeheader()

    def write_record(self, record: dict) -> None:
        self.csv_dict_writer.writerow(record)

    def write_end(self) -> None:
        pass

class JSONLWriter(Writer):

    def __init__(self, f: t.IO, **kwargs):
        self.f = f
        self.kwargs = kwargs

    def write_begin(self) -> None:
        pass

    def write_record(self, record: dict) -> None:
        json.dump(record, self.f, **self.kwargs)
        self.f.write("\n")

    def write_end(self) -> None:
        pass