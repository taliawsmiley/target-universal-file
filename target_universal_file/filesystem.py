from __future__ import annotations

import typing as t
from abc import ABCMeta
from functools import cached_property

import fsspec

if t.TYPE_CHECKING:
    from target_universal_file.sinks import UniversalFileSink


class FileSystemManagerRegistryMeta(ABCMeta):

    registry: dict[str, type[BaseFileSystemManager]] = {}

    def __new__(cls, name, bases, dct):
        new_cls = super().__new__(cls, name, bases, dct)
        if new_cls.__name__ == "BaseFileSystemManager":
            return new_cls
        if new_cls.protocol not in cls.registry:
            cls.registry[new_cls.protocol] = new_cls
        return new_cls


class BaseFileSystemManager(metaclass=FileSystemManagerRegistryMeta):

    protocol: str

    def __init__(self, stream: UniversalFileSink) -> None:
        self.config = stream.config
        self.logger = stream.logger

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.protocol, auto_mkdir=True)

class LocalFileSystemManager(BaseFileSystemManager):

    protocol = "local"
