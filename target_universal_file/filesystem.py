from __future__ import annotations

import typing as t
from abc import ABCMeta, abstractmethod
from functools import cached_property

import fsspec

if t.TYPE_CHECKING:
    import logging

    from target_universal_file.sinks import UniversalFileSink


class FileSystemManager(metaclass=ABCMeta):

    def __init__(self, config: dict, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    @cached_property
    @abstractmethod
    def filesystem(self) -> fsspec.AbstractFileSystem:
        pass

    @classmethod
    def create_for_sink(cls, stream: UniversalFileSink) -> FileSystemManager:
        protocol = stream.config["filesystem"]["protocol"]
        if protocol == "local":
            return LocalFileSystemManager(config=stream.config, logger=stream.logger)
        error_msg = f"Protocol '{protocol}' not recognized."
        raise RuntimeError(error_msg)


class LocalFileSystemManager(FileSystemManager):

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem("local", auto_mkdir=True)
