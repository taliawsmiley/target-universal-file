from __future__ import annotations
from functools import cached_property
import logging

import fsspec
from abc import ABCMeta, abstractmethod
import typing as t

if t.TYPE_CHECKING:
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
    def create_for_stream(cls, stream: UniversalFileSink) -> FileSystemManager:
        config = stream.config
        protocol = config["filesystem"]["protocol"]
        if protocol == "local":
            return LocalFileSystemManager(
                config=stream.config,
                logger=stream.logger
            )

class LocalFileSystemManager(FileSystemManager):

    @cached_property
    def filesystem(self):
        return fsspec.filesystem("local", auto_mkdir=True)
