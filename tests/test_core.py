"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t
from pathlib import Path

import pytest

from target_universal_file.target import TargetUniversalFile


def build_config(protocol: str, file_type: str) -> dict[str, t.Any]:
    return {
        "protocol": protocol,
        "path": "tests/output",
        "file_type": file_type,
    }


def sink_file(input_path: Path, target: TargetUniversalFile) -> None:
    with Path.open(input_path) as file:
        target.listen(file)


def test_local_jsonl() -> None:
    config = build_config(protocol="local", file_type="jsonl")
    target = TargetUniversalFile(config=config)
    sink_file(Path("tests/data/appliances.singer"), target)


def test_local_csv() -> None:
    config = build_config(protocol="local", file_type="csv")
    target = TargetUniversalFile(config=config)
    sink_file(Path("tests/data/appliances.singer"), target)


@pytest.mark.xfail(reason="parquet objects not implemented")
def test_local_parquet() -> None:
    config = build_config(protocol="local", file_type="parquet")
    target = TargetUniversalFile(config=config)
    sink_file(Path("tests/data/appliances.singer"), target)
