#!/usr/bin/env python3
"""Smoke tests."""
import pytest

from surfexp.cli import pysfxexp


@pytest.fixture(scope="module")
def tmp_directory(tmp_path_factory):
    """Return a temp directory valid for this module."""
    return tmp_path_factory.getbasetemp().as_posix()


def test_pysfxexp(tmp_directory):
    argv = [f"{tmp_directory}/out.toml", "name"]
    pysfxexp(argv=argv)
