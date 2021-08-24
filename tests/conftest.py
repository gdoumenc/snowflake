import contextlib
import os
import pytest
import socket
from aws_xray_sdk import core as xray_core
from unittest.mock import MagicMock


def fixture_example_dir():
    return os.getenv('EXAMPLE_DIR')


def fixture_samples_docs_dir():
    return os.getenv('SAMPLES_DOCS_DIR')


@pytest.fixture
def example_dir():
    yield fixture_example_dir()


@pytest.fixture
def samples_docs_dir():
    yield fixture_samples_docs_dir()


@pytest.fixture
def unused_tcp_port():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(('localhost', 0))
        return sock.getsockname()[1]


def pytest_sessionstart():
    if not os.path.exists(fixture_samples_docs_dir()):
        msg = "Undefined samples folder: (var defined in pytest.ini)."
        raise pytest.UsageError(msg)
    if not os.path.exists(fixture_example_dir()):
        msg = "Undefined example folder: (var defined in pytest.ini)."
        raise pytest.UsageError(msg)

    # mock aws xray
    xray_core.recorder.capture = lambda _: lambda y: y
    xray_core.recorder.current_subsegment = lambda: MagicMock()
