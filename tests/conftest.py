from pathlib import Path

import pytest


def read_file(path: Path):
    with open(path, "r") as f:
        return f.read()


@pytest.fixture
def resources_dir():
    return Path(__file__).parent / "resources"
