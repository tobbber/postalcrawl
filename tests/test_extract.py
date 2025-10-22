from pathlib import Path

import pytest

from postalcrawl.extract.extract import extract_pipeline
from postalcrawl.extract.warc_loaders import offline_record_generator
from postalcrawl.stats import StatCounter


@pytest.fixture
def offline_warc_file():
    return (
        Path(__file__).parent / "resources" / "CC-MAIN-20250612112840-20250612142840-00000.warc.gz"
    )


def test_extract(offline_warc_file):
    stats = StatCounter()
    gen = offline_record_generator(offline_warc_file, stats)
    gen = extract_pipeline(gen, stats)
    assert next(gen)["data"]
