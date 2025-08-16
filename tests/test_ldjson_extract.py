import re
import time
from pathlib import Path

from postalcrawl.preprocess import (
    StringExtract,
    download_record_generator,
    extract_addresses,
    extract_postal_addresses,
    offline_record_generator,
    parse_content_type,
)
from postalcrawl.stat_counter import StatCounter
from tests.conftest import read_file


def sanitize_json_ld(raw_ld_json: str) -> str:
    """
    Sanitizes JSON-LD extracted from HTML by escaping unescaped line breaks inside string literals.
    Assumes double-quoted JSON (which is typical for JSON-LD).
    """
    # Regex to find line breaks inside quoted strings
    pattern = r'("(?:[^"\\]|\\.)*?)\n((?:[^"\\]|\\.)*?")'
    while re.search(pattern, raw_ld_json):
        raw_ld_json = re.sub(pattern, r"\1\\n\2", raw_ld_json)
    return raw_ld_json


def iter_to_n(iterable, n: int):
    """
    Returns the first n items from an iterable.
    """
    for i, item in enumerate(iterable):
        if i >= n:
            break
        yield item


def test_online():
    stats = StatCounter()
    file_id = "crawl-data/CC-MAIN-2025-26/segments/1749709481111.44/warc/CC-MAIN-20250612112840-20250612142840-00000.warc.gz"

    gen = download_record_generator(file_id, stats)
    gen = extract_addresses(gen, stats)

    gen = iter_to_n(gen, 100)
    assert len(list(gen)) == 100


def test_offline():
    filePath = Path(
        "/Users/tobi/Uni/postalcrawlV2/data/CC-MAIN-20250612112840-20250612142840-00009.warc"
    )
    assert filePath.exists()
    stats = StatCounter()
    start_time = time.time()

    gen = offline_record_generator(filePath, stats)

    gen = extract_addresses(gen, stats)
    out = list(gen)
    elapsed = time.time() - start_time
    assert len(out) == 100
    assert elapsed == 0


def test_parse_content_type():
    assert parse_content_type("text/html; charset=UTF-8") == ("text/html", "utf-8")
    assert parse_content_type("text/html; charset=utf-8") == ("text/html", "utf-8")
    assert parse_content_type(None) == ("", None)


def test_extract_addresses_from_ldjson(resources_dir):
    content = read_file(resources_dir / "ldjson.1.json")
    record = StringExtract(
        content=content,
        url="https://example.com",
        warc_date="2023-10-01T00:00:00Z",
        warc_rec_id="12345",
        charset="utf-8",
    )

    gen = iter([record])
    out = extract_postal_addresses(gen, StatCounter())
    assert out
