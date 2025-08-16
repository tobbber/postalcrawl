import re
import time
from collections import defaultdict
from pprint import pprint

import polars as pl
import pytest
from pathlib import Path
import json

from postalcrawl.extract import extract_ld_json
from postalcrawl.preprocess import (
    jsonld_generator,
    generate_ld_json,
    record_generator,
    generate_html_responses,
    filter_postal_address,
    generate_deserialized_json,
    offline_record_generator,
    extract_address_element,
)


@pytest.fixture
def resources_dir():
    return Path(__file__).parent / "resources"

@pytest.fixture
def html_response(resources_dir) -> bytes:
    filepath = resources_dir / "formedge.com.my_shop-displays_unic-modular-glass-showcases.html"
    with open(filepath, "rb") as f:
        return f.read()

def test_ldjson_extract(html_response):
    items = list(extract_ld_json(html_response))
    assert len(items) ==1
    assert items[0].startswith('{"@context":"http:\\/\\/schema.org"')
    data = json.loads(items[0])
    assert isinstance(data, dict)


def sanitize_json_ld(raw_ld_json: str) -> str:
    """
    Sanitizes JSON-LD extracted from HTML by escaping unescaped line breaks inside string literals.
    Assumes double-quoted JSON (which is typical for JSON-LD).
    """
    # Regex to find line breaks inside quoted strings
    pattern = r'("(?:[^"\\]|\\.)*?)\n((?:[^"\\]|\\.)*?")'
    while re.search(pattern, raw_ld_json):
        raw_ld_json = re.sub(pattern, r'\1\\n\2', raw_ld_json)
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
    stats = defaultdict(int)
    file_id = "crawl-data/CC-MAIN-2025-26/segments/1749709481111.44/warc/CC-MAIN-20250612112840-20250612142840-00000.warc.gz"

    gen = record_generator(file_id, stats)
    gen = generate_html_responses(gen, stats)
    gen = generate_ld_json(gen, stats)
    gen = filter_postal_address(gen, stats)
    gen = generate_deserialized_json(gen, stats)

    gen = iter_to_n(gen, 100)
    assert len(list(gen)) == 100

def test_offline():
    filePath = Path("/Users/tobi/Uni/postalcrawlV2/data/CC-MAIN-20250612112840-20250612142840-00009.warc")
    assert filePath.exists()

    stats = defaultdict(int)
    gen = offline_record_generator(filePath, stats=stats)
    gen = generate_html_responses(gen , stats)
    gen = generate_ld_json(gen, stats)
    gen = filter_postal_address(gen, stats)
    gen = generate_deserialized_json(gen, stats)
    gen = extract_address_element(gen, stats)
    # gen = iter_to_n(gen, 10)
    # alle = list(gen)
    start_time = time.time()
    first = next(gen)
    try:
        for i, item in enumerate(gen):
            pass
    except Exception as e:
        raise e

    elapsed = time.time() - start_time
    assert elapsed < 1

    assert stats == {}
