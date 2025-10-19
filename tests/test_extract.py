import json
import time
from pathlib import Path

import polars as pl
import pytest
from tqdm import tqdm

from postalcrawl.extract.clean_address import _clean_address_fields
from postalcrawl.extract.extract import (
    StringRecord,
    extract_ld_json,
    extract_postal_addresses,
    filter_html_responses,
    extract_addresses,
)
from postalcrawl.extract.utils import parse_content_type
from postalcrawl.extract.warc_loaders import download_record_generator, offline_record_generator
from postalcrawl.models import PostalAddress
from postalcrawl.stats import StatCounter
from postalcrawl.utils import download_file_id, file_segment_info
from tests.conftest import read_file


@pytest.mark.dev
def test_online_record_generator():
    file_id = "crawl-data/CC-MAIN-2025-26/segments/1749709481111.44/warc/CC-MAIN-20250612112840-20250612142840-00000.warc.gz"
    stats = StatCounter()
    gen = download_record_generator(file_id, stats)
    rec1 = next(gen)
    rec2 = next(gen)
    rec3 = next(gen)
    rec4 = next(gen)
    rec5 = next(gen)
    assert rec1.rec_type == "warcinfo"
    assert rec2.rec_type == "request"
    assert rec3.rec_type == "response"
    assert rec4.rec_type == "metadata"
    assert rec5.rec_type == "request"


@pytest.fixture
def offline_warc_gen():
    filePath = Path(
        "/Users/tobi/Uni/postalcrawlV2/data/CC-MAIN-20250612112840-20250612142840-00009.warc.gz"
    )
    assert filePath.exists()
    stats = StatCounter()
    gen = offline_record_generator(filePath, stats)
    return gen


def test_offline_record_generator(offline_warc_gen):
    gen = offline_warc_gen
    rec1 = next(gen)
    rec2 = next(gen)
    rec3 = next(gen)
    rec4 = next(gen)
    rec5 = next(gen)
    assert rec1.rec_type == "warcinfo"
    assert rec2.rec_type == "request"
    assert rec3.rec_type == "response"
    assert rec4.rec_type == "metadata"
    assert rec5.rec_type == "request"


def test_filter_html_responses(offline_warc_gen):
    gen = offline_warc_gen
    gen = filter_html_responses(gen, StatCounter())
    rec1 = next(gen)
    rec2 = next(gen)
    rec3 = next(gen)
    assert "text" in rec1.http_headers.get_header("Content-Type")
    assert "text" in rec2.http_headers.get_header("Content-Type")
    assert "text" in rec3.http_headers.get_header("Content-Type")
    assert rec1.rec_type == "response"
    assert rec2.rec_type == "response"
    assert rec3.rec_type == "response"


def test_extract_ldjson(resources_dir):
    content = read_file(resources_dir / "response.1.html")
    record = StringRecord(
        content=content,
        url="https://example.com",
        warc_date="2023-10-01T00:00:00Z",
        warc_rec_id="12345",
        charset="utf-8",
    )
    gen = iter([record])
    out = next(extract_ld_json(gen, StatCounter()))
    assert out.content[:65] == '{"@context":"http:\\/\\/schema.org","@type":"LocalBusiness","name":'


def test_extract_addresses(resources_dir):
    content = read_file(resources_dir / "ldjson.1.json")
    record = StringRecord(
        content=content,
        url="https://example.com",
        warc_date="2023-10-01T00:00:00Z",
        warc_rec_id="12345",
        charset="utf-8",
    )
    gen = iter([record])
    out = list(extract_postal_addresses(gen, StatCounter()))
    assert len(out) == 2
    assert out[0].locality == "Sukabumi"
    assert out[0].name is None
    assert out[1].locality == "Sukabumi"
    assert out[1].name == "Loker Tribun"


@pytest.mark.dev
def test_offline_DEV(resources_dir):
    file_id = "crawl-data/CC-MAIN-2025-26/segments/1749709481111.44/warc/CC-MAIN-20250612112840-20250612142840-00000.warc.gz"
    file_path = resources_dir / file_id.split("/")[-1]
    segment, seg_num = file_segment_info(file_id)
    if not file_path.exists():
        download_file_id(file_id, file_path)

    assert file_path.is_file(), f"File {file_path} does not exist."

    start_time = time.time()
    stats = StatCounter()
    gen = offline_record_generator(file_path, stats)
    gen = extract_addresses(gen, stats)
    gen = tqdm(gen)

    df = pl.DataFrame(gen)
    parquet_file = resources_dir / segment / f"{seg_num}.parquet"
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_file, compression="brotli")

    elapsed = time.time() - start_time
    assert elapsed == 0


def test_parse_content_type():
    assert parse_content_type("text/html; charset=UTF-8") == ("text/html", "utf-8")
    assert parse_content_type("text/html; charset=utf-8") == ("text/html", "utf-8")
    assert parse_content_type(None) == ("", None)


def test_extract_addresses_from_ldjson(resources_dir):
    content = read_file(resources_dir / "ldjson.1.json")
    record = StringRecord(
        content=content,
        url="https://example.com",
        warc_date="2023-10-01T00:00:00Z",
        warc_rec_id="12345",
        charset="utf-8",
    )

    gen = iter([record])
    out = extract_postal_addresses(gen, StatCounter())
    assert out


def test_clean_address_fields(resources_dir):
    record = PostalAddress(
        name=" test\nName,",
        street=["test &amp; Street", 27],  # type: ignore
        locality="Potsdam\\/P",
        region="Brandenb\u00fcrg",
        postalCode=12345,  # type: ignore
        country={"@type": "Country", "name": ["Germany"]},  # type: ignore
        url="https://example.com",
        warc_date="2023-10-01T00:00:00Z",
        warc_rec_id="12345",
    )

    gen = iter([record])
    out = next(_clean_address_fields(gen, StatCounter()))
    assert out.name == "test Name,"
    assert out.street == "test & Street, 27"
    assert out.locality == "Potsdam/P"
    assert out.region == "Brandenbürg"
    assert out.postalCode == "12345"
    assert out.country == "Germany"


def test_serialize_stats():
    stats = StatCounter()
    stats.inc("foo")
    assert json.dumps(stats) == "foo"
