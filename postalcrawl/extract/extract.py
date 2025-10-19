import dataclasses
import sys
from typing import Any, Callable, Iterable, Iterator

import msgspec
from loguru import logger
from parsel import Selector
from warcio.recordloader import ArcWarcRecord

from postalcrawl.extract.utils import parse_content_type
from postalcrawl.models import CrawlMetadata, DictRecord, StringRecord
from postalcrawl.stats import StatCounter

logger.remove()
logger.add(sys.stdout, level="INFO")


def filter_html_responses(
    record_generator: Iterable[ArcWarcRecord], stats: StatCounter
) -> Iterator[ArcWarcRecord]:
    """
    Filter WARC records to only include HTTP responses with HTML or XML content types.

    input: WARC records including requests, responses and metadata of any type.
    output: only WARC HTTP response records with HTML or XML content types.
    """
    for record in record_generator:
        if record.rec_type != "response":
            continue
        stats.inc("warc/response")

        content_type = record.http_headers.get_header("Content-Type")
        if content_type is None:
            stats.inc(f"warc/content_type/{None}")
            continue
        media_type, charset = parse_content_type(content_type)
        stats.inc(f"warc/content_type/{media_type}")

        # todo: maybe just check for content type contains text?
        not_html = "html" not in media_type
        not_xml = "xml" not in media_type
        if not_html and not_xml:
            continue
        stats.inc("warc/html_response")
        yield record


def extractor_response_content(
    response_generator: Iterable[ArcWarcRecord], stats: StatCounter
) -> Iterator[StringRecord]:
    """
    Extract and decode the content of WARC HTTP response records.

    input: WARC HTTP response records.
    output: string containing the decoded response content + response metadata.
    """
    for record in response_generator:
        content_type = record.http_headers.get_header("Content-Type")
        media_type, charset = parse_content_type(content_type)
        stats.inc(f"response/charset/{charset or None}")

        raw_content: bytes = record.content_stream().read()
        try:
            content = raw_content.decode(charset or "utf-8", errors="replace")
        except LookupError:  # likely invalid charset, fallback to utf-8
            stats.inc(f"error/charset_unknown/{charset}")
            content = raw_content.decode("utf-8", errors="replace")
        metadata = CrawlMetadata(
            url=record.rec_headers.get_header("WARC-Target-URI"),
            warc_rec_id=record.rec_headers.get_header("WARC-Record-ID"),
            warc_date=record.rec_headers.get_header("WARC-Date"),
        )
        yield content, metadata


def extract_ld_json(
    response_generator: Iterable[StringRecord], stats: StatCounter
) -> Iterator[StringRecord]:
    """
    Extract JSON-LD scripts from HTML content.
    input: Full Html response.
    output: Only the response JSON-LD data: the content of <script type="application/ld+json">...</script> tags.
    """
    for content, metadata in response_generator:
        try:
            ld_jsons = (
                Selector(text=content)
                .xpath("//script[@type='application/ld+json']/text()")
                .getall()
            )
        except ValueError:
            logger.debug(f"Failed to parse content as HTML: {content[:40]}...")
            stats.inc("error/parsel/not_html")
            continue
        for ld_json in ld_jsons:
            yield ld_json, metadata


def deserialize_json_records(
    records: Iterable[StringRecord], stats: StatCounter
) -> Iterator[DictRecord]:
    for content, metadata in records:
        try:
            deserialized: dict = msgspec.json.decode(content)
            yield deserialized, metadata
        except msgspec.DecodeError as e:
            logger.debug(f"Failed to load as JSON with error: {e}\n{content[:60]}")
            stats.inc("error/json/decode_error")
            continue


def deserialize_json(records: Iterable[StringRecord], stats: StatCounter) -> Iterator[DictRecord]:
    for content, metadata in records:
        try:
            data = msgspec.json.decode(content)
            yield data, metadata
        except msgspec.DecodeError as e:
            logger.debug(f"Failed to load as JSON with error: {e}\n{content[:60]}")
            stats.inc("error/json/decode_error")
            continue


def record_to_dict(records: Iterable[tuple[Any, CrawlMetadata]]) -> Iterator[dict]:
    for data, metadata in records:
        yield {"data": data, **dataclasses.asdict(metadata)}


def extract_json_by_condition(
    records: Iterable[DictRecord], condition: Callable[[dict], bool]
) -> Iterator[DictRecord]:
    def subjson_iter(root: dict | list) -> Iterator[dict]:
        if isinstance(root, dict):
            yield root
            for v in root.values():
                yield from subjson_iter(v)
        elif isinstance(root, list):
            for item in root:
                yield from subjson_iter(item)

    for data, metadata in records:
        for subjson in subjson_iter(data):
            if condition(subjson):
                yield subjson, metadata


def extract_addresses(
    record_generator: Iterable[ArcWarcRecord], stats: StatCounter
) -> Iterator[StringRecord]:
    gen = filter_html_responses(record_generator, stats)
    gen = extractor_response_content(gen, stats)
    gen = ((content, metadata) for content, metadata in gen if "postaladdress" in content.lower())
    gen = extract_ld_json(gen, stats)
    gen = ((content, metadata) for content, metadata in gen if "postaladdress" in content.lower())

    yield from gen
