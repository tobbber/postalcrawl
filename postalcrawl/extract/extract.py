import dataclasses
import sys
from typing import Iterable, Iterator

import msgspec
from loguru import logger
from parsel import Selector
from tqdm import tqdm
from warcio.recordloader import ArcWarcRecord

from postalcrawl.extract.clean_address import clean_address_fields
from postalcrawl.extract.utils import parse_content_type
from postalcrawl.models import PostalAddress, StringExtract
from postalcrawl.stats import StatCounter

logger.remove()
logger.add(sys.stdout, level="INFO")


def filter_html_responses(
    record_generator: Iterable[ArcWarcRecord], stats: StatCounter
) -> Iterator[ArcWarcRecord]:
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
) -> Iterator[StringExtract]:
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
        yield StringExtract(
            content=content,
            charset=charset,
            url=record.rec_headers.get_header("WARC-Target-URI"),
            warc_rec_id=record.rec_headers.get_header("WARC-Record-ID"),
            warc_date=record.rec_headers.get_header("WARC-Date"),
        )


def filter_contains_postaladdress(
    response_generator: Iterable[StringExtract], stats: StatCounter
) -> Iterator[StringExtract]:
    for item in response_generator:
        if "postaladdress" in item.content.lower():
            yield item
        else:
            stats.inc("filter/no_postal_address")


def extract_ld_json(
    response_generator: Iterable[StringExtract], stats: StatCounter
) -> Iterator[StringExtract]:
    for record in response_generator:
        content = record.content
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
            yield dataclasses.replace(record, content=ld_json.strip())


def filter_postal_address(
    gen: Iterable[StringExtract], stats: StatCounter
) -> Iterator[StringExtract]:
    logger.debug("remove unwanted json")
    for item in gen:
        if "postaladdress" in item.content.lower():
            stats[f"{filter_postal_address.__name__}:has_postal_address"] += 1
            yield item
        else:
            stats["no_address"] += 1
            continue


def extract_postal_addresses(
    gen: Iterable[StringExtract], stats: StatCounter
) -> Iterator[PostalAddress]:
    def unpack_json(root: dict | list) -> Iterator[dict]:
        if isinstance(root, dict):
            yield root
            for v in root.values():
                yield from unpack_json(v)
        elif isinstance(root, list):
            for item in root:
                yield from unpack_json(item)

    for record in gen:
        try:
            data = msgspec.json.decode(record.content)
        except msgspec.DecodeError as e:
            logger.debug(f"Failed to load as JSON with error: {e}\n{record.content[:60]}")
            stats.inc("error/json/decode_error")
            continue
        for json_data in unpack_json(data):
            address = json_data.get("address")
            if not isinstance(address, dict):
                continue
            if address.get("@type") != "PostalAddress":
                continue
            stats.inc("postal_address/extracted")
            # todo: we should dedupe if same addresses are found but one has a name and the other does not

            name = json_data.get("name")
            country = address.get("addressCountry")
            region = address.get("addressRegion")
            locality = address.get("addressLocality")
            postal_code = address.get("postalCode")
            street_address = address.get("streetAddress")

            yield PostalAddress(
                name=name,
                country=country,
                locality=locality,
                region=region,
                postalCode=postal_code,
                street=street_address,
                url=record.url,
                warc_date=record.warc_date,
                warc_rec_id=record.warc_rec_id,
            )


def extract_addresses(
    record_generator: Iterable[ArcWarcRecord], stats: StatCounter, verbose: bool = False
) -> Iterator[PostalAddress]:
    gen = filter_html_responses(record_generator, stats)
    if verbose:
        gen = tqdm(gen)
    gen = extractor_response_content(gen, stats)
    gen = filter_contains_postaladdress(gen, stats)
    gen = extract_ld_json(gen, stats)
    gen = filter_contains_postaladdress(gen, stats)
    gen = extract_postal_addresses(gen, stats)
    gen = clean_address_fields(gen, stats)
    yield from gen
