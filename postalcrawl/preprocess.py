import dataclasses
import re
import sys
from pathlib import Path
from typing import Iterable, Iterator

import msgspec
import requests
from loguru import logger
from parsel import Selector
from warcio import ArchiveIterator
from warcio.recordloader import ArcWarcRecord
from werkzeug.http import parse_options_header

from postalcrawl.models import PostalAddress, StringExtract
from postalcrawl.stat_counter import StatCounter

logger.remove()
logger.add(sys.stdout, level="INFO")


def file_segment_info(file_id: str) -> tuple[str, str]:
    splits = file_id.split("/")
    segment = splits[3]
    sub_id = splits[-1]
    segment_number = re.search(r"-(\d{5})\.warc\.gz$", sub_id).group(1)  # pyright: ignore [reportOptionalMemberAccess]
    return segment, segment_number


def crawl_url(file_id: str) -> str:
    return "https://data.commoncrawl.org/" + file_id


# def process_url(file_id: str, outdir: Path, overwrite: bool = False) -> Path | None:
#     # define files and skip if already exists
#     assert outdir.is_dir()
#     segment, segment_number = file_segment_info(file_id)
#     out_file = outdir / segment / f"{segment_number}.parquet"
#     out_file.parent.mkdir(exist_ok=True, parents=True)
#     if not overwrite and out_file.with_suffix(".done").is_file():
#         logger.info(f"[{segment=} number={segment_number}] output file already exists. Skipping...")
#         return
#
#     # process file
#     start = time.perf_counter()
#     logger.info(f"[{segment=} number={segment_number}] Processing file...")
#     try:
#         df = pd.DataFrame(jsonld_generator(file_id))
#     except Exception as e:
#         # write error file
#         err_file = out_file.with_suffix(".error")
#         with open(err_file, "w") as f:
#             f.write(str(e))
#         logger.error(f"[{segment=} number={segment_number}] Error: {e}")
#         time.sleep(1)  # just to ensure we dont gently retry
#         return
#     elapsed = time.perf_counter() - start
#     df.to_parquet(out_file, compression="brotli")
#
#     file_size = os.stat(out_file).st_size
#     logger.info(
#         f"[{segment=} number={segment_number}] Took: {elapsed:.2f} seconds for {len(df)} records"
#     )
#     logger.debug(
#         f"[{segment=} number={segment_number}] Wrote {file_size / 1024**2:.4} MB ({len(df)} recs) to {out_file}"
#     )
#     # write status file and delete error file if successful
#     out_file.with_suffix(".done").touch()
#     if out_file.with_suffix(".error").is_file():
#         out_file.with_suffix(".error").unlink()
#     return out_file


def download_record_generator(file_id: str, stats: StatCounter) -> Iterator[ArcWarcRecord]:
    data_stream = requests.get(crawl_url(file_id), stream=True)
    data_stream.raise_for_status()
    record_iter = ArchiveIterator(data_stream.raw, arc2warc=True)
    for record in record_iter:
        stats.inc("warc/record")
        yield record


def offline_record_generator(file_path: Path, stats: StatCounter) -> Iterator[ArcWarcRecord]:
    with open(file_path, "rb") as stream:
        for record in ArchiveIterator(stream, arc2warc=True):
            stats.inc("warc/record")
            yield record


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


def parse_content_type(content_type: str | None) -> tuple[str, str | None]:
    media_type, options = parse_options_header(content_type)
    charset = options.get("charset")
    if charset:
        charset = charset.lower()
    return media_type, charset


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


def checkpoint_dataclass_to_jsonl(
    gen: Iterable[StringExtract], stats: StatCounter
) -> Iterator[StringExtract]:
    def serialize_dataclass(dataclass) -> bytes:
        d = dataclasses.asdict(dataclass)
        return msgspec.json.encode(d, order="deterministic")

    with open("ldjson_checkpoint.jsonl", "ab") as f:
        for item in gen:
            yield item
            s = serialize_dataclass(item)
            f.write(s + b"\n")


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


def extract_address_from_json(root):
    if isinstance(root, list):
        for item in root:
            if isinstance(item, dict):
                yield from extract_address_from_json(item)
    if isinstance(root, dict):
        for key, value in root.items():
            if (key == "address") and (value.get("@type") == "PostalAddress"):
                yield root
            else:
                if isinstance(value, dict):
                    yield from extract_address_from_json(value)


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
            yield PostalAddress(
                name=json_data.get("name"),
                addressCountry=address.get("addressCountry"),
                addressLocality=address.get("addressLocality"),
                addressRegion=address.get("addressRegion"),
                postalCode=address.get("postalCode"),
                streetAddress=address.get("streetAddress"),
                url=record.url,
                warc_date=record.warc_date,
                warc_rec_id=record.warc_rec_id,
            )


def extract_addresses(
    record_generator: Iterable[ArcWarcRecord], stats: StatCounter
) -> Iterator[PostalAddress]:
    gen = filter_html_responses(record_generator, stats)
    gen = filter_html_responses(gen, stats)
    gen = extractor_response_content(gen, stats)
    gen = filter_contains_postaladdress(gen, stats)
    gen = extract_ld_json(gen, stats)
    gen = filter_contains_postaladdress(gen, stats)
    gen = extract_postal_addresses(gen, stats)
    yield from gen
