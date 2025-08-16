import json
from dataclasses import dataclass, asdict
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from sys import prefix
from typing import Iterator

import msgspec
import pandas as pd
import requests
from loguru import logger
from parsel import Selector
from warcio import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

logger.remove()
logger.add(sys.stdout, level="INFO")


def file_segment_info(file_id: str) -> tuple[str, str]:
    splits = file_id.split("/")
    segment = splits[3]
    sub_id = splits[-1]
    segment_number = re.search(r"-(\d{5})\.warc\.gz$", sub_id).group(1)
    return segment, segment_number


def crawl_url(file_id: str) -> str:
    return "https://data.commoncrawl.org/" + file_id


def process_url(file_id: str, outdir: Path, overwrite: bool = False) -> Path | None:
    # define files and skip if already exists
    assert outdir.is_dir()
    segment, segment_number = file_segment_info(file_id)
    out_file = outdir / segment / f"{segment_number}.parquet"
    out_file.parent.mkdir(exist_ok=True, parents=True)
    if not overwrite and out_file.with_suffix(".done").is_file():
        logger.info(
            f"[{segment=} number={segment_number}] output file already exists. Skipping..."
        )
        return

    # process file
    start = time.perf_counter()
    logger.info(f"[{segment=} number={segment_number}] Processing file...")
    try:
        df = pd.DataFrame(jsonld_generator(file_id))
    except Exception as e:
        # write error file
        err_file = out_file.with_suffix(".error")
        with open(err_file, "w") as f:
            f.write(str(e))
        logger.error(f"[{segment=} number={segment_number}] Error: {e}")
        time.sleep(1)  # just to ensure we dont gently retry
        return
    elapsed = time.perf_counter() - start
    df.to_parquet(out_file, compression="brotli")

    file_size = os.stat(out_file).st_size
    logger.info(
        f"[{segment=} number={segment_number}] Took: {elapsed:.2f} seconds for {len(df)} records"
    )
    logger.debug(
        f"[{segment=} number={segment_number}] Wrote {file_size/1024**2:.4} MB ({len(df)} recs) to {out_file}"
    )
    # write status file and delete error file if successful
    out_file.with_suffix(".done").touch()
    if out_file.with_suffix(".error").is_file():
        out_file.with_suffix(".error").unlink()
    return out_file


def extract_charset(content: bytes) -> str | None:
    charset_pattern = re.compile(rb"charset=[\'\"\s]?([\w-]+)[\'\"]?")
    charset_match = charset_pattern.search(content.lower())
    if charset_match:
        charset = charset_match.group(0).decode("utf-8")
        charset = charset.removeprefix("charset=").strip("\"' ")
        return charset
    return None


# def extract_address_content(content: bytes) -> bytes:
#     lower = content.lower()
#     text_start: int = lower.find(b'"address"')
#     text_end: int = lower.rfind(b'"address"')
#     text_start = max(text_start - LEFT_PADDING, 0)
#     text_end = min(text_end + RIGHT_PADDING, len(content) - 1)
#     return content[text_start:text_end]

def bytes_to_string(raw_bytes: bytes, charset: str | None) -> str:
    try:
        return raw_bytes.decode(charset or "utf-8")
    except (LookupError, UnicodeDecodeError):
        return raw_bytes.decode("utf-8", errors="replace")


def record_generator(file_id: str, stats: defaultdict[int]) -> Iterator[ArcWarcRecord]:
    data_stream = requests.get(crawl_url(file_id), stream=True)
    data_stream.raise_for_status()
    record_iter = ArchiveIterator(data_stream.raw, arc2warc=True)
    for record in record_iter:
        yield record

def offline_record_generator(file_path: Path, stats: defaultdict[int]) -> Iterator[ArcWarcRecord]:
    with open(file_path, "rb") as stream:
        for record in ArchiveIterator(stream, arc2warc=True):
            yield record



def generate_html_responses(record_generator: Iterator[ArcWarcRecord],stats: defaultdict[int]) -> Iterator[ArcWarcRecord]:
    for record in record_generator:
        if record.rec_type != "response":
            continue
        stats["response"] += 1
        content_type = record.http_headers.get_header("Content-Type")
        if content_type is None:
            continue
        stats["has_content_type"] += 1
        if "html" not in content_type.lower() and "xml" not in content_type.lower():
            continue
            stats["not_html"] += 1
        yield record



@dataclass
class StringExtract:
    content: str
    charset: str
    url: str
    warc_rec_id: str
    warc_date: str

@dataclass
class DictExtract:
    content: dict
    charset: str
    url: str
    warc_rec_id: str
    warc_date: str

@dataclass
class PostalAddress:
    name: str
    addressCountry: str
    addressLocality: str
    addressRegion: str
    postalCode: str
    streetAddress: str
    addressCountry: str
    url: str
    warc_date: str
    warc_rec_id: str

def generate_ld_json(response_generator: Iterator[ArcWarcRecord], stats: defaultdict[int]) -> Iterator[StringExtract]:
    logger.debug("generating ld+json")
    for record in response_generator:
        content = record.content_stream().read()
        charset = extract_charset(content)

        # todo: remove this filter if you generalize the extractor
        if not b"postaladdress" in content.lower():
            stats["no_address"] += 1
            continue
        content_string = bytes_to_string(content, charset)
        try:
            selector = Selector(text=content_string)
            xpath_query= selector.xpath("//script[@type='application/ld+json']/text()").getall()
        except ValueError as e:
            stats["not_html"] += 1
            continue
        for item in xpath_query:
                yield StringExtract(
                    content=item.strip(),
                    charset=charset,
                    url= record.rec_headers.get_header("WARC-Target-URI"),
                    warc_rec_id =  record.rec_headers.get_header("WARC-Record-ID"),
                    warc_date = record.rec_headers.get_header("WARC-Date"),
                )

def deserialize_json(json_string: str) -> dict:
    return msgspec.json.decode(json_string)

def generate_deserialized_json(gen:   Iterator[StringExtract], stats: defaultdict[int]) -> Iterator[DictExtract]:
    # todo: use orjson  msgspec for faster json deserialization
    logger.debug("json deserialized")
    for item in gen:
        try:
            # data= json.loads(item.content)
            data = deserialize_json(item.content)
            stats["deserialized"] += 1
            item.content = data
            yield item
        except msgspec.DecodeError as e:
            stats["decode_error"] +=1
            continue


def filter_postal_address(gen: Iterator[StringExtract], stats: defaultdict[int]) -> Iterator[StringExtract]:



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



def extract_address_element(gen: Iterator[DictExtract], stats: defaultdict[int]):

    # DictExtract
    for item in gen:
        for named_address in extract_address_from_json(item.content):
            address = named_address["address"]
            stats["extracted_address"] += 1
            yield PostalAddress(
                name=named_address.get("name"),
                addressCountry=address.get("addressCountry"),
                addressLocality=address.get("addressLocality"),
                addressRegion=address.get("addressRegion"),
                postalCode=address.get("postalCode"),
                streetAddress=address.get("streetAddress"),
                url=item.url,
                warc_date=item.warc_date,
                warc_rec_id=item.warc_rec_id
            )

    # jsonpath_expression = jsonpath_ng.parse("$..address")
    for item in gen:
        yield


def jsonld_generator(file_id: str):
    logger.debug("generating json string")
    stats = defaultdict(int)
    gen = record_generator(file_id, stats)
    gen = generate_html_responses(gen , stats)
    gen = generate_ld_json(gen, stats)
    gen = filter_postal_address(gen, stats)
    dict_gen = generate_deserialized_json(gen, stats)

    yield from dict_gen
    # sanitize and load json
    print(stats)
    segment, offset = file_segment_info(file_id)
    logger.debug(
        f"[{segment=} {offset=}] done processing. stats: {dict(stats)} ({file_id=})"
    )

