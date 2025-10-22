from collections import Counter

import polars as pl

from json_extract import json_extract_schema, json_extract_text
from xml_extract import xml_extract_schema, xml_extract_text

EXTRACTORS = [
    json_extract_schema,
    json_extract_text,
    xml_extract_schema,
    xml_extract_text,
]


def load_content(raw: bytes, charset: str, status_counter) -> str | None:
    # uses content_charset and content columns
    try:
        content = raw.decode(charset or "utf-8")
    except LookupError:
        status_counter["error.decode.unknownCharset"] += 1
        return None
    except UnicodeDecodeError:
        status_counter["error.decode.unicodeDecodeError"] += 1
        return None
    return content


def extract_addresses(
    lf: pl.LazyFrame, extractor: callable, stats_counter: Counter
) -> pl.LazyFrame:
    def generator():
        row_iter = lf.select("content", "url", "content_charset").collect().iter_rows()
        for row in row_iter:
            content, url, content_charset = row
            text = load_content(content, content_charset, stats_counter)
            if text is None:
                continue
            yield extractor(text, url, stats_counter)

    flat_values = (item for sublist in generator() for item in sublist)
    return pl.DataFrame(flat_values).lazy()
