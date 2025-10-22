import re

from warcio import ArchiveIterator

LEFT_PADDING = 1600
RIGHT_PADDING = 2400


def extract_charset(content: bytes) -> str | None:
    charset_pattern = re.compile(rb"charset=[\'\"\s]?([\w-]+)[\'\"]?")
    charset_match = charset_pattern.search(content.lower())
    if charset_match:
        charset = charset_match.group(0).decode("utf-8")
        charset = charset.removeprefix("charset=").strip("\"' ")
        return charset
    return None


def extract_address_content(content: bytes) -> bytes:
    lower = content.lower()
    text_start: int = lower.find(b'"address"')
    text_end: int = lower.rfind(b'"address"')
    text_start = max(text_start - LEFT_PADDING, 0)
    text_end = min(text_end + RIGHT_PADDING, len(content) - 1)
    return content[text_start:text_end]


def address_gen(stream, status_counter):
    status_counter = status_counter
    record_iter = ArchiveIterator(stream, arc2warc=True)
    for i, record in enumerate(record_iter):
        # print(status_counter)
        if record.rec_type != "response":
            continue
        status_counter["preprocess:response"] += 1
        # check response content type
        content_type = record.http_headers.get_header("Content-Type")
        if content_type is None:
            continue
        status_counter["preprocess:has_content_type"] += 1
        if "html" not in content_type.lower() and "xml" not in content_type.lower():
            continue
        status_counter["preprocess:xml_content_type"] += 1

        content: bytes = record.content_stream().read()
        if b'"address"' not in content.lower():
            continue
        status_counter["preprocess:contains_address"] += 1

        yield {
            "content": extract_address_content(content),
            "url": record.rec_headers.get_header("WARC-Target-URI"),
            "content_charset": extract_charset(content),
            "offset": i,
            "warc_rec_id": record.rec_headers.get_header("WARC-Record-ID"),
            "warc_date": record.rec_headers.get_header("WARC-Date"),
        }
