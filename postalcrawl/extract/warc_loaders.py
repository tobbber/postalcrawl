import gzip
from pathlib import Path
from typing import Iterator

import requests
from warcio import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from postalcrawl.stats import StatCounter


def download_record_generator(file_id: str, stats: StatCounter) -> Iterator[ArcWarcRecord]:
    url = "https://data.commoncrawl.org/" + file_id
    data_stream = requests.get(url, stream=True)
    data_stream.raise_for_status()
    record_iter = ArchiveIterator(data_stream.raw, arc2warc=True)
    for record in record_iter:
        stats.inc("warc/record")
        yield record


def offline_record_generator(file_path: Path, stats: StatCounter) -> Iterator[ArcWarcRecord]:
    # Support transparently reading gzip-compressed WARC files
    opener = gzip.open if file_path.suffix == ".gz" else open
    with opener(file_path, "rb") as stream:
        for record in ArchiveIterator(stream, arc2warc=True):
            stats.inc("warc/record")
            yield record
