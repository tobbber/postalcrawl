import dataclasses

import msgspec

from postalcrawl.models import StringRecord


def test_serialize():
    def serialize_dataclass(dc) -> bytes:
        d = dataclasses.asdict(dc)
        return msgspec.json.encode(d, order="deterministic")

    data = StringRecord(
        content=" foo bar bang dfas;dfjasdhfpoasdifasdfnadslfakurtiaufjansdf;asdf",
        charset="utf-8",
        url="https://example.com",
        warc_rec_id="1234567890",
        warc_date="2023-10-01T12:00:00Z",
    )
    s = serialize_dataclass(data)
    assert isinstance(s, bytes)
