import dataclasses


@dataclasses.dataclass
class StringExtract:
    content: str
    charset: str | None
    url: str
    warc_rec_id: str
    warc_date: str


@dataclasses.dataclass
class DictExtract:
    content: dict
    charset: str
    url: str
    warc_rec_id: str
    warc_date: str


@dataclasses.dataclass
class PostalAddress:
    name: str | None
    addressCountry: str | None
    addressLocality: str | None
    addressRegion: str | None
    postalCode: str | None
    streetAddress: str | None
    addressCountry: str | None
    url: str
    warc_date: str
    warc_rec_id: str
