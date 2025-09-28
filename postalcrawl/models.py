from dataclasses import dataclass
from typing import TypeVar


@dataclass(frozen=True, slots=True)
class CrawlMetadata:
    url: str
    warc_rec_id: str
    warc_date: str

# @dataclass(frozen=True, slots=True)
# class StringRecord:
#     content: str
#     url: str
#     warc_rec_id: str
#     warc_date: str

StringRecord = tuple[str, CrawlMetadata]

DictRecord = tuple[dict, CrawlMetadata]



@dataclass(frozen=True, slots=True)
class PostalAddress:
    # extraction info
    url: str
    warc_date: str
    warc_rec_id: str
    # address
    name: str | None
    street: str | None
    locality: str | None
    postalCode: str | None = None
    region: str | None = None
    country: str | None = None

    def __str__(self):
        return f"""
        {self.name}
        {self.street}
        {self.locality}, {self.postalCode} 
        {self.region} 
        {self.country}
        """
