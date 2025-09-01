import html
from typing import Any, Iterable, Iterator

from postalcrawl.models import PostalAddress
from postalcrawl.stats import StatCounter


def ensure_string(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        # this case mostly happens when country uses https://schema.org/Country
        return ensure_string(value.get("name"))
    if isinstance(value, list):
        strings = [ensure_string(v) for v in value]
        return ", ".join(s for s in strings if s)
    if isinstance(value, int) or isinstance(value, float):
        return str(value)
    raise TypeError(f"Unsupported type {type(value)}")


def remove_escaped_slashes(value: str) -> str:
    return value.replace("\\/", "/")


def reverse_unicode_escape(s: str) -> str:
    if "\\u" in s.lower():
        return s.encode("utf-8").decode("unicode_escape")
    return s


def html_unescape(s: str) -> str:
    if "&" in s:
        return html.unescape(s)
    return s


def clear_string(value: Any) -> str | None:
    s = ensure_string(value)
    if s is None:
        return None
    s = remove_escaped_slashes(s)
    s = reverse_unicode_escape(s)
    s = html_unescape(s)
    s = s.replace("\n", " ")  # replace newlines with space
    s = s.strip()  # remove leading and trailing whitespace
    if not s:
        return None
    return s


def _clean_address_fields(
    address_generator: Iterable[PostalAddress], stats: StatCounter
) -> Iterator[PostalAddress]:
    def field_has_content(s: str | None) -> bool:
        if s is None:
            return False
        return any(c.isalnum() for c in s)

    for address in address_generator:
        cleaned = PostalAddress(
            name=clear_string(address.name),
            country=clear_string(address.country),
            locality=clear_string(address.locality),
            region=clear_string(address.region),
            postalCode=clear_string(address.postalCode),
            street=clear_string(address.street),
            url=address.url,
            warc_date=address.warc_date,
            warc_rec_id=address.warc_rec_id,
        )
        if not field_has_content(cleaned.street):
            stats.inc("address/skip/no_street_address")
            continue

        has_locality = field_has_content(cleaned.locality)
        has_postal_code = field_has_content(cleaned.postalCode)
        if not has_postal_code and not has_locality:
            # there is no way to validate an address that has no locality or postal code
            stats.inc("address/skip/no_locality")
            continue
        yield cleaned
