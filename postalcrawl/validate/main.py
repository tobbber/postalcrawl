import asyncio
import json
from pathlib import Path
from typing import AsyncIterator, Iterable, Iterator

import msgspec
import polars as pl

from postalcrawl.validate.osm_validator import OsmValidator

EXTRACT_ROOT = Path("/Users/tobi/Uni/postalcrawlV2/data/extracted_addresses")
VALIDATE_ROOT = Path("/Users/tobi/Uni/postalcrawlV2/data/validated_addresses")
NOMINATIM_URL = "http://localhost:9020"
# NOMINATIM_URL = "https://nominatim.openstreetmap.org"
MAX_CONCURRENT = 200


def get_all_subdicts(root: dict | list) -> Iterator[dict]:
    """yield all sub-dicts in a nested dict/list structure"""
    if isinstance(root, dict):
        yield root
        for v in root.values():
            yield from get_all_subdicts(v)
    elif isinstance(root, list):
        for item in root:
            yield from get_all_subdicts(item)


async def query_validator(
    validator: OsmValidator, extract_rows: Iterable[dict]
) -> AsyncIterator[dict]:
    async def await_with_context(future, *context):
        result = await future
        return (result, *context)

    tasks = []
    for row in extract_rows:
        url = row["url"]
        warc_rec_id = row["warc_rec_id"]
        warc_date = row["warc_date"]
        try:
            data = msgspec.json.decode(row["data"])
        except msgspec.DecodeError:
            continue

        for subdict in get_all_subdicts(data):
            if "address" not in subdict.keys():
                continue
            address = subdict.get("address")
            if not isinstance(address, dict):
                continue
            if address.get("@type") != "PostalAddress":
                continue
            future = validator.query_validator(
                name=subdict.get("name") or subdict.get("legalName"),  # pyright: ignore [reportArgumentType]
                street=address.get("streetAddress"),  # pyright: ignore [reportArgumentType]
                city=address.get("addressLocality"),  # pyright: ignore [reportArgumentType]
                postalcode=address.get("postalCode"),  # pyright: ignore [reportArgumentType]
                country=address.get("addressCountry"),  # pyright: ignore [reportArgumentType]
                state=address.get("addressRegion"),  # pyright: ignore [reportArgumentType]
            )
            extract_data = {
                "warc_rec_id": warc_rec_id,
                "warc_date": warc_date,
                "warc_url": url,
                "extract": subdict,
            }
            future = await_with_context(future, extract_data)
            tasks.append(future)

    for future in asyncio.as_completed(tasks):
        result = await future
        response, extract_data = result  # both are dicts
        if response is None:
            continue
        yield {"osm": response, **extract_data}


async def main(skip_existing: bool = True):
    all_files = list(EXTRACT_ROOT.glob("**/*.parquet"))
    validator = OsmValidator(NOMINATIM_URL, max_concurrent=MAX_CONCURRENT)

    for extract_file in all_files:
        outfile = VALIDATE_ROOT / extract_file.relative_to(EXTRACT_ROOT).with_suffix(".json")
        outfile.parent.mkdir(parents=True, exist_ok=True)
        if skip_existing and outfile.exists():
            print(f"Skipping existing file: {outfile}")
            continue
        print(f"Validating {extract_file} -> {outfile}")

        df = pl.read_parquet(extract_file)
        results = [rec async for rec in query_validator(validator, df.iter_rows(named=True))]
        with open(outfile, "w") as f:
            json.dump(results, f)


if __name__ == "__main__":
    asyncio.run(main())
