import asyncio
from tqdm import tqdm
from tqdm.asyncio import tqdm as atqdm
import json
from pathlib import Path
from typing import AsyncIterator, Iterable, Iterator
import gzip

import msgspec
import polars as pl

import asyncio

import yarl
from loguru import logger
from niquests import AsyncSession, HTTPError
from urllib3 import Retry


class OsmValidator:
    def __init__(self, nominatim_url: str, max_concurrent: int = 200):
        # todo: implement per-second rate limiting to support nominatims official api (max 1 req/s)
        #  could be done by a semaphore wrapper that keeps track of last acquire time
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = AsyncSession(retries=Retry(total=5, backoff_factor=1))
        self.endpoint: yarl.URL = (
            yarl.URL(nominatim_url)
            .with_path("/search")
            .with_query(
                format="geocodejson",
                limit=1,
                addressdetails=1,
                namedetails=1,  # include name variations (e.g. multilang) and old names in result
                extratags=1,  # enable for things like opening hours, phone numbers, etc.
            )
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    @staticmethod
    def ensure_string(value) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            if value.strip() == "":
                return None
            return value
        if isinstance(value, dict):
            # this case mostly happens when country uses https://schema.org/Country
            return OsmValidator.ensure_string(value.get("name"))
        if isinstance(value, list):
            strings = [OsmValidator.ensure_string(v) for v in value]
            return ", ".join(s for s in strings if s)
        if isinstance(value, int) or isinstance(value, float):
            return str(value)
        # not supported type
        logger.info("Unsupported address field type: %s", type(value))
        return None

    # async def query_validator(self, query_address: PostalAddress) -> dict | None:
    async def query_validator(
        self, name: str, street: str, city: str, state: str, country: str, postalcode: str
    ) -> dict | None:
        query_params = {
            "amenity": name,
            "street": street,
            "city": city,
            "state": state,
            "country": country,
            "postalcode": postalcode,
        }
        # try:
        query_params = {k: OsmValidator.ensure_string(v) for k, v in query_params.items()}
        query_params = {k: v for k, v in query_params.items() if v is not None}
        if len(query_params) == 0:
            return None
        try:
            url = self.endpoint.update_query(**query_params)
        except ValueError:
            logger.warning(f"Invalid URL for query params: {query_params}")
            return None
        async with self.semaphore:
            resp = await self.session.get(str(url))

        try:
            resp.raise_for_status()
        except HTTPError:
            logger.warning(f"HTTP error for URL: {url}")
            return None
        # except Exception as e:
        #     return None

        response_data = resp.json()
        if response_data:
            if response_data.get("features"):
                return response_data["features"][0]
        return None


EXTRACT_ROOT = Path("/home/tobias/postalcrawl/data/extracted")
VALIDATE_ROOT = EXTRACT_ROOT.parent / "validated"
NOMINATIM_URL = "http://localhost:9020"
# NOMINATIM_URL = "https://nominatim.openstreetmap.org"
MAX_CONCURRENT = 1000


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

            query_params = dict(
                name=subdict.get("name") or subdict.get("legalName"),  # pyright: ignore [reportArgumentType]
                street=address.get("streetAddress"),  # pyright: ignore [reportArgumentType]
                city=address.get("addressLocality"),  # pyright: ignore [reportArgumentType]
                postalcode=address.get("postalCode"),  # pyright: ignore [reportArgumentType]
                country=address.get("addressCountry"),  # pyright: ignore [reportArgumentType]
                state=address.get("addressRegion"),  # pyright: ignore [reportArgumentType]
            )

            future = validator.query_validator(**query_params)
            extract_data = {
                "warc_rec_id": warc_rec_id,
                "warc_date": warc_date,
                "warc_url": url,
                "crawl": subdict,
                "address_query": query_params,
            }
            future = await_with_context(future, extract_data)
            tasks.append(future)

    for future in asyncio.as_completed(tasks):
        result = await future
        response, extract_data = result  # both are dicts
        # if response is None:
        #     continue
        yield {"osm": response, **extract_data}


async def main(skip_existing: bool = True):
    all_files = list(EXTRACT_ROOT.glob("**/*.parquet"))
    async with OsmValidator(NOMINATIM_URL, max_concurrent=MAX_CONCURRENT) as validator:
        pbar = tqdm(all_files)
        for extract_file in pbar:
            outfile = VALIDATE_ROOT / extract_file.relative_to(EXTRACT_ROOT).with_suffix(".json.gz")
            outfile.parent.mkdir(parents=True, exist_ok=True)
            if skip_existing and outfile.exists():
                print(f"Skipping existing file: {outfile}")
                continue
            logger.info(f"Validating {extract_file} -> {outfile}")
            df = pl.read_parquet(extract_file)
            results = [
                rec async for rec in atqdm(query_validator(validator, df.iter_rows(named=True)))
            ]

            with gzip.open(outfile, "wt", encoding="utf-8") as zipfile:
                json.dump(results, zipfile, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
