import asyncio
from typing import Iterable

import yarl
from loguru import logger
from niquests import AsyncSession
from urllib3 import Retry

from postalcrawl.models import PostalAddress
from postalcrawl.validate.response_model import OsmAddress, OsmResponse


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

    # async def validate_iter(
    #     self, addresses: Iterable[PostalAddress]
    # ) -> list[tuple[PostalAddress, OsmAddress]]:
    #     tasks = [self.query_validator(address) for address in addresses]
    #     results = await asyncio.gather(*tasks)
    #     return [(adr, result) for adr, result in zip(addresses, results) if result is not None]

    # async def query_validator(self, query_address: PostalAddress) -> dict | None:
    async def query_validator(self, name: str, street: str, city: str, state: str, country: str, postalcode: str) -> dict | None:
        query_params = {
            "amenity": name,
            "street": street,
            "city": city,
            "state": state,
            "country": country,
            "postalcode": postalcode,
        }
        # try:
        query_params = {k: v for k, v in query_params.items() if v is not None}
        url = self.endpoint.update_query(**query_params)
        async with self.semaphore:
            resp = await self.session.get(str(url))
        resp.raise_for_status()
        # except Exception as e:
        #     return None

        response_data = resp.json()
        if response_data:
            if response_data.get("features"):
                return  response_data["features"][0]
        return None
