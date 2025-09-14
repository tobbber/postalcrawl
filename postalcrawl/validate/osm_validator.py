import asyncio

import yarl
from loguru import logger
from niquests import AsyncSession
from urllib3 import Retry

from postalcrawl.models import PostalAddress
from postalcrawl.validate.response_model import OsmResponse


class OsmValidator:
    def __init__(
        self,
        nominatim_url: str,
        request_delay: int = 0,
        http_session=None,
        max_concurrent: int = 200,
    ):
        self.request_delay = request_delay
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = http_session or AsyncSession(retries=Retry(total=5, backoff_factor=1))
        self.endpoint: yarl.URL = (
            yarl.URL(nominatim_url)
            .with_path("/search")
            .with_query(
                format="geocodejson",
                limit=1,
                addressdetails=1,
                namedetails=0,  # include name variations (e.g. multilang) and old names in result
                extratags=0,  # enable for things like opening hours, phone numbers, etc.
                layer="address",
            )
        )

    async def validate_iter(
        self, addresses: list[PostalAddress]
    ) -> list[tuple[PostalAddress, dict]]:
        tasks = [self.query_validator(address) for address in addresses]
        results = await asyncio.gather(*tasks)
        return [(adr, result) for adr, result in zip(addresses, results) if result is not None]

    async def query_validator(self, query_address: PostalAddress) -> dict | None:
        query_params = {
            "amenity": query_address.name,
            "street": query_address.street,
            "city": query_address.locality,
            "state": query_address.region,
            "country": query_address.country,
            "postalcode": query_address.postalCode,
        }
        query_params = {k: v for k, v in query_params.items() if v is not None}
        url = self.endpoint.update_query(**query_params)
        logger.debug(f"Address query: {url}")

        async with self.semaphore:
            resp = await self.session.get(str(url))
        resp.raise_for_status()

        await asyncio.sleep(self.request_delay)
        osm_data = OsmResponse(**resp.json())
        return osm_data.flatten()
