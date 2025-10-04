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
