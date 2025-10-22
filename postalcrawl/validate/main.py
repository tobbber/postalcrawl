import asyncio
from typing import Iterable, Iterator

from loguru import logger
from tqdm import tqdm

from postalcrawl.record import Record
from postalcrawl.utils import project_root, read_from_jsongz, write_to_jsongz
from postalcrawl.validate.osm_validator import OsmValidator

EXTRACT_ROOT = project_root() / "data" / "extracted"
VALIDATE_ROOT = EXTRACT_ROOT.parent / "validated"
NOMINATIM_URL = "http://localhost:9020"
# NOMINATIM_URL = "https://nominatim.openstreetmap.org"
MAX_CONCURRENT = 512


def iterate_nested_dicts(records: Iterable[Record[dict]]) -> Iterator[Record[dict]]:
    def get_all_subdicts(root: dict | list) -> Iterator[dict]:
        """yield all sub-dicts in a nested dict/list structure"""
        if isinstance(root, dict):
            yield root
            for v in root.values():
                yield from get_all_subdicts(v)
        elif isinstance(root, list):
            for item in root:
                yield from get_all_subdicts(item)

    for record in records:
        data = record["data"]
        for subdict in get_all_subdicts(data):
            out: Record[dict] = {"data": subdict, "crawl_metadata": record["crawl_metadata"]}
            yield out


def dict_contains_address(record: Record[dict]) -> bool:
    data = record["data"]
    if "address" not in data.keys():
        return False
    address = data.get("address")
    if not isinstance(address, dict):
        return False
    if address.get("@type") != "PostalAddress":
        return False
    return True


async def main(skip_existing: bool = False):
    all_files = list(EXTRACT_ROOT.glob("**/*.json.gz"))
    print(all_files[:10])
    async with OsmValidator(NOMINATIM_URL, max_concurrent=MAX_CONCURRENT) as validator:
        for extract_file in tqdm(all_files):
            outfile = VALIDATE_ROOT / extract_file.relative_to(EXTRACT_ROOT)
            outfile.parent.mkdir(parents=True, exist_ok=True)
            if skip_existing and outfile.exists():
                print(f"Skipping existing file: {outfile}")
                continue
            logger.info(f"Validating {extract_file} -> {outfile}")

            records: list[Record[dict]] = read_from_jsongz(extract_file)
            gen = (rec for rec in records)
            gen = iterate_nested_dicts(gen)
            gen = (rec for rec in gen if dict_contains_address(rec))
            gen = (validator.record_query_validator(rec) for rec in gen)
            tasks = list(gen)
            results = await asyncio.gather(*tasks)
            results = [res for res in results if res is not None]
            write_to_jsongz(results, outfile=outfile)


if __name__ == "__main__":
    asyncio.run(main())
