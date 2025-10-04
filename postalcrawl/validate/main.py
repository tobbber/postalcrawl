import asyncio
from pathlib import Path
from typing import Iterable, Iterator

import msgspec
import polars as pl

from postalcrawl.validate.osm_validator import OsmValidator


EXTRACT_ROOT = Path("/Users/tobi/dev/postalcrawlV2/data/extracted_addresses")
VALIDATE_ROOT = Path("/Users/tobi/dev/postalcrawlV2/data/validated_addresses")
NOMINATIM_URL = "http://localhost:9020"
# NOMINATIM_URL = "https://nominatim.openstreetmap.org"
MAX_CONCURRENT = 5

def get_all_subdicts(root: dict | list) -> Iterator[dict]:
    """ yield all sub-dicts in a nested dict/list structure """
    if isinstance(root, dict):
        yield root
        for v in root.values():
            yield from get_all_subdicts(v)
    elif isinstance(root, list):
        for item in root:
            yield from get_all_subdicts(item)


def flatten_nested_dict_iter(d, parent_key='') -> Iterator[tuple[str, any]]:
    if isinstance(d, dict):
        for k, v in d.items():
            yield from flatten_nested_dict_iter(v, parent_key=f'{parent_key}.{k}' if parent_key else k)
    elif isinstance(d, list):
        for i, v in enumerate(d):
            yield from flatten_nested_dict_iter(v, parent_key=f'{parent_key}.{i}' if parent_key else str(i))
    else:
        yield parent_key, d


async def query_validator(validator: OsmValidator, extract_rows: Iterable[dict]) -> Iterator[dict]:

    async def await_with_context(future, *context):
            result = await future
            return (result, *context)

    tasks = []
    for row in extract_rows:
        url = row['url']
        warc_rec_id = row['warc_rec_id']
        warc_date = row['warc_date']
        try:
            data = msgspec.json.decode(row['data'])
        except msgspec.DecodeError:
            continue

        for subdict in get_all_subdicts(data):
            if not "address" in subdict.keys():
                continue
            address = subdict.get("address")
            if not isinstance(address, dict):
                continue
            if address.get("@type") != "PostalAddress":
                continue
            future = validator.query_validator(
                name=subdict.get("name") or subdict.get("legalName"),
                street=address.get("streetAddress"),
                city=address.get("addressLocality"),
                postalcode=address.get("postalCode"),
                country=address.get("addressCountry"),
                state=address.get("addressRegion"),
            )
            extract_data = {
                "warc_rec_id": warc_rec_id,
                "warc_date": warc_date,
                "warc_url": url,
                "extract": subdict,
            }
            future = await_with_context(future, extract_data)
            tasks.append(future)

    for result in asyncio.as_completed(*tasks):
        response, extract_data = result # both are dicts
        if response is None:
            continue
        nested_dict = {"osm": response, **extract_data}
        flat_dict = {k: v for k, v in flatten_nested_dict_iter(nested_dict)}
        yield flat_dict




async def validate_parquet(input_path: Path, output_path: Path):

    validator = OsmValidator(NOMINATIM_URL, max_concurrent=MAX_CONCURRENT)
    df = pl.read_parquet(input_path)
    results = await query_validator(validator, df.iter_rows(named=True))
    out_df = pl.DataFrame(results)
    out_df.write_parquet(output_path, compression="brotli")

async def main(skip_existing: bool = True):
    all_files = list(EXTRACT_ROOT.glob("**/*.parquet"))

    for extract_file in all_files:
        outfile= VALIDATE_ROOT / extract_file.relative_to(EXTRACT_ROOT)
        if skip_existing and outfile.exists():
            print(f"Skipping existing file: {outfile}")
            continue
        print(f"Validating {extract_file} -> {outfile}")
        await validate_parquet(extract_file, outfile)

    # p = Path('/Users/tobi/dev/postalcrawlV2/data/extracted_addresses/1751905933612.63/00000.parquet')
    # out_path = p.with_suffix('.validated.json')
    # await validate_parquet(p, out_path)


    # tasks = (joblib.delayed(extract)(p) for p in paths[:1])
    # joblib.Parallel(n_jobs=6, verbose=20)(tasks)


if __name__ == "__main__":
    asyncio.run(main())