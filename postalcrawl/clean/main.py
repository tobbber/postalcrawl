import re
from pathlib import Path
from typing import Iterable, Iterator

import polars as pl
from postal.parser import parse_address as postal_parse_address
from tqdm import tqdm

from postalcrawl.utils import project_root, read_from_jsongz
from postalcrawl.validate.refine import ensure_string

VALIDATED_ROOT = project_root() / "data" / "validated"
DATASET_DIR = project_root() / "data" / "dataset"

COLUMNS = [
    "name",
    "street",
    "house",
    "city",
    "postalcode",
    "state",
    "country",
    "countrycode",
]
TARGET_COLUMNS = [f"target_{col}" for col in COLUMNS]


def generate_address_rows(records: Iterable[dict]) -> Iterator[dict]:
    for record in records:
        if record.get("osm") is None:
            continue
        query_data = record["address_query"]
        target_data = record["osm"]["properties"]["geocoding"]
        assert target_data is not None
        assert query_data is not None
        row = dict(
            name=query_data.get("name"),
            street=query_data.get("street"),
            city=query_data.get("city"),
            state=query_data.get("state"),
            country=query_data.get("country"),
            postalcode=query_data.get("postalcode"),
            target_name=target_data.get("name"),
            target_street=target_data.get("street"),
            target_house=target_data.get("housenumber"),
            target_city=target_data.get("city"),
            target_state=target_data.get("state"),
            target_country=target_data.get("country"),
            target_postalcode=target_data.get("postcode"),
            target_countrycode=target_data.get("country_code"),
        )
        row = {k: ensure_string(v) for k, v in row.items()}
        yield row


def generate_section_rows(section_dir: Path) -> Iterator[dict]:
    assert section_dir.is_dir(), f"Not a directory: {section_dir}"
    section_files = list(section_dir.glob("*.json.gz"))
    for file_path in tqdm(section_files):
        file_records = read_from_jsongz(file_path)
        yield from generate_address_rows(file_records)


def split_street_number_field(records: Iterable[dict]) -> Iterator[dict]:
    def split_street_number(street: str) -> tuple[str | None, str | None]:
        parsed: list[tuple[str, str]] = postal_parse_address(street)
        d = {field: value for value, field in parsed}
        road = d.get("road")
        road = road.title() if road else None
        house_number = d.get("house_number")
        house_number = house_number.title() if house_number else None
        return road, house_number

    for record in records:
        if record.get("street") is None:
            record["street"] = None
            record["house"] = None
        else:
            street, house = split_street_number(record["street"])
            record["street"] = street
            record["house"] = house
        yield record


def add_country_code(records: Iterable[dict]) -> Iterator[dict]:
    for record in records:
        country = record.get("country")
        if country and re.match(r"^[A-Za-z]{2}$", country):
            record["countrycode"] = country.lower()
        else:
            record["countrycode"] = None
        yield record


def create_section_datasets():
    for section_dir in VALIDATED_ROOT.iterdir():
        outfile = section_dir / "addresses.parquet"
        # if outfile.exists():
        #     print(f"Section dataset already exists: {outfile}. Skipping...")
        #     continue
        row_gen = generate_section_rows(section_dir)
        row_gen = split_street_number_field(row_gen)
        row_gen = add_country_code(row_gen)
        row_gen = (
            row
            for row in row_gen
            if any(row[c] is not None for c in ["street", "city", "postalcode"])
        )
        df = pl.DataFrame(list(row_gen))
        df = df.unique()
        df = df.select([*COLUMNS, *TARGET_COLUMNS])

        df.write_parquet(outfile, compression="brotli")


def create_csvs(parquet_file: Path):
    df = pl.read_parquet(parquet_file)
    df.select(COLUMNS).write_csv(DATASET_DIR / "values.csv")
    (
        df.select(TARGET_COLUMNS)
        .rename(dict(zip(TARGET_COLUMNS, COLUMNS)))
        .write_csv(DATASET_DIR / "targets.csv")
    )


def main():
    create_section_datasets()

    section_datasets = list(VALIDATED_ROOT.glob("*/addresses.parquet"))
    df = pl.scan_parquet(section_datasets)

    df = df.unique()
    df = df.drop_nulls(subset=TARGET_COLUMNS)
    df.sink_parquet(DATASET_DIR / "dataset.parquet", compression="brotli")
    create_csvs(DATASET_DIR / "dataset.parquet")


if __name__ == "__main__":
    main()
