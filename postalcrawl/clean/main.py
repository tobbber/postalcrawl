from typing import Iterable

import polars as pl
from tqdm import tqdm

from postalcrawl.utils import project_root, read_from_jsongz
from postalcrawl.validate.refine import ensure_string

VALIDATED_ROOT = project_root() / "data" / "validated"


def select_address_rows(records: Iterable[dict]):
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
        yield


def main():
    for section in VALIDATED_ROOT.iterdir():
        if not section.is_dir():
            continue
        rows = []
        section_files = list(section.glob("*.json.gz"))
        for vf in tqdm(section_files):
            file_records: dict = read_from_jsongz(vf)
            for record in file_records:
                if record.get("osm") is None:
                    continue
                row = dict(
                    name=ensure_string(record["address_query"].get("name")),
                    street=ensure_string(record["address_query"].get("street")),
                    city=ensure_string(record["address_query"].get("city")),
                    state=ensure_string(record["address_query"].get("state")),
                    country=ensure_string(record["address_query"].get("country")),
                    postalcode=ensure_string(record["address_query"].get("postalcode")),
                    target_name=ensure_string(record["osm"]["properties"]["geocoding"].get("name")),
                    target_street=ensure_string(
                        record["osm"]["properties"]["geocoding"].get("street")
                    ),
                    target_house=ensure_string(
                        record["osm"]["properties"]["geocoding"].get("housenumber")
                    ),
                    target_city=ensure_string(record["osm"]["properties"]["geocoding"].get("city")),
                    target_state=ensure_string(
                        record["osm"]["properties"]["geocoding"].get("state")
                    ),
                    target_country=ensure_string(
                        record["osm"]["properties"]["geocoding"].get("country")
                    ),
                    target_postalcode=ensure_string(
                        record["osm"]["properties"]["geocoding"].get("postcode")
                    ),
                    target_countrycode=ensure_string(
                        record["osm"]["properties"]["geocoding"].get("country_code")
                    ),
                )
                rows.append(row)
            if not all(isinstance(c, str) or c is None for c in row.values()):
                print(f"Non-string value found in record: {record}")

        df = pl.DataFrame(rows)
        out_file = section / "validated.parquet"
        df.write_parquet(out_file)


if __name__ == "__main__":
    main()
