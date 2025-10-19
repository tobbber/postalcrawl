import json
from pathlib import Path
from pprint import pprint
from typing import Any, Iterator

import polars as pl
from tqdm import tqdm


def flatten_nested_dict_iter(d, parent_key="") -> Iterator[tuple[str, Any]]:
    if isinstance(d, dict):
        for k, v in d.items():
            yield from flatten_nested_dict_iter(
                v, parent_key=f"{parent_key}.{k}" if parent_key else k
            )
    elif isinstance(d, list):
        for i, v in enumerate(d):
            yield from flatten_nested_dict_iter(
                v, parent_key=f"{parent_key}.{i}" if parent_key else str(i)
            )
    else:
        yield parent_key, d


def main():
    validated_dir = Path("/home/tobias/postalcrawl/data/validated")
    for jsonfile in tqdm(list(validated_dir.glob("**/*.json"))):
        with open(jsonfile, encoding="utf-8") as f:
            outfile = jsonfile.with_suffix(".flat.parquet")
            data = json.load(f)
            flat_data = []
            for row in data:
                flat_data.append(dict(flatten_nested_dict_iter(row)))
            df = pl.DataFrame(data=flat_data, infer_schema_length=1000)
            df.write_parquet(outfile)


if __name__ == "__main__":
    main()
