# This file contains the execution logic to extract addresses from WARC files

import json
import time
from pathlib import Path

import joblib
import polars as pl
from loguru import logger

from postalcrawl.extract.extract import (
    extract_addresses,
    record_to_dict,
)
from postalcrawl.extract.warc_loaders import download_record_generator
from postalcrawl.stats import StatCounter
from postalcrawl.utils import file_segment_info, project_root

CC_PATHS_FILE = project_root() / "warc_paths" / "2025-30.warc.paths"
ADDRESS_OUT_DIR = project_root() / "data" / "extracted"


def extract_addresses_from_file_id(file_id: str, dest_dir: Path, skip_existing: bool = True):
    start_time = time.perf_counter()
    # io setup
    segment, seg_num = file_segment_info(file_id)
    parquet_path = Path(dest_dir) / segment / f"{seg_num}.parquet"
    if skip_existing and parquet_path.exists():
        logger.info(f"Skipping existing file: {parquet_path}")
        return
    else:
        logger.info(f"[{segment=} {seg_num=}] Starting...")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # data processing
    try:
        stats = StatCounter()
        gen = download_record_generator(file_id, stats)
        gen = extract_addresses(gen, stats)
        gen = record_to_dict(gen)

        df = pl.DataFrame((x for x in gen))
        df = df.unique()
        df.write_parquet(parquet_path, compression="brotli")
        with open(parquet_path.with_name(parquet_path.stem + ".stats.json"), "w") as f:
            json.dump(stats, f)
        elapsed = time.perf_counter() - start_time
        logger.info(
            f"[segment={segment} number={seg_num}] Extracted {len(df)} addresses. Elapsed time: {elapsed:.2f}s."
        )

    except Exception as ex:
        logger.error(f"Error processing file {file_id}: {ex}")
        error_file = parquet_path.with_suffix(".error")
        with open(error_file, "w") as f:
            f.write(str(ex))
        return


def main(source_paths_file: Path, output_dir: Path):
    assert source_paths_file.is_file(), f"{source_paths_file=} is not a file"
    assert output_dir.is_dir(), f"{output_dir=} is not a directory"

    with open(CC_PATHS_FILE, "r") as f:
        paths = [p.strip() for p in f.readlines()]

    def extract(file_id: str):
        return extract_addresses_from_file_id(file_id, output_dir, True)

    tasks = (joblib.delayed(extract)(p) for p in paths[:])
    joblib.Parallel(n_jobs=6, verbose=20)(tasks)


if __name__ == "__main__":
    main(CC_PATHS_FILE, ADDRESS_OUT_DIR)
