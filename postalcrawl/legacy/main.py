import asyncio
import hashlib
import html
import io
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypedDict
from urllib.parse import urlparse
from rapidfuzz.distance import DamerauLevenshtein

import geopy.adapters
import polars as pl
import requests
from geopy.extra.rate_limiter import AsyncRateLimiter
from geopy.geocoders import Nominatim
from loguru import logger
from postal.parser import parse_address as postal_parse_address
from tqdm.asyncio import tqdm
from urllib3 import HTTPResponse

from extract import extract_addresses
from extract import json_extract_schema, xml_extract_schema, xml_extract_text, json_extract_text
from process_url import address_gen

#########################  setup logging  ########################################
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    format="| <green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
)


#########################  utils  ########################################
@dataclass
class WarcInfo:
    id: str
    section: str
    section_part: str

    def url(self) -> str:
        return f"https://data.commoncrawl.org/{self.id}"

    def downlaad_path(self, root_dir: Path) -> Path:
        return self.base_file_path(root_dir).with_suffix(".warc.gz")

    def base_file_path(self, root_dir: Path) -> Path:
        return Path(root_dir) / self.section / self.section_part


def download_file(url: str, target_path: Path):
    logger.debug(f"downloading {url} to {target_path}")
    target_path.parent.mkdir(exist_ok=True, parents=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(target_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return target_path


######################### load warc paths info ########################################
def load_warc_file_info(warc_paths_file: Path) -> list[WarcInfo]:
    def get_file_info(line):
        return WarcInfo(
            id=line.replace("\n", ""),
            section=line.split("/")[3],
            section_part=re.search(r"-(\d{5})\.warc\.gz", line).group(1),
        )

    with open(warc_paths_file) as f:
        return [get_file_info(line) for line in f]


######################### warc file streams ########################################
def warc_file_stream(file_path: Path):
    assert file_path.is_file()
    fp = open(file_path, "rb")
    return fp


def warc_download_stream(url: str):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response.raw


######################### warc file streams ########################################
def preprocess(stream: io.BufferedReader | HTTPResponse, stats_counter: Counter) -> pl.LazyFrame:
    # processes on level file-id (section-id/segment-nr)
    data_stream = address_gen(stream, stats_counter)
    lf = pl.LazyFrame(data_stream)
    stream.close()
    return lf


######################### extraction ########################################
def extract_schema(lf: pl.LazyFrame, stats_counter: Counter) -> pl.LazyFrame:
    extract = pl.concat(
        [
            extract_addresses(lf, json_extract_schema, stats_counter),
            extract_addresses(lf, xml_extract_schema, stats_counter),
        ]
    )
    return extract


def extract_text(lf: pl.LazyFrame, stats_counter: Counter) -> pl.LazyFrame:
    extract = pl.concat(
        [
            extract_addresses(lf, json_extract_text, stats_counter),
            extract_addresses(lf, xml_extract_text, stats_counter),
        ]
    )
    return extract


######################### cleaning ########################################


def clean_schema_extract(lf: pl.LazyFrame, stats_counter: Counter) -> pl.LazyFrame:
    def remove_slash_escapes(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(pl.col(pl.String).exclude("url").str.replace_all("\\/", "/"))

    def reverse_unicode_escape(lf: pl.LazyFrame) -> pl.LazyFrame:
        cols = pl.col(pl.String).exclude("url")
        return lf.with_columns(
            pl.when(cols.str.contains(r"\\u"))
            .then(
                cols.map_elements(
                    lambda s: s.encode("utf-8").decode("unicode_escape"), return_dtype=pl.String
                )
            )
            .otherwise(cols)
        )

    def html_unescape(lf: pl.LazyFrame) -> pl.LazyFrame:
        cols = pl.col(pl.String).exclude("url")
        return lf.with_columns(
            pl.when(cols.str.contains("&"))
            .then(cols.map_elements(html.unescape, return_dtype=pl.String))
            .otherwise(cols)
        )

    def empty_string_to_none(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(pl.col(pl.String).exclude("url").replace(r"\s*", pl.lit(None)))

    def remove_extra_space_chars(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(pl.String).str.replace_all("\n", " ").str.replace(r"\s+", " ")
        )

    def filter_special_chars(lf: pl.LazyFrame, column: str) -> pl.LazyFrame:
        """Remove unnecesasry chars that are not in the language alphabet"""
        any_lang_regex = r".*[\p{L}\p{N}]+.*"
        return lf.filter(pl.col(column).str.contains(any_lang_regex))

    stats_counter["clean:count_before"] += lf.select(pl.len()).collect().item()
    cleaned = (
        lf.drop("postOfficeBoxNumber")
        .pipe(remove_slash_escapes)
        .pipe(reverse_unicode_escape)
        .pipe(html_unescape)
        .pipe(empty_string_to_none)
        .filter(pl.col("locality").is_not_null())
        .filter(pl.col("street").is_not_null())
        .pipe(filter_special_chars, column="locality")  # drop street without characters e.g. " - "
        .pipe(filter_special_chars, column="street")  # drop street without characters e.g. " - "
        .pipe(remove_extra_space_chars)
        .unique(
            pl.selectors.all().exclude("url"), keep="first"
        )  # current data uses this, but is unnecessary
    )

    stats_counter["clean:count_after"] += cleaned.select(pl.len()).collect().item()
    return cleaned


#########################  OSM validation  ########################################
class OSM_Structured_Query(TypedDict):
    amenity: str
    street: str
    city: str
    country: str
    state: str
    postalcode: str


async def query_osm(osm_query_func: callable, index: int, query: OSM_Structured_Query) -> dict:
    query = {k: v for k, v in query.items() if v is not None}
    out = await osm_query_func(
        query=query, addressdetails=True, exactly_one=True, namedetails=False, extratags=True
    )

    logger.debug(f"osm {query=} result={out}")
    if out is None:
        return {"join_index": index}

    return {"join_index": index, **out.raw}


async def async_osm_validate(df: pl.DataFrame, host_url: str) -> pl.DataFrame:
    nominatim_url = urlparse(host_url)
    osm_client = Nominatim(
        domain=nominatim_url.netloc,
        scheme=nominatim_url.scheme,
        adapter_factory=geopy.adapters.AioHTTPAdapter,
        timeout=0,
        user_agent="github/@tobbber",
    )
    df = df.with_row_index(name="join_index")
    async with osm_client:
        osm_query_fn: callable = AsyncRateLimiter(
            osm_client.geocode, min_delay_seconds=1 / 1000, max_retries=10
        )
        results = []
        for chunk in df.iter_slices(2000):
            tasks = []
            for row in chunk.select(
                "join_index", "name", "street", "locality", "postalCode", "region", "country"
            ).rows():
                index, name, street, locality, postalCode, region, country = row
                query = OSM_Structured_Query(
                    amenity=name,
                    street=street,
                    city=locality,
                    postalcode=postalCode,
                    state=region,
                    country=country,
                )
                task = query_osm(osm_query_fn, index, query)
                tasks.append(task)
            async for result in tqdm(
                asyncio.as_completed(tasks), total=len(df), initial=len(results)
            ):
                result = await result
                results.append(result)
            # for result in asyncio.as_completed(tasks): # without tqdm
            #     result = await result
            #     results.append(result)

    df_osm = (
        pl.DataFrame(results)
        .with_columns(pl.col("join_index").cast(pl.UInt32))
        .rename(lambda s: f"osm:{s}" if s != "join_index" else s)
        .with_columns(pl.col("osm:address").struct.unnest().name.prefix("osm:address:"))
        .drop("osm:address")
    )

    logger.debug(f"osm df columns: {df_osm.columns}")
    return df.join(df_osm, on="join_index", how="inner")


def validate(lf: pl.LazyFrame, host_url: str) -> pl.LazyFrame:
    df = asyncio.run(async_osm_validate(lf.collect(), host_url=host_url))
    return df.lazy()


#########################  OSM alignment ########################################
###  postallib parsing  ###
SplitStreetCallable = Callable[[str], tuple[str | None, str | None]]


def postal_parse(lf: pl.LazyFrame) -> pl.LazyFrame:
    def split_street_number(street: str) -> tuple[str | None, str | None]:
        parsed: list[tuple[str, str]] = postal_parse_address(street)
        d = {field: value for value, field in parsed}
        road = d.get("road")
        road = road.title() if road else None
        house_number = d.get("house_number")
        house_number = house_number.title() if house_number else None
        return road, house_number

    return (
        lf.with_columns(
            pl.col("street")
            .map_elements(split_street_number, return_dtype=pl.List(pl.String))
            .alias("temp:parsed_street")
        )
        .with_columns(
            pl.col("temp:parsed_street").list.get(0).alias("parsed:road"),
            pl.col("temp:parsed_street").list.get(1).alias("parsed:house_number"),
        )
        .drop("temp:parsed_street")
    )


### etc ###
def _unpack_wikidata(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.when(pl.col("osm:extratags").is_not_null())
        .then(
            pl.col("osm:extratags")
            .struct.field("wikidata")
            .map_elements(lambda id: f"https://www.wikidata.org/wiki/{id}", return_dtype=pl.String)
            .alias("osm:wikidata_url")
        )
        .otherwise(pl.lit(None))
        .alias("osm:wikidata")
    ).drop("osm:extratags")


def _add_hash_col(
    lf: pl.LazyFrame, hash_columns: list[str], new_column_name: str = "hash"
) -> pl.LazyFrame:
    return lf.with_columns(
        pl.concat_str(hash_columns, separator=",", ignore_nulls=True)
        .map_elements(lambda s: hashlib.md5(s.encode("utf-8")).hexdigest(), return_dtype=pl.String)
        .alias(new_column_name)
    )


def _cast_column(
    lf: pl.LazyFrame, column: str, cast_to: pl.datatypes.classes.DataTypeClass
) -> pl.LazyFrame:
    return lf.with_columns(pl.col(column).cast(cast_to))


def select_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    keep_columns = [
        "name",
        "osm:name",
        "osm:address:amenity",
        "street",
        "parsed:house_number",  # house number extracted from street
        "osm:address:house_number",
        "parsed:road",  # road name extracted from street
        "osm:address:road",
        "postalCode",
        "osm:address:postcode",
        "locality",
        # osm values that may contain locality
        "osm:address:city",
        "osm:address:town",
        "osm:address:village",
        "osm:address:borough",
        "osm:address:neighbourhood",
        "osm:address:hamlet",
        "osm:address:municipality",
        "country",
        "osm:address:country",
        "osm:address:country_code",
        "region",
        # osm values that may contain region
        "osm:address:state",
        "osm:address:county",
        "osm:address:region",
        "osm:address:state_district",
        "osm:address:province",
        "osm:address:ISO3166-2-lvl4",
        "url",
        # osm id and type allow reverse lookup
        "osm:osm_id",
        "osm:osm_type",
        "osm:place_rank",
        # might contain wikidata reference
        "osm:extratags",
        "osm:lat",
        "osm:lon",
    ]
    all_cols = set(lf.collect_schema().names())
    cols = [c for c in keep_columns if c in all_cols]  # use list to keep order
    return lf.select(cols)


def align_validation_pipeline(lf: pl.LazyFrame) -> pl.LazyFrame:
    COLS_ADDRESS = ("street", "locality", "postalCode", "country", "region")

    def _pick_nearest_column(
        lf: pl.LazyFrame,
        query_col: str,
        new_col: str,
        preferred_cols: list[str],
        backup_cols: list[str],
    ) -> pl.LazyFrame:
        def min_edit_distance(query_value: str, candidates: list[str]) -> str | None:
            # return value from candidates with minimum edit distance to query_value
            valid_values = [
                v for v in candidates if v is not None
            ]  # remove None values from candidates
            if not valid_values:
                return None
            if query_value is None:
                return valid_values[0]
            return min(valid_values, key=lambda v: DamerauLevenshtein.distance(query_value, v))

        all_cols = lf.collect_schema().names()
        preferred_cols = [c for c in preferred_cols if (c in all_cols)]
        backup_cols = [c for c in backup_cols if (c in all_cols)]
        return lf.with_columns(
            pl.struct(query_col, *preferred_cols, *backup_cols)
            .map_elements(
                lambda row: (
                    min_edit_distance(
                        query_value=row[query_col], candidates=[row[col] for col in preferred_cols]
                    )
                    or min_edit_distance(
                        query_value=row[query_col], candidates=[row[col] for col in backup_cols]
                    )
                ),
                return_dtype=pl.String,
            )
            .alias(new_col)
        )

    return (
        lf.drop_nulls(subset="osm:osm_id")  # drop values that failed to validate
        .pipe(select_columns)  # only keep relevant columns
        .pipe(_add_hash_col, hash_columns=["name", *COLS_ADDRESS])
        .pipe(_unpack_wikidata)  # create a wikipedia link from validation references
        .pipe(postal_parse)
        .with_columns(  # cast some columns to the correct type
            pl.col("osm:osm_type").cast(pl.Enum(["way", "node", "relation"])),
            pl.col("osm:lat").cast(pl.Float64),
            pl.col("osm:lon").cast(pl.Float64),
        )
        .with_columns(pl.col("osm:name").replace("", None))
        # Add country code column
        .with_columns(
            pl.when(pl.col("country").str.contains("^[a-zA-Z]{2}$"))
            .then(pl.col("country").str.to_lowercase())
            .otherwise(None)
            .alias("country_code")
        )
        .pipe(
            _pick_nearest_column,
            query_col="locality",
            new_col="clean:locality",
            preferred_cols=[
                "osm:address:city",
                "osm:address:town",
            ],
            backup_cols=[
                "osm:address:village",
                "osm:address:borough",
                "osm:address:neighbourhood",
                "osm:address:hamlet",
                "osm:address:county",
            ],
        )
        .pipe(
            _pick_nearest_column,
            query_col="region",
            new_col="clean:region",
            preferred_cols=["osm:address:state", "osm:address:region"],
            backup_cols=[
                "osm:address:county",
                "osm:address:state_district",
                "osm:address:province",
            ],
        )
    )


def add_url_tld_column(lf: pl.LazyFrame, new_column: str) -> pl.LazyFrame:
    return lf.with_columns(
        pl.col("url")
        .map_elements(lambda url: urlparse(url).netloc, return_dtype=pl.String)
        .alias(new_column)
    )
