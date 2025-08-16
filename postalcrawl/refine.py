import html
from collections import Counter

import polars as pl

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
