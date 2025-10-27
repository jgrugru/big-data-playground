from datetime import date
from pathlib import Path

import polars as pl

from src.utils.timing import fn_timer


def read_in_with_polars_pd(filepath: Path) -> pl.DataFrame:
    df = pl.scan_parquet(
        source=filepath,
        schema={
            "name": pl.Utf8,
            "address": pl.Utf8,
            "email": pl.Utf8,
            "date_of_birth": pl.Utf8,
            "biological_kids": pl.Int64,
            "gender": pl.Utf8,
        },
    )
    return df


@fn_timer(log_output=False)
def filter_out_males(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(pl.col("gender") == "female")


@fn_timer(log_output=False)
def get_name_and_address_char_match_rows(df: pl.LazyFrame) -> pl.LazyFrame:
    filtered_df = df.with_columns(pl.col("address").str.extract(r"^(?:\d+\s+)*(\p{L})", 1).alias("first_char_of_address"))

    filtered_df = filtered_df.with_columns(pl.col("name").str.slice(0, 1).alias("first_char_of_name"))

    filtered_df = filtered_df.filter(pl.col("first_char_of_address") == pl.col("first_char_of_name"))
    return filtered_df


@fn_timer(log_output=False)
def get_sixty_year_olds(df: pl.DataFrame) -> pl.DataFrame:
    today = date.today()

    # Calculate age using a conditional expression
    df_with_age = df.with_columns(pl.col("date_of_birth").str.to_datetime("%Y-%m-%d").dt.date().alias("date_of_birth"))

    df_with_age = df.with_columns(((pl.lit(today, dtype=pl.Date) - pl.col("date_of_birth").cast(pl.Date)).dt.total_days() / 365).cast(pl.Int32).alias("age_days"))

    return df_with_age.filter(pl.col("age_days") == 60)


@fn_timer(log_output=False)
def get_no_digits_in_email(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter([~pl.col("email").str.contains(r"\d")])


# @fn_timer(log_output=False)
def get_candidates_with_21_kids(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("biological_kids") == 21)


@fn_timer(log_output=False)
def main():
    parquet_input_path = Path("/Users/jeffgruenbaum/projects/wex-option-allocations/pandas_optimization/mock_10million_data.parquet")

    df: pl.DataFrame = read_in_with_polars_pd(parquet_input_path)

    # 1
    candidates = filter_out_males(df=df)

    # 2
    candidates = get_name_and_address_char_match_rows(df=candidates)

    # 3
    candidates = get_sixty_year_olds(df=candidates)

    # 4
    candidates = get_no_digits_in_email(df=candidates)

    # 5
    candidates = get_candidates_with_21_kids(df=candidates)

    candidates = candidates.select(pl.all().exclude(["first_char_of_address", "first_char_of_name", "age_days"]))

    # print(candidates.collect())


if __name__ == "__main__":
    main()

"""
2025-10-26 20:11:56.670 | INFO     | __main__:wrapper:22 - 'filter_out_males': 0.0011391639709472656 seconds
2025-10-26 20:11:56.671 | INFO     | __main__:wrapper:22 - 'get_name_and_address_char_match_rows': 0.0008897781372070312 seconds
2025-10-26 20:11:56.673 | INFO     | __main__:wrapper:22 - 'get_sixty_year_olds': 0.001531839370727539 seconds
2025-10-26 20:11:56.673 | INFO     | __main__:wrapper:22 - 'get_no_digits_in_email': 0.00022482872009277344 seconds
2025-10-26 20:11:56.674 | INFO     | __main__:wrapper:22 - 'main': 0.007554054260253906 seconds
"""
