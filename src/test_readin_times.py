from pathlib import Path
from timeit import timeit

import dask.dataframe as dd
import duckdb
import pandas as pd
import polars as pl


def read_in_with_dask_pd(filepath: Path) -> dd:
    return dd.read_parquet(path=filepath)


def read_in_with_polars_pd(filepath: Path) -> pl.DataFrame:
    return pl.scan_parquet(
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


def read_in_with_pandas(filepath: Path) -> pd.DataFrame:
    return pd.read_parquet(filepath)


def read_in_with_duck_db(filepath: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{filepath}'").df()
    return df


if __name__ == "__main__":
    filepath = Path("/Users/jeffgruenbaum/projects/big-data-playground/data/mock_10million_data.parquet")
    n = 5
    output = timeit("df = read_in_with_dask_pd(filepath)", number=n, globals={**locals(), **globals()})
    print(f"Average time for dask.read_parquet was {output / n } seconds")

    output = timeit("df = read_in_with_polars_pd(filepath)", number=n, globals={**locals(), **globals()})
    print(f"Average time for polars.read_parquet was {output / n } seconds")

    output = timeit("df = read_in_with_pandas(filepath)", number=n, globals={**locals(), **globals()})
    print(f"Average time for pandas.read_parquet was {output / n } seconds")

    output = timeit("read_in_with_duck_db(filepath)", number=n, globals={**locals(), **globals()})
    print(f"Average time for duckdb was {output / n } seconds")

"""
Average time for dask.read_parquet was 0.0030716000124812125 seconds
Average time for polars.read_parquet was 6.727499421685935e-05 seconds
Average time for pandas.read_parquet was 4.082090950012207 seconds
Average time for duckdb was 6.022534733405337 seconds
"""
