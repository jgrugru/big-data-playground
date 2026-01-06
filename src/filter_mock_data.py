from pathlib import Path
from typing import Literal

import pandas as pd

from src.utils.timing import fn_timer


@fn_timer(log_output=False)
def read_in_csv_file(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    return df


@fn_timer(log_output=False)
def read_in_parquet_file(filepath: Path) -> pd.DataFrame:
    df = pd.read_parquet(filepath)
    return df


@fn_timer(log_input=False, log_output=False)
def filter_by_starts_with_str(df: pd.DataFrame, col: str, starts_with_str: str) -> pd.DataFrame:
    mask = df[col].str.startswith(starts_with_str)
    return df.loc[mask]


@fn_timer(log_input=False, log_output=False)
def filter_by_ends_with_str(df: pd.DataFrame, col: str, ends_with_str: str) -> pd.DataFrame:
    mask = df[col].str.endswith(ends_with_str)
    return df.loc[mask]


@fn_timer(log_input=False, log_output=False)
def filter_by_gender(df: pd.DataFrame, gender: Literal["male", "female"]) -> pd.DataFrame:
    mask = df["gender"] == gender
    return df.loc[mask]


def main():
    """
    We live in a distopyian society of 1 million people where the qualifications to run for mayor are:
     - live in New Mexico
     - have 35 bioligical kids
     - have the name Sarah
     - must be female
     - have a legit email account

    Let's find all qualified, mayorial candidates.
    """
    # csv_input_path = Path("pandas_optimization/mock_million_data.csv")
    parquet_input_path = Path("pandas_optimization/mock_million_data.parquet")
    dtypes = {
        "name": "string",
        "address": "string",
        "email": "string",
        "date_of_birth": "string",
        "biological_kids": "int8",
        "gender": "category",
    }

    # ----Read in mock data----
    # takes about 0.73 seconds
    # mock_data_df = read_in_csv_file(csv_input_path)
    # takes about 0.37 seconds ~ 49% less time (1.97× faster)
    df: pd.DataFrame = read_in_parquet_file(parquet_input_path)
    typed_df = df.astype(dtypes)

    # ----Filter by gender----
    # takes about 0.034 seconds
    # filter_by_gender(df, "female")
    # takes about 0.02 seconds ~ 41% less time (1.70× faster)
    df = filter_by_gender(typed_df, "female")

    # ----Filter name Sarah----
    # takes about 0.076 seconds
    # filter_by_starts_with_str(df, "name", "Sarah")
    # takes about 0.061 seconds ~ 20% less time (1.25× faster)
    df = filter_by_starts_with_str(typed_df, "name", "Sarah")

    # ----Filter legit emails----
    # takes about 0.097 seconds
    # filter_by_ends_with_str(df, "email", ".net")
    # takes about 0.083 seconds ~ 14% less time (1.17× faster)
    df = filter_by_ends_with_str(typed_df, "email", ".net")
    print(df[["name", "email"]])


if __name__ == "__main__":
    main()
