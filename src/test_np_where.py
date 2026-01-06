from pathlib import Path

import numpy as np
import pandas as pd

from utils.timing import fn_timer


@fn_timer(log_output=False)
def read_in_parquet_file(filepath: Path) -> pd.DataFrame:
    df = pd.read_parquet(filepath)
    return df


@fn_timer(log_input=False, log_output=False)
def lambda_apply_df(df: pd.DataFrame):
    filtered = df[
        df.apply(
            lambda row: (row["gender"].lower() == "female" and row["biological_kids"] > 5 and str(row["name"]).split()[0].lower().startswith("j"),),
            axis=1,
        )
    ]
    return filtered


@fn_timer(log_input=False, log_output=False)
def np_where_df(df: pd.DataFrame):
    mask = np.where(
        (df["gender"].str.lower() == "female"),
        np.where(
            (df["biological_kids"] > 5),
            np.where(
                df["name"].str.split().str[0].str.lower().str.startswith("j"),
                True,
                False,
            ),
            False,
        ),
        False,
    )
    return df.loc[mask]


@fn_timer(log_output=False)
def main():
    parquet_input_path = Path("data/mock_10million_data.parquet")
    dtypes = {
        "name": "string",
        "address": "string",
        "email": "string",
        "date_of_birth": "datetime64[ns]",
        "biological_kids": "int8",
        "gender": "category",
    }

    df: pd.DataFrame = read_in_parquet_file(parquet_input_path)
    df = df.astype(dtypes)

    print("starting lambda apply")
    lambda_apply = lambda_apply_df(df)
    print(lambda_apply)

    print("starting np where")
    np_where = np_where_df(df)
    print(np_where)


if __name__ == "__main__":
    main()
