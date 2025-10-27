from pathlib import Path

import dask.dataframe as dd

from src.utils.timing import fn_timer


def read_in_parquet_file(filepath: Path) -> dd:
    return dd.read_parquet(path=filepath)


# @fn_timer(log_output=False)
# def filter_out_males(df: pd.DataFrame) -> pd.DataFrame:
#     return df.loc[df["gender"] == "female"]

# @fn_timer(log_output=False)
# def get_name_and_address_char_match_rows(df: pd.DataFrame) -> pd.DataFrame:
#     filtered_df = df.assign(
#         first_char_of_address=df["address"].str.extract(r"^\s*\d+\s*([^\s])", expand=False)
#     )
#     filtered_df = filtered_df.assign(
#         first_char_of_name=filtered_df["name"].str.slice(0, 1)
#     )
#     filtered_df = filtered_df.loc[filtered_df["first_char_of_address"] == filtered_df["first_char_of_name"]]
#     return filtered_df

# @fn_timer(log_output=False)
# def get_sixty_year_olds(df: pd.DataFrame) -> pd.DataFrame:
#     current_datetime = pd.Timestamp('now')
#     filtered_df = df.assign(
#         age=((current_datetime - df["date_of_birth"]).dt.days / 365).astype("int")
#     )
#     filtered_df = filtered_df.loc[filtered_df["age"] == 60]
#     return filtered_df

# @fn_timer(log_output=False)
# def get_no_digits_in_email(df: pd.DataFrame) -> pd.DataFrame:
#     return df.loc[~df["email"].str.contains(r"\d", na=False)]

# @fn_timer(log_output=False)
# def get_candidates_with_21_kids(df: pd.DataFrame) -> pd.DataFrame:
#     return df.loc[df["biological_kids"] == 21]


@fn_timer(log_output=False)
def main():
    parquet_input_path = Path("/Users/jeffgruenbaum/projects/wex-option-allocations/pandas_optimization/mock_10million_data.parquet")
    # dtypes = {
    #     "name": "string",
    #     "address": "string",
    #     "email": "string",
    #     "date_of_birth": "datetime64[ns]",
    #     "biological_kids": "int8",
    #     "gender": "category",
    # }

    df = read_in_parquet_file(parquet_input_path)
    print(df)
    # typed_df = df.astype(dtypes)

    # # 1
    # candidates = filter_out_males(df=typed_df)

    # # 2
    # candidates = get_name_and_address_char_match_rows(df=candidates)

    # # 3
    # candidates = get_sixty_year_olds(df=candidates)

    # # 4
    # candidates = get_no_digits_in_email(df=candidates)

    # # 5
    # candidates = get_candidates_with_21_kids(df=candidates)

    # candidates = candidates.drop(["first_char_of_address", "first_char_of_name", "age"], axis=1).reset_index(drop=True)


if __name__ == "__main__":
    main()
