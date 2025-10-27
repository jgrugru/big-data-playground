from pathlib import Path

import duckdb

from src.utils.timing import fn_timer

SQL = r"""
WITH base AS (
  SELECT
      name,
      address,
      email,
      -- robustly coerce to DATE whether parquet has DATE/TIMESTAMP/STRING
      COALESCE(
        CAST(date_of_birth AS DATE),
        CAST(try_strptime(CAST(date_of_birth AS VARCHAR), '%Y-%m-%d') AS DATE),
        CAST(try_strptime(CAST(date_of_birth AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS DATE)
      ) AS dob,
      biological_kids,
      gender
  FROM read_parquet(?)
),
females AS (
  SELECT * FROM base WHERE gender = 'female'
),
chars AS (
  SELECT
      *,
      -- first letter after optional leading house number/spaces
      regexp_extract(address, '^(?:\\s*\\d+\\s*)*([A-Za-z])', 1) AS first_char_of_address,
      substr(name, 1, 1) AS first_char_of_name
  FROM females
),
char_match AS (
  SELECT * FROM chars WHERE first_char_of_address = first_char_of_name
),
aged AS (
  SELECT
      *,
      -- birthday-accurate integer age
      date_diff('year', dob, current_date) AS age_years
  FROM char_match
),
sixty AS (
  SELECT * FROM aged WHERE age_years = 60
),
no_digit_email AS (
  SELECT * FROM sixty WHERE NOT regexp_matches(email, '[0-9]')
),
kids AS (
  SELECT * FROM no_digit_email WHERE biological_kids = 21
)
SELECT
  name, address, email, dob AS date_of_birth, biological_kids, gender
FROM kids
"""


def read_in_with_duck_db(filepath: Path):
    con = duckdb.connect()
    return con.execute(SQL, [str(filepath)]).df()


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

    df = read_in_with_duck_db(parquet_input_path)
    print(df)
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
