from pathlib import Path

import pandas as pd
from faker import Faker
from tqdm import tqdm


def make_row(fake: Faker) -> dict:
    address = fake.address().replace("\n", ", ")
    dob = fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat()
    return {
        "name": fake.name(),
        "address": address,
        "email": fake.email(),
        "date_of_birth": dob,
        "biological_kids": fake.pyint(min_value=0, max_value=69),  # 69 kids is the current guinness world record for most kids by one individual
        "gender": fake.random_element(elements=("male", "female")),
    }


def main():
    csv_output_path = Path("pandas_optimization/mock_million_data.csv")
    parquet_output_path = Path("pandas_optimization/mock_million_data.parquet")

    faker_client = Faker()
    n = 10_000_000
    rows = []

    for _ in tqdm(range(n)):
        new_row = make_row(faker_client)
        rows.append(new_row)

    output_df = pd.DataFrame(rows)
    output_df.to_csv(csv_output_path, index=False)
    output_df.to_parquet(parquet_output_path, index=False, compression="snappy")


if __name__ == "__main__":
    main()
