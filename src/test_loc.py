import time
from functools import partial
from pathlib import Path
from typing import Callable

import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from devtools import debug
from matplotlib.ticker import FuncFormatter

from src.utils.timing import fn_timer


@fn_timer(log_output=False)
def read_in_parquet_file(filepath: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    # df = pd.read_parquet(filepath)
    df = dd.read_parquet(path=filepath).compute()
    dtypes = {
        "name": "string",
        "address": "string",
        "email": "string",
        "date_of_birth": "datetime64[ns]",
        "biological_kids": "int8",
        "gender": "category",
    }
    df_w_dtypes = df.astype(dtypes)
    return df, df_w_dtypes


@fn_timer(log_input=False, log_output=False)
def filter_by_starts_with_str(df: pd.DataFrame, col: str, starts_with_str: str) -> pd.DataFrame:
    mask = df[col].str.startswith(starts_with_str)
    return df.loc[mask]


@fn_timer(log_input=False, log_output=False)
def filter_by_ends_with_str(df: pd.DataFrame, col: str, ends_with_str: str) -> pd.DataFrame:
    mask = df[col].str.endswith(ends_with_str)
    return df.loc[mask]


@fn_timer(log_input=False, log_output=False)
def filter_by_gender_eq(df: pd.DataFrame, value: str) -> pd.DataFrame:
    return df.loc[df["gender"] == value]


@fn_timer(log_input=False, log_output=False)
def filter_by_gender_isin(df: pd.DataFrame, values: tuple[str, ...]) -> pd.DataFrame:
    return df.loc[df["gender"].isin(values)]


@fn_timer(log_input=False, log_output=False)
def filter_kids_eq(df: pd.DataFrame, k: int) -> pd.DataFrame:
    return df.loc[df["biological_kids"] == k]


@fn_timer(log_input=False, log_output=False)
def filter_kids_ge(df: pd.DataFrame, k: int) -> pd.DataFrame:
    return df.loc[df["biological_kids"] >= k]


@fn_timer(log_input=False, log_output=False)
def filter_kids_between(df: pd.DataFrame, low: int, high: int, inclusive: str = "both") -> pd.DataFrame:
    return df.loc[df["biological_kids"].between(low, high, inclusive=inclusive)]


def get_function_times(df: pd.DataFrame, fn: Callable, n: int, each_iteration_diff: int) -> dict[int, float]:
    plotting_points = {}
    for count in range(n, 0, -1):
        function_times = np.array(list())
        for i in range(15):
            n = (count) * each_iteration_diff
            n_df = df.head(n)
            print(n)

            t1 = time.time()
            fn(df=n_df)
            t2 = time.time()
            function_time = t2 - t1
            function_times = np.append(function_times, function_time)
        plotting_points[n] = function_times.mean()

    return plotting_points


def compare_fn_on_dfs(untyped_df: pd.DataFrame, typed_df: pd.DataFrame, fn: Callable, iterations: int, each_iteration_df: int, plot_title: str, plot_filepath: Path):
    plotting_points_untyped_df = get_function_times(untyped_df, fn, iterations, each_iteration_df)
    plotting_points_typed_df = get_function_times(typed_df, fn, iterations, each_iteration_df)

    debug(plotting_points_untyped_df)
    debug(plotting_points_typed_df)

    x1 = sorted(plotting_points_untyped_df.keys())
    y1 = [plotting_points_untyped_df[k] for k in x1]
    np1 = np.array(y1)

    x2 = sorted(plotting_points_typed_df.keys())
    y2 = [plotting_points_typed_df[k] for k in x2]
    np2 = np.array(y2)

    average_speedup = (np1 / np2).mean()

    plt.plot(x1, y1, marker="o", label="untyped_df")
    plt.plot(x2, y2, marker="o", label="typed_df")
    plt.xlabel("df_size_n")
    plt.ylabel("seconds_s")
    plt.title(f"{plot_title} | Avg speedup: {average_speedup:.2f}×")
    plt.grid(True)
    plt.legend()

    formatter = FuncFormatter(lambda x, _: f"{int(x):,}")
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.savefig(plot_filepath, format="png")
    plt.clf()


def main():
    parquet_input_path = Path("pandas_optimization/mock_10million_data.parquet")
    untyped_df, typed_df = read_in_parquet_file(parquet_input_path)

    ROW_COUNT = int(len(untyped_df))
    ITERATIONS = 20
    EACH_ITERATION_DIFF = int(ROW_COUNT / ITERATIONS)

    compare_fn_on_dfs(
        untyped_df=untyped_df,
        typed_df=typed_df,
        fn=partial(filter_by_ends_with_str, col="email", ends_with_str=".net"),
        iterations=ITERATIONS,
        each_iteration_df=EACH_ITERATION_DIFF,
        plot_title="df['email'].str.endswith(['.net']) performance -> O(n)",
        plot_filepath=Path("pandas_optimization/plots/filter_by_ends_with_str.png"),
    )

    # compare_fn_on_dfs(
    #     untyped_df=untyped_df,
    #     typed_df=typed_df,
    #     fn=partial(filter_by_starts_with_str, col="name", starts_with_str="Sarah"),
    #     iterations=ITERATIONS,
    #     each_iteration_df=EACH_ITERATION_DIFF,
    #     plot_title="df['name'].str.startswith('Sarah') performance -> O(n)",
    #     plot_filepath=Path("pandas_optimization/plots/filter_by_starts_with_str.png"),
    # )

    # compare_fn_on_dfs(
    #     untyped_df=untyped_df,
    #     typed_df=typed_df,
    #     fn=partial(filter_by_gender_eq, value="female"),
    #     iterations=ITERATIONS,
    #     each_iteration_df=EACH_ITERATION_DIFF,
    #     plot_title="df.loc[df['gender] == value] performance -> O(n)",
    #     plot_filepath=Path("pandas_optimization/plots/filter_by_gender_eq.png"),
    # )

    # compare_fn_on_dfs(
    #     untyped_df=untyped_df,
    #     typed_df=typed_df,
    #     fn=partial(filter_by_gender_isin, values={"female"}),
    #     iterations=ITERATIONS,
    #     each_iteration_df=EACH_ITERATION_DIFF,
    #     plot_title="df.loc[df['gender'].isin(values)] performance -> O(n)",
    #     plot_filepath=Path("pandas_optimization/plots/filter_by_gender_isin.png"),
    # )

    # compare_fn_on_dfs(
    #     untyped_df=untyped_df,
    #     typed_df=typed_df,
    #     fn=partial(filter_kids_eq, k=30),
    #     iterations=ITERATIONS,
    #     each_iteration_df=EACH_ITERATION_DIFF,
    #     plot_title="df.loc[df['gender'].isin(values)] performance -> O(n)",
    #     plot_filepath=Path("pandas_optimization/plots/filter_kids_eq.png"),
    # )


if __name__ == "__main__":
    main()
