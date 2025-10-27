import functools
import time
from typing import Callable

from loguru import logger


def fn_timer(print_fn: Callable = logger.info, log_input: bool = True, log_output: bool = True):

    def decorator(func: Callable):
        name = func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # if log_input:
            #     print_fn(f"Entering '{name}' (args={args}, kwargs={kwargs})")
            # else:
            #     print_fn(f"Entering '{name}'.")
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            if log_output:
                print_fn(f"'{name}': {elapsed_time} seconds -- (result={result})")
            else:
                print_fn(f"'{name}': {elapsed_time} seconds")

            return result

        return wrapper

    return decorator
