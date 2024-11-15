import logging
import time
from functools import wraps


def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"Function {func.__name__} took {end_time - start_time:.4f} seconds")
        return result

    return wrapper


def rate_limiter(max_requests_per_second):
    min_interval = 1 / max_requests_per_second  # Minimum time between requests
    last_call_time = 0  # Track the last call time

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_call_time
            current_time = time.time()
            elapsed = current_time - last_call_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            last_call_time = time.time()
            return func(*args, **kwargs)

        return wrapper

    return decorator

