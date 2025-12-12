import math
import os
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    limit = int(math.sqrt(n)) + 1
    for i in range(3, limit, 2):
        if n % i == 0:
            return False
    return True


def count_primes_up_to(n: int) -> int:
    time.sleep(0.05)
    return sum(1 for x in range(n) if is_prime(x))


def run_prime_pool(workers=None, n_inputs=None):
    workers = workers or os.cpu_count() or 2
    n_inputs = n_inputs or workers * 3
    inputs = [750_000 + i * 5_000 for i in range(n_inputs)]
    ctx = multiprocessing.get_context("spawn")
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        counts = list(pool.map(count_primes_up_to, inputs))
    elapsed = time.perf_counter() - start
    for n, count in zip(inputs, counts):
        print(f"Primes below {n:,}: {count}")
    print(f"Process pool completed in {elapsed:.2f}s using {workers} workers")


if __name__ == "__main__":
    # Allow running as script for quick testing
    multiprocessing.freeze_support()
    run_prime_pool(workers=4)
