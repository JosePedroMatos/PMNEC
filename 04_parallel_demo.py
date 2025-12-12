import math
import os
import time
import threading
import multiprocessing
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# -------- I/O-bound with threads --------
def fake_download(idx: int) -> str:
    time.sleep(0.2)  # simulate network/disk wait
    return f"item-{idx}"

def run_thread_pool():
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as pool:
        for result in pool.map(fake_download, range(10)):
            print("thread result", result)
    elapsed = time.perf_counter() - start
    print(f"Threaded I/O finished in {elapsed:.2f}s\n")

# -------- CPU-bound with processes --------
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
    time.sleep(0.05)  # keep workers busy
    return sum(1 for x in range(n) if is_prime(x))

def run_process_pool(workers: int | None = None):
    workers = workers or os.cpu_count()
    inputs = [10_000_000 + i * 50_000 for i in range(workers)]
    start = time.perf_counter()
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        counts = list(pool.map(count_primes_up_to, inputs))
    elapsed = time.perf_counter() - start
    for n, count in zip(inputs, counts):
        print(f"Primes below {n:,}: {count}")
    print(f"Process pool completed in {elapsed:.2f}s using {workers} workers\n")

# -------- Shared state with threads --------
counter = 0
counter_lock = threading.Lock()

def add_many(n: int):
    global counter
    for _ in range(n):
        with counter_lock:
            counter += 1

def run_thread_lock_demo():
    global counter
    counter = 0
    threads = [threading.Thread(target=add_many, args=(100_000,)) for _ in range(5)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    print(f"Counter = {counter} (expected 500000)")
    print(f"Locked increments completed in {elapsed:.2f}s\n")

# -------- Producer/consumer with queue --------

def run_queue_demo():
    q: Queue[str] = Queue()
    results: list[tuple[str, str]] = []

    for i in range(5):
        q.put(f"task-{i}")

    def worker(name: str):
        while True:
            try:
                item = q.get_nowait()
            except Exception:
                break
            time.sleep(0.1)
            results.append((name, item))
            q.task_done()

    threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("Queue results (unordered):")
    for entry in results:
        print(entry)
    print()


def main():
    print(f"Logical CPUs: {os.cpu_count()}")

    print("--- Thread pool (I/O-bound) ---")
    run_thread_pool()

    print("--- Process pool (CPU-bound) ---")
    run_process_pool()

    print("--- Shared state with lock ---")
    run_thread_lock_demo()

    print("--- Producer/consumer queue ---")
    run_queue_demo()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    multiprocessing.set_start_method("spawn", force=True)
    main()
