import time
from functools import wraps
import psutil
import threading
from typing import Dict, Any


def numerize(n: int) -> str:
    """display positive number n for readability

    Examples:
        >>> numerize(1_000)
        '1K'
        >>> numerize(1_000_000_000)
        '1B'
    """
    sufix2upbound = {
        "EMPTY": 1e3,
        "K": 1e6,
        "M": 1e9,
        "B": 1e12,
        "END": float("inf"),
    }

    display_n, sufix = n, ""
    for s, base in sufix2upbound.items():
        # number >= 1000B will alway have sufix 'B'
        if s == "END":
            display_n = int(n / 1e9)
            sufix = "B"
            break

        if n < base:
            sufix = "" if s == "EMPTY" else s
            display_n = int(n / (base / 1e3))
            break
    return f"{display_n}{sufix}"


def time_it(func: any):
    """returns result and elapsed time"""

    @wraps(func)
    def inner(*args, **kwargs):
        pref = time.perf_counter()
        result = func(*args, **kwargs)
        delta = time.perf_counter() - pref
        return result, delta

    return inner


def compose_train_files(train_count: int, use_shuffled: bool) -> list[str]:
    prefix = "shuffle_train" if use_shuffled else "train"
    middle = f"of-{train_count}"
    surfix = "parquet"

    train_files = []
    if train_count > 1:
        just_size = 2
        for i in range(train_count):
            sub_file = f"{prefix}-{str(i).rjust(just_size, '0')}-{middle}.{surfix}"
            train_files.append(sub_file)
    else:
        train_files.append(f"{prefix}.{surfix}")

    return train_files


ONE_PERCENT = 0.01
NINETY_NINE_PERCENT = 0.99


def compose_gt_file(filters: float | str | None = None) -> str:
    if filters is None:
        return "neighbors.parquet"

    if filters == ONE_PERCENT:
        return "neighbors_head_1p.parquet"

    if filters == NINETY_NINE_PERCENT:
        return "neighbors_tail_1p.parquet"

    msg = f"Filters not supported: {filters}"
    raise ValueError(msg)


class ResourceMonitor:
    """Monitor system resources during benchmark runs."""

    def __init__(self):
        self.cpu_usages = []
        self.memory_usages = []
        self.disk_read_bytes = 0
        self.disk_write_bytes = 0
        self.monitoring = False
        self.thread = None
        self.start_disk_io = None

    def start_monitoring(self):
        """Start monitoring resources in a background thread."""
        if self.monitoring:
            return
        self.monitoring = True
        self.cpu_usages = []
        self.memory_usages = []
        self.start_disk_io = psutil.disk_io_counters()
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop monitoring and return collected metrics."""
        if not self.monitoring:
            return {}
        self.monitoring = False
        if self.thread:
            self.thread.join(timeout=1.0)

        end_disk_io = psutil.disk_io_counters()
        if self.start_disk_io and end_disk_io:
            self.disk_read_bytes = end_disk_io.read_bytes - self.start_disk_io.read_bytes
            self.disk_write_bytes = end_disk_io.write_bytes - self.start_disk_io.write_bytes

        avg_cpu = sum(self.cpu_usages) / len(self.cpu_usages) if self.cpu_usages else 0.0
        peak_cpu = max(self.cpu_usages) if self.cpu_usages else 0.0
        avg_mem = sum(self.memory_usages) / len(self.memory_usages) if self.memory_usages else 0.0
        peak_mem = max(self.memory_usages) if self.memory_usages else 0.0

        return {
            'avg_cpu_usage': avg_cpu,
            'peak_cpu_usage': peak_cpu,
            'avg_memory_usage': avg_mem / (1024 * 1024),  # Convert to MB
            'peak_memory_usage': peak_mem / (1024 * 1024),  # Convert to MB
            'disk_read_bytes': self.disk_read_bytes,
            'disk_write_bytes': self.disk_write_bytes,
        }

    def _monitor_loop(self):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                self.cpu_usages.append(psutil.cpu_percent(interval=1.0))
                self.memory_usages.append(psutil.virtual_memory().used)
            except Exception:
                # Ignore monitoring errors
                pass
