import logging
from dataclasses import dataclass, field

import numpy as np

log = logging.getLogger(__name__)


@dataclass
class Metric:
    """result metrics"""

    # for load cases
    max_load_count: int = 0

    # for both performace and streaming cases
    insert_duration: float = 0.0
    optimize_duration: float = 0.0
    load_duration: float = 0.0  # insert + optimize

    # for performance cases
    qps: float = 0.0
    # Search latency from SerialSearchRunner (perf_counter deltas): stored as seconds in JSON / live runs.
    # TestResult.read_file(..., trans_unit=True) multiplies these by 1000 for the results UI (ms).
    serial_latency_p99: float = 0.0
    serial_latency_p95: float = 0.0
    recall: float = 0.0
    ndcg: float = 0.0
    conc_num_list: list[int] = field(default_factory=list)
    conc_qps_list: list[float] = field(default_factory=list)
    conc_latency_p99_list: list[float] = field(default_factory=list)
    conc_latency_p95_list: list[float] = field(default_factory=list)
    conc_latency_avg_list: list[float] = field(default_factory=list)

    # for streaming cases
    st_ideal_insert_duration: int = 0
    st_search_stage_list: list[int] = field(default_factory=list)
    st_search_time_list: list[float] = field(default_factory=list)
    st_max_qps_list_list: list[float] = field(default_factory=list)
    st_recall_list: list[float] = field(default_factory=list)
    st_ndcg_list: list[float] = field(default_factory=list)
    st_serial_latency_p99_list: list[float] = field(default_factory=list)
    st_serial_latency_p95_list: list[float] = field(default_factory=list)
    st_conc_failed_rate_list: list[float] = field(default_factory=list)

    # for streaming cases - concurrent latency data per stage
    st_conc_num_list_list: list[list[int]] = field(default_factory=list)
    st_conc_qps_list_list: list[list[float]] = field(default_factory=list)
    st_conc_latency_p99_list_list: list[list[float]] = field(default_factory=list)
    st_conc_latency_p95_list_list: list[list[float]] = field(default_factory=list)
    st_conc_latency_avg_list_list: list[list[float]] = field(default_factory=list)

    # New: Resource usage metrics
    avg_cpu_usage: float = 0.0  # Average CPU usage during run (%)
    peak_cpu_usage: float = 0.0  # Peak CPU usage (%)
    avg_memory_usage: float = 0.0  # Average memory usage (MB)
    peak_memory_usage: float = 0.0  # Peak memory usage (MB)
    disk_read_bytes: int = 0  # Total disk read bytes
    disk_write_bytes: int = 0  # Total disk write bytes

    # Host DB data dir only (CLICKHOUSE_DATA_DIR / MILVUS_DATA_DIR / QDRANT_DATA_DIR / PGVECTOR_DATA_DIR).
    bench_db_host_data_dir_path: str = ""
    bench_db_host_data_dir_bytes_begin: int = 0
    bench_db_host_data_dir_bytes_end: int = 0
    bench_db_host_data_dir_bytes_written: int = 0  # max(0, end - begin) for this case

    # New: Detailed query efficiency metrics
    read_qps: float = 0.0  # Read (search) QPS
    write_qps: float = 0.0  # Write (insert) QPS
    update_qps: float = 0.0  # Update QPS (if supported)
    # Mirrored from serial path or other runners: seconds on disk / pre-trans_unit; ms after read_file(trans_unit=True).
    read_latency_p99: float = 0.0
    write_latency_p99: float = 0.0
    update_latency_p99: float = 0.0
    read_throughput: float = 0.0  # Read throughput (queries/sec)
    write_throughput: float = 0.0  # Write throughput (inserts/sec)
    update_throughput: float = 0.0  # Update throughput (updates/sec)


QURIES_PER_DOLLAR_METRIC = "QP$ (Quries per Dollar)"
LOAD_DURATION_METRIC = "load_duration"
SERIAL_LATENCY_P99_METRIC = "serial_latency_p99"
SERIAL_LATENCY_P95_METRIC = "serial_latency_p95"
MAX_LOAD_COUNT_METRIC = "max_load_count"
QPS_METRIC = "qps"
RECALL_METRIC = "recall"
AVG_CPU_USAGE_METRIC = "avg_cpu_usage"
PEAK_CPU_USAGE_METRIC = "peak_cpu_usage"
AVG_MEMORY_USAGE_METRIC = "avg_memory_usage"
PEAK_MEMORY_USAGE_METRIC = "peak_memory_usage"
DISK_READ_BYTES_METRIC = "disk_read_bytes"
DISK_WRITE_BYTES_METRIC = "disk_write_bytes"
BENCH_DB_HOST_DATA_DIR_PATH_METRIC = "bench_db_host_data_dir_path"
BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC = "bench_db_host_data_dir_bytes_begin"
BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC = "bench_db_host_data_dir_bytes_end"
BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC = "bench_db_host_data_dir_bytes_written"
READ_QPS_METRIC = "read_qps"
WRITE_QPS_METRIC = "write_qps"
UPDATE_QPS_METRIC = "update_qps"
READ_LATENCY_P99_METRIC = "read_latency_p99"
WRITE_LATENCY_P99_METRIC = "write_latency_p99"
UPDATE_LATENCY_P99_METRIC = "update_latency_p99"
READ_THROUGHPUT_METRIC = "read_throughput"
WRITE_THROUGHPUT_METRIC = "write_throughput"
UPDATE_THROUGHPUT_METRIC = "update_throughput"

# Concurrency & other (results-page "Other" section)
INSERT_DURATION_METRIC = "insert_duration"
OPTIMIZE_DURATION_METRIC = "optimize_duration"
NDCG_METRIC = "ndcg"
CONC_NUM_LIST_METRIC = "conc_num_list"
CONC_QPS_LIST_METRIC = "conc_qps_list"
CONC_LATENCY_P99_LIST_METRIC = "conc_latency_p99_list"
CONC_LATENCY_P95_LIST_METRIC = "conc_latency_p95_list"
CONC_LATENCY_AVG_LIST_METRIC = "conc_latency_avg_list"

# Concurrency metrics are not captured or shown in results
CONCURRENCY_METRIC_KEYS = frozenset({
    CONC_NUM_LIST_METRIC,
    CONC_QPS_LIST_METRIC,
    CONC_LATENCY_P99_LIST_METRIC,
    CONC_LATENCY_P95_LIST_METRIC,
    CONC_LATENCY_AVG_LIST_METRIC,
})

metric_unit_map = {
    LOAD_DURATION_METRIC: "s",
    # ms after TestResult.read_file(trans_unit=True); raw JSON is stored in seconds.
    SERIAL_LATENCY_P99_METRIC: "ms",
    SERIAL_LATENCY_P95_METRIC: "ms",
    MAX_LOAD_COUNT_METRIC: "K",
    QURIES_PER_DOLLAR_METRIC: "K",
    AVG_CPU_USAGE_METRIC: "%",
    PEAK_CPU_USAGE_METRIC: "%",
    AVG_MEMORY_USAGE_METRIC: "MB",
    PEAK_MEMORY_USAGE_METRIC: "MB",
    DISK_READ_BYTES_METRIC: "bytes",
    DISK_WRITE_BYTES_METRIC: "bytes",
    BENCH_DB_HOST_DATA_DIR_PATH_METRIC: "",
    BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC: "bytes",
    BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC: "bytes",
    BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC: "bytes",
    READ_QPS_METRIC: "qps",
    WRITE_QPS_METRIC: "qps",
    UPDATE_QPS_METRIC: "qps",
    READ_LATENCY_P99_METRIC: "ms",
    WRITE_LATENCY_P99_METRIC: "ms",
    UPDATE_LATENCY_P99_METRIC: "ms",
    READ_THROUGHPUT_METRIC: "ops/s",
    WRITE_THROUGHPUT_METRIC: "ops/s",
    UPDATE_THROUGHPUT_METRIC: "ops/s",
    INSERT_DURATION_METRIC: "s",
    OPTIMIZE_DURATION_METRIC: "s",
    NDCG_METRIC: "(0-1)",
    CONC_NUM_LIST_METRIC: "",
    CONC_QPS_LIST_METRIC: "qps",
    CONC_LATENCY_P99_LIST_METRIC: "s",
    CONC_LATENCY_P95_LIST_METRIC: "s",
    CONC_LATENCY_AVG_LIST_METRIC: "s",
}

lower_is_better_metrics = [
    LOAD_DURATION_METRIC,
    SERIAL_LATENCY_P99_METRIC,
    SERIAL_LATENCY_P95_METRIC,
    AVG_CPU_USAGE_METRIC,
    PEAK_CPU_USAGE_METRIC,
    AVG_MEMORY_USAGE_METRIC,
    PEAK_MEMORY_USAGE_METRIC,
    DISK_READ_BYTES_METRIC,
    DISK_WRITE_BYTES_METRIC,
    READ_LATENCY_P99_METRIC,
    WRITE_LATENCY_P99_METRIC,
    UPDATE_LATENCY_P99_METRIC,
    INSERT_DURATION_METRIC,
    OPTIMIZE_DURATION_METRIC,
]

metric_order = [
    QPS_METRIC,
    RECALL_METRIC,
    LOAD_DURATION_METRIC,
    SERIAL_LATENCY_P99_METRIC,
    SERIAL_LATENCY_P95_METRIC,
    MAX_LOAD_COUNT_METRIC,
    AVG_CPU_USAGE_METRIC,
    PEAK_CPU_USAGE_METRIC,
    AVG_MEMORY_USAGE_METRIC,
    PEAK_MEMORY_USAGE_METRIC,
    DISK_READ_BYTES_METRIC,
    DISK_WRITE_BYTES_METRIC,
    BENCH_DB_HOST_DATA_DIR_PATH_METRIC,
    BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC,
    BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC,
    BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC,
    READ_QPS_METRIC,
    WRITE_QPS_METRIC,
    UPDATE_QPS_METRIC,
    READ_LATENCY_P99_METRIC,
    WRITE_LATENCY_P99_METRIC,
    UPDATE_LATENCY_P99_METRIC,
    READ_THROUGHPUT_METRIC,
    WRITE_THROUGHPUT_METRIC,
    UPDATE_THROUGHPUT_METRIC,
    INSERT_DURATION_METRIC,
    OPTIMIZE_DURATION_METRIC,
    NDCG_METRIC,
]


def isLowerIsBetterMetric(metric: str) -> bool:
    return metric in lower_is_better_metrics


def calc_recall(count: int, ground_truth: list[int], got: list[int]) -> float:
    recalls = np.zeros(count)
    for i, result in enumerate(got):
        if result in ground_truth:
            recalls[i] = 1

    return np.mean(recalls)


def get_ideal_dcg(k: int):
    ideal_dcg = 0
    for i in range(k):
        ideal_dcg += 1 / np.log2(i + 2)

    return ideal_dcg


def calc_ndcg(ground_truth: list[int], got: list[int], ideal_dcg: float) -> float:
    dcg = 0
    ground_truth = list(ground_truth)
    for got_id in set(got):
        if got_id in ground_truth:
            idx = ground_truth.index(got_id)
            dcg += 1 / np.log2(idx + 2)
    return dcg / ideal_dcg
