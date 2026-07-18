"""
Tooltips and grouping for benchmark result metrics on the results page.
Used by check_results/charts.py to add help text and group metrics under headings.
"""

from vectordb_bench.metric import (
    AVG_CPU_USAGE_METRIC,
    AVG_MEMORY_USAGE_METRIC,
    AVG_MEMORY_USAGE_TOTAL_METRIC,
    BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC,
    BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC,
    BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC,
    BENCH_DB_HOST_DATA_DIR_PATH_METRIC,
    DISK_READ_BYTES_METRIC,
    DISK_WRITE_BYTES_METRIC,
    INSERT_DURATION_METRIC,
    LOAD_DURATION_METRIC,
    MAX_LOAD_COUNT_METRIC,
    NDCG_METRIC,
    OPTIMIZE_DURATION_METRIC,
    PEAK_CPU_USAGE_METRIC,
    PEAK_MEMORY_USAGE_METRIC,
    PEAK_MEMORY_USAGE_TOTAL_METRIC,
    QPS_METRIC,
    QURIES_PER_DOLLAR_METRIC,
    READ_LATENCY_P99_METRIC,
    READ_QPS_METRIC,
    READ_THROUGHPUT_METRIC,
    RECALL_METRIC,
    SERIAL_LATENCY_P99_METRIC,
    SERIAL_LATENCY_P95_METRIC,
    UPDATE_LATENCY_P99_METRIC,
    UPDATE_AVG_CPU_USAGE_METRIC,
    UPDATE_PEAK_CPU_USAGE_METRIC,
    UPDATE_AVG_MEMORY_USAGE_METRIC,
    UPDATE_PEAK_MEMORY_USAGE_METRIC,
    UPDATE_AVG_MEMORY_USAGE_TOTAL_METRIC,
    UPDATE_PEAK_MEMORY_USAGE_TOTAL_METRIC,
    UPDATE_QPS_METRIC,
    UPDATE_THROUGHPUT_METRIC,
    WRITE_LATENCY_P99_METRIC,
    WRITE_QPS_METRIC,
    WRITE_THROUGHPUT_METRIC,
)

# Tooltip text for each metric (description + interpretation)
RESULTS_METRIC_TOOLTIPS: dict[str, str] = {
    QPS_METRIC: (
        "Queries per second (QPS): Maximum search throughput under concurrent load. "
        "Higher is better. Reflects how many nearest-neighbor queries the database can serve per second."
    ),
    RECALL_METRIC: (
        "Recall: Fraction of true nearest neighbors found in the top-k results (e.g. 0.95 = 95%). "
        "Higher is better. Trade-off with latency and QPS; often target 0.9–0.99 for ANN."
    ),
    LOAD_DURATION_METRIC: (
        "Load duration: Time from start of insertion until the database is ready to query (insert + optimize/index build). "
        "Lower is better. In seconds. Indicates insert and index-build efficiency."
    ),
    SERIAL_LATENCY_P99_METRIC: (
        "Serial latency (p99): 99th percentile latency of a single search query in milliseconds. "
        "Lower is better. Measured without concurrent load; affected by network and hardware."
    ),
    SERIAL_LATENCY_P95_METRIC: (
        "Serial latency (p95): 95th percentile latency of a single search query in milliseconds. "
        "Lower is better. Typical tail latency for single-query performance."
    ),
    MAX_LOAD_COUNT_METRIC: (
        "Max load count: Maximum number of vectors the database could load before failure (capacity test). "
        "Higher is better. Reported in thousands (K)."
    ),
    AVG_CPU_USAGE_METRIC: (
        "Average CPU usage: Mean CPU utilization (%) during the benchmark run. "
        "When container-scoped monitoring is on (MONITOR_DB_CONTAINER_STATS), this is the DB container only "
        "(via docker stats), where 100% = one full core, so it can exceed 100% on multi-core; "
        "otherwise it is whole-host CPU (0-100%). Lower is better for resource efficiency."
    ),
    PEAK_CPU_USAGE_METRIC: (
        "Peak CPU usage: Maximum CPU utilization (%) during the run. "
        "Container-scoped when MONITOR_DB_CONTAINER_STATS is on (docker stats, can exceed 100% on multi-core); "
        "else whole-host. Lower is better. Helps size instances and avoid throttling."
    ),
    AVG_MEMORY_USAGE_METRIC: (
        "Average memory usage: Mean RAM used during the run (MB), EXCLUDING OS page cache. "
        "Container-scoped (DB container only) when MONITOR_DB_CONTAINER_STATS is on (docker stats "
        "MemUsage = memory.current - inactive_file); else whole-host used RAM minus reclaimable cache. "
        "Memory-mapped data (e.g. Qdrant vector storage, even with on_disk=false) is NOT included here "
        "— see avg_memory_usage_total for the true resident figure, or the Component Usage sheet's "
        "Cached column for a per-component breakdown. Lower is better."
    ),
    PEAK_MEMORY_USAGE_METRIC: (
        "Peak memory usage: Maximum RAM used during the run (MB), EXCLUDING OS page cache. "
        "Container-scoped (DB container only) when MONITOR_DB_CONTAINER_STATS is on; else whole-host. "
        "Can be well below total data size for mmap-backed DBs — see peak_memory_usage_total for the "
        "figure that includes page cache. Lower is better."
    ),
    AVG_MEMORY_USAGE_TOTAL_METRIC: (
        "Average total memory usage: Mean RAM used during the run (MB), INCLUDING OS page cache — "
        "this is the true resident footprint. Container-scoped: raw cgroup memory.current read via "
        "`docker exec` (no inactive_file subtraction), so mmap-backed data (e.g. Qdrant vector storage "
        "and HNSW index) IS included. Whole-host fallback: total - free. "
        "Falls back to the same value as avg_memory_usage if the cgroup file wasn't readable "
        "(e.g. docker exec disabled). Lower is better; this is the number to compare against "
        "theoretical RAM estimates and total dataset size."
    ),
    PEAK_MEMORY_USAGE_TOTAL_METRIC: (
        "Peak total memory usage: Maximum RAM used during the run (MB), INCLUDING OS page cache. "
        "Container-scoped: raw cgroup memory.current via `docker exec`; whole-host fallback: total - free. "
        "Falls back to peak_memory_usage if the cgroup file wasn't readable. "
        "Lower is better. Use this (not peak_memory_usage) for capacity planning and comparing against "
        "the Theoretical Estimates sheet."
    ),
    DISK_READ_BYTES_METRIC: (
        "Disk read: Bytes read during the run. Container block-IO (DB container only) when "
        "MONITOR_DB_CONTAINER_STATS is on; else whole-host disk read. "
        "Lower is better for I/O efficiency; high values may indicate disk-based indexes or cold caches."
    ),
    DISK_WRITE_BYTES_METRIC: (
        "Disk write: Bytes written during the run. Container block-IO (DB container only) when "
        "MONITOR_DB_CONTAINER_STATS is on; else whole-host disk write. "
        "Lower is better; reflects index build and compaction I/O."
    ),
    BENCH_DB_HOST_DATA_DIR_PATH_METRIC: (
        "Host path for this database’s on-disk files only (Docker bind mount), e.g. CLICKHOUSE_DATA_DIR. "
        "Not the dataset or results dirs. Empty when the DB has no such env path set."
    ),
    BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC: (
        "DB host data dir — net growth during this case: bytes after minus bytes before (floored at 0). "
        "Approximates how much was stored under that directory for this run. "
        "Sizing uses du/walk as the bench user, then sudo/docker du if the directory is root-only (typical ClickHouse/Postgres bind mounts). "
        "Zeros usually mean CLICKHOUSE_DATA_DIR/PGVECTOR_DATA_DIR was unset, the path was empty, or elevated du failed."
    ),
    BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC: (
        "DB host data dir — total bytes on disk at case start (recursive)."
    ),
    BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC: (
        "DB host data dir — total bytes on disk when the case finishes (recursive)."
    ),
    READ_QPS_METRIC: (
        "Read QPS: Search (read) queries per second. Higher is better. Same as QPS in search benchmarks."
    ),
    WRITE_QPS_METRIC: (
        "Write QPS: Insert throughput (vectors per second) during load. "
        "Higher is better. Insert duration = dataset size / write QPS."
    ),
    UPDATE_QPS_METRIC: (
        "Update QPS: Update/upsert throughput (ops/s). Higher is better when applicable."
    ),
    UPDATE_AVG_CPU_USAGE_METRIC: (
        "Update average CPU usage: Mean CPU utilization (%) during the update stage only. "
        "Lower is better for update efficiency."
    ),
    UPDATE_PEAK_CPU_USAGE_METRIC: (
        "Update peak CPU usage: Maximum CPU utilization (%) during the update stage only. "
        "Lower is better."
    ),
    UPDATE_AVG_MEMORY_USAGE_METRIC: (
        "Update average memory usage: Mean RAM used during the update stage (MB), excluding page cache. "
        "Lower is better."
    ),
    UPDATE_PEAK_MEMORY_USAGE_METRIC: (
        "Update peak memory usage: Maximum RAM used during the update stage (MB), excluding page cache. "
        "Lower is better."
    ),
    UPDATE_AVG_MEMORY_USAGE_TOTAL_METRIC: (
        "Update average total memory usage: Mean RAM used during the update stage (MB), including page "
        "cache (true resident figure). Lower is better."
    ),
    UPDATE_PEAK_MEMORY_USAGE_TOTAL_METRIC: (
        "Update peak total memory usage: Maximum RAM used during the update stage (MB), including page "
        "cache (true resident figure). Lower is better."
    ),
    READ_LATENCY_P99_METRIC: (
        "Read latency (p99): 99th percentile search latency in ms. Lower is better."
    ),
    WRITE_LATENCY_P99_METRIC: (
        "Write latency (p99): 99th percentile insert latency in ms. Lower is better."
    ),
    UPDATE_LATENCY_P99_METRIC: (
        "Update latency (p99): 99th percentile update latency in ms. Lower is better."
    ),
    READ_THROUGHPUT_METRIC: (
        "Read throughput: Search throughput (queries/s). Higher is better."
    ),
    WRITE_THROUGHPUT_METRIC: (
        "Write throughput: Insert throughput (inserts/s). Higher is better."
    ),
    UPDATE_THROUGHPUT_METRIC: (
        "Update throughput: Update operations per second. Higher is better."
    ),
    QURIES_PER_DOLLAR_METRIC: (
        "Queries per dollar (QP$): Cost efficiency — how many queries per dollar of database cost. Higher is better."
    ),
    # Concurrency & quality (formerly "Other" section)
    INSERT_DURATION_METRIC: (
        "Insert duration: Time to insert all vectors into the database (seconds). "
        "Lower is better. Does not include index build/optimize time; see load_duration for total time to query-ready."
    ),
    OPTIMIZE_DURATION_METRIC: (
        "Optimize duration: Time for index build/optimization after insert (seconds). "
        "Lower is better. Some databases do this during insert; others as a separate step."
    ),
    NDCG_METRIC: (
        "NDCG (Normalized Discounted Cumulative Gain): Relevance quality of top-k results (0–1). "
        "Higher is better. Measures ranking quality; 1.0 means perfect agreement with ground truth order."
    ),
}

# Group heading -> list of metric keys (order within group preserved)
RESULTS_METRIC_GROUPS: list[tuple[str, list[str]]] = [
    (
        "Search performance",
        [
            QPS_METRIC,
            RECALL_METRIC,
            SERIAL_LATENCY_P99_METRIC,
            SERIAL_LATENCY_P95_METRIC,
            READ_LATENCY_P99_METRIC,
        ],
    ),
    (
        "Load & write",
        [
            LOAD_DURATION_METRIC,
            WRITE_QPS_METRIC,
            WRITE_THROUGHPUT_METRIC,
            WRITE_LATENCY_P99_METRIC,
            MAX_LOAD_COUNT_METRIC,
        ],
    ),
    (
        "Resource utilization",
        [
            AVG_CPU_USAGE_METRIC,
            PEAK_CPU_USAGE_METRIC,
            AVG_MEMORY_USAGE_METRIC,
            PEAK_MEMORY_USAGE_METRIC,
            AVG_MEMORY_USAGE_TOTAL_METRIC,
            PEAK_MEMORY_USAGE_TOTAL_METRIC,
            DISK_READ_BYTES_METRIC,
            DISK_WRITE_BYTES_METRIC,
            BENCH_DB_HOST_DATA_DIR_PATH_METRIC,
            BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC,
            BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC,
            BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC,
        ],
    ),
    (
        "Update & cost",
        [
            UPDATE_QPS_METRIC,
            UPDATE_LATENCY_P99_METRIC,
            UPDATE_THROUGHPUT_METRIC,
            UPDATE_AVG_CPU_USAGE_METRIC,
            UPDATE_PEAK_CPU_USAGE_METRIC,
            UPDATE_AVG_MEMORY_USAGE_METRIC,
            UPDATE_PEAK_MEMORY_USAGE_METRIC,
            UPDATE_AVG_MEMORY_USAGE_TOTAL_METRIC,
            UPDATE_PEAK_MEMORY_USAGE_TOTAL_METRIC,
            QURIES_PER_DOLLAR_METRIC,
        ],
    ),
    (
        "Load & quality",
        [
            INSERT_DURATION_METRIC,
            OPTIMIZE_DURATION_METRIC,
            NDCG_METRIC,
        ],
    ),
]


# Human-readable titles for chart headers (metric key -> display string)
RESULTS_METRIC_DISPLAY_NAMES: dict[str, str] = {
    AVG_MEMORY_USAGE_TOTAL_METRIC: "Avg memory usage (incl. cache)",
    PEAK_MEMORY_USAGE_TOTAL_METRIC: "Peak memory usage (incl. cache)",
    UPDATE_AVG_MEMORY_USAGE_TOTAL_METRIC: "Update avg memory usage (incl. cache)",
    UPDATE_PEAK_MEMORY_USAGE_TOTAL_METRIC: "Update peak memory usage (incl. cache)",
    INSERT_DURATION_METRIC: "Insert duration",
    OPTIMIZE_DURATION_METRIC: "Optimize duration",
    NDCG_METRIC: "NDCG",
    BENCH_DB_HOST_DATA_DIR_PATH_METRIC: "DB host data dir path",
    BENCH_DB_HOST_DATA_DIR_BYTES_WRITTEN_METRIC: "DB data dir bytes written",
    BENCH_DB_HOST_DATA_DIR_BYTES_BEGIN_METRIC: "DB data dir (bytes, start)",
    BENCH_DB_HOST_DATA_DIR_BYTES_END_METRIC: "DB data dir (bytes, end)",
}


def get_results_metric_display_name(metric: str) -> str:
    """Return a short display name for the metric; falls back to capitalized key."""
    return RESULTS_METRIC_DISPLAY_NAMES.get(metric, metric.replace("_", " ").capitalize())


def get_results_metric_tooltip(metric: str) -> str:
    """Return tooltip text for a result metric; generic fallback for unknown metrics (e.g. streaming)."""
    if metric in RESULTS_METRIC_TOOLTIPS:
        return RESULTS_METRIC_TOOLTIPS[metric]
    if metric.startswith("st_"):
        return (
            "Streaming benchmark metric. Values may be per-stage or aggregated. "
            "See benchmark docs for the exact definition of this field."
        )
    return (
        "Benchmark result metric. See benchmark documentation for the definition and interpretation of this field."
    )


def get_results_metric_group_order() -> list[tuple[str, list[str]]]:
    """Return (group_heading, metric_keys) in display order."""
    return RESULTS_METRIC_GROUPS


def group_metrics_for_display(metrics_set: set[str]) -> list[tuple[str, list[str]]]:
    """
    Return list of (group_heading, metrics_in_group) for metrics that are in metrics_set.
    Only includes groups that have at least one metric present; preserves order.
    """
    result: list[tuple[str, list[str]]] = []
    for heading, metric_list in RESULTS_METRIC_GROUPS:
        in_group = [m for m in metric_list if m in metrics_set]
        if in_group:
            result.append((heading, in_group))
    # Any metric not in any group (e.g. future metrics) -> "Other"
    grouped = {m for _, lst in result for m in lst}
    # Don't show read_qps/read_throughput as separate metrics; they duplicate QPS for search benchmarks.
    # db_component_usage_json is a raw JSON blob rendered as its own Excel sheet, not a chart/column.
    _hidden_metrics = {"read_qps", "read_throughput", "db_component_usage_json"}
    other = [m for m in sorted(metrics_set) if m not in grouped and m not in _hidden_metrics]
    if other:
        result.append(("Other", other))
    return result
