"""
Tooltips and grouping for benchmark result metrics on the results page.
Used by check_results/charts.py to add help text and group metrics under headings.
"""

from vectordb_bench.metric import (
    AVG_CPU_USAGE_METRIC,
    AVG_MEMORY_USAGE_METRIC,
    DISK_READ_BYTES_METRIC,
    DISK_WRITE_BYTES_METRIC,
    INSERT_DURATION_METRIC,
    LOAD_DURATION_METRIC,
    MAX_LOAD_COUNT_METRIC,
    NDCG_METRIC,
    OPTIMIZE_DURATION_METRIC,
    PEAK_CPU_USAGE_METRIC,
    PEAK_MEMORY_USAGE_METRIC,
    QPS_METRIC,
    QURIES_PER_DOLLAR_METRIC,
    READ_LATENCY_P99_METRIC,
    READ_QPS_METRIC,
    READ_THROUGHPUT_METRIC,
    RECALL_METRIC,
    SERIAL_LATENCY_P99_METRIC,
    SERIAL_LATENCY_P95_METRIC,
    UPDATE_LATENCY_P99_METRIC,
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
        "Lower is better for resource efficiency; high usage can indicate compute-bound workload."
    ),
    PEAK_CPU_USAGE_METRIC: (
        "Peak CPU usage: Maximum CPU utilization (%) during the run. "
        "Lower is better. Helps size instances and avoid throttling."
    ),
    AVG_MEMORY_USAGE_METRIC: (
        "Average memory usage: Mean RAM used during the run (MB). "
        "Lower is better. Important for cost and avoiding OOM; index size often dominates."
    ),
    PEAK_MEMORY_USAGE_METRIC: (
        "Peak memory usage: Maximum RAM used during the run (MB). "
        "Lower is better. Use for capacity planning."
    ),
    DISK_READ_BYTES_METRIC: (
        "Disk read: Total bytes read from disk during the run. "
        "Lower is better for I/O efficiency; high values may indicate disk-based indexes or cold caches."
    ),
    DISK_WRITE_BYTES_METRIC: (
        "Disk write: Total bytes written to disk during the run. "
        "Lower is better; reflects index build and compaction I/O."
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
            DISK_READ_BYTES_METRIC,
            DISK_WRITE_BYTES_METRIC,
        ],
    ),
    (
        "Update & cost",
        [
            UPDATE_QPS_METRIC,
            UPDATE_LATENCY_P99_METRIC,
            UPDATE_THROUGHPUT_METRIC,
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
    INSERT_DURATION_METRIC: "Insert duration",
    OPTIMIZE_DURATION_METRIC: "Optimize duration",
    NDCG_METRIC: "NDCG",
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
    # Don't show read_qps/read_throughput as separate metrics; they duplicate QPS for search benchmarks
    _redundant_read_metrics = {"read_qps", "read_throughput"}
    other = [m for m in sorted(metrics_set) if m not in grouped and m not in _redundant_read_metrics]
    if other:
        result.append(("Other", other))
    return result
