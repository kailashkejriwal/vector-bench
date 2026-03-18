import re
from collections import defaultdict
from dataclasses import asdict
from vectordb_bench.metric import QPS_METRIC, isLowerIsBetterMetric

# Concurrency metrics are not captured or shown in results
CONCURRENCY_METRIC_KEYS = frozenset({
    "conc_num_list",
    "conc_qps_list",
    "conc_latency_p99_list",
    "conc_latency_p95_list",
    "conc_latency_avg_list",
})
from vectordb_bench.models import CaseResult, ResultLabel

# Keys commonly used for index/tuning display
TUNING_DISPLAY_KEYS = {
    "index", "M", "efConstruction", "ef", "ef_search", "ef_construction",
    "nlist", "nprobe", "lists", "probes", "quantization", "granularity",
    "metric_type", "vector_data_type", "num_candidates", "search_list",
    "m", "num_leaves", "num_leaves_to_search", "quantizer", "reranking",
}


def _to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def get_tuning_display_dict(db_case_config) -> dict:
    """Return a dict of tuning param -> value for summary display."""
    if not db_case_config:
        return {}
    try:
        d = db_case_config.dict()
    except Exception:
        return {}
    out = {}
    for k, v in d.items():
        if v is None or (isinstance(v, str) and v.strip() == ""):
            continue
        snake = _to_snake(k)
        if k in TUNING_DISPLAY_KEYS or snake in {_to_snake(x) for x in TUNING_DISPLAY_KEYS}:
            out[snake] = v
    return out


def bar_display_name_from_db_name(db_name: str, db_base: str) -> str:
    """Return instance label in numerical order, e.g. 'Clickhouse 1', 'Clickhouse 2'."""
    if not db_name:
        return f"{db_base} 1"
    # e.g. "Clickhouse-instance-2" -> "Clickhouse 2"; "Clickhouse" or "Clickhouse-2c8g" -> "Clickhouse 1"
    match = re.search(r"-instance-(\d+)$", db_name)
    if match:
        num = int(match.group(1))
        base = db_name[: match.start()].strip() or db_base
        return f"{base} {num}"
    # single instance: use db name as base, number 1
    return f"{db_name} 1"


def getChartData(
    tasks: list[CaseResult],
    dbNames: list[str],
    caseNames: list[str],
):
    filterTasks = getFilterTasks(tasks, dbNames, caseNames)
    mergedTasks, failedTasks = mergeTasks(filterTasks)
    return mergedTasks, failedTasks


def getFilterTasks(
    tasks: list[CaseResult],
    dbNames: list[str],
    caseNames: list[str],
) -> list[CaseResult]:
    filterTasks = [
        task
        for task in tasks
        if task.task_config.db_name in dbNames and task.task_config.case_config.case_name in caseNames
    ]
    return filterTasks


def _task_instance_merge_keys(tasks: list[CaseResult]) -> dict[int, str]:
    """Assign a unique merge_key per task so multiple instances of the same DB are not collapsed.
    Returns dict mapping id(task) -> merge_key (e.g. 'Clickhouse-instance-1', 'Milvus-instance-2').
    """
    by_db_case = defaultdict(list)
    for t in tasks:
        by_db_case[(t.task_config.db.value, t.task_config.case_config.case.name)].append(t)
    out = {}
    for (db, _), group in by_db_case.items():
        for i, task in enumerate(group):
            out[id(task)] = f"{db}-instance-{i + 1}"
    return out


def mergeTasks(tasks: list[CaseResult]):
    # One row per task (per instance); never merge two different instances into one.
    task_to_key = _task_instance_merge_keys(tasks)
    dbCaseMetricsMap = defaultdict(lambda: defaultdict(dict))
    for task in tasks:
        merge_key = task_to_key.get(id(task), task.task_config.db_name)
        db = task.task_config.db.value
        db_label = task.task_config.db_config.db_label or ""
        version = task.task_config.db_config.version or ""
        case = task.task_config.case_config.case
        case_name = case.name
        dataset_name = case.dataset.data.full_name
        filter_rate = case.filter_rate
        task_metrics = asdict(task.metrics)
        existing = dbCaseMetricsMap[merge_key][case_name]
        existing_metrics = existing.get("metrics", {})
        new_metrics = mergeMetrics(existing_metrics, task_metrics)
        qps_old = existing_metrics.get(QPS_METRIC, 0)
        qps_new = task_metrics.get(QPS_METRIC, 0)
        if qps_new >= qps_old:
            tuning_params = get_tuning_display_dict(task.task_config.db_case_config)
        else:
            tuning_params = existing.get("tuning_params", {})
        dbCaseMetricsMap[merge_key][case_name] = {
            "db": db,
            "db_label": db_label,
            "version": version,
            "dataset_name": dataset_name,
            "filter_rate": filter_rate,
            "metrics": new_metrics,
            "label": getBetterLabel(
                existing.get("label", ResultLabel.FAILED),
                task.label,
            ),
            "tuning_params": tuning_params,
        }

    merged_tasks = []
    failed_tasks = defaultdict(lambda: defaultdict(str))
    for merge_key, case_metrics_map in dbCaseMetricsMap.items():
        for case_name, metric_info in case_metrics_map.items():
            metrics = metric_info["metrics"]
            db = metric_info["db"]
            db_label = metric_info["db_label"]
            version = metric_info["version"]
            label = metric_info["label"]
            dataset_name = metric_info["dataset_name"]
            filter_rate = metric_info["filter_rate"]
            tuning_params = metric_info.get("tuning_params", {})
            bar_display_name = bar_display_name_from_db_name(merge_key, db)
            if label == ResultLabel.NORMAL:
                # Exclude concurrency metrics from results (not captured or shown)
                metrics_filtered = {k: v for k, v in metrics.items() if k not in CONCURRENCY_METRIC_KEYS}
                # Sanitize read_qps/read_throughput: old bug set them to absurd values (e.g. 66M).
                # They should match qps for search benchmarks; cap to qps when clearly wrong.
                qps_val = metrics_filtered.get(QPS_METRIC, 0) or 0
                if isinstance(qps_val, (int, float)) and qps_val >= 0:
                    for key in ("read_qps", "read_throughput"):
                        if key in metrics_filtered:
                            v = metrics_filtered[key]
                            if isinstance(v, (int, float)) and v > 2 * max(qps_val, 1):
                                metrics_filtered[key] = qps_val
                merged_tasks.append(
                    {
                        "db_name": merge_key,
                        "bar_display_name": bar_display_name,
                        "tuning_params": tuning_params,
                        "db": db,
                        "db_label": db_label,
                        "dataset_name": dataset_name,
                        "filter_rate": filter_rate,
                        "version": version,
                        "case_name": case_name,
                        "metricsSet": set(metrics_filtered.keys()),
                        **metrics_filtered,
                    }
                )
            else:
                failed_tasks[case_name][merge_key] = label

    return merged_tasks, failed_tasks


# for same db-label, we use the results with the highest qps
def mergeMetrics(metrics_1: dict, metrics_2: dict) -> dict:
    return metrics_1 if metrics_1.get(QPS_METRIC, 0) > metrics_2.get(QPS_METRIC, 0) else metrics_2


def getBetterMetric(metric, value_1, value_2):
    try:
        if value_1 < 1e-7:
            return value_2
        if value_2 < 1e-7:
            return value_1
        return min(value_1, value_2) if isLowerIsBetterMetric(metric) else max(value_1, value_2)
    except Exception:
        return value_1


def getBetterLabel(label_1: ResultLabel, label_2: ResultLabel):
    return label_2 if label_1 != ResultLabel.NORMAL else label_1
