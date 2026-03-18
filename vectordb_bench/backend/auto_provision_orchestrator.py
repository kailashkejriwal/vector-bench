"""Sequential orchestrator: provision -> run benchmarks -> teardown per DB."""

import logging
from collections import defaultdict

from vectordb_bench.backend.clients import DB
from vectordb_bench.backend.provisioning import (
    connection_info_to_db_config,
    get_provisioner,
)
from vectordb_bench.backend.provisioning.base import InstanceConfig
from vectordb_bench.backend.provisioning.resource_profiles import get_resource_profile
from vectordb_bench.backend.task_runner import CaseRunner
from vectordb_bench.metric import Metric
from vectordb_bench.signals import SIGNAL
from vectordb_bench.models import (
    CaseResult,
    LoadTimeoutError,
    PerformanceTimeoutError,
    ResultLabel,
    TaskStage,
)

log = logging.getLogger(__name__)


def run_with_auto_provision(
    case_runners: list[CaseRunner],
    drop_old: bool,
    send_conn=None,
) -> list[CaseResult]:
    """
    Run case_runners, provisioning DBs sequentially for those with auto_start=True.
    For each DB with auto_start: provision -> run all its runners -> teardown.
    Then run remaining (manual) runners in order.
    send_conn: optional multiprocessing Connection to send (SIGNAL.WIP, idx) after each case.
    """
    auto_runners_by_db: dict[DB, list[CaseRunner]] = defaultdict(list)
    manual_runners: list[CaseRunner] = []

    for r in case_runners:
        if getattr(r.config, "auto_start", False):
            auto_runners_by_db[r.config.db].append(r)
        else:
            manual_runners.append(r)

    results: list[CaseResult] = []
    finished_count = 0

    def run_one(
        runner: CaseRunner,
        use_drop_old: bool,
        cached_load_duration: float | None,
        cached_write_qps: float = 0.0,
    ) -> tuple[CaseResult, float | None, float]:
        nonlocal finished_count
        case_res = CaseResult(
            metrics=Metric(),
            task_config=runner.config,
        )
        actual_drop = use_drop_old and (TaskStage.DROP_OLD in runner.config.stages)
        try:
            log.info(f"start case: {runner.display()}, drop_old={actual_drop}")
            case_res.metrics = runner.run(drop_old=actual_drop)
            log.info(f"finish case: {runner.display()}, result={case_res.metrics}")
            new_cached_load = case_res.metrics.load_duration if actual_drop else cached_load_duration
            new_cached_write_qps = case_res.metrics.write_qps if actual_drop else cached_write_qps
        except (LoadTimeoutError, PerformanceTimeoutError) as e:
            log.warning(f"case {runner.display()} failed (timeout): {e}")
            case_res.label = ResultLabel.OUTOFRANGE
            new_cached_load = cached_load_duration
            new_cached_write_qps = cached_write_qps
        except Exception as e:
            log.warning(f"case {runner.display()} failed: {e}", exc_info=True)
            case_res.label = ResultLabel.FAILED
            new_cached_load = cached_load_duration
            new_cached_write_qps = cached_write_qps
        if not actual_drop and cached_load_duration is not None:
            case_res.metrics.load_duration = cached_load_duration
            if cached_write_qps and (not case_res.metrics.write_qps or case_res.metrics.write_qps == 0):
                case_res.metrics.write_qps = cached_write_qps
                case_res.metrics.write_throughput = cached_write_qps
        if send_conn:
            send_conn.send((SIGNAL.WIP, finished_count))
        finished_count += 1
        return case_res, new_cached_load, new_cached_write_qps

    # Sequential auto-provision per DB
    for db, runners in sorted(auto_runners_by_db.items(), key=lambda x: x[0].name):
        provisioner = get_provisioner(db)
        if not provisioner:
            log.warning(f"No provisioner for {db}, skipping auto-provision runners")
            for r in runners:
                case_res = CaseResult(metrics=Metric(), task_config=r.config, label=ResultLabel.FAILED)
                results.append(case_res)
                if send_conn:
                    send_conn.send((SIGNAL.WIP, finished_count))
                finished_count += 1
            continue

        if not provisioner.is_available():
            log.warning(f"Provisioner for {db} not available (e.g. Docker not running)")
            for r in runners:
                case_res = CaseResult(metrics=Metric(), task_config=r.config, label=ResultLabel.FAILED)
                results.append(case_res)
                if send_conn:
                    send_conn.send((SIGNAL.WIP, finished_count))
                finished_count += 1
            continue

        instance_config: InstanceConfig | None = getattr(
            runners[0].config, "instance_config", None
        )
        data_size = runners[0].ca.dataset.data.size
        dim = runners[0].ca.dataset.data.dim
        resource_profile = get_resource_profile(data_size, dim, instance_config)

        try:
            conn = provisioner.provision(
                resource_profile=resource_profile,
                instance_config=instance_config,
                context={"db": db, "data_size": data_size, "dim": dim},
            )
            new_db_config = connection_info_to_db_config(db, conn)
        except Exception as e:
            log.warning(f"Provision failed for {db}: {e}", exc_info=True)
            for r in runners:
                case_res = CaseResult(metrics=Metric(), task_config=r.config, label=ResultLabel.FAILED)
                results.append(case_res)
                if send_conn:
                    send_conn.send((SIGNAL.WIP, finished_count))
                finished_count += 1
            continue

        try:
            cached_load_duration = None
            cached_write_qps = 0.0
            for i, r in enumerate(runners):
                r.config.db_config = new_db_config
                # Run load for every instance so each gets its own load_duration and write_qps
                # (different instances can have different indexing/config; reusing would show same duration and 0 write_qps)
                use_drop = drop_old
                case_res, cached_load_duration, cached_write_qps = run_one(
                    r, use_drop, cached_load_duration, cached_write_qps
                )
                results.append(case_res)
        finally:
            try:
                provisioner.teardown()
            except Exception as e:
                log.warning(f"Teardown failed for {db}: {e}")

    # Manual runners (existing order) with same drop/cache logic as original
    latest_runner = None
    cached_load_duration = None
    cached_write_qps = 0.0
    for r in manual_runners:
        use_drop = drop_old and not (latest_runner and r == latest_runner)
        case_res, cached_load_duration, cached_write_qps = run_one(
            r, use_drop, cached_load_duration, cached_write_qps
        )
        results.append(case_res)
        if case_res.label == ResultLabel.NORMAL:
            latest_runner = r

    return results
