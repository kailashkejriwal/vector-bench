"""Sequential orchestrator: provision -> run benchmarks -> teardown per DB."""

import logging
import subprocess
import time
from collections import defaultdict

from vectordb_bench import config
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


def _merge_provisioned_db_config(db: DB, provisioned: object, previous: object) -> object:
    """Keep UI-supplied fields that Docker provision does not set (e.g. PgVector table_name)."""
    if previous is None:
        return provisioned
    updates: dict = {}
    prev_label = getattr(previous, "db_label", None)
    if prev_label:
        updates["db_label"] = prev_label
    if db == DB.PgVector:
        prev_table = getattr(previous, "table_name", None)
        if prev_table:
            updates["table_name"] = prev_table
    if not updates:
        return provisioned
    return provisioned.copy(update=updates)


def _next_db_after_auto_teardown(
    db_idx: int,
    sorted_auto_items: list[tuple[DB, list[CaseRunner]]],
    manual_runners: list[CaseRunner],
) -> DB | None:
    if db_idx < len(sorted_auto_items) - 1:
        return sorted_auto_items[db_idx + 1][0]
    if manual_runners:
        return manual_runners[0].config.db
    return None


def _metrics_quiet_window(gap: int, after_db: DB, before_db: DB | None) -> None:
    """Host idle period so CPU/memory/disk charts can show separation between DB runs."""
    if gap <= 0:
        return
    if config.POST_PROVISION_SYNC_BEFORE_COOLDOWN:
        try:
            log.info("POST_PROVISION_SYNC_BEFORE_COOLDOWN: running sync(1) before quiet window")
            subprocess.run(["sync"], timeout=300, check=False)
        except Exception as e:
            log.warning("sync before metrics quiet window failed: %s", e)
    before = before_db.name if before_db else "(end)"
    log.info(
        "METRICS_QUIET_WINDOW sec=%s after_db=%s before_db=%s",
        gap,
        after_db.name,
        before,
    )
    time.sleep(gap)


def run_with_auto_provision(
    case_runners: list[CaseRunner],
    drop_old: bool,
    send_conn=None,
) -> list[CaseResult]:
    """
    Run case_runners, provisioning DBs sequentially for those with auto_start=True.
    For each DB with auto_start: provision -> run all its runners -> teardown.
    Then run remaining (manual) runners in order.
    When POST_PROVISION_TEARDOWN_DELAY_SEC > 0, inserts a metrics quiet window between different DBs
    (auto→auto, last auto→first manual if DB changes, manual→manual when DB changes).
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
    sorted_auto_items = sorted(auto_runners_by_db.items(), key=lambda x: x[0].name)
    last_completed_db: DB | None = None
    cooldown_state: dict[str, bool] = {"first_manual_transition_pre_slept": False}
    for db_idx, (db, runners) in enumerate(sorted_auto_items):
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
        leave_running_after = (
            getattr(instance_config, "leave_container_running", False)
            if instance_config
            else False
        )
        log.info(
            "Auto-provision %s: leave_container_running=%s (container will %s after run)",
            db.name,
            leave_running_after,
            "stay up" if leave_running_after else "be torn down",
        )
        data_size = runners[0].ca.dataset.data.size
        dim = runners[0].ca.dataset.data.dim
        resource_profile = get_resource_profile(data_size, dim, instance_config, db=db)

        try:
            conn = provisioner.provision(
                resource_profile=resource_profile,
                instance_config=instance_config,
                context={"db": db, "data_size": data_size, "dim": dim},
            )
            new_db_config = connection_info_to_db_config(db, conn)
            new_db_config = _merge_provisioned_db_config(db, new_db_config, runners[0].config.db_config)
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
            leave_running = leave_running_after
            try:
                if leave_running:
                    log.info(
                        "Skipping teardown for %s (leave_container_running=True). Container left running for post-benchmark analysis.",
                        db.name,
                    )
                provisioner.teardown(leave_running=leave_running)
            except Exception as e:
                log.warning(f"Teardown failed for {db}: {e}")
            gap = config.POST_PROVISION_TEARDOWN_DELAY_SEC
            next_db = _next_db_after_auto_teardown(db_idx, sorted_auto_items, manual_runners)
            if gap > 0 and next_db is not None and next_db != db:
                if leave_running:
                    log.warning(
                        "POST_PROVISION_TEARDOWN_DELAY_SEC=%s but leave_container_running=True for %s; "
                        "previous container is still running — host metrics may overlap with the next DB.",
                        gap,
                        db.name,
                    )
                _metrics_quiet_window(gap, db, next_db)
                if (
                    db_idx == len(sorted_auto_items) - 1
                    and manual_runners
                    and manual_runners[0].config.db == next_db
                ):
                    cooldown_state["first_manual_transition_pre_slept"] = True
            elif gap > 0 and next_db is None:
                log.info(
                    "POST_PROVISION_TEARDOWN_DELAY_SEC=%s set but no following workload after %s; skipping quiet window.",
                    gap,
                    db.name,
                )
            last_completed_db = db

    # Manual runners (existing order) with same drop/cache logic as original
    latest_runner = None
    cached_load_duration = None
    cached_write_qps = 0.0
    first_manual_runner = True
    for r in manual_runners:
        gap = config.POST_PROVISION_TEARDOWN_DELAY_SEC
        if (
            gap > 0
            and last_completed_db is not None
            and last_completed_db != r.config.db
        ):
            skip = first_manual_runner and cooldown_state["first_manual_transition_pre_slept"]
            if not skip:
                _metrics_quiet_window(gap, last_completed_db, r.config.db)
        first_manual_runner = False
        use_drop = drop_old and not (latest_runner and r == latest_runner)
        case_res, cached_load_duration, cached_write_qps = run_one(
            r, use_drop, cached_load_duration, cached_write_qps
        )
        results.append(case_res)
        last_completed_db = r.config.db
        if case_res.label == ResultLabel.NORMAL:
            latest_runner = r

    return results
