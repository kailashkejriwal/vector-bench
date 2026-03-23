from vectordb_bench.backend.cases import CaseLabel, CaseType
from vectordb_bench.backend.clients import DB
from vectordb_bench.backend.provisioning.base import InstanceConfig
from vectordb_bench.frontend.config.dbCaseConfigs import get_case_config_inputs
from vectordb_bench.models import CaseConfig, CaseConfigParamType, TaskConfig


def _case_label_for_type(case_type: CaseType) -> CaseLabel:
    """Derive CaseLabel from CaseType without relying on case class .label attribute."""
    if case_type in (CaseType.CapacityDim128, CaseType.CapacityDim960):
        return CaseLabel.Load
    if case_type in (CaseType.StreamingPerformanceCase, CaseType.StreamingCustomDataset):
        return CaseLabel.Streaming
    return CaseLabel.Performance


def _fill_default_case_params(db: DB, case: CaseConfig, cfg: dict) -> None:
    """Fill cfg with default values for params that were not shown in the UI (e.g. when isDisplayed was False)."""
    case_label = _case_label_for_type(case.case_id)
    for config_input in get_case_config_inputs(db, case_label):
        key = config_input.label.value
        if key not in cfg and "value" in config_input.inputConfig:
            cfg[key] = config_input.inputConfig["value"]


def _expanded_instances(actived_db_list: list[DB], instance_count: dict[DB, int]):
    """Yield (db, instance_idx) for each instance (0-based)."""
    for db in actived_db_list:
        count = instance_count.get(db, 1)
        for instance_idx in range(count):
            yield db, instance_idx


def generate_tasks(
    actived_db_list: list[DB],
    db_configs: dict,
    actived_case_list: list[CaseConfig],
    all_case_configs: dict,
    db_auto_provision_config: dict | None = None,
    instance_count: dict[DB, int] | None = None,
):
    if db_auto_provision_config is None:
        db_auto_provision_config = {}
    if instance_count is None:
        instance_count = {db: 1 for db in actived_db_list}
    tasks = []
    for db, instance_idx in _expanded_instances(actived_db_list, instance_count):
        key = (db, instance_idx)
        auto_prov = db_auto_provision_config.get(key, {})
        auto_start = auto_prov.get("auto_start", False)
        instance_config = auto_prov.get("instance_config")
        db_config = db_configs[key]
        # Ensure unique label when multiple instances of same DB so results/charts distinguish them
        if instance_count.get(db, 1) > 1 and not (getattr(db_config, "db_label", None) or "").strip():
            db_config = db_config.copy(update={"db_label": f"instance-{instance_idx + 1}"})
        for case in actived_case_list:
            cfg = {key.value: value for key, value in all_case_configs[key][case].items()}
            # Many DBCaseConfig models require an `index` field, while the UI stores the selection under `IndexType`.
            if CaseConfigParamType.IndexType in all_case_configs[key][case] and "index" not in cfg:
                cfg["index"] = all_case_configs[key][case][CaseConfigParamType.IndexType]
            _fill_default_case_params(db, case, cfg)
            task = TaskConfig(
                db=db.value,
                db_config=db_config,
                case_config=case,
                db_case_config=db.case_config_cls(all_case_configs[key][case].get(CaseConfigParamType.IndexType, None))(
                    **cfg
                ),
                auto_start=auto_start,
                instance_config=InstanceConfig(**instance_config) if instance_config and isinstance(instance_config, dict) else None,
            )
            tasks.append(task)

    return tasks
