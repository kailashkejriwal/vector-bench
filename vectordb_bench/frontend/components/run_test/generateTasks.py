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


def generate_tasks(activedDbList: list[DB], dbConfigs, activedCaseList: list[CaseConfig], allCaseConfigs, db_auto_provision_config=None):
    if db_auto_provision_config is None:
        db_auto_provision_config = {}
    tasks = []
    for db in activedDbList:
        auto_prov = db_auto_provision_config.get(db, {})
        auto_start = auto_prov.get("auto_start", False)
        instance_config = auto_prov.get("instance_config")
        for case in activedCaseList:
            cfg = {key.value: value for key, value in allCaseConfigs[db][case].items()}
            # Many DBCaseConfig models require an `index` field, while the UI stores the selection under `IndexType`.
            # Passing both keeps backwards-compatibility (extra fields are ignored) and enables strict models (e.g. OceanBase).
            if CaseConfigParamType.IndexType in allCaseConfigs[db][case] and "index" not in cfg:
                cfg["index"] = allCaseConfigs[db][case][CaseConfigParamType.IndexType]
            # Fill defaults for params that were not displayed (e.g. M, efConstruction when IndexType was not yet set)
            _fill_default_case_params(db, case, cfg)
            task = TaskConfig(
                db=db.value,
                db_config=dbConfigs[db],
                case_config=case,
                db_case_config=db.case_config_cls(allCaseConfigs[db][case].get(CaseConfigParamType.IndexType, None))(
                    **cfg
                ),
                auto_start=auto_start,
                instance_config=InstanceConfig(**instance_config) if instance_config and isinstance(instance_config, dict) else None,
            )
            tasks.append(task)

    return tasks
