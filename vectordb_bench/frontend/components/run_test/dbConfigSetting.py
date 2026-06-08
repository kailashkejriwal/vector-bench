from pydantic import ValidationError
import streamlit as st
import streamlit as st_runtime

from vectordb_bench.backend.clients import DB
from vectordb_bench.backend.provisioning import supports_auto_provision
from vectordb_bench.frontend.components.run_test.placeholder_config import get_placeholder_config
from vectordb_bench.frontend.config.styles import DB_CONFIG_SETTING_COLUMNS
from vectordb_bench.frontend.config.parameter_tooltips import (
    DB_CONFIG_GROUP_ORDER,
    get_db_config_tooltip,
    get_db_config_group,
)
from vectordb_bench.frontend.utils import inputIsPassword

_CACHE_SIZE_KEYS = {"vector_similarity_index_cache_size", "mark_cache_size"}
_READ_METHOD_KEY = "local_filesystem_read_method"
_READ_METHOD_OPTIONS = ["pread", "read", "mmap", "io_uring"]
_CLICKHOUSE_VERSION_KEY = "clickhouse_server_version"
_CLICKHOUSE_BOOLEAN_KEYS = {"query_plan_optimize_lazy_materialization"}
_CLICKHOUSE_QUERY_SIZE_KEY = "max_query_size"
_SIZE_UNITS: dict[str, int] = {
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}


def _bytes_to_value_unit(total_bytes: int) -> tuple[int, str]:
    for unit in ("TB", "GB", "MB"):
        factor = _SIZE_UNITS[unit]
        if total_bytes >= factor and total_bytes % factor == 0:
            return total_bytes // factor, unit
    # Fallback for non-round values: represent in MB.
    return max(1, total_bytes // _SIZE_UNITS["MB"]), "MB"


def _expanded_instances(actived_db_list: list[DB], instance_count: dict[DB, int]):
    """Yield (db, instance_idx) for each instance to configure (0-based index)."""
    for db in actived_db_list:
        count = instance_count.get(db, 1)
        for instance_idx in range(count):
            yield db, instance_idx


def dbConfigSettings(st, actived_db_list: list[DB], instance_count: dict[DB, int] | None = None):
    if instance_count is None:
        instance_count = {db: 1 for db in actived_db_list}
    expander = st.expander("Configurations for the selected databases", True)

    dbConfigs = {}
    db_auto_provision_config = {}
    isAllValid = True
    for activeDb, instance_idx in _expanded_instances(actived_db_list, instance_count):
        dbConfigSettingItemContainer = expander.container()
        dbConfig, auto_start, instance_config = dbConfigSettingItem(
            dbConfigSettingItemContainer,
            activeDb,
            instance_idx=instance_idx,
            instance_total=instance_count.get(activeDb, 1),
        )
        key = (activeDb, instance_idx)
        db_auto_provision_config[key] = {"auto_start": auto_start, "instance_config": instance_config}
        try:
            dbConfigs[key] = activeDb.config_cls(**dbConfig)
        except ValidationError as e:
            isAllValid = False
            errTexts = []
            for err in e.raw_errors:
                errLocs = err.loc_tuple()
                errInfo = err.exc
                errText = f"{', '.join(errLocs)} - {errInfo}"
                errTexts.append(errText)

            dbConfigSettingItemContainer.error(f"{'; '.join(errTexts)}")

    return dbConfigs, isAllValid, db_auto_provision_config


def dbConfigSettingItem(st, activeDb: DB, instance_idx: int = 0, instance_total: int = 1):
    display_name = (
        f"{activeDb.value} — Instance {instance_idx + 1}"
        if instance_total > 1
        else activeDb.value
    )
    key_prefix = f"{activeDb.name}-inst{instance_idx}-"
    st.markdown(
        f"<div style='font-weight: 600; font-size: 20px; margin-top: 16px;'>{display_name}</div>",
        unsafe_allow_html=True,
    )

    auto_start = False
    instance_config = None
    # Show checkbox for every database; enable only when we have a provisioner
    can_auto_provision = supports_auto_provision(activeDb)
    auto_start = st.checkbox(
        "Auto-start container for this database",
        value=False,
        key=f"{key_prefix}auto-start",
        disabled=not can_auto_provision,
        help="Start a Docker container for this database, run benchmarks, then tear it down. Connection fields will be filled automatically."
        if can_auto_provision
        else "Auto-provisioning is not available for this database yet.",
    )
    if auto_start:
        # Use a container instead of expander (Streamlit does not allow expanders inside expanders)
        instance_container = st.container()
        with instance_container:
            leave_container_running = st.checkbox(
                "Leave container running after benchmark",
                key=f"{key_prefix}leave-container-running",
                help="Do not stop/remove the container when the run finishes so you can analyze data or run queries (e.g. docker exec -it <container> ...)",
            )
            st.caption(
                "Instance will be started with Docker and shut down after benchmarking."
                if not leave_container_running
                else "Instance will be started with Docker and left running after the run for post-benchmark analysis."
            )
            st.markdown("Instance config (optional)")
            use_custom = st.radio(
                "Config",
                ["Use default", "Custom manifest / overrides"],
                key=f"{key_prefix}instance-config-mode",
            )
            if use_custom == "Custom manifest / overrides":
                manifest_yaml = st.text_area(
                    "Custom manifest (Kubernetes or Docker Compose YAML)",
                    value="",
                    height=120,
                    key=f"{key_prefix}manifest-yaml",
                    placeholder="Optional. Leave empty to use default image with overrides only.",
                    help="Custom YAML to run the DB (e.g. Kubernetes Deployment). Overrides default image and resources.",
                )
                st.markdown("Instance resources (optional)")
                st.caption("Use GB for memory (e.g. 8GB or 8Gi). Higher resources improve build speed and QPS.")
                col1, col2 = st.columns(2)
                cpu = col1.text_input(
                    "CPU",
                    value="",
                    key=f"{key_prefix}override-cpu",
                    placeholder="e.g. 4",
                    help=get_db_config_tooltip("cpu"),
                )
                memory = col2.text_input(
                    "Memory (GB)",
                    value="",
                    key=f"{key_prefix}override-memory",
                    placeholder="e.g. 8Gi or 16GB",
                    help=get_db_config_tooltip("memory"),
                )
                overrides = {}
                if cpu:
                    overrides["cpu"] = cpu
                if memory:
                    overrides["memory"] = memory
                instance_config = {
                    "use_custom_manifest": bool(manifest_yaml and manifest_yaml.strip()),
                    "manifest_yaml": manifest_yaml.strip() if manifest_yaml else None,
                    "manifest_format": "kubernetes" if manifest_yaml and "apiVersion:" in (manifest_yaml or "") else "docker_compose",
                    "resource_overrides": overrides if overrides else None,
                    "leave_container_running": leave_container_running,
                }
            else:
                instance_config = {"leave_container_running": leave_container_running}

    placeholder = get_placeholder_config(activeDb) if auto_start else None
    dbConfigClass = activeDb.config_cls
    schema = dbConfigClass.schema()
    properties = dict(schema.get("properties", {}))
    required_fields = set(schema.get("required", []))
    dbConfig = {}
    flamegraph_toggle_key = "enable_flamegraph"
    flamegraph_dependent_keys = {
        "flamegraph_real_time_period_ns",
        "flamegraph_cpu_time_period_ns",
    }

    # Collect connection/config keys (exclude common short/long)
    config_keys = [
        k for k in properties
        if k not in dbConfigClass.common_short_configs() and k not in dbConfigClass.common_long_configs()
    ]
    # Group keys by heading (from parameter_tooltips); preserve order
    group_order = [g for g, _ in DB_CONFIG_GROUP_ORDER]
    key_to_group = {k: get_db_config_group(k) or "Connection" for k in config_keys}
    grouped: dict[str, list[str]] = {}
    for k in config_keys:
        g = key_to_group[k]
        grouped.setdefault(g, []).append(k)
    # Render each group with a header and a row of columns
    for group_name in group_order:
        if group_name not in grouped:
            continue
        keys_in_group = grouped[group_name]
        # Hide flamegraph sampling fields unless the toggle is enabled in UI/session state.
        if flamegraph_toggle_key in properties:
            flamegraph_state_key = f"{key_prefix}{flamegraph_toggle_key}"
            flamegraph_enabled = st_runtime.session_state.get(
                flamegraph_state_key,
                properties.get(flamegraph_toggle_key, {}).get("default", False),
            )
            if not flamegraph_enabled:
                keys_in_group = [
                    k for k in keys_in_group if k not in flamegraph_dependent_keys
                ]
        if not keys_in_group:
            continue
        st.markdown(group_name)
        columns = st.columns(DB_CONFIG_SETTING_COLUMNS)
        for idx, key in enumerate(keys_in_group):
            column = columns[idx % DB_CONFIG_SETTING_COLUMNS]
            prop = properties[key]
            tooltip = get_db_config_tooltip(key)
            if auto_start and placeholder and key in placeholder:
                dbConfig[key] = placeholder[key]
                column.text_input(
                    key,
                    value=str(placeholder[key]),
                    key=f"{key_prefix}{key}",
                    disabled=True,
                    help="Filled automatically when container starts.",
                )
            elif key == flamegraph_toggle_key:
                default_value = bool(prop.get("default", False))
                dbConfig[key] = column.checkbox(
                    key,
                    value=default_value,
                    key=f"{key_prefix}{key}",
                    help=tooltip or None,
                )
            elif key in _CACHE_SIZE_KEYS:
                default_bytes = int(prop.get("default", 5 * 1024**3) or 5 * 1024**3)
                default_value, default_unit = _bytes_to_value_unit(default_bytes)
                size_col, unit_col = column.columns(2)
                size_value = int(
                    size_col.number_input(
                        f"{key} value",
                        min_value=1,
                        value=default_value,
                        step=1,
                        key=f"{key_prefix}{key}-value",
                        help=tooltip or None,
                    )
                )
                unit_value = unit_col.selectbox(
                    f"{key} unit",
                    options=list(_SIZE_UNITS.keys()),
                    index=list(_SIZE_UNITS.keys()).index(default_unit),
                    key=f"{key_prefix}{key}-unit",
                )
                dbConfig[key] = size_value * _SIZE_UNITS[unit_value]
            elif key == _READ_METHOD_KEY:
                default_method = str(prop.get("default", "pread") or "pread")
                if default_method not in _READ_METHOD_OPTIONS:
                    default_method = "pread"
                dbConfig[key] = column.selectbox(
                    key,
                    options=_READ_METHOD_OPTIONS,
                    index=_READ_METHOD_OPTIONS.index(default_method),
                    key=f"{key_prefix}{key}",
                    help=tooltip or None,
                )
            elif key == _CLICKHOUSE_VERSION_KEY:
                default_version = str(prop.get("default", "25.8") or "25.8").strip()
                dbConfig[key] = column.text_input(
                    key,
                    value=default_version,
                    key=f"{key_prefix}{key}",
                    placeholder="e.g. 25.8 (LTS), 26.5, 26.5.1.1, latest",
                    help=tooltip or None,
                )
            elif key in _CLICKHOUSE_BOOLEAN_KEYS:
                default_value = bool(prop.get("default", False))
                dbConfig[key] = column.checkbox(
                    key,
                    value=default_value,
                    key=f"{key_prefix}{key}",
                    help=tooltip or None,
                )
            elif key == _CLICKHOUSE_QUERY_SIZE_KEY:
                default_bytes = int(prop.get("default", 262_144) or 262_144)
                default_kb = max(0, default_bytes // 1024)
                size_kb = int(
                    column.number_input(
                        f"{key} (KB)",
                        min_value=0,
                        value=default_kb,
                        step=64,
                        key=f"{key_prefix}{key}",
                        help=tooltip or None,
                    )
                )
                dbConfig[key] = size_kb * 1024
            else:
                input_value = column.text_input(
                    key,
                    key=f"{key_prefix}{key}",
                    value=prop.get("default", ""),
                    type="password" if inputIsPassword(key) else "default",
                    placeholder="optional" if key not in required_fields else None,
                    help=tooltip or None,
                )
                if key in required_fields or input_value:
                    dbConfig[key] = input_value

    # Result labeling (common short)
    if dbConfigClass.common_short_configs():
        st.markdown("Result labeling")
        default_label = f"instance-{instance_idx + 1}" if instance_total > 1 else ""
        cols = st.columns(DB_CONFIG_SETTING_COLUMNS)
        for i, key in enumerate(dbConfigClass.common_short_configs()):
            dbConfig[key] = cols[i % DB_CONFIG_SETTING_COLUMNS].text_input(
                key,
                key=f"{key_prefix}{key}",
                value=default_label if key == "db_label" else "",
                type="default",
                placeholder="optional, for labeling results",
                help=get_db_config_tooltip(key) or None,
            )

    # Note (common long)
    for key in dbConfigClass.common_long_configs():
        dbConfig[key] = st.text_area(
            key,
            key=f"{key_prefix}{key}",
            value="",
            placeholder="optional",
            help=get_db_config_tooltip(key) or None,
        )
    return dbConfig, auto_start, instance_config
