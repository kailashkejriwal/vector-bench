from pydantic import ValidationError
import streamlit as st

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


def dbConfigSettings(st, activedDbList: list[DB]):
    expander = st.expander("Configurations for the selected databases", True)

    dbConfigs = {}
    db_auto_provision_config = {}
    isAllValid = True
    for activeDb in activedDbList:
        dbConfigSettingItemContainer = expander.container()
        dbConfig, auto_start, instance_config = dbConfigSettingItem(dbConfigSettingItemContainer, activeDb)
        db_auto_provision_config[activeDb] = {"auto_start": auto_start, "instance_config": instance_config}
        try:
            dbConfigs[activeDb] = activeDb.config_cls(**dbConfig)
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


def dbConfigSettingItem(st, activeDb: DB):
    st.markdown(
        f"<div style='font-weight: 600; font-size: 20px; margin-top: 16px;'>{activeDb.value}</div>",
        unsafe_allow_html=True,
    )

    auto_start = False
    instance_config = None
    # Show checkbox for every database; enable only when we have a provisioner
    can_auto_provision = supports_auto_provision(activeDb)
    auto_start = st.checkbox(
        "Auto-start container for this database",
        value=False,
        key=f"{activeDb.name}-auto-start",
        disabled=not can_auto_provision,
        help="Start a Docker container for this database, run benchmarks, then tear it down. Connection fields will be filled automatically."
        if can_auto_provision
        else "Auto-provisioning is not available for this database yet.",
    )
    if auto_start:
        st.caption("Instance will be started with Docker and shut down after benchmarking.")
        # Use a container instead of expander (Streamlit does not allow expanders inside expanders)
        instance_container = st.container()
        with instance_container:
            st.markdown("Instance config (optional)")
            use_custom = st.radio(
                "Config",
                ["Use default", "Custom manifest / overrides"],
                key=f"{activeDb.name}-instance-config-mode",
            )
            if use_custom == "Custom manifest / overrides":
                manifest_yaml = st.text_area(
                    "Custom manifest (Kubernetes or Docker Compose YAML)",
                    value="",
                    height=120,
                    key=f"{activeDb.name}-manifest-yaml",
                    placeholder="Optional. Leave empty to use default image with overrides only.",
                    help="Custom YAML to run the DB (e.g. Kubernetes Deployment). Overrides default image and resources.",
                )
                st.markdown("Instance resources (optional)")
                st.caption("Use GB for memory (e.g. 8GB or 8Gi). Higher resources improve build speed and QPS.")
                col1, col2 = st.columns(2)
                cpu = col1.text_input(
                    "CPU",
                    value="",
                    key=f"{activeDb.name}-override-cpu",
                    placeholder="e.g. 4",
                    help=get_db_config_tooltip("cpu"),
                )
                memory = col2.text_input(
                    "Memory (GB)",
                    value="",
                    key=f"{activeDb.name}-override-memory",
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
                }

    placeholder = get_placeholder_config(activeDb) if auto_start else None
    dbConfigClass = activeDb.config_cls
    schema = dbConfigClass.schema()
    properties = dict(schema.get("properties", {}))
    required_fields = set(schema.get("required", []))
    dbConfig = {}

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
                    key=f"{activeDb.name}-{key}",
                    disabled=True,
                    help="Filled automatically when container starts.",
                )
            else:
                input_value = column.text_input(
                    key,
                    key=f"{activeDb.name}-{key}",
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
        cols = st.columns(DB_CONFIG_SETTING_COLUMNS)
        for i, key in enumerate(dbConfigClass.common_short_configs()):
            dbConfig[key] = cols[i % DB_CONFIG_SETTING_COLUMNS].text_input(
                key,
                key="%s-%s" % (activeDb.name, key),
                value="",
                type="default",
                placeholder="optional, for labeling results",
                help=get_db_config_tooltip(key) or None,
            )

    # Note (common long)
    for key in dbConfigClass.common_long_configs():
        dbConfig[key] = st.text_area(
            key,
            key="%s-%s" % (activeDb.name, key),
            value="",
            placeholder="optional",
            help=get_db_config_tooltip(key) or None,
        )
    return dbConfig, auto_start, instance_config
