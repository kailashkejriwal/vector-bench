from pydantic import ValidationError
import streamlit as st

from vectordb_bench.backend.clients import DB
from vectordb_bench.backend.provisioning import supports_auto_provision
from vectordb_bench.frontend.components.run_test.placeholder_config import get_placeholder_config
from vectordb_bench.frontend.config.styles import DB_CONFIG_SETTING_COLUMNS
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
            st.markdown("**Instance config (optional)**")
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
                )
                st.caption("Override resources (optional):")
                col1, col2 = st.columns(2)
                cpu = col1.text_input("CPU", value="", key=f"{activeDb.name}-override-cpu", placeholder="e.g. 4")
                memory = col2.text_input("Memory", value="", key=f"{activeDb.name}-override-memory", placeholder="e.g. 8Gi")
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
    columns = st.columns(DB_CONFIG_SETTING_COLUMNS)

    dbConfigClass = activeDb.config_cls
    schema = dbConfigClass.schema()
    property_items = schema.get("properties").items()
    required_fields = set(schema.get("required", []))
    dbConfig = {}
    idx = 0

    # db config (unique) - use placeholder when auto_start
    for key, property in property_items:
        if key not in dbConfigClass.common_short_configs() and key not in dbConfigClass.common_long_configs():
            column = columns[idx % DB_CONFIG_SETTING_COLUMNS]
            idx += 1
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
                    value=property.get("default", ""),
                    type="password" if inputIsPassword(key) else "default",
                    placeholder="optional" if key not in required_fields else None,
                )
                if key in required_fields or input_value:
                    dbConfig[key] = input_value

    # db config (common short labels)
    for key in dbConfigClass.common_short_configs():
        column = columns[idx % DB_CONFIG_SETTING_COLUMNS]
        idx += 1
        dbConfig[key] = column.text_input(
            key,
            key="%s-%s" % (activeDb.name, key),
            value="",
            type="default",
            placeholder="optional, for labeling results",
        )

    # db config (common long text_input)
    for key in dbConfigClass.common_long_configs():
        dbConfig[key] = st.text_area(
            key,
            key="%s-%s" % (activeDb.name, key),
            value="",
            placeholder="optional",
        )
    return dbConfig, auto_start, instance_config
