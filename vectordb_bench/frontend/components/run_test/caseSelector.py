from collections import defaultdict

from vectordb_bench.backend.clients import DB
from vectordb_bench.frontend.components.run_test.inputWidget import inputWidget
from vectordb_bench.frontend.config.dbCaseConfigs import (
    UI_CASE_CLUSTERS,
    UICaseItem,
    UICaseItemCluster,
    get_case_config_inputs,
    get_custom_case_cluter,
    get_custom_streaming_case_cluster,
)
from vectordb_bench.frontend.config.parameter_tooltips import (
    get_case_param_group,
    get_case_param_tooltip,
)
from vectordb_bench.frontend.config.styles import (
    CASE_CONFIG_SETTING_COLUMNS,
    CHECKBOX_INDENT,
    DB_CASE_CONFIG_SETTING_COLUMNS,
)
from vectordb_bench.frontend.utils import addHorizontalLine
from vectordb_bench.models import CaseConfig


def _expanded_instances(actived_db_list: list[DB], instance_count: dict[DB, int]):
    """Yield (db, instance_idx) for each instance (0-based)."""
    for db in actived_db_list:
        count = instance_count.get(db, 1)
        for instance_idx in range(count):
            yield db, instance_idx


def caseSelector(st, actived_db_list: list[DB], instance_count: dict[DB, int] | None = None):
    if instance_count is None:
        instance_count = {db: 1 for db in actived_db_list}
    st.markdown(
        "<div style='height: 24px;'></div>",
        unsafe_allow_html=True,
    )
    st.subheader("STEP 2: Choose the case(s)")
    st.markdown(
        "<div style='color: #647489; margin-bottom: 24px; margin-top: -12px;'>Choose at least one case you want to run the test for. </div>",
        unsafe_allow_html=True,
    )

    actived_case_list: list[CaseConfig] = []
    # Key by (db, instance_idx) so each instance has its own tuning
    db_to_case_cluster_configs = defaultdict(lambda: defaultdict(dict))
    db_to_case_configs = defaultdict(lambda: defaultdict(dict))
    case_clusters = UI_CASE_CLUSTERS + [get_custom_case_cluter(), get_custom_streaming_case_cluster()]
    expanded = list(_expanded_instances(actived_db_list, instance_count))
    for case_cluster in case_clusters:
        actived_case_list += case_cluster_expander(
            st, case_cluster, db_to_case_cluster_configs, actived_db_list, instance_count, expanded
        )
    for key in db_to_case_cluster_configs:
        for ui_case_item in db_to_case_cluster_configs[key]:
            for case in ui_case_item.get_cases():
                db_to_case_configs[key][case] = db_to_case_cluster_configs[key][ui_case_item]

    return actived_case_list, db_to_case_configs


def case_cluster_expander(st, case_cluster: UICaseItemCluster, db_to_case_cluster_configs, actived_db_list: list[DB], instance_count: dict[DB, int], expanded: list[tuple[DB, int]]):
    expander = st.expander(case_cluster.label, False)
    actived_cases: list[CaseConfig] = []
    cluster_config = {}
    if case_cluster.cluster_level_config_inputs:
        expander.markdown("**Filter distributions** — choose which percentages/rates to run (reduces resource use):")
        cols = expander.columns(len(case_cluster.cluster_level_config_inputs))
        for i, config_input in enumerate(case_cluster.cluster_level_config_inputs):
            key = f"cluster-config-{case_cluster.label}-{config_input.label.value}"
            rich_help = get_case_param_tooltip(config_input.label.value, config_input.inputHelp)
            config_with_help = config_input.copy(update={"inputHelp": rich_help})
            cluster_config[config_input.label.value] = inputWidget(
                cols[i], config=config_with_help, key=key
            )
        expander.markdown("---")
    for ui_case_item in case_cluster.uiCaseItems:
        if ui_case_item.isLine:
            addHorizontalLine(expander)
        else:
            ui_case_item.tmp_custom_config.update(cluster_config)
            actived_cases += case_item_checkbox(
                expander, db_to_case_cluster_configs, ui_case_item, expanded, instance_count=instance_count
            )
    return actived_cases


def case_item_checkbox(st, db_to_case_cluster_configs, ui_case_item: UICaseItem, expanded: list[tuple[DB, int]], instance_count: dict[DB, int] | None = None):
    if instance_count is None:
        instance_count = {}
    selected = st.checkbox(ui_case_item.label)
    st.markdown(
        f"<div style='color: #1D2939; margin: -8px 0 20px {CHECKBOX_INDENT}px; font-size: 14px;'>{ui_case_item.description}</div>",
        unsafe_allow_html=True,
    )

    case_config_setting(st.container(), ui_case_item)

    if selected:
        db_case_config_setting(st.container(), db_to_case_cluster_configs, ui_case_item, expanded, instance_count)

    return ui_case_item.get_cases() if selected else []


def case_config_setting(st, ui_case_item: UICaseItem):
    config_inputs = ui_case_item.extra_custom_case_config_inputs
    if len(config_inputs) == 0:
        return

    st.markdown(
        f"<div style='margin: 0 0 24px {CHECKBOX_INDENT}px; font-size: 18px; font-weight: 600;'>Custom Config</div>",
        unsafe_allow_html=True,
    )
    current_group = None
    columns = None
    col_idx = 0
    for config_input in config_inputs:
        group = get_case_param_group(config_input.label.value)
        if group != current_group:
            current_group = group
            if current_group:
                st.markdown(current_group)
            columns = st.columns(
                [1, *[DB_CASE_CONFIG_SETTING_COLUMNS / CASE_CONFIG_SETTING_COLUMNS] * CASE_CONFIG_SETTING_COLUMNS]
            )
            col_idx = 0
        column = columns[1 + col_idx % CASE_CONFIG_SETTING_COLUMNS]
        rich_help = get_case_param_tooltip(config_input.label.value, config_input.inputHelp)
        config_with_help = config_input.copy(update={"inputHelp": rich_help})
        key = f"custom-config-{ui_case_item.label}-{config_input.label.value}"
        ui_case_item.tmp_custom_config[config_input.label.value] = inputWidget(column, config=config_with_help, key=key)
        col_idx += 1


def db_case_config_setting(st, db_to_case_cluster_configs, ui_case_item: UICaseItem, expanded: list[tuple[DB, int]], instance_count: dict[DB, int] | None = None):
    if instance_count is None:
        instance_count = {}
    for db, instance_idx in expanded:
        key = (db, instance_idx)
        db_case_config = db_to_case_cluster_configs[key][ui_case_item]
        configs = [c for c in get_case_config_inputs(db, ui_case_item.caseLabel) if c.isDisplayed(db_case_config)]
        display_name = (
            f"{db.name} — Instance {instance_idx + 1}"
            if instance_count.get(db, 1) > 1
            else db.name
        )
        st.markdown(
            f"<div style='margin: 0 0 24px {CHECKBOX_INDENT}px; font-size: 18px; font-weight: 600;'>{display_name}</div>",
            unsafe_allow_html=True,
        )
        if not configs:
            st.write("Auto")
            continue
        current_group = None
        col_idx = 0
        for config in configs:
            group = get_case_param_group(config.label.value)
            if group != current_group:
                current_group = group
                if current_group:
                    st.markdown(
                        f"<div style='margin-left: {CHECKBOX_INDENT}px; margin-bottom: 8px; font-size: 15px; font-weight: 600;'>{current_group}</div>",
                        unsafe_allow_html=True,
                    )
                columns = st.columns(1 + DB_CASE_CONFIG_SETTING_COLUMNS)
                col_idx = 0
            column = columns[1 + col_idx % DB_CASE_CONFIG_SETTING_COLUMNS]
            rich_help = get_case_param_tooltip(config.label.value, config.inputHelp)
            config_with_help = config.copy(update={"inputHelp": rich_help})
            widget_key = f"{db.name}-inst{instance_idx}-{ui_case_item.label}-{config.label.value}"
            db_case_config[config.label] = inputWidget(column, config=config_with_help, key=widget_key)
            col_idx += 1
