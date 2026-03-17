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


def caseSelector(st, activedDbList: list[DB]):
    st.markdown(
        "<div style='height: 24px;'></div>",
        unsafe_allow_html=True,
    )
    st.subheader("STEP 2: Choose the case(s)")
    st.markdown(
        "<div style='color: #647489; margin-bottom: 24px; margin-top: -12px;'>Choose at least one case you want to run the test for. </div>",
        unsafe_allow_html=True,
    )

    activedCaseList: list[CaseConfig] = []
    dbToCaseClusterConfigs = defaultdict(lambda: defaultdict(dict))
    dbToCaseConfigs = defaultdict(lambda: defaultdict(dict))
    caseClusters = UI_CASE_CLUSTERS + [get_custom_case_cluter(), get_custom_streaming_case_cluster()]
    for caseCluster in caseClusters:
        activedCaseList += caseClusterExpander(st, caseCluster, dbToCaseClusterConfigs, activedDbList)
    for db in dbToCaseClusterConfigs:
        for uiCaseItem in dbToCaseClusterConfigs[db]:
            for case in uiCaseItem.get_cases():
                dbToCaseConfigs[db][case] = dbToCaseClusterConfigs[db][uiCaseItem]

    return activedCaseList, dbToCaseConfigs


def caseClusterExpander(st, caseCluster: UICaseItemCluster, dbToCaseClusterConfigs, activedDbList: list[DB]):
    expander = st.expander(caseCluster.label, False)
    activedCases: list[CaseConfig] = []
    for uiCaseItem in caseCluster.uiCaseItems:
        if uiCaseItem.isLine:
            addHorizontalLine(expander)
        else:
            activedCases += caseItemCheckbox(expander, dbToCaseClusterConfigs, uiCaseItem, activedDbList)
    return activedCases


def caseItemCheckbox(st, dbToCaseClusterConfigs, uiCaseItem: UICaseItem, activedDbList: list[DB]):
    selected = st.checkbox(uiCaseItem.label)
    st.markdown(
        f"<div style='color: #1D2939; margin: -8px 0 20px {CHECKBOX_INDENT}px; font-size: 14px;'>{uiCaseItem.description}</div>",
        unsafe_allow_html=True,
    )

    caseConfigSetting(st.container(), uiCaseItem)

    if selected:
        dbCaseConfigSetting(st.container(), dbToCaseClusterConfigs, uiCaseItem, activedDbList)

    return uiCaseItem.get_cases() if selected else []


def caseConfigSetting(st, uiCaseItem: UICaseItem):
    config_inputs = uiCaseItem.extra_custom_case_config_inputs
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
        key = f"custom-config-{uiCaseItem.label}-{config_input.label.value}"
        uiCaseItem.tmp_custom_config[config_input.label.value] = inputWidget(column, config=config_with_help, key=key)
        col_idx += 1


def dbCaseConfigSetting(st, dbToCaseClusterConfigs, uiCaseItem: UICaseItem, activedDbList: list[DB]):
    for db in activedDbList:
        dbCaseConfig = dbToCaseClusterConfigs[db][uiCaseItem]
        configs = [c for c in get_case_config_inputs(db, uiCaseItem.caseLabel) if c.isDisplayed(dbCaseConfig)]
        st.markdown(
            f"<div style='margin: 0 0 24px {CHECKBOX_INDENT}px; font-size: 18px; font-weight: 600;'>{db.name}</div>",
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
                        f"<div style='margin-left: {CHECKBOX_INDENT}px; margin-bottom: 8px; font-size: 15px; font-weight: 600;'>**{current_group}**</div>",
                        unsafe_allow_html=True,
                    )
                columns = st.columns(1 + DB_CASE_CONFIG_SETTING_COLUMNS)
                col_idx = 0
            column = columns[1 + col_idx % DB_CASE_CONFIG_SETTING_COLUMNS]
            rich_help = get_case_param_tooltip(config.label.value, config.inputHelp)
            config_with_help = config.copy(update={"inputHelp": rich_help})
            key = "%s-%s-%s" % (db, uiCaseItem.label, config.label.value)
            dbCaseConfig[config.label] = inputWidget(column, config=config_with_help, key=key)
            col_idx += 1
