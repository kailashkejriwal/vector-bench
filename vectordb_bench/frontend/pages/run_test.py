import streamlit as st
from vectordb_bench.frontend.components.run_test.autoRefresh import autoRefresh
from vectordb_bench.frontend.components.run_test.caseSelector import caseSelector
from vectordb_bench.frontend.components.run_test.dbConfigSetting import dbConfigSettings
from vectordb_bench.frontend.components.run_test.dbSelector import dbSelector
from vectordb_bench.frontend.components.run_test.generateTasks import generate_tasks
from vectordb_bench.frontend.components.run_test.instanceCountSelector import instance_count_selector
from vectordb_bench.frontend.components.run_test.hideSidebar import hideSidebar
from vectordb_bench.frontend.components.run_test.initStyle import initStyle
from vectordb_bench.frontend.components.run_test.submitTask import submitTask
from vectordb_bench.frontend.components.check_results.nav import NavToResults, NavToPages
from vectordb_bench.frontend.components.check_results.headerIcon import drawHeaderIcon
from vectordb_bench.frontend.components.check_results.stPageConfig import initRunTestPageConfig


def main():
    # set page config
    initRunTestPageConfig(st)

    # init style
    initStyle(st)

    # header
    drawHeaderIcon(st)

    # hide sidebar
    hideSidebar(st)

    # navigate
    NavToPages(st)

    # header
    st.title("Run Your Test")
    # st.write("description [todo]")

    # select db
    db_selector_container = st.container()
    actived_db_list = dbSelector(db_selector_container)

    # instance count per selected database (below DB selection)
    instance_count = {}
    if actived_db_list:
        instance_count_container = st.container()
        instance_count = instance_count_selector(instance_count_container, actived_db_list)

    # db config setting (one panel per instance)
    db_configs = {}
    db_auto_provision_config = {}
    is_all_valid = True
    if actived_db_list:
        db_config_container = st.container()
        db_configs, is_all_valid, db_auto_provision_config = dbConfigSettings(
            db_config_container, actived_db_list, instance_count
        )

    # select case and set db_case_config (per instance)
    case_selector_container = st.container()
    actived_case_list, all_case_configs = caseSelector(
        case_selector_container, actived_db_list, instance_count
    )

    # generate tasks (one per instance per case)
    tasks = (
        generate_tasks(
            actived_db_list,
            db_configs,
            actived_case_list,
            all_case_configs,
            db_auto_provision_config,
            instance_count=instance_count,
        )
        if is_all_valid
        else []
    )

    # submit
    submit_container = st.container()
    submitTask(submit_container, tasks, is_all_valid)

    # nav to results
    NavToResults(st, key="footer-nav-to-results")

    # autofresh
    autoRefresh()


if __name__ == "__main__":
    main()
