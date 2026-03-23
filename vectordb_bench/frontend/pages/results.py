import streamlit as st
from vectordb_bench.backend.cases import CaseLabel
from vectordb_bench.backend.filter import FilterOp
from vectordb_bench.frontend.components.check_results.charts import drawCharts
from vectordb_bench.frontend.components.check_results.expanderStyle import (
    initMainExpanderStyle,
)
from vectordb_bench.frontend.components.check_results.filters import (
    getSectionData,
    getSectionDataMinimal,
    getshownResults,
)
from vectordb_bench.frontend.components.check_results.footer import footer
from vectordb_bench.frontend.components.check_results.priceTable import priceTable
from vectordb_bench.frontend.components.check_results.results_summary import (
    draw_results_summary,
)
from vectordb_bench.frontend.components.check_results.stPageConfig import (
    initResultsPageConfig,
)
from vectordb_bench.frontend.components.check_results.headerIcon import drawHeaderIcon
from vectordb_bench.frontend.components.check_results.charts import drawMetricChart
from vectordb_bench.frontend.components.check_results.nav import (
    NavToQuriesPerDollar,
    NavToRunTest,
    NavToPages,
)
from vectordb_bench.frontend.components.concurrent.charts import drawChartsByCase
from vectordb_bench.frontend.components.get_results.saveAsImage import getResults
from vectordb_bench.frontend.components.qps_recall.charts import drawCharts as drawQpsRecallCharts
from vectordb_bench.frontend.components.streaming.charts import drawChartsByCase as drawStreamingChartsByCase
from vectordb_bench.frontend.components.streaming.data import DisplayedMetric
from vectordb_bench.frontend.components.tables.data import getNewResults
from vectordb_bench.interface import benchmark_runner
from vectordb_bench.metric import QURIES_PER_DOLLAR_METRIC
from vectordb_bench.models import CaseResult


def main():
    initResultsPageConfig(st)

    drawHeaderIcon(st)
    NavToPages(st)

    allResults = benchmark_runner.get_results()

    st.title("Vector Database Benchmark")
    st.caption(
        "Choose your desired test results to display from the sidebar. "
        "Expand the sections below to view different benchmark types. "
        "For your reference, we've included standard benchmarks; "
        "unless explicitly labeled as distributed multi-node, tests use single-node mode by default."
    )

    # Sidebar: shared task selector
    resultSelectorContainer = st.sidebar.container()
    resultSelectorContainer.markdown(
        "<style> div[data-testid='stSidebarNav'] {display: none;} </style>",
        unsafe_allow_html=True,
    )
    resultSelectorContainer.header("Filters")
    selectedResult, selected_labels = getshownResults(resultSelectorContainer, allResults)
    resultSelectorContainer.divider()

    navContainer = st.sidebar.container()
    NavToRunTest(navContainer)
    NavToQuriesPerDollar(navContainer)

    # Export: combine data from all benchmark types (Standard, Int Filter, Label Filter)
    excel_sections = [
        ("Standard Performance", FilterOp.NonFilter),
        ("Int Filter", FilterOp.NumGE),
        ("Label Filter", FilterOp.StrEqual),
    ]
    shownDataAll = []
    failedTasksAll = {}
    for section_name, filter_type in excel_sections:
        shown, failed, _ = getSectionDataMinimal(selectedResult, filter_type)
        for d in shown:
            shownDataAll.append({**d, "benchmark_type": section_name})
        for case_name, db_labels in (failed or {}).items():
            if case_name not in failedTasksAll:
                failedTasksAll[case_name] = {}
            failedTasksAll[case_name].update(db_labels)
    resultesContainer = st.sidebar.container()
    getResults(
        resultesContainer,
        "vectordb_bench",
        results_export_data=(shownDataAll, failedTasksAll, selected_labels or []),
    )

    # No results message
    if not selectedResult:
        st.info(
            "No results to display. Select task runs in the sidebar, or run a new benchmark. "
            "Check that result files (result_*.json) exist in the results directory."
        )
        footer(st.container())
        return

    # Accordion sections
    initMainExpanderStyle(st)

    # 1. Standard Performance (NonFilter)
    with st.expander("**Standard Performance** – search benchmarks without filters", expanded=True):
        section_result = selectedResult
        shownData, failedTasks, showCaseNames = getSectionData(
            st, section_result, FilterOp.NonFilter, key_prefix="std", use_expander=False
        )
        draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
        if not showCaseNames and not failedTasks:
            st.info("No NonFilter benchmark data in the selected run(s).")
        else:
            drawCharts(st, shownData, failedTasks, showCaseNames, use_expander=False)

    # 2. Int Filter (NumGE)
    with st.expander("**Int Filter** – numeric range filters (e.g. id >= X)"):
        section_result = selectedResult
        shownData, failedTasks, showCaseNames = getSectionData(
            st, section_result, FilterOp.NumGE, key_prefix="int", use_expander=False
        )
        draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
        if not showCaseNames and not failedTasks:
            st.info("No Int Filter benchmark data in the selected run(s).")
        else:
            drawCharts(st, shownData, failedTasks, showCaseNames, use_expander=False)

    # 3. Label Filter (StrEqual)
    with st.expander("**Label Filter** – string equality filters (e.g. label = 'X')"):
        section_result = selectedResult
        shownData, failedTasks, showCaseNames = getSectionData(
            st, section_result, FilterOp.StrEqual, key_prefix="label", use_expander=False
        )
        draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
        if not showCaseNames and not failedTasks:
            st.info("No Label Filter benchmark data in the selected run(s).")
        else:
            drawCharts(st, shownData, failedTasks, showCaseNames, use_expander=False)

    # 4. QPS & Recall (scatter)
    with st.expander("**QPS & Recall** – trade-off comparison (NonFilter performance)"):
        def _qps_case_filter(cr: CaseResult) -> bool:
            case = cr.task_config.case_config.case
            return case.label == CaseLabel.Performance and case.filters.type == FilterOp.NonFilter

        section_result = [r for r in selectedResult if _qps_case_filter(r)]
        if not section_result:
            st.info("No NonFilter performance data in the selected run(s).")
        else:
            shownData, failedTasks, showCaseNames = getSectionData(
                st, section_result, FilterOp.NonFilter, key_prefix="qpsrecall", use_expander=False
            )
            draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
            if not showCaseNames and not failedTasks:
                st.info("No data to display after filtering.")
            else:
                drawQpsRecallCharts(st, shownData, showCaseNames, use_expander=False)

    # 5. Concurrent Performance
    with st.expander("**Concurrent Performance** – QPS and latency at varying concurrency"):
        section_result = [r for r in selectedResult if len(r.metrics.conc_num_list) > 0]
        if not section_result:
            st.info("No concurrent benchmark data in the selected run(s).")
        else:
            shownData, failedTasks, showCaseNames = getSectionData(
                st, section_result, FilterOp.NonFilter, key_prefix="conc", use_expander=False
            )
            draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
            if not showCaseNames and not failedTasks:
                st.info("No data to display after filtering.")
            else:
                latency_type = st.radio(
                    "Latency Type",
                    options=["latency_p99", "latency_p95", "latency_avg"],
                    key="conc-latency-type",
                )
                drawChartsByCase(shownData, showCaseNames, st.container(), latency_type=latency_type, use_expander=False)

    # 6. Queries Per Dollar
    with st.expander("**Queries Per Dollar** – QPS vs price"):
        section_result = selectedResult
        shownData, failedTasks, showCaseNames = getSectionData(
            st, section_result, FilterOp.NonFilter, key_prefix="qp", use_expander=False
        )
        draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
        if not showCaseNames and not failedTasks:
            st.info("No data in the selected run(s).")
        else:
            priceTableContainer = st.container()
            priceMap = priceTable(priceTableContainer, shownData, use_expander=False)
            for caseName in showCaseNames:
                data = [d for d in shownData if d["case_name"] == caseName]
                dataWithMetric = []
                for d in data:
                    qps = d.get("qps", 0)
                    price = priceMap.get(d["db"], {}).get(d["db_label"], 0)
                    if qps > 0 and price > 0:
                        d = dict(d)
                        d[QURIES_PER_DOLLAR_METRIC] = d["qps"] / price * 3.6
                        dataWithMetric.append(d)
                if dataWithMetric:
                    chartContainer = st.container()
                    chartContainer.markdown(f"**{caseName}**")
                    key = f"qp-{caseName}-{QURIES_PER_DOLLAR_METRIC}"
                    drawMetricChart(dataWithMetric, QURIES_PER_DOLLAR_METRIC, chartContainer, key=key)

    # 7. Streaming Performance
    with st.expander("**Streaming Performance** – search at fixed insertion rates"):
        section_result = [r for r in selectedResult if len(r.metrics.st_search_stage_list) > 0]
        if not section_result:
            st.info("No streaming benchmark data in the selected run(s).")
        else:
            shownData, failedTasks, showCaseNames = getSectionData(
                st, section_result, FilterOp.NonFilter, key_prefix="stream", use_expander=False
            )
            draw_results_summary(st, shownData, failedTasks, showCaseNames, use_expander=False)
            if not showCaseNames and not failedTasks:
                st.info("No data to display after filtering.")
            else:
                control_panel = st.columns(3)
                compared_with_optimized = control_panel[0].toggle(
                    "Compare with **optimized** performance.",
                    value=True,
                    key="stream-opt",
                )
                x_use_actual_time = control_panel[0].toggle(
                    "Use **actual time** as X-axis.",
                    value=False,
                    key="stream-time",
                )
                latency_type = control_panel[2].radio(
                    "Latency Type",
                    options=["latency_p99", "latency_p95"],
                    index=0,
                    key="stream-latency",
                )
                show_ndcg = control_panel[1].toggle("Show **NDCG** instead of Recall.", value=False, key="stream-ndcg")
                need_adjust = control_panel[1].toggle(
                    "Adjust NDCG/Recall by search stage.",
                    value=True,
                    key="stream-adj",
                )
                if show_ndcg:
                    accuracy_metric = DisplayedMetric.adjusted_ndcg if need_adjust else DisplayedMetric.ndcg
                else:
                    accuracy_metric = DisplayedMetric.adjusted_recall if need_adjust else DisplayedMetric.recall
                latency_metric = (
                    DisplayedMetric.latency_p99 if latency_type == "latency_p99" else DisplayedMetric.latency_p95
                )
                line_chart_displayed_y_metrics = [
                    (DisplayedMetric.qps, "max-qps of concurrency search per stage."),
                    (accuracy_metric, "accuracy per search stage."),
                    (latency_metric, f"serial latency ({latency_type}) per stage."),
                ]
                line_chart_displayed_x_metric = (
                    DisplayedMetric.search_time if x_use_actual_time else DisplayedMetric.search_stage
                )
                drawStreamingChartsByCase(
                    st.container(),
                    shownData,
                    showCaseNames,
                    with_last_optimized_data=compared_with_optimized,
                    line_chart_displayed_x_metric=line_chart_displayed_x_metric,
                    line_chart_displayed_y_metrics=line_chart_displayed_y_metrics,
                )
                from vectordb_bench.frontend.components.streaming.concurrent_detail import (
                    drawConcurrentPerformanceSection,
                )

                for case_name in showCaseNames:
                    case_data_list = [d for d in shownData if d["case_name"] == case_name]
                    if case_data_list:
                        drawConcurrentPerformanceSection(st.container(), case_data_list[0], case_name)

    # 8. Tables
    with st.expander("**Tables** – tabular view of all results"):
        df = getNewResults()
        if df is not None and not df.empty:
            st.dataframe(df, height=600)
        else:
            st.info("No results to display.")

    footer(st.container())


if __name__ == "__main__":
    main()
