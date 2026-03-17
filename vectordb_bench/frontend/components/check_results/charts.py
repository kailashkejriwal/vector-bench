import plotly.express as px

from vectordb_bench.frontend.components.check_results.expanderStyle import (
    initMainExpanderStyle,
)
from vectordb_bench.frontend.config.results_metric_tooltips import (
    get_results_metric_display_name,
    get_results_metric_tooltip,
    group_metrics_for_display,
)
from vectordb_bench.frontend.config.styles import *
from vectordb_bench.metric import (
    AVG_MEMORY_USAGE_METRIC,
    DISK_READ_BYTES_METRIC,
    DISK_WRITE_BYTES_METRIC,
    PEAK_MEMORY_USAGE_METRIC,
    isLowerIsBetterMetric,
    metric_order,
    metric_unit_map,
)
from vectordb_bench.models import ResultLabel


def _metric_value(d: dict, metric: str) -> float:
    """Return a numeric value for the metric; handles list-valued metrics (e.g. from asdict)."""
    v = d.get(metric, 0)
    if isinstance(v, list):
        if len(v) == 0:
            return 0.0
        first = v[0]
        while isinstance(first, list) and first:
            first = first[0]
        return float(first) if isinstance(first, (int, float)) else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    return 0.0


def _to_display_value_and_unit(metric: str, raw_value: float) -> tuple[float, str]:
    """Convert to the highest sensible unit for UX: memory MB→GB, bytes→GB; else return as-is."""
    if metric in (AVG_MEMORY_USAGE_METRIC, PEAK_MEMORY_USAGE_METRIC):
        return (raw_value / 1024.0, " GB")
    if metric in (DISK_READ_BYTES_METRIC, DISK_WRITE_BYTES_METRIC):
        return (raw_value / 1e9, " GB")
    unit = metric_unit_map.get(metric, "")
    return (raw_value, f" {unit}" if unit else "")


def drawCharts(st, allData, failedTasks, caseNames: list[str]):
    initMainExpanderStyle(st)
    for caseName in caseNames:
        chartContainer = st.expander(caseName, True)
        data = [data for data in allData if data["case_name"] == caseName]
        drawChart(data, chartContainer, key_prefix=caseName)

        errorDBs = failedTasks[caseName]
        showFailedDBs(chartContainer, errorDBs)


def showFailedDBs(st, errorDBs):
    failedDBs = [db for db, label in errorDBs.items() if label == ResultLabel.FAILED]
    timeoutDBs = [db for db, label in errorDBs.items() if label == ResultLabel.OUTOFRANGE]

    showFailedText(st, "Failed", failedDBs)
    showFailedText(st, "Timeout", timeoutDBs)


def showFailedText(st, text, dbs):
    if len(dbs) > 0:
        st.markdown(
            f"<div style='margin: -16px 0 12px 8px; font-size: 16px; font-weight: 600;'>{text}: &nbsp;&nbsp;{', '.join(dbs)}</div>",
            unsafe_allow_html=True,
        )


def drawChart(data, st, key_prefix: str):
    metrics_set = set()
    for d in data:
        metrics_set = metrics_set.union(d["metricsSet"])
    grouped = group_metrics_for_display(metrics_set)

    for idx, (group_heading, metrics_in_group) in enumerate(grouped):
        # Only show metrics that have at least one row with chartable value (avoids empty "Other")
        metrics_with_data = [m for m in metrics_in_group if any(_metric_value(d, m) > 1e-7 for d in data)]
        if not metrics_with_data:
            continue
        st.markdown(f"**{group_heading}**")
        for metric in metrics_with_data:
            container = st.container()
            key = f"{key_prefix}-{metric}"
            tooltip = get_results_metric_tooltip(metric)
            drawn = drawMetricChart(data, metric, container, key=key, help_text=tooltip)
            if drawn and tooltip:
                st.caption(tooltip)
        if idx < len(grouped) - 1:
            st.markdown("---")


def getLabelToShapeMap(data):
    labelIndexMap = {}

    dbSet = {d["db"] for d in data}
    for db in dbSet:
        labelSet = {d["db_label"] for d in data if d["db"] == db}
        labelList = list(labelSet)

        usedShapes = set()
        i = 0
        for label in labelList:
            if label not in labelIndexMap:
                loopCount = 0
                while i % len(PATTERN_SHAPES) in usedShapes:
                    i += 1
                    loopCount += 1
                    if loopCount > len(PATTERN_SHAPES):
                        break
                labelIndexMap[label] = i
                i += 1
            else:
                usedShapes.add(labelIndexMap[label] % len(PATTERN_SHAPES))

    labelToShapeMap = {label: getPatternShape(index) for label, index in labelIndexMap.items()}
    return labelToShapeMap


def drawMetricChart(data, metric, st, key: str, help_text: str = ""):  # noqa: ARG001
    """Draw a bar chart for the metric if there is chartable data. Returns True if drawn, False if skipped."""
    dataWithMetric = [d for d in data if _metric_value(d, metric) > 1e-7]
    if len(dataWithMetric) == 0:
        return False

    # Normalize to display value (handles list-valued metrics) and highest unit (e.g. GB)
    display_data = []
    _, display_unit = _to_display_value_and_unit(metric, _metric_value(dataWithMetric[0], metric))
    for d in dataWithMetric:
        raw = _metric_value(d, metric)
        display_val, _ = _to_display_value_and_unit(metric, raw)
        display_data.append({**d, metric: display_val})
    unit = display_unit or metric_unit_map.get(metric, "")
    if unit and not unit.startswith(" "):
        unit = " " + unit

    # Consistent bar order across all charts: same DB order every time
    db_order = sorted(set(d.get("db_name", "") for d in display_data))
    display_data = sorted(display_data, key=lambda d: db_order.index(d.get("db_name", "")) if d.get("db_name") in db_order else 999)

    chart = st.container()

    height = len(display_data) * 24 + 48
    xmin = 0
    xmax = max(d[metric] for d in display_data)
    xpadding = (xmax - xmin) / 16
    xpadding_multiplier = 1.8
    xrange = [xmin, xmax + xpadding * xpadding_multiplier]
    labelToShapeMap = getLabelToShapeMap(display_data)
    title_suffix = "less is better" if isLowerIsBetterMetric(metric) else "more is better"
    fig = px.bar(
        display_data,
        x=metric,
        y="db_name",
        color="db",
        height=height,
        # pattern_shape="db_label",
        # pattern_shape_sequence=SHAPES,
        pattern_shape_map=labelToShapeMap,
        orientation="h",
        hover_data={
            "db": False,
            "db_label": False,
            "db_name": True,
        },
        color_discrete_map=COLOR_MAP,
        text_auto=True,
        title=f"{get_results_metric_display_name(metric)} ({title_suffix})",
    )
    fig.update_xaxes(showticklabels=False, visible=False, range=xrange)
    fig.update_yaxes(
        # showticklabels=False,
        # visible=False,
        title=dict(
            font=dict(
                size=1,
            ),
            text="",
        )
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(
            color="#333",
            size=12,
        ),
        marker=dict(pattern=dict(fillmode="overlay", fgcolor="#fff", fgopacity=1, size=7)),
        texttemplate="%{x:,.4~r}" + unit,
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=48, b=12, pad=8),
        bargap=0.25,
        showlegend=False,
        legend=dict(orientation="h", yanchor="bottom", y=1, xanchor="right", x=1, title=""),
        # legend=dict(orientation="v", title=""),
        yaxis={"categoryorder": "array", "categoryarray": db_order},
        title=dict(
            font=dict(
                size=16,
                color="#666",
                # family="Arial, sans-serif",
            ),
            pad=dict(l=16),
            # y=0.95,
            # yanchor="top",
            # yref="container",
        ),
    )

    chart.plotly_chart(fig, use_container_width=True, key=key)
    return True
