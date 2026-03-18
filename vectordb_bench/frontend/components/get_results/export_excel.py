"""
Export benchmark results to a formatted Excel file.
Captures the same information as the results page: summary, configurations per instance, and metrics per case.
"""

import io
import warnings
from collections import OrderedDict

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from vectordb_bench.frontend.components.check_results.results_summary import (
    get_databases_and_tuning,
    get_query_run_counts,
)
from vectordb_bench.frontend.config.results_metric_tooltips import (
    get_results_metric_display_name,
    group_metrics_for_display,
)
from vectordb_bench.metric import metric_unit_map

# Excel sheet names must be <= 31 characters
EXCEL_SHEET_TITLE_MAX_LEN = 31


def _sheet_title(name: str, fallback: str = "Sheet") -> str:
    """Return a safe Excel sheet title (max 31 chars, no invalid chars)."""
    safe = "".join(c if c.isalnum() or c in " &-_" else "_" for c in (name or "")).strip() or fallback
    if len(safe) > EXCEL_SHEET_TITLE_MAX_LEN:
        safe = safe[:EXCEL_SHEET_TITLE_MAX_LEN]
    return safe


def _readable_param_key(key: str) -> str:
    return key.replace("_", " ").title()


def _metric_value(d: dict, metric: str):
    """Return a value suitable for Excel; flatten list metrics to first element or comma-sep."""
    v = d.get(metric, None)
    if v is None:
        return ""
    if isinstance(v, list):
        if len(v) == 0:
            return ""
        first = v[0]
        while isinstance(first, list) and first:
            first = first[0]
        if isinstance(first, (int, float)):
            return float(first)
        return ", ".join(str(x) for x in v[:10]) + ("..." if len(v) > 10 else "")
    if isinstance(v, (int, float)):
        return float(v) if isinstance(v, float) else v
    return v


def _style_header(cell):
    cell.font = Font(bold=True)
    cell.fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    cell.border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )


def _style_cell(cell):
    cell.border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )
    cell.alignment = Alignment(vertical="center")


def build_results_excel(shown_data: list[dict], failed_tasks: dict) -> bytes:
    """
    Build a formatted Excel workbook from the same data shown on the results page.
    Returns the file as bytes for use with st.download_button.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*31 characters.*")
        wb = Workbook()
    wb.remove(wb.active)

    # --- Summary sheet ---
    ws_summary = wb.create_sheet(_sheet_title("Summary", "Summary"), 0)
    row = 1
    db_tuning = get_databases_and_tuning(shown_data)
    counts = get_query_run_counts(shown_data)

    ws_summary.cell(row=row, column=1, value="Databases in this run")
    ws_summary.cell(row=row, column=1).font = Font(bold=True)
    row += 1
    if db_tuning:
        ws_summary.cell(row=row, column=1, value=", ".join(db_tuning.keys()))
        row += 2
        ws_summary.cell(row=row, column=1, value="Configurations per instance")
        ws_summary.cell(row=row, column=1).font = Font(bold=True)
        row += 1
        param_keys = sorted(set(k for params in db_tuning.values() for k in params.keys()))
        if param_keys:
            ws_summary.cell(row=row, column=1, value="Instance")
            _style_header(ws_summary.cell(row=row, column=1))
            for c, k in enumerate(param_keys, start=2):
                ws_summary.cell(row=row, column=c, value=_readable_param_key(k))
                _style_header(ws_summary.cell(row=row, column=c))
            row += 1
            for instance_name, params in db_tuning.items():
                ws_summary.cell(row=row, column=1, value=instance_name)
                _style_cell(ws_summary.cell(row=row, column=1))
                for c, k in enumerate(param_keys, start=2):
                    v = params.get(k, "")
                    v = getattr(v, "value", v) if v != "" else "—"
                    ws_summary.cell(row=row, column=c, value=str(v))
                    _style_cell(ws_summary.cell(row=row, column=c))
                row += 1
        else:
            for instance_name in db_tuning:
                ws_summary.cell(row=row, column=1, value=instance_name)
                ws_summary.cell(row=row, column=2, value="—")
                _style_cell(ws_summary.cell(row=row, column=1))
                _style_cell(ws_summary.cell(row=row, column=2))
                row += 1
        row += 1
    ws_summary.cell(row=row, column=1, value="Queries run")
    ws_summary.cell(row=row, column=1).font = Font(bold=True)
    row += 1
    if counts:
        ws_summary.cell(row=row, column=1, value="Case")
        ws_summary.cell(row=row, column=2, value="Count")
        _style_header(ws_summary.cell(row=row, column=1))
        _style_header(ws_summary.cell(row=row, column=2))
        row += 1
        for case_name in sorted(counts.keys()):
            n = counts[case_name]
            ws_summary.cell(row=row, column=1, value=case_name)
            ws_summary.cell(row=row, column=2, value=n)
            _style_cell(ws_summary.cell(row=row, column=1))
            _style_cell(ws_summary.cell(row=row, column=2))
            row += 1
    if failed_tasks:
        row += 1
        ws_summary.cell(row=row, column=1, value="Failed / Timeout (by case)")
        ws_summary.cell(row=row, column=1).font = Font(bold=True)
        row += 1
        for case_name, db_labels in failed_tasks.items():
            for db_name, label in db_labels.items():
                ws_summary.cell(row=row, column=1, value=case_name)
                ws_summary.cell(row=row, column=2, value=db_name)
                ws_summary.cell(row=row, column=3, value=str(label))
                _style_cell(ws_summary.cell(row=row, column=1))
                _style_cell(ws_summary.cell(row=row, column=2))
                _style_cell(ws_summary.cell(row=row, column=3))
                row += 1
    for col in range(1, 20):
        ws_summary.column_dimensions[get_column_letter(col)].width = 18

    # --- One sheet per case (unique titles, max 31 chars each) ---
    case_names = list(OrderedDict.fromkeys(d["case_name"] for d in shown_data))
    used_titles = set()
    for idx, case_name in enumerate(case_names):
        base = _sheet_title(case_name, "Case")
        title = base
        n = 1
        while title in used_titles:
            suffix = f"_{n}"
            title = _sheet_title(case_name, "Case")[: (EXCEL_SHEET_TITLE_MAX_LEN - len(suffix))] + suffix
            n += 1
        used_titles.add(title)
        ws = wb.create_sheet(title, idx + 1)
        case_data = [d for d in shown_data if d["case_name"] == case_name]
        if not case_data:
            continue
        metrics_set = set()
        for d in case_data:
            metrics_set = metrics_set.union(d.get("metricsSet", set()))
        grouped = group_metrics_for_display(metrics_set)
        col = 1
        ws.cell(row=1, column=col, value="Instance")
        _style_header(ws.cell(row=1, column=col))
        col += 1
        for group_heading, metrics_in_group in grouped:
            for metric in metrics_in_group:
                if any(_metric_value(d, metric) != "" for d in case_data):
                    unit = metric_unit_map.get(metric, "")
                    header = get_results_metric_display_name(metric)
                    if unit:
                        header = f"{header} ({unit})"
                    ws.cell(row=1, column=col, value=header)
                    _style_header(ws.cell(row=1, column=col))
                    col += 1
        for r, d in enumerate(case_data, start=2):
            ws.cell(row=r, column=1, value=d.get("bar_display_name") or d.get("db_name", ""))
            _style_cell(ws.cell(row=r, column=1))
            c = 2
            for _group_heading, metrics_in_group in grouped:
                for metric in metrics_in_group:
                    if any(_metric_value(x, metric) != "" for x in case_data):
                        val = _metric_value(d, metric)
                        ws.cell(row=r, column=c, value=val)
                        _style_cell(ws.cell(row=r, column=c))
                        c += 1
        for c in range(1, col + 1):
            ws.column_dimensions[get_column_letter(c)].width = 20

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()
