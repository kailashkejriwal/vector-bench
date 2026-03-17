"""
Summary of which queries (cases) were run and how many times.
"""

from collections import Counter

import streamlit as st


def get_query_run_counts(shown_data: list[dict]) -> dict[str, int]:
    """
    Count how many times each query/case was run (one run per database that ran it).
    Returns: { case_name: run_count }
    """
    if not shown_data:
        return {}
    return dict(Counter(d["case_name"] for d in shown_data))


def draw_results_summary(st, shown_data: list[dict], failed_tasks: dict, show_case_names: list[str]):
    """Render summary: which queries were run and how many times."""
    if not shown_data and not failed_tasks:
        return
    counts = get_query_run_counts(shown_data)
    if not counts:
        return

    with st.expander("Summary", expanded=True):
        for case_name in sorted(counts.keys()):
            n = counts[case_name]
            st.markdown(f"{case_name}: {n} time{'s' if n != 1 else ''}")
