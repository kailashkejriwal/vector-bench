"""
Summary of which queries (cases) were run, which databases/instances were selected,
and the tuning parameters used for each.
"""

from collections import Counter, OrderedDict

import streamlit as st


def _readable_param_key(key: str) -> str:
    """Convert param key to readable form: 'ef_construction' -> 'Ef construction'."""
    return key.replace("_", " ").title()


def get_query_run_counts(shown_data: list[dict]) -> dict[str, int]:
    """
    Count how many times each query/case was run (one run per database that ran it).
    Returns: { case_name: run_count }
    """
    if not shown_data:
        return {}
    return dict(Counter(d["case_name"] for d in shown_data))


def get_databases_and_tuning(shown_data: list[dict]) -> OrderedDict[str, dict]:
    """
    Return unique databases/instances (bar_display_name) in display order,
    each with one representative tuning_params dict for the summary.
    """
    if not shown_data:
        return OrderedDict()
    seen = OrderedDict()
    for d in shown_data:
        name = d.get("bar_display_name") or d.get("db_name", "")
        if name and name not in seen:
            seen[name] = d.get("tuning_params") or {}
    return seen


def draw_results_summary(st, shown_data: list[dict], failed_tasks: dict, show_case_names: list[str], use_expander: bool = True):
    """Render summary: databases in the run, tuning per database, and query run counts.
    use_expander=False when inside another expander to avoid Streamlit's nested-expander restriction."""
    if not shown_data and not failed_tasks:
        return
    counts = get_query_run_counts(shown_data)
    db_tuning = get_databases_and_tuning(shown_data)

    if use_expander:
        outer = st.expander("Summary", expanded=True)
    else:
        outer = st.container()
        outer.markdown("**Summary**")
    with outer:
        if db_tuning:
            st.markdown("**Databases in this run**")
            st.markdown(", ".join(db_tuning.keys()))
            st.markdown("")
            st.markdown("**Configurations per instance**")
            for bar_name, params in db_tuning.items():
                if params:
                    parts = [
                        f"{_readable_param_key(k)}: {getattr(v, 'value', v)}"
                        for k, v in sorted(params.items())
                    ]
                    st.markdown(f"- **{bar_name}:** {', '.join(parts)}")
                else:
                    st.markdown(f"- **{bar_name}:** —")
            st.markdown("")
        if counts:
            st.markdown("**Queries run**")
            for case_name in sorted(counts.keys()):
                n = counts[case_name]
                st.markdown(f"{case_name}: {n} time{'s' if n != 1 else ''}")
