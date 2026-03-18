import streamlit as st

from vectordb_bench.backend.clients import DB


def instance_count_selector(st_container, actived_db_list: list[DB]) -> dict[DB, int]:
    """
    Show selected databases with an instance count (min 1) for each.
    Returns a dict mapping each selected DB to its instance count.
    """
    if not actived_db_list:
        return {}

    st_container.markdown(
        "<div style='height: 12px;'></div>",
        unsafe_allow_html=True,
    )
    st_container.subheader("Instance count per database")
    st_container.markdown(
        "<div style='color: #647489; margin-bottom: 24px; margin-top: -12px;'>"
        "Set how many instances of each database to run. Each instance can be configured separately below."
        "</div>",
        unsafe_allow_html=True,
    )

    instance_count: dict[DB, int] = {}
    columns = st_container.columns(min(len(actived_db_list), 4), gap="medium")
    for i, db in enumerate(actived_db_list):
        col = columns[i % len(columns)]
        with col:
            count = st.number_input(
                f"{db.name}",
                min_value=1,
                value=1,
                step=1,
                key=f"instance-count-{db.name}",
                help="Number of instances of this database to benchmark. Each instance has its own config.",
            )
            instance_count[db] = int(count)

    return instance_count
