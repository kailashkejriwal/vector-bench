"""
Test Guide – A simple reference for understanding VectorDBBench tests and results.
Written for users with less technical background.
"""

import streamlit as st
from vectordb_bench.frontend.components.check_results.headerIcon import drawHeaderIcon
from vectordb_bench.frontend.components.check_results.nav import NavToPages
from vectordb_bench.frontend.config.styles import FAVICON, PAGE_TITLE


def init_page_config(st):
    st.set_page_config(
        page_title=f"{PAGE_TITLE} – Test Guide",
        page_icon=FAVICON,
        layout="wide",
        initial_sidebar_state="collapsed",
    )


def main():
    init_page_config(st)
    drawHeaderIcon(st)
    NavToPages(st)

    st.title("Test Guide")
    st.markdown(
        "A simple reference for understanding **what each test does**, **why it matters**, "
        "and **how to read the results**. No technical background required."
    )
    st.divider()

    # ========== Key concepts ==========
    with st.expander("**First, what is a vector database?**", expanded=True):
        st.markdown("""
        Imagine you have millions of photos, and you want to find ones *similar* to a picture you show the system.
        A **vector database** stores items as lists of numbers (called *vectors* or *embeddings*) that capture meaning.
        When you search, it finds items whose numbers are *closest* to your query—like finding nearest neighbors on a map.

        **Why benchmark?** Different databases handle this differently. Some are faster, some scale better,
        some are better when you combine search with filters (e.g., “similar products, but only in stock”).
        This guide explains the tests we run to compare them.
        """)

    # ========== Test categories ==========
    st.header("Test categories")

    # --- Capacity Tests ---
    st.subheader("1. Capacity tests")
    st.markdown(
        "**What it tests:** How much data the database can hold before it fails."
    )
    st.markdown("""
    - **Simple idea:** We keep inserting data until the database cannot take anymore. The final count tells us its capacity.
    - **Two variants:**
      - **128 dimensions:** Smaller vectors (SIFT dataset). Good for testing raw capacity with lighter workloads.
      - **960 dimensions:** Larger vectors (GIST dataset). More realistic for high-dimensional applications.
    - **Why it matters:** If you plan to grow from 1M to 10M+ items, you need a database that can handle it.
    """)
    with st.container(border=True):
        st.markdown("**How to read the result:**")
        st.markdown("- **Max load count** = number of vectors successfully loaded (higher is better)")
        st.markdown("- If a database stops early or crashes, its capacity is limited for your use case.")

    # --- Performance tests ---
    st.subheader("2. Performance (search speed) tests")
    st.markdown(
        "**What it tests:** How quickly and accurately the database answers similarity searches."
    )
    st.markdown("""
    We load a fixed amount of data, then run many search queries. We measure:
    - **Speed** – How many searches per second (QPS)?
    - **Accuracy** – Does it find the right “nearest” items (recall)?
    - **Latency** – How long does the slowest 1% of requests take?
    """)
    st.markdown("""
    **Dataset sizes you’ll see:**
    - **50K, 500K, 1M, 5M, 10M, 100M** – Number of vectors in the test (e.g., 1M = 1 million).
    - **768, 1024, 1536 dimensions** – Vector size. Higher dimensions usually mean harder work for the database.
    """)
    with st.container(border=True):
        st.markdown("**How to read the results:**")
        st.markdown("- **QPS (queries per second):** Higher = more searches per second = faster system.")
        st.markdown("- **Recall (0–1):** Closer to 1 = more accurate (e.g., 0.95 means 95% of true neighbors found).")
        st.markdown("- **Serial latency (p99):** Lower = slower requests are still acceptable (in milliseconds).")
        st.markdown("- **Load duration:** Time to load data and build the index (in seconds). Lower = quicker to get ready.")

    # --- Filter tests ---
    st.subheader("3. Filter tests")
    st.markdown(
        "**What it tests:** Search performance when you add conditions like “only show items where X &gt; Y” or “where category = Z”."
    )
    st.markdown("""
    In real apps, you often don’t search over *everything*. You filter first:
    - *“Similar products, but only in stock.”*
    - *“Similar articles, but from the last 30 days.”*
    - *“Similar users, but in the same region.”*

    We test two kinds of filters:
    - **Int filter (numeric):** e.g., “id ≥ 10000” – only 1% or 99% of data matches.
    - **Label filter (category):** e.g., “category = X” – different label distributions (50%, 10%, 1%, etc.).
    """)
    with st.container(border=True):
        st.markdown("**How to read the results:**")
        st.markdown("- Same metrics as performance tests (QPS, recall, latency), but under filter pressure.")
        st.markdown("- **Filter 1%** = only 1% of data passes the filter (harder). **Filter 99%** = 99% passes (easier).")
        st.markdown("- A database that keeps good recall and QPS under strict filters is better for filtered search.")

    # --- Streaming tests ---
    st.subheader("4. Streaming tests")
    st.markdown(
        "**What it tests:** Search performance *while* new data is constantly being inserted."
    )
    st.markdown("""
    In many systems, data is added continuously (e.g., new users, new products). We simulate that:
    - We insert data at a **fixed rate** (e.g., 500 rows per second).
    - At certain **stages** (e.g., 50% inserted, 80% inserted), we run search tests.
    - We see how QPS, latency, and recall change as the database is “under load.”
    """)
    with st.container(border=True):
        st.markdown("**How to read the results:**")
        st.markdown("- Charts show performance at each stage (e.g., 50%, 80%, 100%).")
        st.markdown("- If performance drops a lot during insertion but recovers after, the database may need tuning for real-time writes.")
        st.markdown("- **Optimized search** (after insertion stops) shows the best-case performance for comparison.")

    # ========== Key metrics summary ==========
    st.divider()
    st.header("Quick reference: key metrics")

    cols = st.columns(2)
    with cols[0]:
        st.markdown("""
        | Metric | Meaning | Better when |
        |--------|---------|-------------|
        | **QPS** | Queries per second | Higher |
        | **Recall** | Accuracy of search (0–1) | Closer to 1 |
        | **Serial latency (p99)** | Slowest 1% of requests (ms) | Lower |
        | **Load duration** | Time to load + index (sec) | Lower |
        | **Max load count** | Max vectors before failure | Higher |
        """)
    with cols[1]:
        st.markdown("""
        | Metric | Meaning | Better when |
        |--------|---------|-------------|
        | **Write QPS** | Insert speed (vectors/s) | Higher |
        | **Peak CPU / Memory** | Resource usage | Lower |
        | **Queries per $** | Cost efficiency | Higher |
        """)

    # ========== Example scenarios ==========
    st.divider()
    st.header("Example: which test for my use case?")

    examples = [
        {
            "scenario": "I need to compare databases for a product search app (millions of items).",
            "tests": "Performance tests with 1M or 10M dataset. Look at QPS and recall.",
        },
        {
            "scenario": "We add new items constantly and need good search during ingestion.",
            "tests": "Streaming tests. Compare performance at different insert stages.",
        },
        {
            "scenario": "Our search always has filters (e.g., region, date range).",
            "tests": "Filter tests (Int or Label). Check that recall and QPS stay acceptable.",
        },
        {
            "scenario": "We plan to grow to tens or hundreds of millions of vectors.",
            "tests": "Capacity tests. Ensure the database can hold your target scale.",
        },
    ]
    for ex in examples:
        with st.container(border=True):
            st.markdown(f"**{ex['scenario']}**")
            st.markdown(f"→ {ex['tests']}")

    st.divider()
    st.caption(
        "For more technical details, see the main benchmark documentation. "
        "To run tests, go to **Run Test**; to view results, go to **Results**."
    )


if __name__ == "__main__":
    main()
