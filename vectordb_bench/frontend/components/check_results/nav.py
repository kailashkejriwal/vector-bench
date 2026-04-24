def _switch_page(st, target: str):
    # Prefer native Streamlit navigation to avoid a hard dependency on streamlit_extras.
    if hasattr(st, "switch_page"):
        st.switch_page(target)
        return
    try:
        from streamlit_extras.switch_page_button import switch_page
    except ModuleNotFoundError:
        st.warning(
            "Page navigation helper is unavailable. Install streamlit-extras or upgrade Streamlit.",
            icon="⚠️",
        )
        return
    switch_page(target)


def NavToRunTest(st):
    st.subheader("Run your test")
    st.write("You can set the configs and run your own test.")
    navClick = st.button("Run Your Test &nbsp;&nbsp;>")
    if navClick:
        _switch_page(st, "pages/run_test.py")


def NavToQuriesPerDollar(st):
    st.subheader("Compare qps with price.")
    navClick = st.button("QP$ (Queries per Dollar) &nbsp;&nbsp;>")
    if navClick:
        _switch_page(st, "pages/results.py")


def NavToResults(st, key="nav-to-results"):
    navClick = st.button("< &nbsp;&nbsp;Back to Results", key=key)
    if navClick:
        _switch_page(st, "pages/results.py")


def NavToPages(st):
    options = [
        {"name": "Run Test", "link": "run_test"},
        {"name": "Results", "link": "results"},
        {"name": "Test Guide", "link": "test_guide"},
        {"name": "Custom Dataset", "link": "custom"},
    ]

    html = ""
    for i, option in enumerate(options):
        html += f'<a href="/{option["link"]}" target="_self" style="text-decoration: none; padding: 0.1px 0.2px;">{option["name"]}</a>'
        if i < len(options) - 1:
            html += '<span style="color: #888; margin: 0 5px;">|</span>'
    st.markdown(html, unsafe_allow_html=True)
