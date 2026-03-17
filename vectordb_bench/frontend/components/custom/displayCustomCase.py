from vectordb_bench.frontend.components.custom.getCustomConfig import CustomCaseConfig
from vectordb_bench.frontend.config.parameter_tooltips import CUSTOM_FIELD_TOOLTIPS


def _help(field: str) -> str:
    return CUSTOM_FIELD_TOOLTIPS.get(field, "")


def displayCustomCase(customCase: CustomCaseConfig, st, key):
    st.markdown("**Dataset identity**")
    columns = st.columns([1, 2])
    customCase.dataset_config.name = columns[0].text_input(
        "Name", key=f"{key}_name", value=customCase.dataset_config.name, help=_help("Name")
    )
    customCase.name = f"{customCase.dataset_config.name} (Performace Case)"
    customCase.dataset_config.dir = columns[1].text_input(
        "Folder Path", key=f"{key}_dir", value=customCase.dataset_config.dir, help=_help("Folder Path")
    )

    st.markdown("**Dimensions & size**")
    columns = st.columns(3)
    customCase.dataset_config.dim = columns[0].number_input(
        "dim", key=f"{key}_dim", value=customCase.dataset_config.dim, help=_help("dim")
    )
    customCase.dataset_config.size = columns[1].number_input(
        "size", key=f"{key}_size", value=customCase.dataset_config.size, help=_help("size")
    )
    customCase.dataset_config.metric_type = columns[2].selectbox(
        "metric type", key=f"{key}_metric_type", options=["L2", "Cosine", "IP"], help=_help("metric type")
    )

    st.markdown("**File names**")
    columns = st.columns(3)
    customCase.dataset_config.train_name = columns[0].text_input(
        "train file name",
        key=f"{key}_train_name",
        value=customCase.dataset_config.train_name,
        help=_help("train file name"),
    )
    customCase.dataset_config.test_name = columns[1].text_input(
        "test file name", key=f"{key}_test_name", value=customCase.dataset_config.test_name, help=_help("test file name")
    )
    customCase.dataset_config.gt_name = columns[2].text_input(
        "ground truth file name", key=f"{key}_gt_name", value=customCase.dataset_config.gt_name, help=_help("ground truth file name")
    )

    st.markdown("**Column mapping**")
    columns = st.columns([1, 1, 2, 2])
    customCase.dataset_config.train_id_name = columns[0].text_input(
        "train id name", key=f"{key}_train_id_name", value=customCase.dataset_config.train_id_name, help=_help("train id name")
    )
    customCase.dataset_config.train_col_name = columns[1].text_input(
        "train emb name", key=f"{key}_train_col_name", value=customCase.dataset_config.train_col_name, help=_help("train emb name")
    )
    customCase.dataset_config.test_col_name = columns[2].text_input(
        "test emb name", key=f"{key}_test_col_name", value=customCase.dataset_config.test_col_name, help=_help("test emb name")
    )
    customCase.dataset_config.gt_col_name = columns[3].text_input(
        "ground truth emb name", key=f"{key}_gt_col_name", value=customCase.dataset_config.gt_col_name, help=_help("ground truth emb name")
    )

    st.markdown("**Filtering / labels**")
    columns = st.columns(2)
    customCase.dataset_config.scalar_labels_name = columns[0].text_input(
        "scalar labels file name",
        key=f"{key}_scalar_labels_file_name",
        value=customCase.dataset_config.scalar_labels_name,
        help=_help("scalar labels file name"),
    )
    default_label_percentages = ",".join(map(str, customCase.dataset_config.with_label_percentages))
    label_percentage_input = columns[1].text_input(
        "label percentages",
        key=f"{key}_label_percantages",
        value=default_label_percentages,
        help=_help("label percentages"),
    )
    try:
        customCase.dataset_config.label_percentages = [
            float(item.strip()) for item in label_percentage_input.split(",") if item.strip()
        ]
    except ValueError as e:
        st.write(f"<span style='color:red'>{e},please input correct number</span>", unsafe_allow_html=True)

    st.markdown("**Description**")
    customCase.description = st.text_area("description", key=f"{key}_description", value=customCase.description, help=_help("description"))
