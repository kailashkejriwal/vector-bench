from vectordb_bench.frontend.components.custom.getCustomConfig import CustomStreamingCaseConfig
from vectordb_bench.frontend.config.parameter_tooltips import CUSTOM_FIELD_TOOLTIPS


def _help(field: str) -> str:
    return CUSTOM_FIELD_TOOLTIPS.get(field, "")


def displayCustomStreamingCase(streamingCase: CustomStreamingCaseConfig, st, key):
    st.markdown("**Dataset identity**")
    columns = st.columns([1, 2])
    streamingCase.dataset_config.name = columns[0].text_input(
        "Name", key=f"{key}_name", value=streamingCase.dataset_config.name, help=_help("Name")
    )
    streamingCase.dataset_config.dir = columns[1].text_input(
        "Folder Path", key=f"{key}_dir", value=streamingCase.dataset_config.dir, help=_help("Folder Path")
    )

    st.markdown("**Dimensions & size**")
    columns = st.columns(2)
    streamingCase.dataset_config.dim = columns[0].number_input(
        "dim", key=f"{key}_dim", value=streamingCase.dataset_config.dim, help=_help("dim")
    )
    streamingCase.dataset_config.size = columns[1].number_input(
        "size", key=f"{key}_size", value=streamingCase.dataset_config.size, help=_help("size")
    )

    st.markdown("**File names**")
    columns = st.columns(3)
    streamingCase.dataset_config.train_name = columns[0].text_input(
        "train file name",
        key=f"{key}_train_name",
        value=streamingCase.dataset_config.train_name,
        help=_help("train file name"),
    )
    streamingCase.dataset_config.test_name = columns[1].text_input(
        "test file name", key=f"{key}_test_name", value=streamingCase.dataset_config.test_name, help=_help("test file name")
    )
    streamingCase.dataset_config.gt_name = columns[2].text_input(
        "ground truth file name", key=f"{key}_gt_name", value=streamingCase.dataset_config.gt_name, help=_help("ground truth file name")
    )

    st.markdown("**Column mapping**")
    columns = st.columns([1, 1, 2, 2])
    streamingCase.dataset_config.train_id_name = columns[0].text_input(
        "train id name", key=f"{key}_train_id_name", value=streamingCase.dataset_config.train_id_name, help=_help("train id name")
    )
    streamingCase.dataset_config.train_col_name = columns[1].text_input(
        "train emb name", key=f"{key}_train_col_name", value=streamingCase.dataset_config.train_col_name, help=_help("train emb name")
    )
    streamingCase.dataset_config.test_col_name = columns[2].text_input(
        "test emb name", key=f"{key}_test_col_name", value=streamingCase.dataset_config.test_col_name, help=_help("test emb name")
    )
    streamingCase.dataset_config.gt_col_name = columns[3].text_input(
        "ground truth emb name", key=f"{key}_gt_col_name", value=streamingCase.dataset_config.gt_col_name, help=_help("ground truth emb name")
    )

    st.markdown("**Description**")
    streamingCase.description = st.text_area("description", key=f"{key}_description", value=streamingCase.description, help=_help("description"))
