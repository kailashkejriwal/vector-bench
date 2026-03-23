"""
Central tooltips for all UI parameters: description + performance impact.
Use GB for memory/storage where applicable. Used to group params under common headings.
"""

# Storage note shown in memory-related tooltips (vector vs index)
STORAGE_NOTE = (
    " Rough storage: vector data ≈ (num_vectors × dim × 4 bytes) in GB; "
    "index storage varies by type (e.g. HNSW often 2–4× raw vectors)."
)

# ---------------------------------------------------------------------------
# Case / index parameter tooltips (key = CaseConfigParamType.value)
# Format: description. Performance impact: ...
# ---------------------------------------------------------------------------
PARAM_TOOLTIPS: dict[str, str] = {
    "IndexType": (
        "Algorithm used to build the vector index (e.g. HNSW, IVF_FLAT). "
        "Performance impact: Choice trades off build time, memory (GB), and search latency vs recall. "
        "HNSW: high recall, faster search; IVF: faster build, lower memory; Flat: exact search, no index."
    ),
    "index": "Index type identifier for this database.",
    "M": (
        "HNSW graph degree: number of bidirectional links per node. "
        "Performance impact: Higher M improves recall and search quality but increases index build time and memory (GB). "
        "Typical range 8–64. Index storage grows roughly with M."
    ),
    "m": (
        "HNSW graph degree (same meaning as M). "
        "Performance impact: Higher m = better recall and more memory (GB); lower m = faster build and less storage."
    ),
    "EFConstruction": (
        "Size of the dynamic candidate list during HNSW construction. "
        "Performance impact: Higher values improve index quality and recall but slow down build and use more memory (GB). "
        "Recommend 100–500 for high recall."
    ),
    "ef_construction": "Same as EFConstruction: build-time candidate list size. Higher = better recall, longer build, more memory (GB).",
    "ef_construct": "Same as ef_construction (e.g. Qdrant). Higher = better recall, longer build, more memory (GB).",
    "EF": "Search-time candidate list size (ef or ef_search). Performance impact: Higher = better recall, higher latency and more CPU.",
    "ef_search": (
        "Number of nearest-neighbor candidates explored at search time. "
        "Performance impact: Higher ef_search improves recall but increases search latency and CPU. "
        "Tune for your latency/recall target."
    ),
    "SearchList": "Search list size (candidates explored). Performance impact: Larger list = higher recall, higher latency.",
    "search_list": "Search list size. Performance impact: Larger = better recall, higher search latency.",
    "Nlist": (
        "Number of clusters (Voronoi cells) in IVF index. "
        "Performance impact: More nlist = finer partitioning, better recall but longer build and more memory (GB). "
        "Rule of thumb: sqrt(n) to 4*sqrt(n) for n vectors."
    ),
    "nlist": "Same as Nlist. Performance impact: More clusters = better recall, slower build, more memory (GB).",
    "Nprobe": (
        "Number of clusters to probe at search time in IVF. "
        "Performance impact: Higher nprobe = better recall, higher search latency. "
        "Typical 1–256; balance with nlist."
    ),
    "nprobe": "Same as Nprobe. Performance impact: More probes = higher recall, higher search latency.",
    "lists": "IVF-style number of lists/clusters. Performance impact: More lists = better recall, longer build, more memory (GB).",
    "probes": "IVF-style number of probes at search. Performance impact: More probes = higher recall, higher latency.",
    "num_candidates": "Number of candidates considered during search. Performance impact: More candidates = better recall, higher latency.",
    "numCandidates": "Same as num_candidates. Performance impact: Higher = better recall, higher search latency.",
    "MaxConnections": "Max connections per node (HNSW). Performance impact: Affects graph connectivity and recall vs memory (GB).",
    "maxConnections": "Max connections per node. Performance impact: Higher = better recall, more memory (GB).",
    "quantization_type": "Quantization method (e.g. SQ, PQ). Performance impact: Reduces memory (GB) and can speed search; may slightly reduce recall.",
    "quantizationType": "Quantization type. Performance impact: Reduces index storage (GB) and can improve throughput; tune for recall.",
    "quantization_ratio": "Ratio or level of quantization. Performance impact: Higher compression = less storage (GB), possible recall loss.",
    "tableQuantizationType": "Table-level quantization. Performance impact: Affects index size (GB) and search accuracy.",
    "sq_type": "Scalar quantization type. Performance impact: Affects index size (GB) and search quality.",
    "refine": "Whether to keep refined (full-precision) vectors. Performance impact: Improves recall, increases memory (GB).",
    "refine_type": "Data type of refine index. Performance impact: Affects memory (GB) and recall.",
    "refine_k": "Refine magnification factor vs k. Performance impact: Larger = better recall, more memory (GB).",
    "refine_ratio": "Refine ratio. Performance impact: Affects recall and memory (GB).",
    "reranking": "Enable reranking (e.g. with PQ). Performance impact: Improves recall, increases search latency.",
    "reranking_metric": "Metric used for reranking. Performance impact: Must match index metric for correct behavior.",
    "quantized_fetch_limit": "Top-k limit using quantized comparison before rerank. Performance impact: Higher = better recall, higher latency.",
    "quantizedFetchLimit": "Same as quantized_fetch_limit. Performance impact: Balances recall and search cost.",
    "pq_param_num_chunks": "Product quantization chunks. Performance impact: More chunks = better accuracy, more memory (GB).",
    "maintenance_work_mem": (
        "Memory (GB) for index build operations. "
        "Performance impact: Larger values speed up index build; set in GB (e.g. 8GB). Do not exceed available RAM."
        + STORAGE_NOTE
    ),
    "max_parallel_workers": (
        "Max parallel workers for index build. "
        "Performance impact: More workers = faster build; recommend (CPU cores - 1). May increase peak memory (GB)."
    ),
    "max_parallel_workers_PgVector": "Max parallel workers for index build. Performance impact: More workers = faster build; recommend (CPU cores - 1).",
    "max_neighbors": "Max neighbors (e.g. DiskANN). Performance impact: Affects search quality and latency.",
    "num_neighbors": "Number of neighbors in graph. Performance impact: Affects recall and search latency.",
    "search_list_size": "Search list size. Performance impact: Larger = higher recall, higher latency.",
    "query_search_list_size": "Query-time search list size. Performance impact: Larger = better recall, higher latency.",
    "query_rescore": "Whether to rescore with full vectors. Performance impact: Improves recall, increases latency.",
    "l_value_ib": "DiskANN build L value. Performance impact: Affects index build time and quality.",
    "l_value_is": "DiskANN search L value. Performance impact: Affects search recall and latency.",
    "storage_layout": "Storage layout (e.g. memory_optimized vs plain). Performance impact: Affects memory (GB) and throughput.",
    "max_alpha": "Max alpha parameter. Performance impact: Tunes search behavior and latency.",
    "num_dimensions": "Number of dimensions (0 = use data dim). Performance impact: Affects index size (GB) and build.",
    "storage_engine": "Storage engine (e.g. InnoDB). Performance impact: Affects durability and write/read performance.",
    "max_cache_size": "Max cache size (GB). Performance impact: Larger cache = better hit rate, more memory (GB).",
    "number_of_shards": "Number of shards. Performance impact: More shards = better write throughput; tune for cluster size.",
    "number_of_replicas": "Number of replicas. Performance impact: More replicas = higher read throughput and availability.",
    "refresh_interval": "Index refresh interval. Performance impact: Shorter = fresher data, more load.",
    "use_rescore": "Use rescore with original vectors. Performance impact: Improves recall, increases latency.",
    "memory_optimized_search": "Use memory-optimized search path. Performance impact: Trades memory (GB) for latency.",
    "on_disk": "Use on-disk vector storage. Performance impact: Reduces memory (GB), may increase latency.",
    "dataset_with_size_type": "Predefined dataset and size. Performance impact: Determines data volume and thus load time, index size (GB), and search load.",
    "insert_rate": "Insertion rate (rows/s). Performance impact: Higher rate = faster load; ensure DB can sustain it.",
    "search_stages": "Fractions of data at which to run search (e.g. [0.2, 0.5, 1.0]). Performance impact: More stages = more measurements, longer test.",
    "concurrencies": "Concurrency levels for search. Performance impact: Higher concurrency = measures max QPS; may increase latency.",
    "optimize_after_write": "Run optimize after all inserts. Performance impact: Improves search performance; adds time after load.",
    "read_dur_after_write": "Search test duration (s) after full insert. Performance impact: Longer = more stable QPS estimate.",
    "label_percentages": "Comma-separated label filter percentages to run (e.g. 0.001,0.5 for 0.1% and 50%). Empty = use all. Performance impact: Fewer values = faster runs, lower resource use.",
    "filter_rates": "Comma-separated int filter rates to run (e.g. 0.01,0.99 for 1% and 99%). Empty = use all. Performance impact: Fewer values = faster runs, lower resource use.",
    "quantization": "ClickHouse HNSW: Vector quantization (bf16, f32, f16, i8, b1). bf16 = good balance; i8/b1 = more compression, lower recall.",
    "granularity": "ClickHouse: Rows per index granule. Larger = fewer granules, faster queries. Default 10M.",
    "hnsw_ef": "Qdrant: Search-time HNSW ef. Higher = better recall, higher latency. 0 = use default.",
    "dynamicEfFactor": "Weaviate: When ef=-1, multiplier for dynamic ef. Default 8.",
    "dynamicEfMin": "Weaviate: When ef=-1, lower bound for dynamic ef. Default 100.",
    "dynamicEfMax": "Weaviate: When ef=-1, upper bound for dynamic ef. Default 500.",
}

# ---------------------------------------------------------------------------
# Group heading for each param (for UI grouping)
# ---------------------------------------------------------------------------
PARAM_GROUPS: dict[str, str] = {
    "IndexType": "Index type & structure",
    "index": "Index type & structure",
    "storage_layout": "Index type & structure",
    "M": "HNSW build",
    "m": "HNSW build",
    "EFConstruction": "HNSW build",
    "ef_construction": "HNSW build",
    "ef_construct": "HNSW build",
    "EF": "HNSW search",
    "ef_search": "HNSW search",
    "SearchList": "HNSW search",
    "search_list": "HNSW search",
    "query_search_list_size": "HNSW search",
    "query_rescore": "HNSW search",
    "Nlist": "IVF build",
    "nlist": "IVF build",
    "Nprobe": "IVF search",
    "nprobe": "IVF search",
    "lists": "IVF",
    "probes": "IVF search",
    "num_candidates": "Search tuning",
    "numCandidates": "Search tuning",
    "MaxConnections": "HNSW build",
    "maxConnections": "HNSW build",
    "quantization_type": "Quantization & refinement",
    "quantizationType": "Quantization & refinement",
    "quantization_ratio": "Quantization & refinement",
    "tableQuantizationType": "Quantization & refinement",
    "sq_type": "Quantization & refinement",
    "refine": "Quantization & refinement",
    "refine_type": "Quantization & refinement",
    "refine_k": "Quantization & refinement",
    "refine_ratio": "Quantization & refinement",
    "reranking": "Quantization & refinement",
    "reranking_metric": "Quantization & refinement",
    "quantized_fetch_limit": "Quantization & refinement",
    "quantizedFetchLimit": "Quantization & refinement",
    "pq_param_num_chunks": "Quantization & refinement",
    "maintenance_work_mem": "Resource & memory (GB)",
    "max_parallel_workers": "Resource & memory (GB)",
    "max_neighbors": "DiskANN / Streaming",
    "num_neighbors": "DiskANN / Streaming",
    "search_list_size": "DiskANN / Streaming",
    "l_value_ib": "DiskANN / Streaming",
    "l_value_is": "DiskANN / Streaming",
    "max_alpha": "DiskANN / Streaming",
    "num_dimensions": "DiskANN / Streaming",
    "storage_engine": "Storage & engine",
    "max_cache_size": "Resource & memory (GB)",
    "number_of_shards": "Cluster & sharding",
    "number_of_replicas": "Cluster & sharding",
    "refresh_interval": "Cluster & sharding",
    "use_rescore": "Search tuning",
    "memory_optimized_search": "Resource & memory (GB)",
    "on_disk": "Storage & engine",
    "dataset_with_size_type": "Dataset",
    "insert_rate": "Streaming load",
    "search_stages": "Streaming search",
    "concurrencies": "Streaming search",
    "optimize_after_write": "Streaming options",
    "read_dur_after_write": "Streaming options",
    "label_percentages": "Filter distribution",
    "filter_rates": "Filter distribution",
    "quantization": "ClickHouse optimization",
    "granularity": "ClickHouse optimization",
    "hnsw_ef": "Qdrant optimization",
    "dynamicEfFactor": "Weaviate optimization",
    "dynamicEfMin": "Weaviate optimization",
    "dynamicEfMax": "Weaviate optimization",
}


def get_case_param_tooltip(param_key: str, existing_help: str = "") -> str:
    """Return rich tooltip for a case config param; fall back to existing_help if no tooltip."""
    return PARAM_TOOLTIPS.get(param_key, existing_help or "").strip() or existing_help


def get_case_param_group(param_key: str) -> str:
    """Return group heading for this param; default 'Parameters'."""
    return PARAM_GROUPS.get(param_key, "Parameters")


# ---------------------------------------------------------------------------
# DB connection/config tooltips (keys from DB config schema)
# ---------------------------------------------------------------------------
DB_CONFIG_TOOLTIPS: dict[str, str] = {
    "host": "Database hostname or IP. Performance impact: Latency to DB affects every request; use same region/network for benchmarks.",
    "port": "Database port. Performance impact: Ensure firewall allows traffic; wrong port causes connection failures.",
    "uri": "Full connection URI (e.g. http://host:port). Performance impact: Same as host/port; use a single URI for consistency.",
    "url": "Service URL. Performance impact: Network distance and TLS add latency.",
    "user": "Database user. No direct performance impact.",
    "password": "Database password. No direct performance impact.",
    "api_key": "API key for managed services. No direct performance impact.",
    "token": "Auth token. No direct performance impact.",
    "db_label": "Label for this DB configuration (e.g. 2c8g). Used in results only.",
    "version": "Database or driver version. Used for result labeling.",
    "note": "Optional note for this run. Used in results only.",
    "cpu": "CPU limit for the instance (e.g. 4). Performance impact: More vCPUs = faster build and higher QPS; specify when overriding resources. Prefer GB for memory.",
    "memory": "Memory limit for the instance in GB (e.g. 8Gi or 16GB). Performance impact: More memory (GB) allows larger indexes and better cache; insufficient memory causes OOM. Vector + index storage scale with data size.",
}

# DB config keys grouped for section headers (order preserved)
DB_CONFIG_GROUP_ORDER: list[tuple[str, list[str]]] = [
    ("Connection", ["host", "port", "uri", "url", "user", "password", "api_key", "token"]),
    ("Result labeling", ["db_label", "version", "note"]),
    ("Instance resources (GB)", ["cpu", "memory"]),
]


def get_db_config_tooltip(key: str) -> str:
    """Return tooltip for a DB config key."""
    return DB_CONFIG_TOOLTIPS.get(key, "")


def get_db_config_group(key: str) -> str | None:
    """Return group name if key is in a known group."""
    for group_name, keys in DB_CONFIG_GROUP_ORDER:
        if key in keys:
            return group_name
    return None


# ---------------------------------------------------------------------------
# Custom case / custom streaming field tooltips (displayCustomCase, displayCustomStreamingCase)
# ---------------------------------------------------------------------------
CUSTOM_FIELD_TOOLTIPS: dict[str, str] = {
    "Name": "Display name for this custom case. Used in results and labels.",
    "Folder Path": "Local directory containing train/test/ground-truth parquet files. Performance impact: Path must be readable; dataset size (GB) affects load time and index size.",
    "dim": "Vector dimension. Performance impact: Higher dim increases memory (GB) and search cost; must match your embedding model.",
    "size": "Number of vectors in the dataset. Performance impact: Larger size = longer load, more index storage (GB), and higher search load.",
    "metric type": "Distance metric (L2, Cosine, IP). Performance impact: Must match how vectors were produced; wrong metric degrades recall.",
    "train file name": "Parquet filename for training vectors. Multiple files: comma-separated base names.",
    "test file name": "Parquet filename for query vectors (e.g. test.parquet).",
    "ground truth file name": "Parquet filename for ground-truth neighbors (e.g. neighbors.parquet). Used to compute recall.",
    "train id name": "Column name for vector ID in train data.",
    "train emb name": "Column name for vector embedding in train data.",
    "test emb name": "Column name for query vector in test file.",
    "ground truth emb name": "Column name for neighbor IDs in ground-truth file.",
    "scalar labels file name": "Optional file with scalar labels for filter tests (e.g. scalar_labels.parquet).",
    "label percentages": "Comma-separated filter fractions (e.g. 0.01,0.05,0.1). Performance impact: More values = more filter runs.",
    "label_percentages": "Comma-separated label filter percentages to run (e.g. 0.001,0.5 for 0.1% and 50%). Empty = use all. Reduces resource usage.",
    "filter_rates": "Comma-separated int filter rates to run (e.g. 0.01,0.99 for 1% and 99%). Empty = use all. Reduces resource usage.",
    "description": "Optional description for this custom case; used in results.",
}
