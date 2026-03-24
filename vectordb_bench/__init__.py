import inspect
import pathlib

import environs

from . import log_util

env = environs.Env()
env.read_env(path=".env", recurse=False)


class config:
    ALIYUN_OSS_URL = "assets.zilliz.com.cn/benchmark/"
    AWS_S3_URL = "assets.zilliz.com/benchmark/"

    LOG_LEVEL = env.str("LOG_LEVEL", "INFO")
    LOG_FILE = env.str("LOG_FILE", "logs/vectordb_bench.log")

    DEFAULT_DATASET_URL = env.str("DEFAULT_DATASET_URL", AWS_S3_URL)
    DATASET_SOURCE = env.str("DATASET_SOURCE", "S3")  # Options "S3" or "AliyunOSS"
    _project_root = pathlib.Path(__file__).resolve().parent.parent
    DATASET_LOCAL_DIR = env.path("DATASET_LOCAL_DIR", "/tmp/vectordb_bench/dataset")
    NUM_PER_BATCH = env.int("NUM_PER_BATCH", 100)
    # Rows to accumulate before one insert (for load phase). Larger = fewer round-trips, better for bulk DBs (e.g. ClickHouse).
    INSERT_BATCH_SIZE = env.int("INSERT_BATCH_SIZE", 100_000)
    TIME_PER_BATCH = 1  # 1s. for streaming insertion.
    MAX_INSERT_RETRY = 5
    MAX_SEARCH_RETRY = 5

    LOAD_MAX_TRY_COUNT = 10

    DROP_OLD = env.bool("DROP_OLD", True)
    USE_SHUFFLED_DATA = env.bool("USE_SHUFFLED_DATA", True)

    NUM_CONCURRENCY = env.list("NUM_CONCURRENCY", [1, 5, 10, 20, 30, 40, 60, 80], subcast=int)

    CONCURRENCY_DURATION = 30

    CONCURRENCY_TIMEOUT = 3600

    # Skip psutil in kill_proc_tree. Default True to avoid PermissionError on macOS/restricted VMs.
    # Set VDB_SKIP_PSUTIL=0 to enable process-tree kill on Stop.
    VDB_SKIP_PSUTIL = env.bool("VDB_SKIP_PSUTIL", True)

    _default_results_dir = pathlib.Path(__file__).parent.joinpath("results")
    RESULTS_LOCAL_DIR = env.path(
        "RESULTS_LOCAL_DIR",
        _default_results_dir,
    )
    RESULTS_DEFAULT_DIR = _default_results_dir  # fallback when configured dir has no results
    CONFIG_LOCAL_DIR = env.path(
        "CONFIG_LOCAL_DIR",
        pathlib.Path(__file__).parent.joinpath("config-files"),
    )

    # ClickHouse data on NVMe/large disk: set to host path (e.g. /mnt/disks/vectordb_bench/clickhouse_data)
    CLICKHOUSE_DATA_DIR = env.str("CLICKHOUSE_DATA_DIR", "")
    # Milvus (Docker standalone) data on NVMe: mounts to /var/lib/milvus in container
    MILVUS_DATA_DIR = env.str("MILVUS_DATA_DIR", "")
    # Qdrant (Docker) storage on NVMe: mounts to /qdrant/storage in container
    QDRANT_DATA_DIR = env.str("QDRANT_DATA_DIR", "")

    K_DEFAULT = 100  # default return top k nearest neighbors during search
    CUSTOM_CONFIG_DIR = pathlib.Path(__file__).parent.joinpath("custom/custom_case.json")

    # Filter test distributions: comma-separated (e.g. "0.001,0.5" or "0.01,0.1,0.99"). Empty = use all.
    LABEL_FILTER_PERCENTAGES = env.str("LABEL_FILTER_PERCENTAGES", "")
    INT_FILTER_RATES = env.str("INT_FILTER_RATES", "")

    CAPACITY_TIMEOUT_IN_SECONDS = 24 * 3600  # 24h
    LOAD_TIMEOUT_DEFAULT = 24 * 3600  # 24h
    LOAD_TIMEOUT_768D_100K = 24 * 3600  # 24h
    LOAD_TIMEOUT_768D_1M = 24 * 3600  # 24h
    LOAD_TIMEOUT_768D_10M = 240 * 3600  # 10d
    LOAD_TIMEOUT_768D_100M = 2400 * 3600  # 100d

    LOAD_TIMEOUT_1536D_500K = 24 * 3600  # 24h
    LOAD_TIMEOUT_1536D_5M = 240 * 3600  # 10d

    LOAD_TIMEOUT_1024D_1M = 24 * 3600  # 24h
    LOAD_TIMEOUT_1024D_10M = 240 * 3600  # 10d

    OPTIMIZE_TIMEOUT_DEFAULT = 24 * 3600  # 24h
    OPTIMIZE_TIMEOUT_768D_100K = 24 * 3600  # 24h
    OPTIMIZE_TIMEOUT_768D_1M = 24 * 3600  # 24h
    OPTIMIZE_TIMEOUT_768D_10M = 240 * 3600  # 10d
    OPTIMIZE_TIMEOUT_768D_100M = 2400 * 3600  # 100d

    OPTIMIZE_TIMEOUT_1536D_500K = 24 * 3600  # 24h
    OPTIMIZE_TIMEOUT_1536D_5M = 240 * 3600  # 10d

    OPTIMIZE_TIMEOUT_1024D_1M = 24 * 3600  # 24h
    OPTIMIZE_TIMEOUT_1024D_10M = 240 * 3600  # 10d

    def display(self) -> str:
        return [
            i
            for i in inspect.getmembers(self)
            if not inspect.ismethod(i[1]) and not i[0].startswith("_") and "TIMEOUT" not in i[0]
        ]


log_util.init(config.LOG_LEVEL, pathlib.Path(config.LOG_FILE))
