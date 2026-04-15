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
    # After docker stop, full `docker logs` output is written here before rm (auto-provision teardown).
    SAVE_PROVISIONED_CONTAINER_LOGS = env.bool("SAVE_PROVISIONED_CONTAINER_LOGS", False)
    PROVISIONED_CONTAINER_LOGS_DIR = env.path(
        "PROVISIONED_CONTAINER_LOGS_DIR",
        RESULTS_LOCAL_DIR / "container_logs",
    )
    CONFIG_LOCAL_DIR = env.path(
        "CONFIG_LOCAL_DIR",
        pathlib.Path(__file__).parent.joinpath("config-files"),
    )

    # ClickHouse data on NVMe/large disk: set to host path (e.g. /mnt/disks/vectordb_bench/clickhouse_data)
    CLICKHOUSE_DATA_DIR = env.str("CLICKHOUSE_DATA_DIR", "")
    # Milvus (Docker standalone) data on NVMe: mounts to /var/lib/milvus in container
    MILVUS_DATA_DIR = env.str("MILVUS_DATA_DIR", "")
    # After TCP is open, querynode may not be registered yet (LoadCollection needs currentNodeNum>=1). 0 disables.
    MILVUS_EXTRA_READINESS_WAIT_SEC = env.int("MILVUS_EXTRA_READINESS_WAIT_SEC", 10)
    # Retry col.load() / load(refresh=True) when Milvus reports resource group / query node not ready.
    MILVUS_LOAD_RETRY_MAX_ATTEMPTS = env.int("MILVUS_LOAD_RETRY_MAX_ATTEMPTS", 40)
    MILVUS_LOAD_RETRY_INTERVAL_SEC = env.int("MILVUS_LOAD_RETRY_INTERVAL_SEC", 3)
    # Qdrant (Docker) storage on NVMe: mounts to /qdrant/storage in container
    QDRANT_DATA_DIR = env.str("QDRANT_DATA_DIR", "")
    # REST API timeout for qdrant-client (seconds). Library default is 5s; create_collection can exceed that on slow disks.
    QDRANT_CLIENT_TIMEOUT_SEC = env.int("QDRANT_CLIENT_TIMEOUT_SEC", 120)
    # PgVector (Docker): host path mounted to Postgres PGDATA for persistence / large disk (e.g. NVMe).
    PGVECTOR_DATA_DIR = env.str("PGVECTOR_DATA_DIR", "")
    # Image for auto-provisioned PgVector. Docker Hub often has no :latest; pin a pgNN tag.
    PGVECTOR_DOCKER_IMAGE = env.str("PGVECTOR_DOCKER_IMAGE", "pgvector/pgvector:pg16")
    # Docker /dev/shm for PgVector. Default ~64m breaks parallel index builds (DSM). Raise if you set a large maintenance_work_mem.
    # Set empty to omit --shm-size (not recommended).
    PGVECTOR_DOCKER_SHM_SIZE = env.str("PGVECTOR_DOCKER_SHM_SIZE", "8g")
    # Seconds of idle time on the host when switching from one DB workload to another (0 = no delay).
    # Used after auto teardown (once removal is confirmed) before the next auto DB, before the first manual
    # runner if its DB differs from the last auto DB, and between manual runners when the DB changes.
    POST_PROVISION_TEARDOWN_DELAY_SEC = env.int("POST_PROVISION_TEARDOWN_DELAY_SEC", 0)
    # Run `sync` once before each quiet window (may help disk-usage panels settle; does not drop page cache).
    POST_PROVISION_SYNC_BEFORE_COOLDOWN = env.bool("POST_PROVISION_SYNC_BEFORE_COOLDOWN", False)
    # Max seconds to poll `docker inspect` until the container is gone after rm (clean handoff for host metrics).
    DOCKER_CONTAINER_REMOVAL_WAIT_TIMEOUT_SEC = env.int("DOCKER_CONTAINER_REMOVAL_WAIT_TIMEOUT_SEC", 120)
    # Omit `docker run --memory`; container can grow until the host runs out of RAM (useful for heavy Milvus on VMs).
    # Still obeys `--cpus` from resource_profile unless you override cpu in instance_config.
    PROVISION_DOCKER_MEMORY_UNLIMITED = env.bool("PROVISION_DOCKER_MEMORY_UNLIMITED", False)
    # Before each auto-provision start and after teardown (unless leave_container_running), delete
    # CLICKHOUSE/MILVUS/QDRANT/PGVECTOR host data dirs for a clean baseline. Never deletes DATASET_LOCAL_DIR or RESULTS_LOCAL_DIR.
    PROVISION_CLEAR_HOST_DATA_AFTER_RUN = env.bool("PROVISION_CLEAR_HOST_DATA_AFTER_RUN", True)
    # If rmtree hits permission denied (Docker leaves root-owned files on bind mounts), retry after
    # `sudo -n chown -R <uid>:<gid>` (needs passwordless sudo for chown on that path, or run bench as root).
    PROVISION_CLEAR_HOST_DATA_SUDO_CHOWN = env.bool("PROVISION_CLEAR_HOST_DATA_SUDO_CHOWN", True)
    # Cap ClickHouse max_threads / merge_tree_max_threads on every native connection and vector searches.
    # Experimental vector similarity can hit "Too many threads" if this is high and NUM_CONCURRENCY is large.
    # Default 2; set to 1 if errors persist; set 0 to omit (server defaults only — may fail under concurrency).
    CLICKHOUSE_QUERY_MAX_THREADS = env.int("CLICKHOUSE_QUERY_MAX_THREADS", 2)

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
