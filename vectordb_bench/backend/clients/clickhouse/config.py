from abc import abstractmethod
from typing import TypedDict

from pydantic import BaseModel, SecretStr

from ..api import DBCaseConfig, DBConfig, IndexType, MetricType


class ClickhouseConfigDict(TypedDict):
    user: str
    password: str
    host: str
    port: int
    database: str
    secure: bool
    enable_flamegraph: bool
    flamegraph_real_time_period_ns: int
    flamegraph_cpu_time_period_ns: int
    vector_similarity_index_cache_size: int
    mark_cache_size: int
    local_filesystem_read_method: str
    clickhouse_server_version: str


class ClickhouseConfig(DBConfig):
    user: str = "clickhouse"
    password: SecretStr
    host: str = "localhost"
    port: int = 9000  # native TCP (clickhouse-driver); HTTP is 8123
    db_name: str = "default"
    secure: bool = False
    enable_flamegraph: bool = False
    flamegraph_real_time_period_ns: int = 10_000_000
    flamegraph_cpu_time_period_ns: int = 10_000_000
    # Server-level setting (bytes). Docs default is 5 GiB.
    vector_similarity_index_cache_size: int = 5 * 1024 * 1024 * 1024
    # Server-level mark cache size (bytes). Default is 5 GiB.
    mark_cache_size: int = 5 * 1024 * 1024 * 1024
    # Session-level filesystem read method.
    local_filesystem_read_method: str = "pread"
    # Docker auto-provision image tag/version (LTS default).
    clickhouse_server_version: str = "25.8"

    def to_dict(self) -> ClickhouseConfigDict:
        pwd_str = self.password.get_secret_value()
        return {
            "host": self.host,
            "port": self.port,
            "database": self.db_name,
            "user": self.user,
            "password": pwd_str,
            "secure": self.secure,
            "enable_flamegraph": self.enable_flamegraph,
            "flamegraph_real_time_period_ns": self.flamegraph_real_time_period_ns,
            "flamegraph_cpu_time_period_ns": self.flamegraph_cpu_time_period_ns,
            "vector_similarity_index_cache_size": self.vector_similarity_index_cache_size,
            "mark_cache_size": self.mark_cache_size,
            "local_filesystem_read_method": self.local_filesystem_read_method,
            "clickhouse_server_version": self.clickhouse_server_version,
        }


class ClickhouseIndexConfig(BaseModel, DBCaseConfig):

    metric_type: MetricType | None = None
    vector_data_type: str | None = "Float32"  # Data type of vectors. Can be Float32 or Float64 or BFloat16
    create_index_before_load: bool = True
    create_index_after_load: bool = False
    query_type: str = "order_by_limit"  # order_by_limit | distance_threshold
    distance_threshold: float = 0.5

    def parse_metric(self) -> str:
        if not self.metric_type:
            return ""
        return self.metric_type.value

    def parse_metric_str(self) -> str:
        if self.metric_type == MetricType.L2:
            return "L2Distance"
        if self.metric_type == MetricType.COSINE:
            return "cosineDistance"
        if self.metric_type == MetricType.IP:
            return "dotProduct"
        if self.metric_type == MetricType.L1:
            return "L1Distance"
        if self.metric_type == MetricType.LINFINITY:
            return "LinfDistance"
        if self.metric_type == MetricType.LP:
            return "LpDistance"
        return "cosineDistance"

    @abstractmethod
    def session_param(self):
        pass


class ClickhouseHNSWConfig(ClickhouseIndexConfig):
    M: int | None  # Default in clickhouse in 32
    efConstruction: int | None  # Default in clickhouse in 128
    ef: int | None = None
    index: IndexType = IndexType.HNSW
    quantization: str | None = "bf16"  # Default is bf16. Possible values are f64, f32, f16, bf16, i8, b1
    granularity: int | None = 10_000_000  # Size of the index granules. By default, in CH it's equal 10.000.000

    def index_param(self) -> dict:
        return {
            "vector_data_type": self.vector_data_type,
            "metric_type": self.parse_metric_str(),
            "index_type": self.index.value,
            "quantization": self.quantization,
            "granularity": self.granularity,
            "params": {"M": self.M, "efConstruction": self.efConstruction},
        }

    def search_param(self) -> dict:
        return {
            "metric_type": self.parse_metric_str(),
            "params": {
                "ef": self.ef,
                "query_type": self.query_type,
                "distance_threshold": self.distance_threshold,
            },
        }

    def session_param(self) -> dict:
        return {
            "allow_experimental_vector_similarity_index": 1,
        }


class ClickhouseQBitConfig(ClickhouseIndexConfig):
    index: IndexType = IndexType.QBIT
    element_type: str = "Float32"  # Element type: Float32, Float64, BFloat16
    precision_bits: int = 16  # Runtime precision control (8, 16, 32, 64)
    create_index_before_load: bool = False  # QBit doesn't use traditional indexes
    create_index_after_load: bool = False

    def index_param(self) -> dict:
        return {
            "vector_data_type": f"QBit({self.element_type}, {{dim}})",  # Placeholder for dimension
            "metric_type": self.parse_metric_str(),
            "index_type": self.index.value,
            "element_type": self.element_type,
            "precision_bits": self.precision_bits,
            "params": {},
        }

    def search_param(self) -> dict:
        return {
            "metric_type": self.parse_metric_str(),
            "params": {
                "precision_bits": self.precision_bits,
                "query_type": self.query_type,
                "distance_threshold": self.distance_threshold,
            },
        }

    def session_param(self) -> dict:
        return {
            "allow_experimental_vector_similarity_index": 1,
        }


class ClickhouseFlatConfig(ClickhouseIndexConfig):
    index: IndexType = IndexType.Flat
    create_index_before_load: bool = False  # Flat search doesn't use indexes
    create_index_after_load: bool = False

    def index_param(self) -> dict:
        return {
            "vector_data_type": self.vector_data_type,
            "metric_type": self.parse_metric_str(),
            "index_type": self.index.value,
            "params": {},
        }

    def search_param(self) -> dict:
        return {
            "metric_type": self.parse_metric_str(),
            "params": {
                "query_type": self.query_type,
                "distance_threshold": self.distance_threshold,
            },
        }

    def session_param(self) -> dict:
        return {
            "allow_experimental_vector_similarity_index": 1,
        }
