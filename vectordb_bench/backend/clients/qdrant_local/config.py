from pydantic import BaseModel, SecretStr

from vectordb_bench import config

from ..api import DBCaseConfig, DBConfig, MetricType


class QdrantLocalConfig(DBConfig):
    url: SecretStr
    # If None, QDRANT_CLIENT_TIMEOUT_SEC from env is used in to_dict()
    timeout: int | None = None

    def to_dict(self) -> dict:
        timeout = self.timeout if self.timeout is not None else int(config.QDRANT_CLIENT_TIMEOUT_SEC)
        return {
            "url": self.url.get_secret_value(),
            **({"timeout": timeout} if timeout and timeout > 0 else {}),
        }


# Sentinel: for optional integer params where 0 means "use Qdrant default (unset)".
def _none_if_zero(value: int | None) -> int | None:
    if value is None or value == 0:
        return None
    return value


class QdrantLocalIndexConfig(BaseModel, DBCaseConfig):
    """Full set of tunable options for a self-hosted Qdrant collection.

    Every field defaults to the value Qdrant itself uses when the parameter is
    left unset, so a run with all defaults reproduces vanilla Qdrant behaviour.
    """

    metric_type: MetricType | None = None

    # --- HNSW index (HnswConfigDiff) ---
    m: int = 16
    ef_construct: int = 100
    full_scan_threshold: int = 10000  # in KB
    max_indexing_threads: int = 0  # 0 = auto (all cores)
    hnsw_on_disk: bool = False  # store the HNSW graph on disk
    payload_m: int = 0  # 0 = unset; per-payload HNSW links for tenant/payload indexing

    # --- Vector storage ---
    on_disk: bool | None = False  # store raw vectors on disk (memmap)
    vector_datatype: str = "float32"  # float32 | uint8 | float16

    # --- Optimizers (OptimizersConfigDiff) ---
    deleted_threshold: float = 0.2
    vacuum_min_vector_number: int = 1000
    default_segment_number: int = 0
    max_segment_size: int = 0  # in KB, 0 = unset (unlimited)
    memmap_threshold: int = 0  # in KB, 0 = unset
    indexing_threshold: int = 20000  # in KB
    flush_interval_sec: int = 5
    max_optimization_threads: int = 0  # 0 = unset (auto)

    # --- Write-ahead log (WalConfigDiff) ---
    wal_capacity_mb: int = 32
    wal_segments_ahead: int = 0

    # --- Collection level ---
    shard_number: int = 1
    replication_factor: int = 1
    write_consistency_factor: int = 1
    on_disk_payload: bool = True

    # --- Quantization ---
    quantization_mode: str = "none"  # none | scalar | product | binary
    sq_quantile: float = 0.99
    sq_always_ram: bool = False
    pq_compression: str = "x16"  # x4 | x8 | x16 | x32 | x64
    pq_always_ram: bool = False
    bq_always_ram: bool = False

    # --- Search params (SearchParams) ---
    hnsw_ef: int | None = 0  # 0 = use Qdrant default
    exact: bool = False
    indexed_only: bool = False
    quant_rescore: bool = False
    quant_oversampling: float = 1.0
    quant_ignore: bool = False

    # --- Benchmark update stage (not a Qdrant setting) ---
    enable_update_stage: bool = False
    update_ratio: float = 0.001
    update_batch_size: int = 100

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "Euclid"

        if self.metric_type == MetricType.IP:
            return "Dot"

        return "Cosine"

    def _parse_datatype(self):
        from qdrant_client.http.models import Datatype

        return {
            "float32": Datatype.FLOAT32,
            "uint8": Datatype.UINT8,
            "float16": Datatype.FLOAT16,
        }.get(self.vector_datatype, Datatype.FLOAT32)

    def _quantization_config(self):
        if self.quantization_mode == "scalar":
            from qdrant_client.http.models import (
                ScalarQuantization,
                ScalarQuantizationConfig,
                ScalarType,
            )

            return ScalarQuantization(
                scalar=ScalarQuantizationConfig(
                    type=ScalarType.INT8,
                    quantile=self.sq_quantile,
                    always_ram=self.sq_always_ram,
                ),
            )
        if self.quantization_mode == "product":
            from qdrant_client.http.models import (
                CompressionRatio,
                ProductQuantization,
                ProductQuantizationConfig,
            )

            compression = {
                "x4": CompressionRatio.X4,
                "x8": CompressionRatio.X8,
                "x16": CompressionRatio.X16,
                "x32": CompressionRatio.X32,
                "x64": CompressionRatio.X64,
            }.get(self.pq_compression, CompressionRatio.X16)
            return ProductQuantization(
                product=ProductQuantizationConfig(
                    compression=compression,
                    always_ram=self.pq_always_ram,
                ),
            )
        if self.quantization_mode == "binary":
            from qdrant_client.http.models import (
                BinaryQuantization,
                BinaryQuantizationConfig,
            )

            return BinaryQuantization(
                binary=BinaryQuantizationConfig(always_ram=self.bq_always_ram),
            )
        return None

    def index_param(self) -> dict:
        from qdrant_client.http.models import (
            HnswConfigDiff,
            OptimizersConfigDiff,
            WalConfigDiff,
        )

        hnsw_config = HnswConfigDiff(
            m=self.m,
            ef_construct=self.ef_construct,
            full_scan_threshold=self.full_scan_threshold,
            max_indexing_threads=self.max_indexing_threads,
            on_disk=self.hnsw_on_disk,
            payload_m=_none_if_zero(self.payload_m),
        )

        optimizers_config = OptimizersConfigDiff(
            deleted_threshold=self.deleted_threshold,
            vacuum_min_vector_number=self.vacuum_min_vector_number,
            default_segment_number=self.default_segment_number,
            max_segment_size=_none_if_zero(self.max_segment_size),
            memmap_threshold=_none_if_zero(self.memmap_threshold),
            indexing_threshold=self.indexing_threshold,
            flush_interval_sec=self.flush_interval_sec,
            max_optimization_threads=_none_if_zero(self.max_optimization_threads),
        )

        wal_config = WalConfigDiff(
            wal_capacity_mb=self.wal_capacity_mb,
            wal_segments_ahead=self.wal_segments_ahead,
        )

        return {
            "distance": self.parse_metric(),
            "m": self.m,
            "ef_construct": self.ef_construct,
            "on_disk": self.on_disk,
            "datatype": self._parse_datatype(),
            "hnsw_config": hnsw_config,
            "optimizers_config": optimizers_config,
            "wal_config": wal_config,
            "quantization_config": self._quantization_config(),
            "shard_number": self.shard_number,
            "replication_factor": self.replication_factor,
            "write_consistency_factor": self.write_consistency_factor,
            "on_disk_payload": self.on_disk_payload,
            "indexing_threshold": self.indexing_threshold,
        }

    def search_param(self) -> dict:
        search_params = {
            "exact": self.exact,
            "indexed_only": self.indexed_only,
        }

        if self.hnsw_ef and self.hnsw_ef != 0:
            search_params["hnsw_ef"] = self.hnsw_ef

        if self.quantization_mode != "none":
            from qdrant_client.http.models import QuantizationSearchParams

            search_params["quantization"] = QuantizationSearchParams(
                ignore=self.quant_ignore,
                rescore=self.quant_rescore,
                oversampling=self.quant_oversampling,
            )

        return search_params
