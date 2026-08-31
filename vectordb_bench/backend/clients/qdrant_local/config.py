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


# Memory tier ("default" leaves the setting unset so Qdrant picks its own default).
def _parse_memory_tier(value: str):
    from qdrant_client.http.models import Memory

    return {
        "pinned": Memory.PINNED,
        "cached": Memory.CACHED,
        "cold": Memory.COLD,
    }.get(value)


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
    hnsw_inline_storage: bool = False  # store quantized vectors inline with the HNSW graph (requires qdrant-client>=1.19.0)
    hnsw_memory: str = "default"  # default | pinned | cached | cold (memory tier for the HNSW graph)

    # --- Vector storage ---
    on_disk: bool | None = False  # store raw vectors on disk (memmap)
    vector_datatype: str = "float32"  # float32 | uint8 | float16 | turbo4
    vector_memory: str = "default"  # default | pinned | cached | cold (memory tier for raw vectors)

    # --- Optimizers (OptimizersConfigDiff) ---
    deleted_threshold: float = 0.2
    vacuum_min_vector_number: int = 1000
    default_segment_number: int = 0
    max_segment_size: int = 0  # in KB, 0 = unset (unlimited)
    memmap_threshold: int = 0  # in KB, 0 = unset
    indexing_threshold: int = 20000  # in KB
    # If True (default), indexing is disabled (indexing_threshold forced to 0) for the whole
    # load stage and only restored/finished afterwards, for maximum bulk-insert throughput.
    # If False, indexing_threshold is left at its configured value throughout the load, so
    # Qdrant builds/updates the HNSW index for each segment concurrently with ingestion -
    # closer to a real-world streaming-write workload, at the cost of slower insertion.
    disable_indexing_during_load: bool = True
    flush_interval_sec: int = 5
    max_optimization_threads: int = 0  # 0 = unset (auto)
    prevent_unoptimized: bool = False  # refuse to serve search from unoptimized segments

    # --- Write-ahead log (WalConfigDiff) ---
    wal_capacity_mb: int = 32
    wal_segments_ahead: int = 0
    wal_retain_closed: int = 0  # 0 = unset; number of closed WAL segments to retain for faster recovery

    # --- Collection level ---
    shard_number: int = 1
    replication_factor: int = 1
    write_consistency_factor: int = 1
    on_disk_payload: bool = True
    payload_memory: str = "default"  # default | pinned | cached | cold (memory tier for payload storage)

    # --- Quantization ---
    quantization_mode: str = "none"  # none | scalar | product | binary | turbo
    sq_quantile: float = 0.99
    sq_always_ram: bool = False
    pq_compression: str = "x16"  # x4 | x8 | x16 | x32 | x64
    pq_always_ram: bool = False
    bq_always_ram: bool = False
    turbo_bits: str = "bits1_5"  # bits1 | bits1_5 | bits2 | bits4 (TurboQuant compression level)
    turbo_always_ram: bool = False
    quant_memory: str = "default"  # default | pinned | cached | cold (memory tier for the active quantization)
    binary_encoding: str = "one_bit"  # one_bit | two_bits | one_and_half_bits (binary quantization storage encoding)
    binary_query_encoding: str = "default"  # default | binary | scalar4bits | scalar8bits (query-time encoding)

    # --- Search params (SearchParams) ---
    hnsw_ef: int | None = 0  # 0 = use Qdrant default
    exact: bool = False
    indexed_only: bool = False
    quant_rescore: bool = False
    quant_oversampling: float = 1.0
    quant_ignore: bool = False
    search_acorn: bool = False  # ACORN filtered-HNSW search strategy

    # --- Search request behaviour (client.query_points kwargs, not SearchParams) ---
    search_consistency: str = "default"  # default | all | majority | quorum
    search_timeout_sec: int = 0  # 0 = unset (use client/server default)

    # --- Write request behaviour (client.upsert kwargs) ---
    wait: bool = True  # wait for the operation to be applied before returning
    write_ordering: str = "weak"  # weak | medium | strong
    # Vectors sent per client.upsert() call (the actual network/wire batch size). Independent
    # of the benchmark's higher-level "insert_batch_size" (how many vectors are accumulated
    # client-side before handing off to the DB adapter) - insert_embeddings() always re-chunks
    # into requests of this size before sending. Larger values mean far fewer round trips for
    # large datasets (e.g. 1M vectors / 100 = 10,000 requests vs. /1000 = 1,000 requests), which
    # dominates ingestion time far more than insert_batch_size does. Qdrant's own qdrant_cloud
    # client in this repo defaults to 500; 100 here was needlessly conservative. This is treated
    # as an upper bound: it is automatically clamped down per-request based on vector dimension
    # to stay under max_upsert_request_mb (see below), since REST/JSON serializes each float as
    # text (~20 bytes/float, not 4 bytes binary) so high dim x high batch size can otherwise
    # exceed Qdrant's request-size limit (default 32 MiB) with a "JSON payload ... larger than
    # allowed" 400 error.
    upsert_batch_size: int = 500
    # Safety cap (MB) on the estimated JSON size of a single client.upsert() request; used to
    # auto-clamp upsert_batch_size for high-dimensional vectors. Qdrant's own REST default limit
    # is 32 MiB (33554432 bytes, configurable server-side via QDRANT__SERVICE__MAX_REQUEST_SIZE_MB);
    # this defaults a bit under that to leave headroom for server-side overhead.
    max_upsert_request_mb: float = 28.0

    # --- Benchmark update stage (not a Qdrant setting) ---
    enable_update_stage: bool = False
    update_ratio: float = 0.001
    update_batch_size: int = 100

    def parse_write_ordering(self):
        from qdrant_client.http.models import WriteOrdering

        return {
            "weak": WriteOrdering.WEAK,
            "medium": WriteOrdering.MEDIUM,
            "strong": WriteOrdering.STRONG,
        }.get(self.write_ordering, WriteOrdering.WEAK)

    def parse_search_consistency(self):
        from qdrant_client.http.models import ReadConsistencyType

        return {
            "all": ReadConsistencyType.ALL,
            "majority": ReadConsistencyType.MAJORITY,
            "quorum": ReadConsistencyType.QUORUM,
        }.get(self.search_consistency)

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
            "turbo4": Datatype.TURBO4,
        }.get(self.vector_datatype, Datatype.FLOAT32)

    def _quantization_config(self):
        quant_memory = _parse_memory_tier(self.quant_memory)
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
                    memory=quant_memory,
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
                    memory=quant_memory,
                ),
            )
        if self.quantization_mode == "binary":
            from qdrant_client.http.models import (
                BinaryQuantization,
                BinaryQuantizationConfig,
                BinaryQuantizationEncoding,
                BinaryQuantizationQueryEncoding,
            )

            encoding = {
                "one_bit": BinaryQuantizationEncoding.ONE_BIT,
                "two_bits": BinaryQuantizationEncoding.TWO_BITS,
                "one_and_half_bits": BinaryQuantizationEncoding.ONE_AND_HALF_BITS,
            }.get(self.binary_encoding, BinaryQuantizationEncoding.ONE_BIT)
            query_encoding = {
                "default": BinaryQuantizationQueryEncoding.DEFAULT,
                "binary": BinaryQuantizationQueryEncoding.BINARY,
                "scalar4bits": BinaryQuantizationQueryEncoding.SCALAR4BITS,
                "scalar8bits": BinaryQuantizationQueryEncoding.SCALAR8BITS,
            }.get(self.binary_query_encoding, BinaryQuantizationQueryEncoding.DEFAULT)
            return BinaryQuantization(
                binary=BinaryQuantizationConfig(
                    always_ram=self.bq_always_ram,
                    memory=quant_memory,
                    encoding=encoding,
                    query_encoding=query_encoding,
                ),
            )
        if self.quantization_mode == "turbo":
            from qdrant_client.http.models import (
                TurboQuantBitSize,
                TurboQuantization,
                TurboQuantQuantizationConfig,
            )

            bits = {
                "bits1": TurboQuantBitSize.BITS1,
                "bits1_5": TurboQuantBitSize.BITS1_5,
                "bits2": TurboQuantBitSize.BITS2,
                "bits4": TurboQuantBitSize.BITS4,
            }.get(self.turbo_bits, TurboQuantBitSize.BITS1_5)
            return TurboQuantization(
                turbo=TurboQuantQuantizationConfig(
                    bits=bits,
                    always_ram=self.turbo_always_ram,
                    memory=quant_memory,
                ),
            )
        return None

    def index_param(self) -> dict:
        from qdrant_client.http.models import (
            HnswConfigDiff,
            OptimizersConfigDiff,
            PayloadStorageParams,
            WalConfigDiff,
        )

        hnsw_config = HnswConfigDiff(
            m=self.m,
            ef_construct=self.ef_construct,
            full_scan_threshold=self.full_scan_threshold,
            max_indexing_threads=self.max_indexing_threads,
            on_disk=self.hnsw_on_disk,
            payload_m=_none_if_zero(self.payload_m),
            inline_storage=self.hnsw_inline_storage,
            memory=_parse_memory_tier(self.hnsw_memory),
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
            prevent_unoptimized=self.prevent_unoptimized,
        )

        wal_config = WalConfigDiff(
            wal_capacity_mb=self.wal_capacity_mb,
            wal_segments_ahead=self.wal_segments_ahead,
            wal_retain_closed=_none_if_zero(self.wal_retain_closed),
        )

        payload_memory = _parse_memory_tier(self.payload_memory)

        return {
            "distance": self.parse_metric(),
            "m": self.m,
            "ef_construct": self.ef_construct,
            "on_disk": self.on_disk,
            "datatype": self._parse_datatype(),
            "vector_memory": _parse_memory_tier(self.vector_memory),
            "hnsw_config": hnsw_config,
            "optimizers_config": optimizers_config,
            "wal_config": wal_config,
            "quantization_config": self._quantization_config(),
            "shard_number": self.shard_number,
            "replication_factor": self.replication_factor,
            "write_consistency_factor": self.write_consistency_factor,
            "on_disk_payload": self.on_disk_payload,
            "payload_config": PayloadStorageParams(memory=payload_memory) if payload_memory is not None else None,
            "indexing_threshold": self.indexing_threshold,
        }

    def search_param(self) -> dict:
        search_params = {
            "exact": self.exact,
            "indexed_only": self.indexed_only,
        }

        if self.search_acorn:
            from qdrant_client.http.models import AcornSearchParams

            search_params["acorn"] = AcornSearchParams(enable=True)

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
