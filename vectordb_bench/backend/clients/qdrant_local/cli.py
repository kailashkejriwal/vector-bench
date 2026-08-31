from typing import Annotated, Unpack

import click
from pydantic import SecretStr

from vectordb_bench.backend.clients import DB
from vectordb_bench.cli.cli import (
    CommonTypedDict,
    cli,
    click_parameter_decorators_from_typed_dict,
    run,
)

DBTYPE = DB.QdrantLocal


class QdrantLocalTypedDict(CommonTypedDict):
    url: Annotated[
        str,
        click.option("--url", type=str, help="Qdrant url", required=True),
    ]
    # HNSW index
    m: Annotated[
        int,
        click.option("--m", type=int, default=16, help="HNSW index parameter m (graph degree)"),
    ]
    ef_construct: Annotated[
        int,
        click.option("--ef-construct", type=int, default=100, help="HNSW ef_construct (build-time)"),
    ]
    full_scan_threshold: Annotated[
        int,
        click.option("--full-scan-threshold", type=int, default=10000, help="Full-scan threshold (KB)"),
    ]
    max_indexing_threads: Annotated[
        int,
        click.option("--max-indexing-threads", type=int, default=0, help="Max HNSW indexing threads (0=auto)"),
    ]
    hnsw_on_disk: Annotated[
        bool,
        click.option("--hnsw-on-disk", type=bool, default=False, help="Store the HNSW graph on disk"),
    ]
    payload_m: Annotated[
        int,
        click.option("--payload-m", type=int, default=0, help="Payload HNSW links per node (0=disabled)"),
    ]
    hnsw_inline_storage: Annotated[
        bool,
        click.option(
            "--hnsw-inline-storage",
            type=bool,
            default=False,
            help="Store quantized vectors inline with the HNSW graph (requires qdrant-client>=1.19.0)",
        ),
    ]
    hnsw_memory: Annotated[
        str,
        click.option(
            "--hnsw-memory",
            type=click.Choice(["default", "pinned", "cached", "cold"]),
            default="default",
            help="Memory tier for the HNSW graph",
        ),
    ]
    # Vector storage
    on_disk: Annotated[
        bool,
        click.option("--on-disk", type=bool, default=False, help="Store the raw vectors on disk"),
    ]
    vector_datatype: Annotated[
        str,
        click.option(
            "--vector-datatype",
            type=click.Choice(["float32", "uint8", "float16", "turbo4"]),
            default="float32",
            help="Stored vector element type",
        ),
    ]
    vector_memory: Annotated[
        str,
        click.option(
            "--vector-memory",
            type=click.Choice(["default", "pinned", "cached", "cold"]),
            default="default",
            help="Memory tier for raw vectors",
        ),
    ]
    # Optimizers
    deleted_threshold: Annotated[
        float,
        click.option("--deleted-threshold", type=float, default=0.2, help="Deleted vector fraction before optimize"),
    ]
    vacuum_min_vector_number: Annotated[
        int,
        click.option("--vacuum-min-vector-number", type=int, default=1000, help="Min vectors before vacuum"),
    ]
    default_segment_number: Annotated[
        int,
        click.option("--default-segment-number", type=int, default=0, help="Target segments per shard (0=auto)"),
    ]
    max_segment_size: Annotated[
        int,
        click.option("--max-segment-size", type=int, default=0, help="Max segment size in KB (0=unset)"),
    ]
    memmap_threshold: Annotated[
        int,
        click.option("--memmap-threshold", type=int, default=0, help="Memmap threshold in KB (0=unset)"),
    ]
    indexing_threshold: Annotated[
        int,
        click.option("--indexing-threshold", type=int, default=20000, help="Indexing threshold in KB"),
    ]
    disable_indexing_during_load: Annotated[
        bool,
        click.option(
            "--disable-indexing-during-load/--no-disable-indexing-during-load",
            type=bool,
            default=True,
            help="Disable indexing (force indexing_threshold=0) for the whole load stage, restoring it "
            "afterwards. Pass --no-disable-indexing-during-load to index concurrently with ingestion instead.",
        ),
    ]
    flush_interval_sec: Annotated[
        int,
        click.option("--flush-interval-sec", type=int, default=5, help="Flush interval in seconds"),
    ]
    max_optimization_threads: Annotated[
        int,
        click.option("--max-optimization-threads", type=int, default=0, help="Max optimization threads (0=auto)"),
    ]
    prevent_unoptimized: Annotated[
        bool,
        click.option(
            "--prevent-unoptimized",
            type=bool,
            default=False,
            help="Refuse to serve search from unoptimized segments",
        ),
    ]
    # WAL
    wal_capacity_mb: Annotated[
        int,
        click.option("--wal-capacity-mb", type=int, default=32, help="WAL segment size in MB"),
    ]
    wal_segments_ahead: Annotated[
        int,
        click.option("--wal-segments-ahead", type=int, default=0, help="WAL segments created ahead"),
    ]
    wal_retain_closed: Annotated[
        int,
        click.option(
            "--wal-retain-closed",
            type=int,
            default=0,
            help="Number of closed WAL segments to retain for faster recovery (0=unset)",
        ),
    ]
    # Collection level
    shard_number: Annotated[
        int,
        click.option("--shard-number", type=int, default=1, help="Number of shards"),
    ]
    replication_factor: Annotated[
        int,
        click.option("--replication-factor", type=int, default=1, help="Replicas per shard"),
    ]
    write_consistency_factor: Annotated[
        int,
        click.option("--write-consistency-factor", type=int, default=1, help="Replicas that must confirm a write"),
    ]
    on_disk_payload: Annotated[
        bool,
        click.option("--on-disk-payload", type=bool, default=True, help="Store payload on disk"),
    ]
    payload_memory: Annotated[
        str,
        click.option(
            "--payload-memory",
            type=click.Choice(["default", "pinned", "cached", "cold"]),
            default="default",
            help="Memory tier for payload storage",
        ),
    ]
    # Quantization
    quantization_mode: Annotated[
        str,
        click.option(
            "--quantization-mode",
            type=click.Choice(["none", "scalar", "product", "binary", "turbo"]),
            default="none",
            help="Vector quantization mode",
        ),
    ]
    sq_quantile: Annotated[
        float,
        click.option("--sq-quantile", type=float, default=0.99, help="Scalar quantization quantile"),
    ]
    sq_always_ram: Annotated[
        bool,
        click.option("--sq-always-ram", type=bool, default=False, help="Keep scalar-quantized vectors in RAM"),
    ]
    pq_compression: Annotated[
        str,
        click.option(
            "--pq-compression",
            type=click.Choice(["x4", "x8", "x16", "x32", "x64"]),
            default="x16",
            help="Product quantization compression ratio",
        ),
    ]
    pq_always_ram: Annotated[
        bool,
        click.option("--pq-always-ram", type=bool, default=False, help="Keep product-quantized vectors in RAM"),
    ]
    bq_always_ram: Annotated[
        bool,
        click.option("--bq-always-ram", type=bool, default=False, help="Keep binary-quantized vectors in RAM"),
    ]
    turbo_bits: Annotated[
        str,
        click.option(
            "--turbo-bits",
            type=click.Choice(["bits1", "bits1_5", "bits2", "bits4"]),
            default="bits1_5",
            help="TurboQuant compression level",
        ),
    ]
    turbo_always_ram: Annotated[
        bool,
        click.option("--turbo-always-ram", type=bool, default=False, help="Keep turbo-quantized vectors in RAM"),
    ]
    quant_memory: Annotated[
        str,
        click.option(
            "--quant-memory",
            type=click.Choice(["default", "pinned", "cached", "cold"]),
            default="default",
            help="Memory tier for the active quantization",
        ),
    ]
    binary_encoding: Annotated[
        str,
        click.option(
            "--binary-encoding",
            type=click.Choice(["one_bit", "two_bits", "one_and_half_bits"]),
            default="one_bit",
            help="Binary quantization storage encoding",
        ),
    ]
    binary_query_encoding: Annotated[
        str,
        click.option(
            "--binary-query-encoding",
            type=click.Choice(["default", "binary", "scalar4bits", "scalar8bits"]),
            default="default",
            help="Binary quantization query-time encoding",
        ),
    ]
    # Search params
    hnsw_ef: Annotated[
        int,
        click.option("--hnsw-ef", type=int, default=0, help="Search-time HNSW ef (0=Qdrant default)"),
    ]
    exact: Annotated[
        bool,
        click.option("--exact", type=bool, default=False, help="Exact (brute-force) search"),
    ]
    indexed_only: Annotated[
        bool,
        click.option("--indexed-only", type=bool, default=False, help="Search indexed segments only"),
    ]
    quant_rescore: Annotated[
        bool,
        click.option("--quant-rescore", type=bool, default=False, help="Rescore quantized results with originals"),
    ]
    quant_oversampling: Annotated[
        float,
        click.option("--quant-oversampling", type=float, default=1.0, help="Quantization oversampling factor"),
    ]
    quant_ignore: Annotated[
        bool,
        click.option("--quant-ignore", type=bool, default=False, help="Ignore quantized vectors at search time"),
    ]
    search_acorn: Annotated[
        bool,
        click.option(
            "--search-acorn",
            type=bool,
            default=False,
            help="Use ACORN filtered-HNSW search strategy",
        ),
    ]
    # Search request behaviour (client.query_points kwargs)
    search_consistency: Annotated[
        str,
        click.option(
            "--search-consistency",
            type=click.Choice(["default", "all", "majority", "quorum"]),
            default="default",
            help="Read consistency for search requests",
        ),
    ]
    search_timeout_sec: Annotated[
        int,
        click.option(
            "--search-timeout-sec",
            type=int,
            default=0,
            help="Per-request search timeout in seconds (0=unset)",
        ),
    ]
    # Write request behaviour (client.upsert kwargs)
    wait: Annotated[
        bool,
        click.option(
            "--wait/--no-wait",
            type=bool,
            default=True,
            help="Wait for write operations to be applied before returning",
        ),
    ]
    write_ordering: Annotated[
        str,
        click.option(
            "--write-ordering",
            type=click.Choice(["weak", "medium", "strong"]),
            default="weak",
            help="Write ordering guarantee for upserts",
        ),
    ]
    upsert_batch_size: Annotated[
        int,
        click.option(
            "--upsert-batch-size",
            type=int,
            default=500,
            help="Vectors per client.upsert() call (real network batch size, independent of "
            "the runner's --insert-batch-size). Higher = far fewer round trips for large loads. "
            "Auto-reduced for high-dim vectors to respect --max-upsert-request-mb.",
        ),
    ]
    max_upsert_request_mb: Annotated[
        float,
        click.option(
            "--max-upsert-request-mb",
            type=float,
            default=28.0,
            help="Safety cap (MB) on the estimated JSON size of one upsert() request; "
            "upsert-batch-size is auto-reduced for high-dim vectors to stay under this "
            "(Qdrant's REST default request-size limit is 32 MB).",
        ),
    ]


@cli.command()
@click_parameter_decorators_from_typed_dict(QdrantLocalTypedDict)
def QdrantLocal(**parameters: Unpack[QdrantLocalTypedDict]):
    from .config import QdrantLocalConfig, QdrantLocalIndexConfig

    run(
        db=DBTYPE,
        db_config=QdrantLocalConfig(url=SecretStr(parameters["url"])),
        db_case_config=QdrantLocalIndexConfig(
            m=parameters["m"],
            ef_construct=parameters["ef_construct"],
            full_scan_threshold=parameters["full_scan_threshold"],
            max_indexing_threads=parameters["max_indexing_threads"],
            hnsw_on_disk=parameters["hnsw_on_disk"],
            payload_m=parameters["payload_m"],
            hnsw_inline_storage=parameters["hnsw_inline_storage"],
            hnsw_memory=parameters["hnsw_memory"],
            on_disk=parameters["on_disk"],
            vector_datatype=parameters["vector_datatype"],
            vector_memory=parameters["vector_memory"],
            deleted_threshold=parameters["deleted_threshold"],
            vacuum_min_vector_number=parameters["vacuum_min_vector_number"],
            default_segment_number=parameters["default_segment_number"],
            max_segment_size=parameters["max_segment_size"],
            memmap_threshold=parameters["memmap_threshold"],
            indexing_threshold=parameters["indexing_threshold"],
            disable_indexing_during_load=parameters["disable_indexing_during_load"],
            flush_interval_sec=parameters["flush_interval_sec"],
            max_optimization_threads=parameters["max_optimization_threads"],
            prevent_unoptimized=parameters["prevent_unoptimized"],
            wal_capacity_mb=parameters["wal_capacity_mb"],
            wal_segments_ahead=parameters["wal_segments_ahead"],
            wal_retain_closed=parameters["wal_retain_closed"],
            shard_number=parameters["shard_number"],
            replication_factor=parameters["replication_factor"],
            write_consistency_factor=parameters["write_consistency_factor"],
            on_disk_payload=parameters["on_disk_payload"],
            payload_memory=parameters["payload_memory"],
            quantization_mode=parameters["quantization_mode"],
            sq_quantile=parameters["sq_quantile"],
            sq_always_ram=parameters["sq_always_ram"],
            pq_compression=parameters["pq_compression"],
            pq_always_ram=parameters["pq_always_ram"],
            bq_always_ram=parameters["bq_always_ram"],
            turbo_bits=parameters["turbo_bits"],
            turbo_always_ram=parameters["turbo_always_ram"],
            quant_memory=parameters["quant_memory"],
            binary_encoding=parameters["binary_encoding"],
            binary_query_encoding=parameters["binary_query_encoding"],
            hnsw_ef=parameters["hnsw_ef"],
            exact=parameters["exact"],
            indexed_only=parameters["indexed_only"],
            quant_rescore=parameters["quant_rescore"],
            quant_oversampling=parameters["quant_oversampling"],
            quant_ignore=parameters["quant_ignore"],
            search_acorn=parameters["search_acorn"],
            search_consistency=parameters["search_consistency"],
            search_timeout_sec=parameters["search_timeout_sec"],
            wait=parameters["wait"],
            write_ordering=parameters["write_ordering"],
            upsert_batch_size=parameters["upsert_batch_size"],
            max_upsert_request_mb=parameters["max_upsert_request_mb"],
        ),
        **parameters,
    )
