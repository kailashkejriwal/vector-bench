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
    # Vector storage
    on_disk: Annotated[
        bool,
        click.option("--on-disk", type=bool, default=False, help="Store the raw vectors on disk"),
    ]
    vector_datatype: Annotated[
        str,
        click.option(
            "--vector-datatype",
            type=click.Choice(["float32", "uint8", "float16"]),
            default="float32",
            help="Stored vector element type",
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
    flush_interval_sec: Annotated[
        int,
        click.option("--flush-interval-sec", type=int, default=5, help="Flush interval in seconds"),
    ]
    max_optimization_threads: Annotated[
        int,
        click.option("--max-optimization-threads", type=int, default=0, help="Max optimization threads (0=auto)"),
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
    # Quantization
    quantization_mode: Annotated[
        str,
        click.option(
            "--quantization-mode",
            type=click.Choice(["none", "scalar", "product", "binary"]),
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
            on_disk=parameters["on_disk"],
            vector_datatype=parameters["vector_datatype"],
            deleted_threshold=parameters["deleted_threshold"],
            vacuum_min_vector_number=parameters["vacuum_min_vector_number"],
            default_segment_number=parameters["default_segment_number"],
            max_segment_size=parameters["max_segment_size"],
            memmap_threshold=parameters["memmap_threshold"],
            indexing_threshold=parameters["indexing_threshold"],
            flush_interval_sec=parameters["flush_interval_sec"],
            max_optimization_threads=parameters["max_optimization_threads"],
            wal_capacity_mb=parameters["wal_capacity_mb"],
            wal_segments_ahead=parameters["wal_segments_ahead"],
            shard_number=parameters["shard_number"],
            replication_factor=parameters["replication_factor"],
            write_consistency_factor=parameters["write_consistency_factor"],
            on_disk_payload=parameters["on_disk_payload"],
            quantization_mode=parameters["quantization_mode"],
            sq_quantile=parameters["sq_quantile"],
            sq_always_ram=parameters["sq_always_ram"],
            pq_compression=parameters["pq_compression"],
            pq_always_ram=parameters["pq_always_ram"],
            bq_always_ram=parameters["bq_always_ram"],
            hnsw_ef=parameters["hnsw_ef"],
            exact=parameters["exact"],
            indexed_only=parameters["indexed_only"],
            quant_rescore=parameters["quant_rescore"],
            quant_oversampling=parameters["quant_oversampling"],
            quant_ignore=parameters["quant_ignore"],
        ),
        **parameters,
    )
