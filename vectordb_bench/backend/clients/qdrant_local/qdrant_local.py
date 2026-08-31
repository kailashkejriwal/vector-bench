"""Wrapper around the Qdrant over VectorDB"""

import logging
import time
from collections.abc import Iterable
from contextlib import contextmanager

from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import ResponseHandlingException
from qdrant_client.http.models import (
    Batch,
    CollectionStatus,
    FieldCondition,
    Filter as QdrantFilter,
    KeywordIndexParams,
    OptimizersConfigDiff,
    PayloadSchemaType,
    Range,
    SearchParams,
    VectorParams,
)

from vectordb_bench.backend.filter import Filter, FilterOp

from ..api import VectorDB
from .config import QdrantLocalIndexConfig, _none_if_zero

log = logging.getLogger(__name__)

SECONDS_WAITING_FOR_INDEXING_API_CALL = 5
_CREATE_COLLECTION_RETRIES = 5
_CREATE_COLLECTION_RETRY_BASE_SEC = 2.0

# REST/JSON serializes each vector component as decimal text, not 4-byte binary, so a JSON
# upsert body is much larger than the raw vector data. Measured empirically (qdrant-client
# Batch model, dim 128/768/1536): ~20.3 bytes per float, stable across dimensions. Rounded up
# with margin (id/payload/braces/commas overhead) so estimates stay conservative.
_JSON_BYTES_PER_FLOAT = 20.5
_JSON_POINT_OVERHEAD_BYTES = 64


def _safe_upsert_batch_size(dim: int, requested_batch_size: int, max_request_mb: float) -> int:
    """Clamp requested_batch_size so an upsert() JSON request for `dim`-sized vectors stays
    under max_request_mb, avoiding Qdrant's "JSON payload ... larger than allowed" 400 error.
    """
    bytes_per_vector = dim * _JSON_BYTES_PER_FLOAT + _JSON_POINT_OVERHEAD_BYTES
    max_bytes = max(1.0, max_request_mb) * 1024 * 1024
    safe_size = max(1, int(max_bytes / bytes_per_vector))
    return min(max(1, requested_batch_size), safe_size)


def _is_transient_qdrant_api_error(err: BaseException) -> bool:
    msg = str(err).lower()
    if isinstance(err, ResponseHandlingException):
        return True
    return any(
        s in msg
        for s in (
            "disconnected",
            "timeout",
            "connection reset",
            "connection refused",
            "broken pipe",
            "temporarily unavailable",
        )
    )


def qdrant_collection_exists(client: QdrantClient, collection_name: str) -> bool:
    collection_exists = True

    try:
        client.get_collection(collection_name)
    except Exception:
        collection_exists = False

    return collection_exists


class QdrantLocal(VectorDB):
    supported_filter_types: list[FilterOp] = [
        FilterOp.NonFilter,
        FilterOp.NumGE,
        FilterOp.StrEqual,
    ]

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: QdrantLocalIndexConfig,
        collection_name: str = "QdrantLocalCollection",
        drop_old: bool = False,
        name: str = "QdrantLocal",
        with_scalar_labels: bool = False,
        **kwargs,
    ):
        """Initialize wrapper around the qdrant."""
        self.name = name
        self.db_config = db_config
        self.case_config = db_case_config
        self.search_parameter = self.case_config.search_param()
        self.collection_name = collection_name
        self.client = None
        self.with_scalar_labels = with_scalar_labels
        self.query_filter: QdrantFilter | None = None

        # Cache write/search request behaviour from the case config.
        self._wait = self.case_config.wait
        self._write_ordering = self.case_config.parse_write_ordering()
        requested_upsert_batch_size = max(1, int(self.case_config.upsert_batch_size))
        self._upsert_batch_size = _safe_upsert_batch_size(
            dim, requested_upsert_batch_size, self.case_config.max_upsert_request_mb
        )
        if self._upsert_batch_size < requested_upsert_batch_size:
            log.warning(
                f"upsert_batch_size={requested_upsert_batch_size} would produce a JSON request of "
                f"~{requested_upsert_batch_size * (dim * _JSON_BYTES_PER_FLOAT + _JSON_POINT_OVERHEAD_BYTES) / (1024 * 1024):.1f} MB "  # noqa: E501
                f"for dim={dim}, exceeding the {self.case_config.max_upsert_request_mb:.0f} MB safety cap "
                "(Qdrant's REST default request-size limit is 32 MiB). Clamping upsert_batch_size to "
                f"{self._upsert_batch_size} for this run to avoid a 'JSON payload ... larger than allowed' error."
            )
        self._search_consistency = self.case_config.parse_search_consistency()
        self._search_timeout = _none_if_zero(self.case_config.search_timeout_sec)

        self._primary_field = "pk"
        self._scalar_label_field = "label"
        self._vector_field = "vector"

        client = QdrantClient(**self.db_config)

        # Lets just print the parameters here for double check
        log.info(f"Case config: {self.case_config.index_param()}")
        log.info(f"Search parameter: {self.search_parameter}")

        if drop_old and qdrant_collection_exists(client, self.collection_name):
            log.info(f"{self.name} client drop_old collection: {self.collection_name}")
            client.delete_collection(self.collection_name)

        if not qdrant_collection_exists(client, self.collection_name):
            log.info(f"{self.name} create collection: {self.collection_name}")
            self._create_collection(dim, client)

        client = None

    @contextmanager
    def init(self):
        """
        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
            >>>     self.search_embedding()
        """
        # create connection
        self.client = QdrantClient(**self.db_config)
        yield
        self.client = None
        del self.client

    def _create_collection(self, dim: int, qdrant_client: QdrantClient):
        ip = self.case_config.index_param()
        log.info(f"Create collection: {self.collection_name}")
        log.info(
            f"Index parameters: m={ip['m']}, ef_construct={ip['ef_construct']}, "
            f"on_disk={ip['on_disk']}, shard_number={ip['shard_number']}, "
            f"replication_factor={ip['replication_factor']}, "
            f"quantization={self.case_config.quantization_mode}"
        )

        for attempt in range(1, _CREATE_COLLECTION_RETRIES + 1):
            try:
                qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=dim,
                        distance=ip["distance"],
                        on_disk=ip["on_disk"],
                        datatype=ip["datatype"],
                        memory=ip["vector_memory"],
                    ),
                    hnsw_config=ip["hnsw_config"],
                    optimizers_config=ip["optimizers_config"],
                    wal_config=ip["wal_config"],
                    quantization_config=ip["quantization_config"],
                    shard_number=ip["shard_number"],
                    replication_factor=ip["replication_factor"],
                    write_consistency_factor=ip["write_consistency_factor"],
                    on_disk_payload=ip["on_disk_payload"],
                    payload=ip["payload_config"],
                )

                qdrant_client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=self._primary_field,
                    field_schema=PayloadSchemaType.INTEGER,
                )
                if self.with_scalar_labels:
                    qdrant_client.create_payload_index(
                        collection_name=self.collection_name,
                        field_name=self._scalar_label_field,
                        field_schema=KeywordIndexParams(type=PayloadSchemaType.KEYWORD),
                    )
                return
            except Exception as e:
                if "already exists!" in str(e):
                    return
                if attempt < _CREATE_COLLECTION_RETRIES and _is_transient_qdrant_api_error(e):
                    delay = _CREATE_COLLECTION_RETRY_BASE_SEC * attempt
                    log.warning(
                        "Qdrant create_collection attempt %s/%s failed (%s); retry in %.1fs",
                        attempt,
                        _CREATE_COLLECTION_RETRIES,
                        e,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                log.warning("Failed to create collection: %s error: %s", self.collection_name, e)
                raise e from None

    def optimize(self, data_size: int | None = None):
        assert self.client, "Please call self.init() before"
        configured_threshold = self.case_config.index_param()["indexing_threshold"]
        if self.case_config.disable_indexing_during_load:
            # Re-enable indexing now that loading is complete (insert_embeddings() disables it
            # for fast bulk insertion; this is the single, reliable place to restore it, since
            # optimize() is only ever called once, after the whole load has finished).
            self.client.update_collection(
                collection_name=self.collection_name,
                optimizer_config=OptimizersConfigDiff(indexing_threshold=configured_threshold),
            )
            log.info(
                f"Restored indexing_threshold={configured_threshold} for collection: {self.collection_name}; "
                "waiting for indexing to complete"
            )
        else:
            # indexing_threshold was never touched during load, so indexing has been running
            # concurrently with ingestion the whole time; just wait for any remaining tail to finish.
            log.info(
                f"disable_indexing_during_load=False; indexing_threshold stayed at {configured_threshold} "
                f"throughout the load for collection: {self.collection_name}. Waiting for indexing to catch up."
            )
        # wait for vectors to be fully indexed
        try:
            while True:
                info = self.client.get_collection(self.collection_name)
                time.sleep(SECONDS_WAITING_FOR_INDEXING_API_CALL)
                if info.status != CollectionStatus.GREEN:
                    continue
                if info.status == CollectionStatus.GREEN:
                    msg = (
                        f"Stored vectors: {info.points_count}, Indexed vectors: {info.indexed_vectors_count}, "
                        f"Collection status: {info.status}"
                    )
                    if configured_threshold != 0 and info.points_count > 0 and info.indexed_vectors_count == 0:
                        log.warning(
                            f"Collection {self.collection_name} is GREEN but indexed_vectors_count=0 with "
                            f"{info.points_count} points stored; searches will fall back to brute-force scan. "
                            "This is expected for very small/low-dim collections, but unexpected otherwise."
                        )
                    log.info(f"Finishing building index for collection: {self.collection_name}")
                    log.info(msg)
                    return

        except Exception as e:
            log.warning(f"QdrantLocal ready to search error: {e}")
            raise e from None

    def prepare_filter(self, filters: Filter) -> None:
        """Store filter for use in search_embedding (NumGE: pk >= int_value; StrEqual: label == value)."""
        if filters.type == FilterOp.NonFilter:
            self.query_filter = None
        elif filters.type == FilterOp.NumGE:
            self.query_filter = QdrantFilter(
                must=[
                    FieldCondition(
                        key=self._primary_field,
                        range=Range(gte=getattr(filters, "int_value", 0)),
                    ),
                ]
            )
        elif filters.type == FilterOp.StrEqual:
            self.query_filter = QdrantFilter(
                must=[
                    FieldCondition(
                        key=self._scalar_label_field,
                        match={"value": getattr(filters, "label_value", "")},
                    ),
                ]
            )
        else:
            raise ValueError(f"QdrantLocal does not support filter type: {filters.type}")

    def insert_embeddings(
        self,
        embeddings: Iterable[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        **kwargs,
    ) -> tuple[int, Exception]:
        """Insert embeddings into the database.

        Args:
            embeddings(list[list[float]]): list of embeddings
            metadata(list[int]): list of metadata
            labels_data(list[str]|None): list of label values for StrEqual filter (required when with_scalar_labels)
            kwargs: other arguments

        Returns:
            tuple[int, Exception]: number of embeddings inserted and exception if any
        """
        assert self.client is not None
        embeddings_list = list(embeddings)
        assert len(embeddings_list) == len(metadata)
        insert_count = 0

        if self.case_config.disable_indexing_during_load:
            # Disable indexing for quick insertion. insert_embeddings() is called once per
            # insert-batch-size chunk (potentially many times across a large load, and inside a
            # subprocess whose in-memory state never propagates back to the main process), so we
            # can't reliably track "already disabled" with an instance flag here. Instead we
            # re-assert disabled=0 before every call (idempotent, cheap) and rely on optimize() —
            # which always runs once, in the main process, only after the whole load completes — to
            # restore the real threshold exactly once. This avoids leaving indexing permanently
            # disabled if the load is interrupted (timeout/kill/stop) between batches.
            self.client.update_collection(
                collection_name=self.collection_name,
                optimizer_config=OptimizersConfigDiff(indexing_threshold=0),
            )
        # else: indexing_threshold stays at its configured value (set at collection creation),
        # so Qdrant indexes each segment as it fills up while ingestion is still in progress -
        # simulating a real-world concurrent write+index workload instead of a bulk-load mode.
        try:
            upsert_batch_size = self._upsert_batch_size
            t0 = time.perf_counter()
            for offset in range(0, len(embeddings_list), upsert_batch_size):
                t1 = time.perf_counter()
                vectors = embeddings_list[offset : offset + upsert_batch_size]
                ids = metadata[offset : offset + upsert_batch_size]
                if self.with_scalar_labels and labels_data is not None:
                    labels = labels_data[offset : offset + upsert_batch_size]
                    payloads = [
                        {self._primary_field: pk, self._scalar_label_field: labels[i]}
                        for i, pk in enumerate(ids)
                    ]
                else:
                    payloads = [{self._primary_field: v} for v in ids]
                t2 = time.perf_counter()
                log.info(f"Time to create batch: {t2 - t1} seconds")
                _ = self.client.upload_points(
                    collection_name=self.collection_name,
                    wait=self._wait,
                    ordering=self._write_ordering,
                    points=Batch(ids=ids, payloads=payloads, vectors=vectors),
                    parallel=4,
                    batch_size=self._upsert_batch_size,
                )
                t3 = time.perf_counter()
                log.info(f"Time to upsert batch: {t3 - t2} seconds")
                insert_count += len(ids)
            t4 = time.perf_counter()
            log.info(f"Time to insert data: {t4 - t0} seconds")
        except Exception as e:
            log.info(f"Failed to insert data, {e}")
            return insert_count, e
        else:
            return insert_count, None

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        timeout: int | None = None,
        **kwargs,
    ) -> list[int]:
        """Perform a search on a query embedding and return results with score.
        Should call self.init() first. Uses self.query_filter set by prepare_filter().
        """
        assert self.client is not None

        res = self.client.query_points(
            collection_name=self.collection_name,
            query=query,
            limit=k,
            query_filter=self.query_filter,
            search_params=SearchParams(**self.search_parameter),
            consistency=self._search_consistency,
            timeout=timeout if timeout is not None else self._search_timeout,
        ).points

        return [result.id for result in res]

    def update_embeddings(
        self,
        embeddings: Iterable[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        **kwargs,
    ) -> tuple[int, Exception | None]:
        assert self.client is not None
        emb_list = list(embeddings)
        if len(emb_list) != len(metadata):
            return 0, ValueError("embeddings and metadata length must match")
        if self.with_scalar_labels and labels_data is None:
            return 0, ValueError("labels_data must be provided for scalar-label collections")

        updated = 0
        upsert_batch_size = self._upsert_batch_size
        try:
            for offset in range(0, len(emb_list), upsert_batch_size):
                vectors = emb_list[offset : offset + upsert_batch_size]
                ids = metadata[offset : offset + upsert_batch_size]
                if self.with_scalar_labels:
                    labels = labels_data[offset : offset + upsert_batch_size]
                    payloads = [{self._primary_field: pk, self._scalar_label_field: labels[i]} for i, pk in enumerate(ids)]
                else:
                    payloads = [{self._primary_field: pk} for pk in ids]
                self.client.upsert(
                    collection_name=self.collection_name,
                    wait=self._wait,
                    ordering=self._write_ordering,
                    points=Batch(ids=ids, payloads=payloads, vectors=vectors),
                )
                updated += len(ids)
            return updated, None
        except Exception as e:
            log.info(f"Failed to update data, {e}")
            return updated, e
