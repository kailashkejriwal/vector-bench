"""Wrapper around the Qdrant over VectorDB"""

import logging
import time
from collections.abc import Iterable
from contextlib import contextmanager

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Batch,
    CollectionStatus,
    FieldCondition,
    Filter as QdrantFilter,
    HnswConfigDiff,
    KeywordIndexParams,
    OptimizersConfigDiff,
    PayloadSchemaType,
    Range,
    SearchParams,
    VectorParams,
)

from vectordb_bench.backend.filter import Filter, FilterOp

from ..api import VectorDB
from .config import QdrantLocalIndexConfig

log = logging.getLogger(__name__)

SECONDS_WAITING_FOR_INDEXING_API_CALL = 5
QDRANT_BATCH_SIZE = 100


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
        log.info(f"Create collection: {self.collection_name}")
        log.info(
            f"Index parameters: m={self.case_config.index_param()['m']}, "
            f"ef_construct={self.case_config.index_param()['ef_construct']}, "
            f"on_disk={self.case_config.index_param()['on_disk']}"
        )

        # If the on_disk is true, we enable both on disk index and vectors.
        try:
            qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=dim,
                    distance=self.case_config.index_param()["distance"],
                    on_disk=self.case_config.index_param()["on_disk"],
                ),
                hnsw_config=HnswConfigDiff(
                    m=self.case_config.index_param()["m"],
                    ef_construct=self.case_config.index_param()["ef_construct"],
                    on_disk=self.case_config.index_param()["on_disk"],
                ),
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

        except Exception as e:
            if "already exists!" in str(e):
                return
            log.warning(f"Failed to create collection: {self.collection_name} error: {e}")
            raise e from None

    def optimize(self, data_size: int | None = None):
        assert self.client, "Please call self.init() before"
        # wait for vectors to be fully indexed
        try:
            while True:
                info = self.client.get_collection(self.collection_name)
                time.sleep(SECONDS_WAITING_FOR_INDEXING_API_CALL)
                if info.status != CollectionStatus.GREEN:
                    continue
                if info.status == CollectionStatus.GREEN:
                    log.info(f"Finishing building index for collection: {self.collection_name}")
                    msg = (
                        f"Stored vectors: {info.points_count}, Indexed vectors: {info.indexed_vectors_count}, "
                        f"Collection status: {info.status}"
                    )
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

        # disable indexing for quick insertion
        self.client.update_collection(
            collection_name=self.collection_name,
            optimizer_config=OptimizersConfigDiff(indexing_threshold=0),
        )
        try:
            for offset in range(0, len(embeddings_list), QDRANT_BATCH_SIZE):
                vectors = embeddings_list[offset : offset + QDRANT_BATCH_SIZE]
                ids = metadata[offset : offset + QDRANT_BATCH_SIZE]
                if self.with_scalar_labels and labels_data is not None:
                    labels = labels_data[offset : offset + QDRANT_BATCH_SIZE]
                    payloads = [
                        {self._primary_field: pk, self._scalar_label_field: labels[i]}
                        for i, pk in enumerate(ids)
                    ]
                else:
                    payloads = [{self._primary_field: v} for v in ids]
                _ = self.client.upsert(
                    collection_name=self.collection_name,
                    wait=True,
                    points=Batch(ids=ids, payloads=payloads, vectors=vectors),
                )
                insert_count += len(ids)
            # enable indexing after insertion
            self.client.update_collection(
                collection_name=self.collection_name,
                optimizer_config=OptimizersConfigDiff(indexing_threshold=100),
            )

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
        ).points

        return [result.id for result in res]
