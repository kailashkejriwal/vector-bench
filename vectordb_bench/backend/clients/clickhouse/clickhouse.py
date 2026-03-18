"""Wrapper around the ClickHouse vector database over VectorDB using native protocol (clickhouse-driver)."""

import logging
from contextlib import contextmanager
from typing import Any

import numpy as np
from clickhouse_driver import Client as NativeClient

from vectordb_bench import config

from .. import IndexType, MetricType
from ..api import VectorDB
from ...filter import Filter, FilterOp
from .config import ClickhouseConfigDict, ClickhouseIndexConfig

log = logging.getLogger(__name__)


class Clickhouse(VectorDB):
    """ClickHouse vector DB client using native protocol (clickhouse-driver) for faster inserts."""

    supported_filter_types = [FilterOp.NonFilter, FilterOp.NumGE, FilterOp.StrEqual]

    def __init__(
        self,
        dim: int,
        db_config: ClickhouseConfigDict,
        db_case_config: ClickhouseIndexConfig,
        collection_name: str = "CHVectorCollection",
        drop_old: bool = False,
        with_scalar_labels: bool = False,
        **kwargs,
    ):
        self.db_config = db_config
        self.case_config = db_case_config
        self.table_name = collection_name
        self.dim = dim
        self.with_scalar_labels = with_scalar_labels
        self._scalar_label_field = "label"

        self.index_param = self.case_config.index_param()
        self.search_param = self.case_config.search_param()
        self.session_param = self.case_config.session_param()

        self._index_name = "clickhouse_index"
        self._primary_field = "id"
        self._vector_field = "embedding"

        self.conn = self._create_connection()

        if drop_old:
            log.info(f"Clickhouse client drop table : {self.table_name}")
            self._drop_table()
            self._create_table(dim)
            if self.case_config.create_index_before_load:
                self._create_index()

        self.conn.disconnect()
        self.conn = None

        self._filter_gt: int | None = None
        self._filter_label_value: str | None = None

    def prepare_filter(self, filters: Filter) -> None:
        """Store filter for use in search_embedding (NumGE: id >= int_value; StrEqual: label == value)."""
        if filters.type == FilterOp.NonFilter:
            self._filter_gt = None
            self._filter_label_value = None
        elif filters.type == FilterOp.NumGE:
            self._filter_gt = getattr(filters, "int_value", 0)
            self._filter_label_value = None
        elif filters.type == FilterOp.StrEqual:
            self._filter_gt = None
            self._filter_label_value = getattr(filters, "label_value", "")
        else:
            raise ValueError(f"Clickhouse does not support filter type: {filters.type}")

    @contextmanager
    def init(self) -> None:
        """
        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
            >>>     self.search_embedding()
        """
        self.conn = self._create_connection()
        self._performance_tuning()
        try:
            yield
        finally:
            if hasattr(self.conn, "disconnect"):
                self.conn.disconnect()
            elif hasattr(self.conn, "close"):
                self.conn.close()
            self.conn = None

    def _create_connection(self) -> NativeClient:
        return NativeClient(
            host=self.db_config["host"],
            port=self.db_config["port"],
            database=self.db_config["database"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            secure=self.db_config.get("secure", False),
            settings=self.session_param,
        )

    def _drop_index(self):
        assert self.conn is not None, "Connection is not initialized"
        try:
            self.conn.execute(
                f"ALTER TABLE {self.db_config['database']}.{self.table_name} DROP INDEX {self._index_name}"
            )
        except Exception as e:
            log.warning(
                "Failed to drop index on table %s.%s: %s",
                self.db_config["database"],
                self.table_name,
                e,
            )
            raise e from None

    def _drop_table(self):
        assert self.conn is not None, "Connection is not initialized"
        try:
            self.conn.execute(
                f'DROP TABLE IF EXISTS {self.db_config["database"]}.{self.table_name}'
            )
        except Exception as e:
            log.warning(
                "Failed to drop table %s.%s: %s",
                self.db_config["database"],
                self.table_name,
                e,
            )
            raise e from None

    def _performance_tuning(self):
        """Skip index materialization during insert and allow large insert blocks for faster bulk load."""
        if self.conn is None:
            return
        try:
            self.conn.execute("SET materialize_skip_indexes_on_insert = 1")
            # Allow large blocks so server accepts our INSERT_BATCH_SIZE without splitting.
            block_size = max(100_000, getattr(config, "INSERT_BATCH_SIZE", 10_000))
            self.conn.execute(f"SET max_insert_block_size = {block_size}")
        except Exception as e:
            log.warning("Could not apply insert performance settings: %s", e)

    def _create_index(self):
        assert self.conn is not None, "Connection is not initialized"
        try:
            if self.index_param["index_type"] == IndexType.HNSW.value:
                if (
                    self.index_param["quantization"]
                    and self.index_param["params"]["M"]
                    and self.index_param["params"]["efConstruction"]
                ):
                    query = (
                        f"ALTER TABLE {self.db_config['database']}.{self.table_name} "
                        f"ADD INDEX {self._index_name} {self._vector_field} "
                        f"TYPE vector_similarity('hnsw', '{self.index_param['metric_type']}', {self.dim}, "
                        f"'{self.index_param['quantization']}', "
                        f"{self.index_param['params']['M']}, {self.index_param['params']['efConstruction']}) "
                        f"GRANULARITY {self.index_param['granularity']}"
                    )
                else:
                    query = (
                        f"ALTER TABLE {self.db_config['database']}.{self.table_name} "
                        f"ADD INDEX {self._index_name} {self._vector_field} "
                        f"TYPE vector_similarity('hnsw', '{self.index_param['metric_type']}', {self.dim}) "
                        f"GRANULARITY {self.index_param['granularity']}"
                    )
                self.conn.execute(query)
            elif self.index_param["index_type"] == IndexType.QBIT.value:
                # QBit doesn't require traditional vector indexes
                log.info("QBit index type selected - no traditional index creation needed")
            elif self.index_param["index_type"] == IndexType.Flat.value:
                # Flat search doesn't use indexes
                log.info("Flat index type selected - no index creation needed")
            else:
                log.warning("Unsupported index type: %s", self.index_param["index_type"])
        except Exception as e:
            log.warning(
                "Failed to create Clickhouse vector index on table %s: %s",
                self.table_name,
                e,
            )
            raise e from None

    def _create_table(self, dim: int):
        assert self.conn is not None, "Connection is not initialized"
        try:
            # Handle QBit data type specially
            if self.index_param['index_type'] == IndexType.QBIT.value:
                vector_type = self.index_param['vector_data_type'].format(dim=dim)
            else:
                vector_type = f"Array({self.index_param['vector_data_type']})"

            cols = (
                f"{self._primary_field} UInt32, "
                f"{self._vector_field} {vector_type} CODEC(NONE)"
            )
            if self.with_scalar_labels:
                cols += f", {self._scalar_label_field} String"
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self.db_config['database']}.{self.table_name} "
                f"({cols}, "
                f"CONSTRAINT same_length CHECK length(embedding) = {dim}) "
                f"ENGINE = MergeTree() "
                f"ORDER BY {self._primary_field}"
            )
        except Exception as e:
            log.warning("Failed to create Clickhouse table %s: %s", self.table_name, e)
            raise e from None

    def optimize(self, data_size: int | None = None):
        pass

    def _post_insert(self):
        pass

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        **kwargs: Any,
    ) -> tuple[int, Exception | None]:
        assert self.conn is not None, "Connection is not initialized"

        try:
            # clickhouse-driver: INSERT with row-oriented data (list of tuples).
            emb_arr = np.asarray(embeddings, dtype=np.float32)
            if self.with_scalar_labels and labels_data is not None:
                assert len(labels_data) == len(metadata), "labels_data length must match metadata"
                data = list(zip(metadata, emb_arr.tolist(), labels_data))
                self.conn.execute(
                    f"INSERT INTO {self.db_config['database']}.{self.table_name} "
                    f"({self._primary_field}, {self._vector_field}, {self._scalar_label_field}) VALUES",
                    data,
                )
            else:
                data = list(zip(metadata, emb_arr.tolist()))
                self.conn.execute(
                    f"INSERT INTO {self.db_config['database']}.{self.table_name} "
                    f"({self._primary_field}, {self._vector_field}) VALUES",
                    data,
                )
            return len(metadata), None
        except Exception as e:
            log.warning(
                "Failed to insert data into Clickhouse table (%s): %s",
                self.table_name,
                e,
            )
            return 0, e

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        timeout: int | None = None,
    ) -> list[int]:
        assert self.conn is not None, "Connection is not initialized"

        db = self.db_config["database"]
        table = self.table_name
        apply_num_filter = getattr(self, "_filter_gt", None) is not None
        apply_label_filter = getattr(self, "_filter_label_value", None) is not None
        gt = self._filter_gt if apply_num_filter else 0
        label_val = self._filter_label_value if apply_label_filter else ""
        # Pass as list so clickhouse-driver sends Array; tuple can be sent as Tuple and
        # ClickHouse's arrayCosineDistance may require Array for the query vector.
        query_as_list = list(query)

        if apply_label_filter:
            where_clause = f"WHERE {self._scalar_label_field} = %(label_val)s "
        elif apply_num_filter:
            where_clause = f"WHERE {self._primary_field} >= %(gt)s "
        else:
            where_clause = ""

        # Handle QBit search with precision control
        if self.index_param["index_type"] == IndexType.QBIT.value:
            precision_bits = self.search_param["params"].get("precision_bits", 16)
            if where_clause:
                sql = (
                    f"SELECT {self._primary_field} FROM {db}.{table} "
                    f"{where_clause}"
                    f"ORDER BY L2DistanceTransposed({self._vector_field}, cast(%(query)s AS Array(Float32)), {precision_bits}) "
                    f"LIMIT %(k)s"
                )
            else:
                sql = (
                    f"SELECT {self._primary_field} FROM {db}.{table} "
                    f"ORDER BY L2DistanceTransposed({self._vector_field}, cast(%(query)s AS Array(Float32)), {precision_bits}) "
                    f"LIMIT %(k)s"
                )
        else:
            # Handle traditional distance functions
            if self.case_config.metric_type == MetricType.COSINE:
                distance_func = "cosineDistance"
            elif self.case_config.metric_type == MetricType.L2:
                distance_func = "L2Distance"
            elif self.case_config.metric_type == MetricType.IP:
                distance_func = "dotProduct"
            elif self.case_config.metric_type == MetricType.L1:
                distance_func = "L1Distance"
            elif self.case_config.metric_type == MetricType.LINFINITY:
                distance_func = "LinfDistance"
            elif self.case_config.metric_type == MetricType.LP:
                distance_func = "LpDistance"
            else:
                distance_func = "L2Distance"  # Default fallback

            if where_clause:
                sql = (
                    f"SELECT {self._primary_field} FROM {db}.{table} "
                    f"{where_clause}"
                    f"ORDER BY {distance_func}({self._vector_field}, cast(%(query)s AS Array(Float32))) "
                    f"LIMIT %(k)s"
                )
            else:
                sql = (
                    f"SELECT {self._primary_field} FROM {db}.{table} "
                    f"ORDER BY {distance_func}({self._vector_field}, cast(%(query)s AS Array(Float32))) "
                    f"LIMIT %(k)s"
                )

        params: dict[str, Any] = {"query": query_as_list, "k": k}
        if apply_num_filter:
            params["gt"] = gt
        if apply_label_filter:
            params["label_val"] = label_val

        result = self.conn.execute(sql, params)
        return [int(row[0]) for row in result]
