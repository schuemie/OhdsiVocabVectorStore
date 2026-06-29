from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from dotenv import load_dotenv
from psycopg import sql
from psycopg import connect as psycopg_connect
from psycopg.rows import dict_row
from pgvector.psycopg import register_vector

from Logging import open_log
from Settings import Settings

load_dotenv()

DEFAULT_SIZES = [10000, 25000, 100000, 250000]
DEFAULT_QUERY_COUNT = 1000
DEFAULT_TOP_K = 10
DEFAULT_BATCH_SIZE = 1000
DEFAULT_WARMUP_COUNT = 25
DEFAULT_THREADS = 1


@dataclass
class CacheMetadata:
    source_schema: str
    source_table: str
    max_sample_size: int
    query_count: int
    dimension: int
    store_type: str
    normalized_metric: str


@dataclass
class SetupTiming:
    load_seconds: float
    index_seconds: float
    total_seconds: float


@dataclass
class QuerySummary:
    engine: str
    sample_size: int
    query_count: int
    top_k: int
    setup: SetupTiming
    average_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    qps: float


@dataclass
class BenchmarkRun:
    sample_size: int
    duckdb: QuerySummary
    pgvector: QuerySummary


@dataclass
class CachePaths:
    root: Path
    corpus_parquet: Path
    query_parquet: Path
    metadata_json: Path


@dataclass
class CorpusRow:
    sample_rank: int
    concept_id: int
    term_type: str
    embedding: List[float]


@dataclass
class QueryRow:
    query_rank: int
    concept_id: int
    term_type: str
    embedding: List[float]


class DuckDBQueryRunner:
    def __init__(self, connection: Any, table_name: str, top_k: int, query_sql: str):
        self.connection = connection
        self.table_name = table_name
        self.top_k = top_k
        self.query_sql = query_sql

    def __call__(self, query_vector: Sequence[float]) -> List[Any]:
        # top_k is inlined as a literal in query_sql so the HNSW index scan engages.
        result = self.connection.execute(self.query_sql, [list(query_vector)])
        return result.fetchall()


class PgVectorQueryRunner:
    def __init__(self, connection: Any, query_sql: Any, top_k: int):
        self.connection = connection
        self.query_sql = query_sql
        self.top_k = top_k

    def __call__(self, query_vector: Sequence[float]) -> List[Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(self.query_sql, (list(query_vector), self.top_k))
            return cursor.fetchall()


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark nearest-neighbor search speed for DuckDB (VSS/HNSW) versus PGVector (HNSW)."
        )
    )
    parser.add_argument("config_path", help="Path to the YAML settings file")
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=DEFAULT_SIZES,
        help="Sample sizes to benchmark. Default: 10000 25000 100000 250000",
    )
    parser.add_argument(
        "--query-count",
        type=int,
        default=DEFAULT_QUERY_COUNT,
        help="How many held-out query vectors to benchmark with. Default: 1000",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K,
        help="How many nearest neighbors to fetch per query. Default: 10",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Streaming batch size while extracting and loading sample data. Default: 1000",
    )
    parser.add_argument(
        "--warmup-count",
        type=int,
        default=DEFAULT_WARMUP_COUNT,
        help="How many held-out queries to run as warmup before timing. Default: 25",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=DEFAULT_THREADS,
        help="DuckDB threads setting used during benchmark setup. Default: 1",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Rebuild the local parquet cache even if it already exists.",
    )
    return parser.parse_args(list(argv))


def load_settings(config_path: str) -> Settings:
    with open(config_path, encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return Settings(config)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable '{name}' must be set.")
    return value


def make_cache_paths(settings: Settings) -> CachePaths:
    root = Path(settings.log_folder) / "duckdb_pgvector_benchmark"
    return CachePaths(
        root=root,
        corpus_parquet=root / "sample_corpus.parquet",
        query_parquet=root / "heldout_queries.parquet",
        metadata_json=root / "cache_metadata.json",
    )


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_embedding(raw_embedding: Any) -> List[float]:
    if isinstance(raw_embedding, str):
        value = raw_embedding.strip()
        if not value:
            raise ValueError("Embedding text value is empty.")
        if value[0] == "[" and value[-1] == "]":
            value = value[1:-1]
        if not value:
            return []
        return [float(part.strip()) for part in value.split(",") if part.strip()]
    if isinstance(raw_embedding, np.ndarray):
        return raw_embedding.astype(np.float32).tolist()
    if isinstance(raw_embedding, (list, tuple)):
        return [float(value) for value in raw_embedding]
    raise TypeError(f"Unsupported embedding value type: {type(raw_embedding)!r}")


def normalize_embedding(raw_embedding: Any) -> List[float]:
    parsed = parse_embedding(raw_embedding)
    array = np.asarray(parsed, dtype=np.float32)
    norm = float(np.linalg.norm(array))
    if norm > 0:
        array = array / norm
    return array.astype(np.float32).tolist()


def sanitize_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)


def build_sample_cache(
    settings: Settings,
    cache_paths: CachePaths,
    sizes: Sequence[int],
    query_count: int,
    batch_size: int,
    refresh_cache: bool,
) -> CacheMetadata:
    ensure_directory(cache_paths.root)
    max_sample_size = max(sizes)
    requested = CacheMetadata(
        source_schema=require_env("VOCAB_SCHEMA"),
        source_table=require_env("VOCAB_VECTOR_TABLE"),
        max_sample_size=max_sample_size,
        query_count=query_count,
        dimension=-1,
        store_type=settings.store_type,
        normalized_metric="l2_normalized",
    )

    if not refresh_cache and cache_paths.corpus_parquet.exists() and cache_paths.query_parquet.exists() and cache_paths.metadata_json.exists():
        with open(cache_paths.metadata_json, encoding="utf-8") as file:
            cached = CacheMetadata(**json.load(file))
        if (
            cached.source_schema == requested.source_schema
            and cached.source_table == requested.source_table
            and cached.max_sample_size == requested.max_sample_size
            and cached.query_count == requested.query_count
            and cached.store_type == requested.store_type
            and cached.normalized_metric == requested.normalized_metric
        ):
            return cached

    for path in [cache_paths.corpus_parquet, cache_paths.query_parquet, cache_paths.metadata_json]:
        if path.exists():
            path.unlink()

    source_schema = require_env("VOCAB_SCHEMA")
    source_table = require_env("VOCAB_VECTOR_TABLE")
    source_connection_string = require_env("VOCAB_CONNECTION_STRING").replace("+psycopg", "")
    source_query = sql.SQL(
        "SELECT concept_id, term_type, embedding_vector::text AS embedding_vector "
        "FROM {schema}.{table} "
        "ORDER BY concept_id, term_type, ctid "
        "LIMIT {limit}"
    ).format(
        schema=sql.Identifier(source_schema),
        table=sql.Identifier(source_table),
        limit=sql.Literal(max_sample_size + query_count),
    )

    with psycopg_connect(source_connection_string, row_factory=dict_row) as source_connection:
        with source_connection.cursor() as result:
            result.execute(source_query)
            first_row = result.fetchone()
            if first_row is None:
                raise RuntimeError("Source vector table is empty; cannot build benchmark cache.")

            dimension = len(parse_embedding(first_row["embedding_vector"]))
            corpus_schema = pa.schema(
                [
                    pa.field("sample_rank", pa.int64()),
                    pa.field("concept_id", pa.int64()),
                    pa.field("term_type", pa.string()),
                    pa.field("embedding", pa.list_(pa.float32())),
                ]
            )
            query_schema = pa.schema(
                [
                    pa.field("query_rank", pa.int64()),
                    pa.field("concept_id", pa.int64()),
                    pa.field("term_type", pa.string()),
                    pa.field("embedding", pa.list_(pa.float32())),
                ]
            )
            corpus_writer = pq.ParquetWriter(str(cache_paths.corpus_parquet), corpus_schema)
            query_writer = pq.ParquetWriter(str(cache_paths.query_parquet), query_schema)

            corpus_buffer: List[CorpusRow] = []
            query_buffer: List[QueryRow] = []
            sample_rank = 0
            query_rank = 0

            def flush_corpus_buffer() -> None:
                nonlocal corpus_buffer
                if not corpus_buffer:
                    return
                sample_rank_array = pa.array([row.sample_rank for row in corpus_buffer], type=pa.int64())
                concept_id_array = pa.array([row.concept_id for row in corpus_buffer], type=pa.int64())
                term_type_array = pa.array([row.term_type for row in corpus_buffer], type=pa.string())
                embedding_array = pa.array([row.embedding for row in corpus_buffer], type=pa.list_(pa.float32()))
                table = pa.Table.from_arrays(
                    [sample_rank_array, concept_id_array, term_type_array, embedding_array],
                    schema=corpus_schema,
                )
                corpus_writer.write_table(table)
                corpus_buffer = []

            def flush_query_buffer() -> None:
                nonlocal query_buffer
                if not query_buffer:
                    return
                query_rank_array = pa.array([row.query_rank for row in query_buffer], type=pa.int64())
                concept_id_array = pa.array([row.concept_id for row in query_buffer], type=pa.int64())
                term_type_array = pa.array([row.term_type for row in query_buffer], type=pa.string())
                embedding_array = pa.array([row.embedding for row in query_buffer], type=pa.list_(pa.float32()))
                table = pa.Table.from_arrays(
                    [query_rank_array, concept_id_array, term_type_array, embedding_array],
                    schema=query_schema,
                )
                query_writer.write_table(table)
                query_buffer = []

            try:
                current_row = first_row
                total_needed = max_sample_size + query_count
                while True:
                    if current_row is None:
                        break
                    sample_rank += 1
                    normalized = normalize_embedding(current_row["embedding_vector"])
                    if sample_rank <= max_sample_size:
                        corpus_buffer.append(
                            CorpusRow(
                                sample_rank=sample_rank,
                                concept_id=int(current_row["concept_id"]),
                                term_type=str(current_row["term_type"]),
                                embedding=normalized,
                            )
                        )
                        if len(corpus_buffer) >= batch_size:
                            flush_corpus_buffer()
                    else:
                        query_rank += 1
                        if query_rank > query_count:
                            break
                        query_buffer.append(
                            QueryRow(
                                query_rank=query_rank,
                                concept_id=int(current_row["concept_id"]),
                                term_type=str(current_row["term_type"]),
                                embedding=normalized,
                            )
                        )
                        if len(query_buffer) >= batch_size:
                            flush_query_buffer()

                    if sample_rank >= total_needed:
                        break
                    current_row = result.fetchone()

                if sample_rank < max_sample_size:
                    raise RuntimeError(
                        f"Source vector table only provided {sample_rank} rows, but {max_sample_size} were requested."
                    )
                if query_rank < query_count:
                    raise RuntimeError(
                        f"Source vector table only provided {query_rank} held-out query rows, but {query_count} were requested."
                    )
            finally:
                flush_corpus_buffer()
                flush_query_buffer()
                corpus_writer.close()
                query_writer.close()

    metadata = CacheMetadata(
        source_schema=requested.source_schema,
        source_table=requested.source_table,
        max_sample_size=max_sample_size,
        query_count=query_count,
        dimension=dimension,
        store_type=settings.store_type,
        normalized_metric=requested.normalized_metric,
    )
    with open(cache_paths.metadata_json, "w", encoding="utf-8") as file:
        json.dump(asdict(metadata), file, indent=2)
    logging.info(
        "Created local benchmark cache: corpus=%s, queries=%s, dimension=%s",
        cache_paths.corpus_parquet,
        cache_paths.query_parquet,
        dimension,
    )
    return metadata


def load_query_vectors(query_parquet: Path) -> List[List[float]]:
    table = pq.read_table(query_parquet)
    embeddings = table.column("embedding").to_pylist()
    return [list(map(float, embedding)) for embedding in embeddings]


def duckdb_install_vss(connection: Any) -> None:
    connection.execute("INSTALL vss")
    connection.execute("LOAD vss")


def duckdb_create_index(connection: Any, table_name: str, embedding_column: str) -> str:
    index_name = f"{sanitize_name(table_name)}_{embedding_column}_hnsw_idx"
    # Cosine metric must match the array_cosine_distance operator used at query
    # time for the HNSW index scan to engage.
    statement = f"CREATE INDEX {index_name} ON {table_name} USING HNSW ({embedding_column}) WITH (metric = 'cosine')"
    connection.execute(statement)
    return statement


def duckdb_query_sql(table_name: str, dimension: int, top_k: int) -> str:
    # LIMIT must be a literal constant and the query vector cast to the indexed
    # fixed-size FLOAT[dimension] type for the HNSW index scan to engage.
    return (
        f"SELECT concept_id, term_type FROM {table_name} "
        f"ORDER BY array_cosine_distance(embedding, ?::FLOAT[{dimension}]) LIMIT {top_k}"
    )


def duckdb_query_uses_index(connection: Any, candidate: str, probe_vector: Sequence[float]) -> bool:
    try:
        plan_rows = connection.execute(f"EXPLAIN {candidate}", [list(probe_vector)]).fetchall()
    except Exception:  # pragma: no cover - EXPLAIN best effort
        return False
    plan_text = "\n".join(str(value) for row in plan_rows for value in row).upper()
    return "HNSW_INDEX_SCAN" in plan_text


def build_duckdb_runner(connection: Any, table_name: str, top_k: int, dimension: int, probe_vector: Sequence[float]) -> Tuple[DuckDBQueryRunner, str]:
    candidate = duckdb_query_sql(table_name, dimension, top_k)
    connection.execute(candidate, [list(probe_vector)]).fetchall()
    if not duckdb_query_uses_index(connection, candidate, probe_vector):
        logging.warning("DuckDB query did not use the HNSW index: %s", candidate)
    return DuckDBQueryRunner(connection=connection, table_name=table_name, top_k=top_k, query_sql=candidate), candidate


def duckdb_load_table(
    duckdb_database_path: Path,
    corpus_parquet: Path,
    sample_size: int,
    dimension: int,
    threads: int,
) -> Tuple[Any, SetupTiming, str, str]:
    duckdb = importlib.import_module("duckdb")

    if duckdb_database_path.exists():
        duckdb_database_path.unlink()
    connection = duckdb.connect(str(duckdb_database_path))
    try:
        connection.execute(f"PRAGMA threads={threads}")
        duckdb_install_vss(connection)
        connection.execute("SET hnsw_enable_experimental_persistence = true")

        table_name = f"benchmark_vectors_{sample_size}"
        corpus_path_sql = str(corpus_parquet).replace("'", "''")
        create_sql = (
            f"CREATE TABLE {table_name} AS "
            f"SELECT sample_rank, concept_id, term_type, CAST(embedding AS FLOAT[{dimension}]) AS embedding "
            f"FROM read_parquet('{corpus_path_sql}') "
            f"WHERE sample_rank <= {sample_size} ORDER BY sample_rank"
        )
        load_start = time.perf_counter()
        connection.execute(create_sql)
        load_seconds = time.perf_counter() - load_start

        index_start = time.perf_counter()
        index_sql = duckdb_create_index(connection, table_name, "embedding")
        index_seconds = time.perf_counter() - index_start
        return connection, SetupTiming(load_seconds=load_seconds, index_seconds=index_seconds, total_seconds=load_seconds + index_seconds), table_name, index_sql
    except Exception:
        try:
            connection.close()
        except Exception:
            pass
        raise


def pgvector_vector_type(settings: Settings) -> str:
    return "vector" if settings.store_type == settings.PGVECTOR else "halfvec"


def pgvector_index_opclass(settings: Settings) -> str:
    return "vector_cosine_ops" if settings.store_type == settings.PGVECTOR else "halfvec_cosine_ops"


def pgvector_create_table(connection: Any, schema: str, table_name: str, vector_type: str, dimension: int) -> None:
    schema_sql = sql.Identifier(schema).as_string(connection)
    table_sql = sql.Identifier(table_name).as_string(connection)
    connection.execute(
        f"CREATE TABLE {schema_sql}.{table_sql} (concept_id BIGINT, term_type VARCHAR(32), embedding_vector {vector_type}({dimension}))"
    )


def pgvector_copy_from_parquet(
    connection: Any,
    schema: str,
    table_name: str,
    corpus_parquet: Path,
    sample_size: int,
    vector_type: str,
) -> int:
    parquet_file = pq.ParquetFile(str(corpus_parquet))
    copied_rows = 0
    copy_stmt = sql.SQL(
        "COPY {schema}.{table} (concept_id, term_type, embedding_vector) FROM STDIN WITH (FORMAT BINARY)"
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table_name))

    with connection.cursor() as cursor:
        with cursor.copy(copy_stmt) as copy:
            copy.set_types(["int8", "varchar", vector_type])
            for row_group_index in range(parquet_file.num_row_groups):
                if copied_rows >= sample_size:
                    break
                row_group = parquet_file.read_row_group(row_group_index)
                remaining = sample_size - copied_rows
                if row_group.num_rows > remaining:
                    row_group = row_group.slice(0, remaining)
                concept_ids = row_group.column("concept_id").to_pylist()
                term_types = row_group.column("term_type").to_pylist()
                embeddings = row_group.column("embedding").to_pylist()
                for concept_id, term_type, embedding in zip(concept_ids, term_types, embeddings):
                    copy.write_row([int(concept_id), str(term_type), [float(value) for value in embedding]])
                    copied_rows += 1
                    if copied_rows >= sample_size:
                        break
    return copied_rows


def pgvector_create_index(connection: Any, schema: str, table_name: str, opclass: str) -> str:
    index_name = f"{sanitize_name(table_name)}_embedding_hnsw_idx"
    schema_sql = sql.Identifier(schema).as_string(connection)
    table_sql = sql.Identifier(table_name).as_string(connection)
    index_sql = sql.Identifier(index_name).as_string(connection)
    statement = f"CREATE INDEX {index_sql} ON {schema_sql}.{table_sql} USING hnsw (embedding_vector {opclass})"
    connection.execute(statement)
    return statement


def build_pgvector_runner(connection: Any, schema: str, table_name: str, top_k: int, vector_type: str) -> PgVectorQueryRunner:
    query_stmt = sql.SQL(
        "SELECT concept_id, term_type FROM {schema}.{table} ORDER BY embedding_vector <=> %s::{vector_type} LIMIT %s"
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table_name),
        vector_type=sql.SQL(vector_type),
    )
    return PgVectorQueryRunner(connection=connection, query_sql=query_stmt, top_k=top_k)


def summarize_timings(engine: str, sample_size: int, setup: SetupTiming, timings_seconds: Sequence[float], query_count: int, top_k: int) -> QuerySummary:
    array = [float(value) * 1000.0 for value in timings_seconds]
    sorted_array = sorted(array)

    def percentile(values: Sequence[float], pct: float) -> float:
        if not values:
            return 0.0
        if len(values) == 1:
            return float(values[0])
        position = (len(values) - 1) * pct / 100.0
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        if lower == upper:
            return float(values[lower])
        fraction = position - lower
        return float(values[lower] + (values[upper] - values[lower]) * fraction)

    return QuerySummary(
        engine=engine,
        sample_size=sample_size,
        query_count=query_count,
        top_k=top_k,
        setup=setup,
        average_ms=float(statistics.fmean(array)),
        median_ms=float(statistics.median(array)),
        p95_ms=percentile(sorted_array, 95.0),
        p99_ms=percentile(sorted_array, 99.0),
        min_ms=float(min(array)),
        max_ms=float(max(array)),
        qps=float(query_count / max(float(sum(timings_seconds)), 1e-12)),
    )


def benchmark_runner(
    runner: Callable[[Sequence[float]], List[Any]],
    query_vectors: Sequence[Sequence[float]],
    warmup_count: int,
) -> List[float]:
    warmup_count = max(0, min(warmup_count, len(query_vectors)))
    for query_vector in query_vectors[:warmup_count]:
        runner(query_vector)

    timings: List[float] = []
    for query_vector in query_vectors:
        start = time.perf_counter()
        runner(query_vector)
        timings.append(time.perf_counter() - start)
    return timings


def run_for_sample_size(
    settings: Settings,
    cache_paths: CachePaths,
    metadata: CacheMetadata,
    sample_size: int,
    query_vectors: Sequence[Sequence[float]],
    top_k: int,
    warmup_count: int,
    duckdb_threads: int,
) -> BenchmarkRun:
    benchmark_root = cache_paths.root / f"sample_{sample_size}"
    ensure_directory(benchmark_root)
    dimension = metadata.dimension

    duckdb_database_path = benchmark_root / f"benchmark_{sample_size}.duckdb"
    duckdb_connection, duckdb_setup, duckdb_table_name, duckdb_index_sql = duckdb_load_table(
        duckdb_database_path=duckdb_database_path,
        corpus_parquet=cache_paths.corpus_parquet,
        sample_size=sample_size,
        dimension=dimension,
        threads=duckdb_threads,
    )
    pg_connection = None
    try:
        duckdb_runner, duckdb_query_sql = build_duckdb_runner(
            connection=duckdb_connection,
            table_name=duckdb_table_name,
            top_k=top_k,
            dimension=dimension,
            probe_vector=query_vectors[0],
        )
        duckdb_timings = benchmark_runner(duckdb_runner, query_vectors, warmup_count)
        duckdb_summary = summarize_timings(
            engine="duckdb",
            sample_size=sample_size,
            setup=duckdb_setup,
            timings_seconds=duckdb_timings,
            query_count=len(query_vectors),
            top_k=top_k,
        )

        pg_schema = require_env("VOCAB_SCHEMA")
        pg_connection = psycopg_connect(require_env("VOCAB_CONNECTION_STRING").replace("+psycopg", ""))
        register_vector(pg_connection)
        vector_type = pgvector_vector_type(settings)
        opclass = pgvector_index_opclass(settings)
        pg_table_name = f"benchmark_vector_{sample_size}"
        with pg_connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                    schema=sql.Identifier(pg_schema),
                    table=sql.Identifier(pg_table_name),
                )
            )
            pgvector_create_table(pg_connection, pg_schema, pg_table_name, vector_type, dimension)
        pg_load_start = time.perf_counter()
        copied_rows = pgvector_copy_from_parquet(
            connection=pg_connection,
            schema=pg_schema,
            table_name=pg_table_name,
            corpus_parquet=cache_paths.corpus_parquet,
            sample_size=sample_size,
            vector_type=vector_type,
        )
        pg_load_seconds = time.perf_counter() - pg_load_start
        pg_index_start = time.perf_counter()
        pg_index_sql = pgvector_create_index(pg_connection, pg_schema, pg_table_name, opclass)
        pg_index_seconds = time.perf_counter() - pg_index_start
        pg_connection.commit()
        pg_setup = SetupTiming(
            load_seconds=pg_load_seconds,
            index_seconds=pg_index_seconds,
            total_seconds=pg_load_seconds + pg_index_seconds,
        )
        pg_runner = build_pgvector_runner(pg_connection, pg_schema, pg_table_name, top_k, vector_type)
        pg_timings = benchmark_runner(pg_runner, query_vectors, warmup_count)
        pg_summary = summarize_timings(
            engine="pgvector",
            sample_size=sample_size,
            setup=pg_setup,
            timings_seconds=pg_timings,
            query_count=len(query_vectors),
            top_k=top_k,
        )

        logging.info(
            "Sample %s: DuckDB table=%s index=%s; PGVector rows=%s table=%s index=%s",
            sample_size,
            duckdb_table_name,
            duckdb_index_sql,
            copied_rows,
            pg_table_name,
            pg_index_sql,
        )
        return BenchmarkRun(sample_size=sample_size, duckdb=duckdb_summary, pgvector=pg_summary)
    finally:
        try:
            duckdb_connection.close()
        except Exception:
            pass
        try:
            if pg_connection is not None:
                pg_connection.close()
        except Exception:
            pass


def benchmark_to_dict(result: BenchmarkRun) -> Dict[str, Any]:
    data = asdict(result)
    return data


def write_results(results: Sequence[BenchmarkRun], output_json: Path, output_csv: Path) -> None:
    rows: List[Dict[str, Any]] = []
    for result in results:
        for summary in [result.duckdb, result.pgvector]:
            row = asdict(summary)
            row["setup"] = asdict(summary.setup)
            rows.append(row)

    with open(output_json, "w", encoding="utf-8") as file:
        json.dump([benchmark_to_dict(result) for result in results], file, indent=2)

    csv_header = [
        "engine",
        "sample_size",
        "query_count",
        "top_k",
        "setup_load_seconds",
        "setup_index_seconds",
        "setup_total_seconds",
        "average_ms",
        "median_ms",
        "p95_ms",
        "p99_ms",
        "min_ms",
        "max_ms",
        "qps",
    ]
    with open(output_csv, "w", encoding="utf-8") as file:
        file.write(",".join(csv_header) + "\n")
        for row in rows:
            setup = row["setup"]
            values = [
                row["engine"],
                str(row["sample_size"]),
                str(row["query_count"]),
                str(row["top_k"]),
                f"{setup['load_seconds']:.6f}",
                f"{setup['index_seconds']:.6f}",
                f"{setup['total_seconds']:.6f}",
                f"{row['average_ms']:.6f}",
                f"{row['median_ms']:.6f}",
                f"{row['p95_ms']:.6f}",
                f"{row['p99_ms']:.6f}",
                f"{row['min_ms']:.6f}",
                f"{row['max_ms']:.6f}",
                f"{row['qps']:.6f}",
            ]
            file.write(",".join(values) + "\n")


def main(argv: Sequence[str]) -> None:
    args = parse_args(argv)
    settings = load_settings(args.config_path)
    ensure_directory(Path(settings.log_folder))
    log_file = Path(settings.log_folder) / "logBenchmarkVectorSearch.txt"
    open_log(str(log_file))

    sizes = sorted(set(args.sizes))
    if not sizes:
        raise ValueError("At least one sample size must be provided.")
    if any(size <= 0 for size in sizes):
        raise ValueError("All sample sizes must be positive integers.")
    if args.query_count <= 0:
        raise ValueError("query_count must be positive.")
    if args.top_k <= 0:
        raise ValueError("top_k must be positive.")
    if args.warmup_count < 0:
        raise ValueError("warmup_count cannot be negative.")

    cache_paths = make_cache_paths(settings)
    metadata = build_sample_cache(
        settings=settings,
        cache_paths=cache_paths,
        sizes=sizes,
        query_count=args.query_count,
        batch_size=args.batch_size,
        refresh_cache=args.refresh_cache,
    )
    query_vectors = load_query_vectors(cache_paths.query_parquet)

    if len(query_vectors) != args.query_count:
        raise RuntimeError(
            f"Expected {args.query_count} held-out query vectors, but found {len(query_vectors)} in the cache."
        )

    logging.info(
        "Starting benchmark: sizes=%s, query_count=%s, top_k=%s, dimension=%s",
        sizes,
        args.query_count,
        args.top_k,
        metadata.dimension,
    )

    results: List[BenchmarkRun] = []
    for sample_size in sizes:
        logging.info("Running benchmark for sample size %s", sample_size)
        result = run_for_sample_size(
            settings=settings,
            cache_paths=cache_paths,
            metadata=metadata,
            sample_size=sample_size,
            query_vectors=query_vectors,
            top_k=args.top_k,
            warmup_count=args.warmup_count,
            duckdb_threads=args.duckdb_threads,
        )
        results.append(result)
        logging.info("Completed benchmark for sample size %s", sample_size)

    output_json = cache_paths.root / "benchmark_results.json"
    output_csv = cache_paths.root / "benchmark_results.csv"
    write_results(results, output_json, output_csv)

    for result in results:
        for summary in [result.duckdb, result.pgvector]:
            logging.info(
                "%s | n=%s | avg=%.3f ms | p95=%.3f ms | setup=%.3f s",
                summary.engine,
                summary.sample_size,
                summary.average_ms,
                summary.p95_ms,
                summary.setup.total_seconds,
            )

    logging.info("Benchmark results written to %s and %s", output_json, output_csv)


if __name__ == "__main__":
    main(sys.argv[1:])







