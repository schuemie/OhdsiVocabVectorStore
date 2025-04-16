import logging
import os
import sys
from typing import List

import psycopg
import pyarrow.parquet as pq
import yaml
from dotenv import load_dotenv
from psycopg import sql, connection
from pgvector.psycopg import register_vector
from sqlalchemy import create_engine
from tqdm import tqdm

from Settings import Settings
from Logging import open_log

load_dotenv()


def get_vector_size(parquet_folder: str):
    """
    Get the size of the vector from the first Parquet file in the folder.
    """
    file_list = sorted([f for f in os.listdir(parquet_folder) if f.endswith(".parquet")])
    if len(file_list) == 0:
        raise Exception("No Parquet files found in the specified folder.")
    file_path = os.path.join(parquet_folder, file_list[0])
    parquet_file = pq.ParquetFile(file_path)
    row_group = parquet_file.read_row_group(0)
    return row_group.num_columns - 2

def create_table_in_pgvector(conn: connection, schema: str, table: str, vector_type: str, dimensions: int):
    statement = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {schema}.{table} (concept_id INT , standard_concept_id INT,  embedding_vector {vector_type}({dimensions}))").format(
        vector_type=sql.SQL(vector_type),
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        dimensions=sql.Literal(dimensions)
    )
    conn.execute(statement)

def load_vectors_in_pgvector(settings: Settings):
    # Unable to register_vector when using sqlalchemy, so using psycopg directly:
    conn = psycopg.connect(os.getenv("vocab_connection_string").replace("+psycopg", ""))

    # conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
    register_vector(conn)
    vector_type = "vector" if settings.store_type == settings.PGVECTOR else "halfvec"

    # Create table if it doesn't exist
    vector_size = get_vector_size(settings.embeddings_folder)
    create_table_in_pgvector(conn, settings.schema, settings.vector_table, vector_type, vector_size)

    cur = conn.cursor()
    statement = sql.SQL("COPY {schema}.{table} (concept_id, standard_concept_id, embedding_vector) FROM STDIN WITH (FORMAT BINARY)").format(
        schema=sql.Identifier(settings.schema),
        table=sql.Identifier(settings.vector_table)
    )
    with cur.copy(statement) as copy:
        copy.set_types(["int4", "int4", vector_type])

        # Iterate over Parquet files:
        total_count = 0
        file_list = sorted([f for f in os.listdir(settings.embeddings_folder) if f.endswith(".parquet")])
        for i in tqdm(range(0, len(file_list))):
            file_name = file_list[i]
            logging.info(f"Processing Parquet file '{file_name}'")
            file_path = os.path.join(settings.embeddings_folder, file_name)
            parquet_file = pq.ParquetFile(file_path)
            for row_group_idx in range(parquet_file.num_row_groups):
                row_group = parquet_file.read_row_group(row_group_idx)
                concept_ids = row_group.column("concept_id").to_pylist()
                standard_concept_ids = row_group.column("standard_concept_id").to_pylist()
                embedding_columns = [row_group.column(i).to_pylist() for i in range(2, row_group.num_columns)]
                logging.info(f"- Inserting {len(concept_ids)} vectors")
                # Iterate over rows
                for j, embedding in enumerate(zip(*embedding_columns)):
                    concept_id = int(concept_ids[j])
                    standard_concept_id = int(standard_concept_ids[j])
                    copy.write_row([concept_id, standard_concept_id, embedding])
                total_count = total_count + len(concept_ids)
                logging.info(f"- Inserted {total_count} vectors in total")
            # Flush data
        while conn.pgconn.flush() == 1:
            pass

    query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
        schema=sql.Identifier(settings.schema),
        table=sql.Identifier(settings.vector_table)
    )
    result = cur.execute(query)
    count = result.fetchone()[0]
    logging.info(f"Index size is now {count} records")
    conn.commit()


def main(args: List[str]):
    with open(args[0]) as file:
        config = yaml.safe_load(file)
    settings = Settings(config)
    os.makedirs(settings.log_folder, exist_ok=True)
    open_log(os.path.join(settings.log_folder, "logUploadEmbeddingVectors.txt"))

    logging.info("Starting uploading embedding vectors")
    load_vectors_in_pgvector(settings=settings)
    logging.info("Finished uploading embedding vectors")



if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Must provide path to yaml file as argument")
    else:
        main(sys.argv[1:])
