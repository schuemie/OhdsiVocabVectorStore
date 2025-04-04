import json
import logging
import os
import sys
from typing import List

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml
from dotenv import load_dotenv
from numpy import ndarray
from sqlalchemy import create_engine, select, MetaData, Table, func
from sqlalchemy.engine import Engine

from CreateEmbeddingsSettings import CreateEmbeddingsSettings
from Logging import open_log

load_dotenv()


def create_query(engine: Engine, use_mapped_terms: bool, use_synonyms: bool) -> select:
    metadata = MetaData()
    terms = Table('terms', metadata, autoload_with=engine)

    unique_names = select(
        terms.c.concept_id,
        terms.c.concept_name,
    ).group_by(
        terms.c.concept_id,
        terms.c.concept_name
    ).subquery()
    query = select(
        unique_names.c.concept_id,
        func.group_concat(unique_names.c.concept_name, '; ').label('concatenated_terms')
    ).group_by(
        unique_names.c.concept_id
    )
    if not use_mapped_terms:
        query = query.where(terms.c.source != 'mapped')
    if not use_synonyms:
        query = query.where(terms.c.source != 'synonym')
    return query


def create_embedding(text: List[str]) -> ndarray:
    payload = json.dumps({"input": text})
    headers = {
        'api-key': os.environ["genai_embed_key"],
        'Content-Type': 'application/json'
    }
    response = requests.post(
        os.environ["genai_embed_endpoint"],
        headers=headers,
        data=payload
    )
    if response.status_code != 200:
        logging.error(f"Error: {response.status_code} - {response.text}")
        raise Exception(f"Error: {response.status_code} - {response.text}")
    response_data = response.json()
    if 'data' not in response_data:
        logging.error(f"Error: {response_data}")
        raise Exception(f"Error: {response_data}")
    embeddings = [data['embedding'] for data in response_data['data']]
    embeddings = np.array(embeddings)
    return embeddings


def store_in_parquet(concept_ids: List[int],
                     embeddings: ndarray,
                     file_name: str) -> None:
    concept_id_array = pa.array(concept_ids)
    embedding_arrays = [pa.array(embeddings[:, i]) for i in range(embeddings.shape[1])]

    table = pa.Table.from_arrays(
        arrays=[concept_id_array] + embedding_arrays,
        names=["concept_id"] + [f"embedding_{i}" for i in range(embeddings.shape[1])]
    )
    pq.write_table(table, file_name)


def main(args: List[str]):
    with open(args[0]) as file:
        config = yaml.safe_load(file)
    settings = CreateEmbeddingsSettings(config)
    open_log(settings.log_path)
    logging.info("Starting to create embedding vectors")
    os.makedirs(settings.parquet_folder, exist_ok=True)

    engine = create_engine(f"sqlite:///{settings.sqlite_path}")
    query = create_query(engine=engine, use_synonyms=settings.use_synonyms, use_mapped_terms=settings.use_mapped_terms)

    total_count = 0
    with engine.connect() as connection:
        result_proxy = connection.execute(query)
        while True:
            chunk = result_proxy.fetchmany(settings.batch_size)
            if not chunk:
                break
            file_name = f"EmbeddingVectors{total_count + 1}_{total_count + len(chunk)}.parquet"
            file_name = os.path.join(settings.parquet_folder, file_name)
            if not os.path.isfile(file_name):
                texts = [row.concatenated_terms for row in chunk]
                concept_ids = [row.concept_id for row in chunk]
                texts = [t[:settings.max_text_characters] for t in texts]
                embeddings = create_embedding(texts)
                store_in_parquet(concept_ids=concept_ids,
                                 embeddings=embeddings,
                                 file_name=file_name)
            total_count += len(chunk)
            logging.info(f"Created {len(chunk)} embedding vectors, total: {total_count}")
    logging.info("Finished creating embedding vectors")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Must provide path to yaml file as argument")
    else:
        main(sys.argv[1:])
