import logging
import os
import sys
from typing import List

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, select, cast, String, union_all, MetaData, Table, Column, Integer, or_, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import aliased

from Logging import open_log
from Settings import Settings

load_dotenv()


def create_query(engine: Engine,
                 settings: Settings) -> select:
    metadata = MetaData()

    concept = Table('concept',
                    metadata,
                    schema=settings.schema,
                    autoload_with=engine)
    concept_synonym = Table('concept_synonym',
                            metadata,
                            schema=settings.schema,
                            autoload_with=engine)
    concept_relationship = Table('concept_relationship',
                                 metadata,
                                 schema=settings.schema,
                                 autoload_with=engine)

    standard_concepts = ['S']
    if settings.include_classification_concepts:
        standard_concepts.append('C')

    # Get concept names
    query1 = select(
        concept.c.concept_id,
        concept.c.concept_name,
        cast('name', String).label('source')
    ).where(
        concept.c.standard_concept.in_(standard_concepts)
    )
    if settings.domain_ids:
        query1 = query1.where(concept.c.domain_id.in_(settings.domain_ids))
    if settings.classification_vocabularies:
        query1 = query1.where(or_(concept.c.standard_concept == 'S',
                                  concept.c.vocabulary_id.in_(settings.classification_vocabularies)))
    if settings.restrict_to_used_concepts:
        concept_record_count = Table('concept_record_count',
                                     metadata,
                                     schema=settings.schema,
                                     autoload_with=engine)
        query1 = query1.join(
            concept_record_count, concept.c.concept_id == concept_record_count.c.concept_id
        )

    standard_concept_ids = query1.subquery()

    # Get concept synonyms
    query2 = select(
        concept_synonym.c.concept_id,
        concept_synonym.c.concept_synonym_name.label('concept_name'),
        cast('synonym', String).label('source')
    ).where(
        concept_synonym.c.concept_id.in_(select(standard_concept_ids.c.concept_id)),
    )

    # Get mapped concepts
    mapped_concept_ids = select(
        concept_relationship.c.concept_id_1.label('concept_id')
    ).join(
        standard_concept_ids, concept_relationship.c.concept_id_2 == standard_concept_ids.c.concept_id
    ).where(
        concept_relationship.c.relationship_id == 'Maps to'
    ).group_by(
        concept_relationship.c.concept_id_1
    ).subquery()

    query3 = select(
        concept.c.concept_id,
        concept.c.concept_name,
        cast('mapped', String).label('source')
    ).where(
        concept.c.concept_id.in_(select(mapped_concept_ids.c.concept_id)),
    ).group_by(
        concept.c.concept_id,
        concept.c.concept_name
    )

    # Get synonyms for mapped concepts
    query4 = select(
        concept_synonym.c.concept_id,
        concept_synonym.c.concept_synonym_name.label('concept_name'),
        cast('mapped synonym', String).label('source')
    ).where(
        concept_synonym.c.concept_id.in_(select(mapped_concept_ids.c.concept_id)),
    )

    final_query = union_all(query1, query2, query3, query4)
    return final_query


def log_counts(target_engine: Engine, terms_table: Table):
    query = select(
        terms_table.c.source,
        func.count(terms_table.c.concept_id).label('count')
    ).group_by(
        terms_table.c.source
    )
    with target_engine.connect() as connection:
        result_set = connection.execution_options(stream_results=True).execute(query)
        for row in result_set:
            logging.info(f"Source: {row[0]}, Count: {row[1]}")
    logging.info("Finished logging counts")


def main(args: List[str]):
    with open(args[0]) as file:
        config = yaml.safe_load(file)
    settings = Settings(config)
    os.makedirs(settings.log_folder, exist_ok=True)
    os.makedirs(os.path.dirname(settings.terms_db_path), exist_ok=True)
    open_log(os.path.join(settings.log_folder, "logDownloadTerms.txt"))

    logging.info("Starting downloading vocabularies")
    source_engine = create_engine(os.getenv("vocab_connection_string"))
    query = create_query(engine=source_engine,
                         settings=settings)

    target_engine = create_engine(f"sqlite:///{settings.terms_db_path}")
    metadata = MetaData()
    terms_table = Table('terms', metadata,
                        Column('concept_id', Integer),
                        Column('concept_name', String),
                        Column('source', String))
    metadata.create_all(bind=target_engine, tables=[terms_table])

    with source_engine.connect() as source_connection, target_engine.connect() as target_connection:
        terms_result_set = source_connection.execution_options(stream_results=True).execute(query)
        total_inserted = 0
        while True:
            chunk = terms_result_set.fetchmany(settings.download_batch_size)
            if not chunk:
                break
            rows = [row._mapping for row in chunk]
            with target_connection.begin() as transaction:
                target_connection.execute(terms_table.insert(), rows)
                transaction.commit()
            total_inserted += len(rows)
            logging.info(f"Inserted {len(rows)} rows, total inserted: {total_inserted}")
    logging.info("Finished downloading vocabularies")
    log_counts(target_engine, terms_table)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Must provide path to yaml file as argument")
    else:
        main(sys.argv[1:])
