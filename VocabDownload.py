import logging
import os
import sys
from typing import List

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, select, cast, String, union_all, MetaData, Table, Column, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import aliased

from Logging import open_log
from VocabDownloadSettings import VocabDownloadSettings

load_dotenv()


def create_query(engine: Engine,
                 domain_ids: List[str],
                 include_classification_concepts: bool) -> select:
    """
    Create a SQLAlchemy query to fetch concept names, synonyms, and mapped concepts from the source database.
    :param engine: SQLAlchemy engine connected to the source database.
    :param domain_ids: List of domain IDs to filter the concepts.
    :return: SQLAlchemy select object representing the query.
    """

    metadata = MetaData()

    concept = Table('concept', metadata, autoload_with=engine)
    concept_synonym = Table('concept_synonym', metadata, autoload_with=engine)
    concept_relationship = Table('concept_relationship', metadata, autoload_with=engine)

    standard_concepts = ['S']
    if include_classification_concepts:
        standard_concepts.append('C')

    # Get concept names
    query1 = select(
        concept.c.concept_id,
        concept.c.concept_name,
        cast('name', String).label('source')
    ).where(
        concept.c.standard_concept.in_(standard_concepts)
    )

    if domain_ids:
        query1 = query1.where(concept.c.domain_id.in_(domain_ids))

    # Get concept synonyms
    query2 = select(
        concept_synonym.c.concept_id,
        concept_synonym.c.concept_synonym_name.label('concept_name'),
        cast('synonym', String).label('source')
    ).select_from(concept_synonym).join(
        concept, concept_synonym.c.concept_id == concept.c.concept_id
    ).where(
        concept.c.standard_concept.in_(standard_concepts)
    ).group_by(
        concept_synonym.c.concept_id,
        concept_synonym.c.concept_synonym_name
    )

    if domain_ids:
        query2 = query2.where(concept.c.domain_id.in_(domain_ids))

    # Get mapped concepts
    target_concept = aliased(concept)
    source_concept = aliased(concept)
    query3 = select(
        target_concept.c.concept_id,
        source_concept.c.concept_name.label('concept_name'),
        cast('mapped', String).label('source')
    ).select_from(target_concept).join(
        concept_relationship, target_concept.c.concept_id == concept_relationship.c.concept_id_2
    ).join(
        source_concept, concept_relationship.c.concept_id_1 == source_concept.c.concept_id
    ).where(
        target_concept.c.standard_concept.in_(standard_concepts),
        concept_relationship.c.relationship_id == 'Maps to',
        target_concept.c.concept_id != source_concept.c.concept_id
    ).group_by(
        target_concept.c.concept_id,
        source_concept.c.concept_name
    )

    if domain_ids:
        query3 = query3.where(target_concept.c.domain_id.in_(domain_ids))

    final_query = union_all(query1, query2, query3)
    return final_query


def main(args: List[str]):
    with open(args[0]) as file:
        config = yaml.safe_load(file)
    settings = VocabDownloadSettings(config)
    os.makedirs(os.path.dirname(settings.log_path), exist_ok=True)
    os.makedirs(os.path.dirname(settings.sqlite_path), exist_ok=True)
    open_log(settings.log_path)
    logging.info("Starting downloading vocabularies")
    source_engine = create_engine(os.getenv("source_connection_string"))
    query = create_query(engine=source_engine,
                         domain_ids=settings.domain_ids,
                         include_classification_concepts=settings.include_classification_concepts)

    target_engine = create_engine(f"sqlite:///{settings.sqlite_path}")
    metadata = MetaData()
    terms_table = Table('terms', metadata,
                        Column('concept_id', Integer),
                        Column('concept_name', String),
                        Column('source', String))
    metadata.create_all(bind=target_engine, tables=[terms_table])

    with source_engine.connect() as source_connection, target_engine.connect() as target_connection:
        result_proxy = source_connection.execution_options(stream_results=True).execute(query)
        total_inserted = 0
        while True:
            chunk = result_proxy.fetchmany(settings.batch_size)
            if not chunk:
                break
            rows = [row._mapping for row in chunk]
            with target_connection.begin() as transaction:
                target_connection.execute(terms_table.insert(), rows)
                transaction.commit()
            total_inserted += len(rows)
            logging.info(f"Inserted {len(rows)} rows, total inserted: {total_inserted}")
    logging.info("Finished downloading vocabularies")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Must provide path to yaml file as argument")
    else:
        main(sys.argv[1:])
