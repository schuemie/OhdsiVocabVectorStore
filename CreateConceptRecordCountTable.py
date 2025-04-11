import logging
import os
import sys
from typing import List

import psycopg
import yaml
from psycopg import sql
from dotenv import load_dotenv

from Logging import open_log
from UploadEmbeddingVectorsSettings import UploadEmbeddingVectorsSettings

load_dotenv()


def main(args: List[str]):
    with open(args[0]) as file:
        config = yaml.safe_load(file)
    settings = UploadEmbeddingVectorsSettings(config)
    os.makedirs(os.path.dirname(settings.log_path), exist_ok=True)
    open_log(settings.log_path)

    conn = psycopg.connect(conninfo=os.getenv("target_connection_string"),
                           autocommit=True)

    logging.info("Uploading observed concept counts to temp table")
    statement = sql.SQL("CREATE TEMP TABLE obs_concept_counts (concept_id INT PRIMARY KEY, record_count FLOAT);")
    conn.execute(statement)
    with conn.cursor() as cursor:
        with open('ObservedConceptCounts.csv', 'r') as file:
            with cursor.copy("COPY obs_concept_counts FROM STDIN WITH CSV HEADER") as copy:
                for line in file:
                    copy.write(line)

    logging.info("Including ancestors, creating concept_record_count table")
    statement = sql.SQL("""
    CREATE TABLE {schema}.concept_record_count AS
    SELECT ancestor_concept_id AS concept_id,
         SUM(record_count) AS record_count
    FROM obs_concept_counts
    INNER JOIN {schema}.concept_ancestor
        ON concept_id = descendant_concept_id
    GROUP BY ancestor_concept_id;
    """).format(
        schema=sql.Identifier(settings.schema)
    )
    conn.execute(statement)

    logging.info("Creating index")
    statement = sql.SQL("CREATE INDEX IF NOT EXISTS idx_concept_count_concept_id ON {schema}.concept_record_count (concept_id);").format(
        schema=sql.Identifier(settings.schema)
    )
    conn.execute(statement)

    logging.info("Finished creating concept_record_count table")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Must provide path to yaml file as argument")
    else:
        main(sys.argv[1:])
