import logging
import os
import sys
from typing import List

import psycopg
import yaml
from psycopg import sql
from dotenv import load_dotenv
from sqlalchemy import create_engine

from Logging import open_log
from Settings import Settings

load_dotenv()


def main(args: List[str]):
    with open(args[0]) as file:
        config = yaml.safe_load(file)
    settings = Settings(config)
    os.makedirs(settings.log_folder, exist_ok=True)
    open_log(os.path.join(settings.log_folder, "logCreateConceptRecordCountTable.txt"))

    engine = create_engine(os.getenv("vocab_connection_string"))
    conn = engine.raw_connection()
    cursor = conn.cursor()

    logging.info("Uploading observed concept counts to temp table")
    statement = sql.SQL("CREATE TEMP TABLE obs_concept_counts (concept_id INT PRIMARY KEY, record_count FLOAT);")
    cursor.execute(statement)
    with open('ConceptRecordCounts.csv', 'r') as file:
        with cursor.copy("COPY obs_concept_counts FROM STDIN WITH CSV HEADER") as copy:
            for line in file:
                copy.write(line)
    cursor.execute("COMMIT;")
    logging.info("Including ancestors, creating concept_record_count table")
    statement = sql.SQL("""
    CREATE TABLE {schema}.{table} AS
    SELECT ancestor_concept_id AS concept_id,
         SUM(record_count) AS record_count
    FROM obs_concept_counts
    INNER JOIN {schema}.concept_ancestor
        ON concept_id = descendant_concept_id
    GROUP BY ancestor_concept_id;
    """).format(
        schema=sql.Identifier(settings.schema),
        table=sql.Identifier(settings.record_count_table)
    )
    cursor.execute(statement)
    cursor.execute("COMMIT;")

    logging.info("Creating index")
    statement = sql.SQL("CREATE INDEX IF NOT EXISTS idx_concept_count_concept_id ON {schema}.{table} (concept_id);").format(
        schema=sql.Identifier(settings.schema),
        table=sql.Identifier(settings.record_count_table)
    )
    cursor.execute(statement)
    cursor.execute("COMMIT;")
    cursor.close()
    logging.info("Finished creating concept_record_count table")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Must provide path to yaml file as argument")
    else:
        main(sys.argv[1:])
