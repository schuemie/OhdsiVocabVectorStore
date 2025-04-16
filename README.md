OhdsiVocabVectorStore
=====================

These scripts can be used to create embedding vectors for standard concepts in the OHDSI vocabulary, and upload them to a vector store.
The embedding vectors are not only created for the standard concept name, but (optionally) also for the concept synonyms and source terms mapped to standard concepts.
Synonyms are simply concatenated to the standard concept name, and the embedding vector is created for the concatenated string.
Mapped terms have their own embedding vector, and can be used to search for the standard concept.
Research has shown this helps to improve the performance when searching for concepts in the vocabulary.

The script assumes the vocabulary is stored in a database server, and the embedding vectors are uploaded to a vector store on the same database server.
Optionally (recommend), the script can also upload concept record counts, aggregated across the OHDSI Evidence Network.
Filtering to concept actually used in practice can improve performance for most tasks, as well as sorting by record count.
The script currently only supports PostgreSQL, but other database servers can be added in the future.
The embedding vectors are created using the Azure OpenAI API, but other APIs can be added in the future.


# Pre-requisite

The project is built in python 3.9, and project dependency needs to be installed 

Create a new Python virtual environment

```console
python -m venv venv;
source venv/bin/activate;
```

Install the packages in requirements.txt

```console
pip install -r requirements.txt
```

# Modifying settings

The scripts all use the same YAML configuration file: `Settings.yaml`.
Please modify as required.

All scripts use the same connection string to connect to the database server.
This should be specified using the `vocab_connection_string` environment variable, using `SqlAlchemy` connection string format.
For example, it should look like this:

```bash
export vocab_connection_string="postgresql+psycopg://username:password@hostname:port/database?options=-csearch_path%3Dvocab_schema"
```

To create the embeddings you need to set the following environment variables: `genai_embed_key` and `genai_embed_endpoint`.
These should be the key and endpoint for your Azure OpenAI resource, and may look like this:

```bash
export genai_embed_key="abcdefghijklmnopqrs"
export genai_embed_endpoint="https://genaiapimna.some.com/openai-embeddings/openai/deployments/text-embedding-3-large/embeddings?api-version=2022-12-01"
```

# Create the concept record count table

The `CreateConceptRecordCount.py` script creates a table with the concept record counts.
This script will also aggregate counts across concept ancestors, using the `concept_ancestor` table on the server.

Run the script using the following command:

```bash
PYTHONPATH=./: python CreateConceptRecordCount.py Settings.yaml
```


# Download the OHDSI vocabulary concept terms

We must download the names of the concepts in the vocabulary to a local SQLite database. 
This is done using the `VocabDownload.py` script.
Note that you can specify the domains to download, and whether to include classification concepts in the `Settings.yaml` file.
If you've created the concept record count table, you can also restrict to standard concepts that are actually used in practice (and source concepts that map to them), by setting the `restrict_to_used_concepts` flag to `True`.

You can run the download script:

```bash
PYTHONPATH=./: python DownloadTerms.py Settings.yaml
```


# Create the embedding vectors

The `CreateEmbeddings.py` script creates the embedding vectors for the vocabulary. 
The current implementation uses the Azure OpenAI API to create the embeddings. 
The terms are read from the SQLite database created in the previous step, and the embedding vectors written to parquet files.

You can run the embedding script:
```bash
PYTHONPATH=./: python CreateEmbeddings.py Settings.yaml
```

# Upload the embedding vectors to a vector store

The `UploadEmbeddings.py` script uploads the embedding vectors to a table in the database server.
The table is created in the same schema as the concept record count table, and the name of the table is specified in the `Settings.yaml` file.
The table will be created if it does not exist.

Run the upload script:
```bash
PYTHONPATH=./: python UploadEmbeddings.py Settings.yaml
```

## Creating the vector index
Once the vectors are loaded, an index needs to be created. 

```sql
SET maintenance_work_mem = '10GB'
SET max_parallel_maintenance_workers = 4
CREATE INDEX ON vocab_vectors_schema.concept_vector USING hnsw (embedding_vector halfvec_cosine_ops)
```
Where `vocab_vectors_schema` is the schema where the vectors are stored and `concept_vector` is the table name.

Note that, on Windows, the maximum maintenance work memory is 1.9GB, until Postgres 18: https://commitfest.postgresql.org/patch/5343/

You can keep track of the progress of creating the index using the following command:

```sql
SELECT phase, round(100.0 * blocks_done / nullif(blocks_total, 0), 1) AS "%" FROM pg_stat_progress_create_index;
````

Finally, we also create indices on the concept id and standard concept id columns, to speed up queries.

```sql
CREATE INDEX ON vocab_vectors_schema.concept_vector(concept_id);
CREATE INDEX ON vocab_vectors_schema.concept_vector(standard_concept_id);
```


