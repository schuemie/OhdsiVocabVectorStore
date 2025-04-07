OhdsiVocabVectorStore
=====================

These scripts can be used to create embedding vectors for standard concepts in the OHDSI vocabulary, and upload them to a vector store.
The embedding vectors are not only created for the standard concept name, but (optionally) also for the concept synonyms and source terms mapped to standard concepts.
These other concept names are simply concatenated to the standard concept name, and the embedding vector is created for the concatenated string.
Research has shown this helps to improve the performance when searching for concepts in the vocabulary.

The scripts ultimately create a table on a database server with the embedding vectors for the vocabulary.
It is recommended to combine this with the tables of the original vocabulary, so that the embedding vector table can be joined with tables in the vocabulary.

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

# Download the OHDSI vocabulary concept names

We assume you have the OHDSI Vocabulary on a database server somewhere. 
We must download the names of the concepts in the vocabulary to a local SQLite database. 
This is done using the `VocabDownload.py` script.

To run the download script, you need to set the `source_connection_string` environment variable. 
This should be a connection string to your database server. 
The string should specify the database and schema if applicable. 
For example, if you are using PostgreSQL, it should look like this:

```bash
export source_connection_string="postgresql://username:password@hostname:port/database?options=-csearch_path%3Dvocab_schema"
```

Note that you may need to install appropriate database drivers (SQL connector and SqlAlchemy) for your database. The current `requirements.txt` file includes drivers for DataBricks.

You must also modify `VocabDownload.yaml` to specify where the vocabulary (and the log) should be written to. 
Here you can also specify which domains to download. Leave empty to fetch all domains.

After that, you can run the download script:

```bash
PYTHONPATH=./: python VocabDownload.py VocabDownload.yaml
```


# Create the embedding vectors

The `CreateEmbeddings.py` script creates the embedding vectors for the vocabulary. 
The current implementation uses the Azure OpenAI API to create the embeddings. 
The terms are read from the SQLite database created in the previous step, and the embedding vectors written to parquet files.
You need to set the following environment variables: `genai_embed_key` and `genai_embed_endpoint`.
These should be the key and endpoint for your Azure OpenAI resource, and may look like this:

```bash
export genai_embed_key="abcdefghijklmnopqrs"
export genai_embed_endpoint="https://genaiapimna.some.com/openai-embeddings/openai/deployments/text-embedding-3-large/embeddings?api-version=2022-12-01"
```

You must modify `CreateEmbeddings.yaml` to specify file locations, and whether to include concept synonyms and source terms mapped to standard concepts (recommended).

After that, you can run the embedding script:
```bash
PYTHONPATH=./: python CreateEmbeddings.py CreateEmbeddings.yaml
```

# Upload the embedding vectors to a vector store

The `UploadEmbeddings.py` script uploads the embedding vectors to a vector store.
The current implementation only supports PGVector, but other vector stores can be added in the future.

You need to set the `target_connection_string` environment variable.
This should be a connection string to your database server, and may look like this:

```bash
export target_connection_string="postgresql://username:password@hostname:port/database"
```

You must also modify `UploadEmbeddings.yaml` to specify the target schema and the table name for the vector store.
The table will be created if it does not exist.

After that, you can run the upload script:
```bash
PYTHONPATH=./: python UploadEmbeddings.py UploadEmbeddings.yaml
```

## Creating the vector index
Once the vectors are loaded, an index needs to be created. 

```sql
SET maintenance_work_mem = '10GB'
SET max_parallel_maintenance_workers = 4
CREATE INDEX ON vocab_vectors_schema.vocab_vectors_table USING hnsw (embedding halfvec_cosine_ops)
```

Where `vocab_vectors_schema` is the schema where the vectors are stored and `vocab_vectors_table` is the table name.

Note that, on Windows, the maximum maintenance work memory is 1.9GB, until Postgres 18: https://commitfest.postgresql.org/patch/5343/

