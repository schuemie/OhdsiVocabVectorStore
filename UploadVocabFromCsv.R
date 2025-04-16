# R script for uploading the Vocabulary from local 'CSV' files to a (Postgres) database.
# Assumes the CSV files were downloaded from Athena. The script first creates empty
# tables using the CDM 5.4.0 DDLs, then uploads the data, and finally created the 
# indices as prescribed in the CDM.
# Uploading will be faster if bulk uploading has been configured. For Postgres,
# this means settings the POSTGRES_PATH environmental variable to your Postgres
# binaries (e.g. "/Library/PostgreSQL/16/bin")

library(DatabaseConnector)
library(lubridate)

fromFolder <- "E:/Vocabulary/Feb2025"

toConnection <- connect(
  dbms = "postgresql",
  connectionString = keyring::key_get("vocab_server_connection_string"),
  user = keyring::key_get("vocab_server_user"),
  password = keyring::key_get("vocab_server_password")
)
toDatabaseSchema <- "vocabulary_feb2025"


# No changes below this lines --------------------------------------------------
tableNames <- tolower(gsub(".csv", "", list.files(fromFolder, "*.csv")))
vocabTables <- c("concept", 
                 "concept_ancestor", 
                 "concept_class", 
                 "concept_relationship",
                 "concept_synonym",
                 "domain",  
                 "drug_strength",
                 "source_to_concept_map",
                 "vocabulary")
tableNames <- tableNames[tableNames %in% vocabTables]


# Create tables ----------------------------------------------------------------
sql <- readLines("https://raw.githubusercontent.com/OHDSI/CommonDataModel/v5.4.0/inst/ddl/5.4/postgresql/OMOPCDM_postgresql_5.4_ddl.sql")
pattern <- paste(paste0("@cdmDatabaseSchema.", toupper(vocabTables)), collapse = "|")
starts <- grep(pattern, sql)
ends <- grep("\\);", sql)
allSql <- c()
for (start in starts) {
  end <- min(ends[ends > start])
  allSql <- c(allSql, sql[start:end])
}
renderTranslateExecuteSql(
  connection = toConnection,
  sql = paste(allSql, collapse = "\n"),
  cdmDatabaseSchema = toDatabaseSchema
)


# Copy table contents ----------------------------------------------------------
for (i in 1:length(tableNames)) {
  message(sprintf("Copying table %s", tableNames[i]))
  # Athena CSV files are actually tab-delimited:
  data <- readr::read_tsv(file = file.path(fromFolder, sprintf("%s.csv", toupper(tableNames[i]))),
                          show_col_types = FALSE,
                          na = "")
  for (j in grep("_date$", colnames(data))) {
    # Note: as.Date() is much too slow, so using lubridate:
    data[[j]] <- ymd(as.character(as.integer(data[[j]])))
  }

  insertTable(
    connection = toConnection,
    databaseSchema = toDatabaseSchema,
    tableName = tableNames[i],
    data = data,
    dropTableIfExists = FALSE,
    createTable = FALSE,
    progressBar = TRUE
  )
}


# Create indices ---------------------------------------------------------------
sql <- readLines("https://raw.githubusercontent.com/OHDSI/CommonDataModel/v5.4.0/inst/ddl/5.4/postgresql/OMOPCDM_postgresql_5.4_indices.sql")
pattern <- paste(paste0("@cdmDatabaseSchema.", vocabTables), collapse = "|")
sql <- sql[grepl(pattern, sql)]

renderTranslateExecuteSql(
  connection = toConnection,
  sql = paste(sql, collapse = "\n"),
  cdmDatabaseSchema = toDatabaseSchema
)

disconnect(toConnection)
