# FCC BDC: load staged CSV files from Google Cloud Storage into BigQuery
# Public portfolio version
#
# Purpose:
#   Load FCC Broadband Data Collection CSV files from GCS into a BigQuery table
#   for downstream SQL analysis.
#
# Sensitive values are intentionally not hardcoded.
# Configure via environment variables before running.

suppressPackageStartupMessages({
  library(bigrquery)
  library(glue)
})

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

required_env <- c(
  "BQ_PROJECT_ID",
  "BQ_DATASET",
  "BQ_TABLE_COVERAGE",
  "GCS_SOURCE_URIS",
  "GOOGLE_APPLICATION_CREDENTIALS"
)

missing_env <- required_env[Sys.getenv(required_env) == ""]
if (length(missing_env) > 0) {
  stop(
    paste(
      "Missing required environment variables:",
      paste(missing_env, collapse = ", ")
    )
  )
}

project_id <- Sys.getenv("BQ_PROJECT_ID")
bq_dataset <- Sys.getenv("BQ_DATASET")
bq_table_coverage <- Sys.getenv("BQ_TABLE_COVERAGE")
gcs_source_uris <- Sys.getenv("GCS_SOURCE_URIS")
credentials_path <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Optional behavior flags
delete_existing_table <- tolower(Sys.getenv("DELETE_EXISTING_TABLE", unset = "true")) == "true"
autodetect_schema <- tolower(Sys.getenv("AUTODETECT_SCHEMA", unset = "true")) == "true"

# ---------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------

bq_auth(path = credentials_path)
message("BigQuery authentication successful.")

# ---------------------------------------------------------------------
# Ensure dataset exists
# ---------------------------------------------------------------------

dataset_ref <- bq_dataset(project = project_id, dataset = bq_dataset)

if (!bq_dataset_exists(dataset_ref)) {
  message("Dataset does not exist. Creating dataset: ", bq_dataset)
  bq_dataset_create(dataset_ref)
} else {
  message("Dataset exists: ", bq_dataset)
}

# ---------------------------------------------------------------------
# Define destination table
# ---------------------------------------------------------------------

table_ref_coverage <- bq_table(
  project = project_id,
  dataset = bq_dataset,
  table = bq_table_coverage
)

# ---------------------------------------------------------------------
# Optional clean start
# ---------------------------------------------------------------------

if (delete_existing_table) {
  message("Deleting existing table if present: ", bq_table_coverage)
  bq_table_delete(table_ref_coverage, not_found_ok = TRUE)
}

# ---------------------------------------------------------------------
# Load from GCS to BigQuery
# ---------------------------------------------------------------------

message(glue(
  "Loading data from GCS into BigQuery:\n",
  "  Project: {project_id}\n",
  "  Dataset: {bq_dataset}\n",
  "  Table:   {bq_table_coverage}\n",
  "  Source:  {gcs_source_uris}"
))

load_job <- bq_perform_load(
  x = table_ref_coverage,
  source_uris = gcs_source_uris,
  source_format = "CSV",
  csv_options = list(skipLeadingRows = 1L),
  write_disposition = "WRITE_TRUNCATE",
  autodetect = autodetect_schema
)

bq_job_wait(load_job)

message("BigQuery load complete.")

# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------

table_meta <- bq_table_meta(table_ref_coverage)
message("Rows loaded: ", format(table_meta$numRows, big.mark = ","))

# Optional preview query
preview_query <- glue("
  SELECT *
  FROM `{project_id}.{bq_dataset}.{bq_table_coverage}`
  LIMIT 10
")

preview_job <- bq_project_query(project_id, query = preview_query)
preview_df <- bq_table_download(preview_job)

print(preview_df)