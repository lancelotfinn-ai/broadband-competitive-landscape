# FCC BDC download-to-cloud workflow
# Public portfolio version
#
# Purpose:
# 1. Query the FCC BDC API for fixed broadband availability CSV files
# 2. Trigger a Cloud Function to transfer selected files
# 3. Optionally reorganize GCS objects into state-based paths
#
# Sensitive values are intentionally NOT hardcoded.
# Set configuration through environment variables before running.

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(readr)
  library(googleCloudStorageR)
  library(stringr)
})

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

required_env <- c(
  "FCC_USERNAME",
  "FCC_HASH_VALUE",
  "FCC_BASE_URL",
  "FCC_TARGET_DATE",
  "FCC_TARGET_CATEGORY",
  "CLOUD_FUNCTION_URL",
  "GCS_BUCKET_NAME",
  "GCS_KEY_FILE_PATH"
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

FCC_USERNAME <- Sys.getenv("FCC_USERNAME")
FCC_HASH_VALUE <- Sys.getenv("FCC_HASH_VALUE")
BASE_URL <- Sys.getenv("FCC_BASE_URL")               # e.g. "https://bdc.fcc.gov"
TARGET_DATE_PATH <- Sys.getenv("FCC_TARGET_DATE")    # e.g. "2024-12-31"
TARGET_CATEGORY <- Sys.getenv("FCC_TARGET_CATEGORY") # e.g. "State"
TARGET_SUBCATEGORY <- Sys.getenv("FCC_TARGET_SUBCATEGORY", unset = "")
TARGET_TECHNOLOGY_TYPE <- Sys.getenv("FCC_TARGET_TECHNOLOGY_TYPE", unset = "")
TARGET_SPEED_TIER <- Sys.getenv("FCC_TARGET_SPEED_TIER", unset = "")
CLOUD_FUNCTION_URL <- Sys.getenv("CLOUD_FUNCTION_URL")
GCS_BUCKET_NAME <- Sys.getenv("GCS_BUCKET_NAME")
GCS_KEY_FILE_PATH <- Sys.getenv("GCS_KEY_FILE_PATH")

# Optional behavior flags
REORGANIZE_GCS <- tolower(Sys.getenv("REORGANIZE_GCS", unset = "true")) == "true"
DRY_RUN <- tolower(Sys.getenv("DRY_RUN", unset = "false")) == "true"

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

assert_http_success <- function(response, context) {
  if (http_error(response)) {
    error_message <- content(response, "text", encoding = "UTF-8")
    stop(
      paste0(
        context,
        " failed. HTTP Status: ",
        status_code(response),
        "\nDetails: ",
        error_message
      )
    )
  }
}

list_gcs_files <- function(bucket_name) {
  message("Listing existing files in GCS bucket: ", bucket_name)
  gcs_objects <- gcs_list_objects(bucket_name)

  if (is.null(gcs_objects) || nrow(gcs_objects) == 0) {
    return(character(0))
  }

  basename(gcs_objects$name)
}

reorganize_gcs_objects <- function(bucket_name, dry_run = FALSE) {
  message("Listing all objects in bucket: ", bucket_name, " for possible reorganization.")
  all_gcs_objects <- gcs_list_objects(bucket_name)

  if (is.null(all_gcs_objects) || nrow(all_gcs_objects) == 0) {
    message("No objects found in the bucket. Nothing to reorganize.")
    return(invisible(NULL))
  }

  files_to_reorganize <- all_gcs_objects %>%
    filter(str_detect(name, "\\.csv/.*\\.csv$")) %>%
    mutate(
      fcc_file_name = basename(name),
      old_folder_name = dirname(name)
    ) %>%
    filter(old_folder_name == fcc_file_name)

  if (nrow(files_to_reorganize) == 0) {
    message("No files found that match the self-named-folder pattern.")
    return(invisible(NULL))
  }

  message("Found ", nrow(files_to_reorganize), " files to reorganize.")

  for (i in seq_len(nrow(files_to_reorganize))) {
    current_file <- files_to_reorganize[i, ]
    source_object_name <- current_file$name
    fcc_file_name <- current_file$fcc_file_name

    state_code_match <- str_match(fcc_file_name, "bdc_(\\d{2})_")
    if (length(state_code_match) < 2 || is.na(state_code_match[2])) {
      warning("Could not extract state code from file name: ", fcc_file_name)
      next
    }

    state_code <- state_code_match[2]
    new_target_path <- paste0("state_", state_code, "/", fcc_file_name)

    message("Reorganizing: ", source_object_name, " -> ", new_target_path)

    if (dry_run) {
      next
    }

    tryCatch(
      {
        gcs_copy_object(
          source_object = source_object_name,
          source_bucket = bucket_name,
          destination_object = new_target_path,
          destination_bucket = bucket_name
        )

        gcs_delete_object(
          object_name = source_object_name,
          bucket = bucket_name
        )

        message("Successfully reorganized: ", fcc_file_name)
      },
      error = function(e) {
        warning("Failed to reorganize file '", fcc_file_name, "': ", e$message)
      }
    )

    Sys.sleep(0.5)
  }

  invisible(NULL)
}

fetch_fcc_file_list <- function() {
  list_endpoint_path <- paste0("/api/public/map/downloads/listAvailabilityData/", TARGET_DATE_PATH)
  list_full_url <- paste0(BASE_URL, list_endpoint_path)

  list_query_params <- list(
    category = if (nzchar(TARGET_CATEGORY)) TARGET_CATEGORY else NULL,
    subcategory = if (nzchar(TARGET_SUBCATEGORY)) TARGET_SUBCATEGORY else NULL,
    technology_type = if (nzchar(TARGET_TECHNOLOGY_TYPE)) TARGET_TECHNOLOGY_TYPE else NULL,
    speed_tier = if (nzchar(TARGET_SPEED_TIER)) TARGET_SPEED_TIER else NULL
  )
  list_query_params <- list_query_params[!vapply(list_query_params, is.null, logical(1))]

  list_headers <- add_headers(
    username = FCC_USERNAME,
    hash_value = FCC_HASH_VALUE
  )

  message("Calling FCC API: ", list_full_url)
  response <- GET(list_full_url, list_headers, query = list_query_params)
  assert_http_success(response, "FCC file list request")

  file_list_json <- content(response, "text", encoding = "UTF-8")
  file_list_df <- fromJSON(file_list_json, flatten = TRUE)

  if (!"data" %in% names(file_list_df) || !is.data.frame(file_list_df$data)) {
    stop("Unexpected API response format: 'data' element is missing or not a data frame.")
  }

  file_list_df$data
}

trigger_cloud_function <- function(target_files, existing_gcs_files, dry_run = FALSE) {
  for (i in seq_len(nrow(target_files))) {
    current_file <- target_files[i, ]
    file_id_to_download <- current_file$file_id
    expected_filename <- current_file$file_name

    if (expected_filename %in% existing_gcs_files) {
      message(
        "Skipping file '", current_file$file_name,
        "' (ID: ", file_id_to_download, ") because it already exists in GCS."
      )
      next
    }

    payload <- list(
      data_type = "availability",
      file_id = file_id_to_download
    )

    message(
      "Triggering Cloud Function for '", current_file$file_name,
      "' (ID: ", file_id_to_download, ")",
      " | State: ", current_file$state_name,
      " | Technology: ", current_file$technology_code_desc
    )

    if (dry_run) {
      next
    }

    cf_response <- POST(
      url = CLOUD_FUNCTION_URL,
      body = toJSON(payload, auto_unbox = TRUE),
      encode = "json"
    )

    if (http_status(cf_response)$category == "Success") {
      message(
        "Cloud Function triggered successfully for file ID ",
        file_id_to_download,
        ". Status: ", status_code(cf_response)
      )
    } else {
      warning(
        "Cloud Function call failed for file ID ", file_id_to_download,
        ". Status: ", status_code(cf_response),
        ". Reason: ", http_status(cf_response)$reason,
        ". Error details: ", content(cf_response, "text", encoding = "UTF-8")
      )
    }

    Sys.sleep(1)
  }

  invisible(NULL)
}

# ---------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------

auth_report <- gcs_auth(json_file = GCS_KEY_FILE_PATH)
message("GCS authentication successful: ", isTRUE(auth_report$success))

if (REORGANIZE_GCS) {
  reorganize_gcs_objects(GCS_BUCKET_NAME, dry_run = DRY_RUN)
}

existing_gcs_files <- list_gcs_files(GCS_BUCKET_NAME)
file_list_data <- fetch_fcc_file_list()

target_files <- file_list_data %>%
  filter(file_type == "csv") %>%
  filter(technology_type == "Fixed Broadband") %>%
  select(
    file_id,
    data_type = category,
    file_name,
    state_name,
    technology_code,
    technology_code_desc
  )

message("Found ", nrow(target_files), " potential target files for transfer.")

trigger_cloud_function(
  target_files = target_files,
  existing_gcs_files = existing_gcs_files,
  dry_run = DRY_RUN
)

message("Finished FCC download-to-cloud workflow.")