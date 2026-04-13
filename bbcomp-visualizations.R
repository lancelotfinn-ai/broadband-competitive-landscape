# Selected figures from FCC BDC broadband competition analysis
# Public portfolio version
#
# Purpose:
#   Query BigQuery tables produced by the broadband competition workflow
#   and generate a small set of representative charts.
#
# Sensitive values are intentionally not hardcoded.
# Configure via environment variables before running.

suppressPackageStartupMessages({
  library(bigrquery)
  library(ggplot2)
  library(dplyr)
  library(tidyr)
  library(scales)
  library(ggrepel)
})

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

required_env <- c(
  "BQ_PROJECT_ID",
  "BQ_DATASET",
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
dataset_name <- Sys.getenv("BQ_DATASET")
credentials_path <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")
output_dir <- Sys.getenv("OUTPUT_DIR", unset = "figures")

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

bq_auth(path = credentials_path)
message("BigQuery authentication successful.")

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

run_query <- function(sql) {
  job <- bq_project_query(project_id, query = sql)
  bq_table_download(job)
}

save_plot <- function(plot_obj, filename, width = 10, height = 6) {
  ggsave(
    filename = file.path(output_dir, filename),
    plot = plot_obj,
    width = width,
    height = height,
    dpi = 300
  )
}

tech_labels <- c(
  "10" = "Copper/DSL",
  "40" = "Cable/Coax",
  "50" = "Fiber",
  "60" = "GEO Satellite",
  "61" = "LEO Satellite",
  "70" = "Unlicensed FW",
  "71" = "Licensed FW",
  "72" = "LBR FW"
)

tech_order <- c(50, 40, 71, 70, 72, 10, 61, 60)

# ---------------------------------------------------------------------
# 1) Reach by technology
# ---------------------------------------------------------------------

sql_reach_all <- paste0("
  SELECT technology, COUNT(DISTINCT location_id) AS numLocs
  FROM `", project_id, ".", dataset_name, ".national_coverage`
  GROUP BY technology
")

sql_reach_100_20 <- paste0("
  SELECT technology, COUNT(DISTINCT location_id) AS numLocs
  FROM `", project_id, ".", dataset_name, ".national_coverage`
  WHERE max_advertised_download_speed >= 100
    AND max_advertised_upload_speed >= 20
    AND low_latency = 1
  GROUP BY technology
")

reach_all <- run_query(sql_reach_all) %>%
  mutate(category = "All Locations")

reach_100_20 <- run_query(sql_reach_100_20) %>%
  mutate(category = "100/20+ Low Latency")

reach_plot_data <- bind_rows(reach_all, reach_100_20) %>%
  filter(technology != 0) %>%
  mutate(
    numLocs = as.numeric(numLocs),
    category = factor(category, levels = c("All Locations", "100/20+ Low Latency")),
    technology_name = factor(
      technology,
      levels = tech_order,
      labels = tech_labels[as.character(tech_order)]
    )
  ) %>%
  complete(technology_name, category, fill = list(numLocs = 0))

p_reach <- ggplot(reach_plot_data, aes(x = technology_name, y = numLocs, fill = category)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Number of Locations Served by Technology",
    x = "Technology",
    y = "Number of Locations",
    fill = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(hjust = 0.5),
    legend.position = "bottom"
  )

print(p_reach)
save_plot(p_reach, "reach_by_technology.png")

# ---------------------------------------------------------------------
# 2) ISP portfolio typology
# ---------------------------------------------------------------------

sql_typology <- paste0("
  SELECT techtypology, COUNT(*) AS numISPs
  FROM `", project_id, ".", dataset_name, ".frn_techtypology`
  GROUP BY techtypology
  ORDER BY numISPs DESC
")

typology_df <- run_query(sql_typology)

custom_order <- c(
  "Fiber only",
  "Cable & Fiber",
  "DSL & Fiber",
  "DSL, Cable & Fiber",
  "Cable, No Fiber",
  "Fiber & FW",
  "Multiple Wireline Technologies & FW",
  "FW only",
  "DSL only",
  "Other (incl. satellite)"
)

typology_df <- typology_df %>%
  mutate(
    techtypology = factor(techtypology, levels = custom_order),
    numISPs = as.numeric(numISPs),
    chart_label = ifelse(
      numISPs < 100,
      NA_character_,
      paste0(techtypology, ": ", comma(numISPs))
    )
  )

p_typology <- ggplot(typology_df, aes(x = "", y = numISPs, fill = techtypology)) +
  geom_col(width = 1, color = "white") +
  coord_polar(theta = "y") +
  geom_text(
    aes(label = chart_label),
    position = position_stack(vjust = 0.5),
    size = 3
  ) +
  labs(
    title = "ISP Technology Portfolios",
    fill = "Technology Typology",
    x = NULL,
    y = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text = element_blank(),
    panel.grid = element_blank(),
    plot.title = element_text(hjust = 0.5),
    legend.position = "right"
  )

print(p_typology)
save_plot(p_typology, "isp_portfolio_typology.png", width = 11, height = 7)

# ---------------------------------------------------------------------
# 3) Industry structure: number of ISPs and weighted average footprint
# ---------------------------------------------------------------------

sql_isp_size_by_tech <- paste0("
  SELECT
    technology,
    SUM(numLocs * numLocs) / SUM(numLocs) AS weighted_average_numLocs,
    COUNT(frn) AS numISPs
  FROM (
    SELECT frn, technology, COUNT(DISTINCT location_id) AS numLocs
    FROM `", project_id, ".", dataset_name, ".national_coverage`
    GROUP BY frn, technology
  ) AS isp_tech_footprints
  GROUP BY technology
")

isp_size_df <- run_query(sql_isp_size_by_tech) %>%
  filter(technology != 0) %>%
  mutate(
    weighted_average_numLocs = as.numeric(weighted_average_numLocs),
    numISPs = as.numeric(numISPs),
    technology_name = factor(
      technology,
      levels = names(tech_labels),
      labels = tech_labels
    )
  )

p_structure <- ggplot(isp_size_df, aes(x = numISPs, y = weighted_average_numLocs)) +
  geom_point(size = 4, alpha = 0.8) +
  geom_text_repel(aes(label = technology_name), max.overlaps = Inf) +
  scale_x_log10(labels = comma) +
  scale_y_log10(labels = comma) +
  labs(
    title = "Industry Structure by Technology",
    subtitle = "Both axes shown on a log scale",
    x = "Number of ISPs",
    y = "Weighted Average Footprint (Locations)"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5)
  )

print(p_structure)
save_plot(p_structure, "industry_structure_by_technology.png")

# ---------------------------------------------------------------------
# 4) Speed trends over time by technology
# ---------------------------------------------------------------------

sql_speed_down <- paste0("
  SELECT *
  FROM `", project_id, ".", dataset_name, ".techspeeds_long`
")

sql_speed_up <- paste0("
  SELECT *
  FROM `", project_id, ".", dataset_name, ".techspeeds_long_up`
")

speed_down <- run_query(sql_speed_down) %>%
  mutate(speed_type = "Download", geom_avg_speed = geomavg_download_speed) %>%
  select(data_period, technology, speed_type, geom_avg_speed)

speed_up <- run_query(sql_speed_up) %>%
  mutate(speed_type = "Upload", geom_avg_speed = geomavg_upload_speed) %>%
  select(data_period, technology, speed_type, geom_avg_speed)

speed_df <- bind_rows(speed_down, speed_up) %>%
  mutate(
    technology_name = recode(
      as.character(technology),
      !!!tech_labels
    ),
    data_period = factor(
      data_period,
      levels = c("2023-06", "2023-12", "2024-06", "2024-12"),
      ordered = TRUE
    )
  ) %>%
  filter(!is.na(technology_name))

p_speed_trends <- ggplot(
  speed_df,
  aes(
    x = data_period,
    y = geom_avg_speed,
    color = speed_type,
    group = interaction(technology_name, speed_type)
  )
) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  facet_wrap(~ technology_name, nrow = 1, strip.position = "bottom") +
  scale_y_log10(labels = comma) +
  labs(
    title = "Broadband Speed Trends by Technology",
    subtitle = "Geometric average speeds by reporting period",
    x = NULL,
    y = "Speed (Mbps, log scale)",
    color = NULL
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5),
    axis.text.x = element_blank(),
    legend.position = "bottom",
    panel.spacing = unit(0.2, "lines")
  )

print(p_speed_trends)
save_plot(p_speed_trends, "speed_trends_by_technology.png", width = 14, height = 4.5)

message("Selected figures complete. Output written to: ", output_dir)