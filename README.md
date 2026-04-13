# broadband-competitive-landscape
# Broadband Competition Analysis (BigQuery + R)

This repository contains an end-to-end data pipeline for analyzing the competitive landscape of broadband service in the United States using FCC Broadband Data Collection (BDC) data.

The project combines cloud-based data ingestion, large-scale SQL analysis, and visualization in R to construct interpretable measures of broadband competition across geographic areas.

For the outputs of this analysis when it was conducted in May 2025, see this article: https://connectednation.org/blog/the-broadband-competitive-landscape-on-the-eve-of-bead

---

## Overview

The goal of this project is to move from highly granular, location-level broadband availability data to interpretable measures of competition across geographic areas.

The workflow:
- ingests raw FCC data via API,
- stages and loads it into BigQuery,
- constructs competition metrics and typologies using SQL,
- and produces analytical outputs and visualizations in R.

The underlying analysis supports a broader policy-oriented examination of broadband competition in the context of the BEAD (Broadband Equity, Access, and Deployment) program.

---

## Repository Structure
broadband-competitive-landscape/
│
├── fcc_download_to_gcs.R # Download FCC data → Google Cloud Storage
├── fcc_analysis_rbigquery.R # Load GCS data → BigQuery
├── broadband-competitive-landscape.sql # Core analytical SQL
├── bbcomp-visualizations.R # Charts and analysis from BigQuery
├── README.md

---

## End-to-End Workflow

### 1. Data acquisition and staging (R)
**File:** `fcc_download_to_gcs.R`

- Calls the FCC BDC API to identify relevant data files
- Triggers a Cloud Function to download files
- Transfers data into Google Cloud Storage (GCS)
- Optionally reorganizes files by state

**Output:** Raw FCC CSV files stored in GCS

---

### 2. Data loading into BigQuery (R)
**File:** `fcc_analysis_rbigquery.R`

- Loads staged CSV files from GCS into BigQuery
- Creates or overwrites a national coverage table
- Uses wildcard paths to ingest multi-state datasets

**Output:** BigQuery table of broadband availability data

---

### 3. Transformation and analysis (SQL)
**File:** `broadband-competitive-landscape.sql`

- Filters data to 100/20 Mbps low-latency services
- Constructs location-level indicators (fiber, wireline, competition)
- Aggregates to Census block, tract, and county levels
- Builds a competitive typology across technologies

**Output:** Analytical tables describing broadband competition

---

### 4. Visualization and interpretation (R)
**File:** `bbcomp-visualizations.R`

- Queries BigQuery analytical tables
- Produces charts on:
  - technology reach
  - ISP portfolio structure
  - industry structure
  - speed trends
- Supports interpretation of broadband market structure

**Output:** Figures and summary statistics

---

## Data and Structure

The analysis is based on FCC BDC data, reported at the **Broadband Serviceable Location (BSL)** level.

This provides extremely granular data:
- Each observation corresponds to a specific serviceable location (roughly, an address)
- Many statistics are based on **proportions of locations served** by different technologies (fiber, cable, DSL, fixed wireless, LEO satellite)

However, the dataset has important structural constraints:

- Exact geographic coordinates are not publicly available (CostQuest fabric restrictions)
- BSLs are associated with **Census blocks**, which are the finest usable geographic unit

As a result:
- Metrics are constructed at the **location level**
- Aggregated to **blocks**
- Further aggregated to **tract and county levels**

In practice, tract-level outputs are often preferred because block-level maps are computationally heavy at national scale.

---

## Methodology

### Filtering and preprocessing
- Focus on broadband services meeting **100/20 Mbps thresholds**
- Aligns with FCC definitions and BEAD policy goals
- Excludes high-latency technologies

### Location-level feature engineering
- Counts providers by technology
- Constructs indicators:
  - fiber availability
  - any wireline availability
  - multi-wireline competition
  - robust competition (3+ providers)

### Aggregation
- Block-level metrics computed as **averages of location-level indicators**
  - Interpretable as share of locations served

### Classification
- Continuous measures discretized into tiers (heuristic thresholds)
- Combined into a **competitive typology**, including:
  - LEO-only areas
  - cable monopolies
  - fiber vs. cable duopolies
  - robust multi-provider competition

### Higher-level rollups
- Census tract and county summaries for analysis and visualization

---

## Key Analytical Considerations

- **Granularity vs. geography**  
  BSL-level data is highly granular, but analysis must rely on geographic aggregation due to data constraints.

- **Partial coverage matters**  
  Many areas are either fully served or unserved, but intermediate cases reveal important dynamics:
  - deployment frontiers
  - gaps within otherwise well-served areas

- **Typology over single metrics**  
  The competitive classification is designed to capture **multi-technology market structure**, not just coverage percentages.

---

## What This Demonstrates

This project demonstrates:

- End-to-end cloud-based data workflows (API → GCS → BigQuery)
- SQL for large-scale policy data analysis
- Transformation of raw administrative data into structured insights
- Multi-level aggregation (location → block → tract → county)
- Integration of SQL and R for analysis and visualization
- Application of economic reasoning to infrastructure data

---

## Notes

- Table names, project IDs, and credentials have been generalized for public release
- Sensitive information (API keys, service accounts) has been removed
- Some thresholds and classifications reflect analytical judgment rather than formal policy definitions
- Scripts are structured for clarity and transparency rather than cost optimization

---

## Context

This analysis underlies:

*The Broadband Competitive Landscape on the Eve of BEAD*  
https://connectednation.org/blog/the-broadband-competitive-landscape-on-the-eve-of-bead

---

## Author

Nathanael Smith  
Policy Economist
