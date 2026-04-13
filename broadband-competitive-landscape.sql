-- BigQuery Standard SQL
-- Broadband competitive landscape analysis
-- Public portfolio version with generalized project and dataset names.
-- Replace YOUR_PROJECT and YOUR_DATASET with your own resources before running.
-- This script demonstrates a BigQuery workflow for classifying broadband
-- competitive conditions using FCC Broadband Data Collection coverage data.
-- Table names and project identifiers have been generalized for public release.

-- ---------------------------------------------------------------------
-- 1) Build a filtered working table at the location level
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` AS
SELECT
  location_id,
  block_geoid,
  COUNT(CASE WHEN technology = 50 THEN 1 END) AS n_tech50,  -- Fiber
  COUNT(CASE WHEN technology = 40 THEN 1 END) AS n_tech40,  -- Cable
  COUNT(CASE WHEN technology = 70 THEN 1 END) AS n_tech70,  -- Fixed wireless
  COUNT(CASE WHEN technology = 71 THEN 1 END) AS n_tech71,  -- Fixed wireless
  COUNT(CASE WHEN technology = 72 THEN 1 END) AS n_tech72,  -- Fixed wireless
  COUNT(CASE WHEN technology = 10 THEN 1 END) AS n_tech10,  -- DSL
  COUNT(CASE WHEN technology = 61 THEN 1 END) AS n_tech61   -- LEO satellite
FROM `YOUR_PROJECT.YOUR_DATASET.coverage`
WHERE low_latency <> 0
  AND max_advertised_download_speed >= 100
  AND max_advertised_upload_speed >= 20
GROUP BY location_id, block_geoid;

-- ---------------------------------------------------------------------
-- 2) Add location-level counts and competitive situation labels
-- ---------------------------------------------------------------------

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN n_tot INT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET n_tot = n_tech50 + n_tech40 + n_tech70 + n_tech71 + n_tech72 + n_tech10 + n_tech61
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN comp_sit_desc STRING;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'Fiber (one)'
WHERE n_tech50 = 1 AND n_tech40 = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'Cable (one)'
WHERE n_tech50 = 0 AND n_tech40 = 1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'Fiber vs. cable'
WHERE n_tech50 = 1 AND n_tech40 = 1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'Fiber vs. cable (multi)'
WHERE n_tech50 > 0 AND n_tech40 > 0 AND (n_tech50 > 1 OR n_tech40 > 1);

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'Fiber only (multi)'
WHERE n_tech50 > 1 AND n_tech40 = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'Cable only (multi)'
WHERE n_tech50 = 0 AND n_tech40 > 1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'DSL or FW only 100/20 (except LEO)'
WHERE n_tech50 = 0
  AND n_tech40 = 0
  AND (n_tech70 > 0 OR n_tech71 > 0 OR n_tech72 > 0 OR n_tech10 > 0);

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET comp_sit_desc = 'LEO only'
WHERE n_tech50 = 0
  AND n_tech40 = 0
  AND n_tech70 = 0
  AND n_tech71 = 0
  AND n_tech72 = 0
  AND n_tech10 = 0
  AND n_tech61 > 0;

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET.bbcomp` AS
SELECT
  comp_sit_desc,
  COUNT(*) AS count
FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
GROUP BY comp_sit_desc;

-- ---------------------------------------------------------------------
-- 3) Fiber coverage at location and block level
-- ---------------------------------------------------------------------

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN fibercov FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET fibercov = 0
WHERE n_tech50 = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET fibercov = 1
WHERE fibercov IS NULL;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN block_fibercov FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET block_fibercov = (
  SELECT x.block_fibercov
  FROM (
    SELECT
      block_geoid,
      AVG(fibercov) AS block_fibercov
    FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
    GROUP BY block_geoid
  ) AS x
  WHERE `YOUR_PROJECT.YOUR_DATASET.techcounts`.block_geoid = x.block_geoid
)
WHERE TRUE;

-- ---------------------------------------------------------------------
-- 4) Multi-wireline and robust wireline competition
-- ---------------------------------------------------------------------

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN multiwireline FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET multiwireline = 1
WHERE n_tech50 + n_tech40 > 1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET multiwireline = 0
WHERE multiwireline IS NULL;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN block_multiwireline FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET block_multiwireline = (
  SELECT x.block_multiwireline
  FROM (
    SELECT
      block_geoid,
      AVG(multiwireline) AS block_multiwireline
    FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
    GROUP BY block_geoid
  ) AS x
  WHERE `YOUR_PROJECT.YOUR_DATASET.techcounts`.block_geoid = x.block_geoid
)
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN robustwlcomp FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET robustwlcomp = 1
WHERE n_tech50 + n_tech40 > 2;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET robustwlcomp = 0
WHERE robustwlcomp IS NULL;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN block_robustwlcomp FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET block_robustwlcomp = (
  SELECT x.block_robustwlcomp
  FROM (
    SELECT
      block_geoid,
      AVG(robustwlcomp) AS block_robustwlcomp
    FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
    GROUP BY block_geoid
  ) AS x
  WHERE `YOUR_PROJECT.YOUR_DATASET.techcounts`.block_geoid = x.block_geoid
)
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN robustwlcomp_tiers NUMERIC;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET robustwlcomp_tiers = 0
WHERE block_robustwlcomp < 0.5;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET robustwlcomp_tiers = 1
WHERE block_robustwlcomp >= 0.5;

-- ---------------------------------------------------------------------
-- 5) Tiered classification of fiber and multi-wireline coverage
-- ---------------------------------------------------------------------

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN fibercov_tiers NUMERIC;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET fibercov_tiers = 0
WHERE block_fibercov < 0.1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET fibercov_tiers = 1
WHERE 0.1 <= block_fibercov AND block_fibercov < 0.99;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET fibercov_tiers = 2
WHERE block_fibercov >= 0.99;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN multiwireline_tiers NUMERIC;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET multiwireline_tiers = 0
WHERE block_multiwireline < 0.1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET multiwireline_tiers = 1
WHERE 0.1 <= block_multiwireline AND block_multiwireline < 0.95;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET multiwireline_tiers = 2
WHERE block_multiwireline >= 0.95;

-- ---------------------------------------------------------------------
-- 6) Any wireline and any terrestrial availability
-- ---------------------------------------------------------------------

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN anywireline FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anywireline = 1
WHERE n_tech50 + n_tech40 > 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anywireline = 0
WHERE anywireline IS NULL;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN block_anywireline FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET block_anywireline = (
  SELECT x.block_anywireline
  FROM (
    SELECT
      block_geoid,
      AVG(anywireline) AS block_anywireline
    FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
    GROUP BY block_geoid
  ) AS x
  WHERE `YOUR_PROJECT.YOUR_DATASET.techcounts`.block_geoid = x.block_geoid
)
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN anywireline_tiers NUMERIC;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anywireline_tiers = 0
WHERE block_anywireline < 0.5;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anywireline_tiers = 1
WHERE 0.5 <= block_anywireline AND block_anywireline < 0.99;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anywireline_tiers = 2
WHERE block_anywireline >= 0.99;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN anyterrestrial FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anyterrestrial = 1
WHERE n_tech50 + n_tech40 + n_tech10 + n_tech71 + n_tech70 + n_tech72 > 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anyterrestrial = 0
WHERE anyterrestrial IS NULL;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN block_anyterrestrial FLOAT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET block_anyterrestrial = (
  SELECT x.block_anyterrestrial
  FROM (
    SELECT
      block_geoid,
      AVG(anyterrestrial) AS block_anyterrestrial
    FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
    GROUP BY block_geoid
  ) AS x
  WHERE `YOUR_PROJECT.YOUR_DATASET.techcounts`.block_geoid = x.block_geoid
)
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN anyterrestrial_tiers NUMERIC;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anyterrestrial_tiers = 0
WHERE block_anyterrestrial < 0.5;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anyterrestrial_tiers = 1
WHERE 0.5 <= block_anyterrestrial AND block_anyterrestrial < 0.99;

UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET anyterrestrial_tiers = 2
WHERE block_anyterrestrial >= 0.99;

-- ---------------------------------------------------------------------
-- 7) Census block-level competitive typology
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET.blocktech` AS
SELECT
  block_geoid,
  AVG(anyterrestrial_tiers) AS anyterrestrial,
  AVG(anywireline_tiers) AS anywireline,
  AVG(fibercov_tiers) AS fibercov,
  AVG(multiwireline_tiers) AS multiwireline,
  AVG(robustwlcomp_tiers) AS robustwlcomp,
  COUNT(location_id) AS numLocs
FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
GROUP BY block_geoid;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.blocktech` ADD COLUMN comptype NUMERIC;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 1
WHERE anyterrestrial = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 2
WHERE anyterrestrial = 1 AND anywireline = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 3
WHERE anyterrestrial = 2 AND anywireline = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 4
WHERE anywireline = 1 AND fibercov = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 5
WHERE anywireline = 1 AND fibercov = 1;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 6
WHERE anywireline = 2 AND fibercov = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 7
WHERE anywireline = 2 AND fibercov > 0 AND multiwireline = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 8
WHERE anywireline = 2
  AND fibercov > 0
  AND multiwireline > 0
  AND fibercov + multiwireline < 4;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 9
WHERE fibercov = 2 AND multiwireline = 2 AND robustwlcomp = 0;

UPDATE `YOUR_PROJECT.YOUR_DATASET.blocktech`
SET comptype = 10
WHERE fibercov = 2 AND multiwireline = 2 AND robustwlcomp = 1;

-- Optional state-specific export example
CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET.state_blocktech_example` AS
SELECT
  *,
  LPAD(CAST(block_geoid AS STRING), 15, '0') AS padded_block_geoid
FROM `YOUR_PROJECT.YOUR_DATASET.blocktech`
WHERE block_geoid < 20000000000000;

-- ---------------------------------------------------------------------
-- 8) Add padded block, tract, county, and state GEOIDs
-- ---------------------------------------------------------------------

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN padded_block_geoid STRING;
UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET padded_block_geoid = LPAD(CAST(block_geoid AS STRING), 15, '0')
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN tract_geoid STRING;
UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET tract_geoid = SUBSTRING(padded_block_geoid, 1, 11)
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN county_geoid STRING;
UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET county_geoid = SUBSTRING(padded_block_geoid, 1, 5)
WHERE TRUE;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.techcounts` ADD COLUMN state_geoid STRING;
UPDATE `YOUR_PROJECT.YOUR_DATASET.techcounts`
SET state_geoid = SUBSTRING(padded_block_geoid, 1, 2)
WHERE TRUE;

-- ---------------------------------------------------------------------
-- 9) Census tract-level analysis
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET.tracttech` AS
SELECT
  tract_geoid,
  AVG(anyterrestrial) AS anyterrestrial,
  AVG(anywireline) AS anywireline,
  AVG(fibercov) AS fibercov,
  AVG(multiwireline) AS multiwireline,
  AVG(robustwlcomp) AS robustwlcomp,
  COUNT(location_id) AS numLocs
FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
GROUP BY tract_geoid;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.tracttech` ADD COLUMN comptype INT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 1
WHERE anyterrestrial < 0.5;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 2
WHERE comptype IS NULL AND anywireline < 0.3;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 3
WHERE comptype IS NULL AND anywireline < 0.8;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 4
WHERE comptype IS NULL AND fibercov < 0.4;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 5
WHERE comptype IS NULL AND multiwireline < 0.4;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 6
WHERE comptype IS NULL AND robustwlcomp < 0.3;

UPDATE `YOUR_PROJECT.YOUR_DATASET.tracttech`
SET comptype = 7
WHERE comptype IS NULL;

-- ---------------------------------------------------------------------
-- 10) County-level analysis
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET.ctytech` AS
SELECT
  county_geoid,
  AVG(anyterrestrial) AS anyterrestrial,
  AVG(anywireline) AS anywireline,
  AVG(fibercov) AS fibercov,
  AVG(multiwireline) AS multiwireline,
  AVG(robustwlcomp) AS robustwlcomp,
  COUNT(location_id) AS numLocs
FROM `YOUR_PROJECT.YOUR_DATASET.techcounts`
GROUP BY county_geoid;

ALTER TABLE `YOUR_PROJECT.YOUR_DATASET.ctytech` ADD COLUMN comptype INT64;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 1
WHERE anyterrestrial < 0.5;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 2
WHERE comptype IS NULL AND anywireline < 0.3;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 3
WHERE comptype IS NULL AND anywireline < 0.8;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 4
WHERE comptype IS NULL AND fibercov < 0.4;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 5
WHERE comptype IS NULL AND multiwireline < 0.4;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 6
WHERE comptype IS NULL AND robustwlcomp < 0.3;

UPDATE `YOUR_PROJECT.YOUR_DATASET.ctytech`
SET comptype = 7
WHERE comptype IS NULL;

-- ---------------------------------------------------------------------
-- 11) Example summary outputs
-- ---------------------------------------------------------------------

SELECT
  comptype,
  COUNT(block_geoid) AS numblocks,
  SUM(numLocs) AS numbls
FROM `YOUR_PROJECT.YOUR_DATASET.blocktech`
GROUP BY comptype
ORDER BY comptype;

SELECT
  comptype,
  COUNT(tract_geoid) AS numTracts,
  SUM(numLocs) AS numLocs
FROM `YOUR_PROJECT.YOUR_DATASET.tracttech`
GROUP BY comptype
ORDER BY comptype;

SELECT
  comptype,
  COUNT(county_geoid) AS numCounties,
  SUM(numLocs) AS numLocs
FROM `YOUR_PROJECT.YOUR_DATASET.ctytech`
GROUP BY comptype
ORDER BY comptype;