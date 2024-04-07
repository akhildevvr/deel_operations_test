# Deel Operations Data Pipeline

This repository contains the data pipeline for Deel's operational analytics. It's structured to use dbt (data build tool) for transforming data within Snowflake and Apache Airflow for orchestrating and monitoring the pipeline.

## Directory Structure

- `dags/`: Contains Apache Airflow DAGs that define the data pipeline workflows.
- `common/scripts/`: Includes Python scripts for common tasks such as connecting to Snowflake and sending notifications to Slack.
- `dbt/`: This directory houses all dbt-related files for transforming data in Snowflake.
  - `analyses/`: Stores dbt analyses.
  - `docs/`: Contains documentation for the dbt models.
  - `macros/`: Custom macros used within dbt models.
  - `models/`: dbt models defining transformations.
  - `publish/`: Definitions of models that are published as final data marts or reporting tables.
  - `transform/`: Contains dbt models specifically for transforming raw data into a structured format.
  - `schema.yml`: dbt schema files that include model and test configurations.
  - `seeds/`: csv files that dbt can load into Snowflake.
  - `snapshots/`: dbt snapshot files for data that should be captured over time.
  - `tests/`: Tests to ensure data quality and correct logic in the dbt models.
- `validation_tests/`: Scripts or definitions for additional data validation tests. such as balance validations.

