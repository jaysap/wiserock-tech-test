# Analytics Engineer Technical Assessment

This project is a solution for the Wise Rock Analytics Engineer technical assessment. It includes a Python-based ETL pipeline to load data from multiple sources into a PostgreSQL database and a series of SQL scripts to perform advanced data analysis and modeling.

## Project Overview

The project is divided into two main parts:

1.  **Part 1: ETL Pipeline (Python)**
    * A robust Python script (`etl/load_data.py`) extracts data from a set of local CSV files and a REST API.
    * It loads this data into a local, Dockerized PostgreSQL database.
    * The pipeline is designed to be configurable and reusable, following best practices for data engineering.

2.  **Part 2: Data Modeling & Analysis (SQL)**
    * A series of SQL scripts (`sql/*.sql`) transform the raw data into a structured analytical layer.
    * These scripts create views to model production data, well spacing, and user interactions.
    * Finally, they execute analytical queries to answer specific questions outlined in the assessment.

---

## Project Structure

The repository is organized into the following directories:

```
/
|-- csv_to_load/                   # Directory for all source CSV files.
|-- etl/
|   |-- load_data.py               # The main Python script for the ETL pipeline.
|   |-- etl_config.py              # Configuration file for the ETL pipeline.
|
|-- sql/
|   |-- part_2a_prod_analytics.sql # SQL script to create the production analytics views.
|   |-- part_2b_user_analytics.sql # SQL script for Wise Rock views and all analytical queries.
|
|-- docker-compose.yml             # Configuration file for the local PostgreSQL database.
|-- requirements.txt               # A list of Python dependencies for the project.
|-- README.md                      # This file.
```
