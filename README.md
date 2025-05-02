# Udacity AWS Data Engineering Nanodegree – Final Project

## Overview

This project is the capstone assignment for the **AWS Data Engineering Nanodegree** offered by Udacity. It demonstrates a full data pipeline built with **Apache Airflow**, using AWS services to move and transform data into a dimensional model suitable for analytics.

The pipeline performs the following steps:

- **Extract** JSON event and song data stored in an Amazon S3 bucket
- **Stage** the raw data into Amazon Redshift staging tables
- **Transform and load** the data into a star schema with one fact table and several dimension tables
- **Run data quality checks** to ensure pipeline integrity

---

## Project Structure

```bash
├── dags/
│ └── final_project.py        # Main DAG definition
├── plugins/
│ ├── helpers/
│ │ └── sql_queries.py        # SQL statements
│ ├── operators/
│ │ ├── create_tables.py      # Custom operator for creating Redshift tables
│ │ ├── data_quality.py       # Runs data quality checks on final tables
│ │ ├── load_dimension.py     # Loads dimension tables
│ │ ├── load_fact.py          # Loads the fact table
│ │ └── stage_redshift.py     # Stages raw data from S3 into Redshift
├── config.py                 # Configuration settings (e.g., S3 paths, Redshift connection IDs)
```
