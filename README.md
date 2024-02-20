# Big Data Engineering - ELT Data Pipelines with Airflow

## Overview

This project is centered on constructing robust data pipelines utilizing Airflow to process and refine two distinct datasets - Airbnb and Census. The primary goal is to curate and load valuable information into a data warehouse via ELT (Extract, Load, Transform) pipelines, alongside creating a datamart for in-depth analytical insights.

## Datasets Summary

- **Airbnb Dataset:**
  - Captures comprehensive information on Sydney's Airbnb listings, including property types, prices, reviews, and related data spanning from May 2020 to April 2021.

- **Census Dataset:**
  - Acquired by the Australian Bureau of Statistics, this dataset contains crucial population and housing data essential for understanding demographics and supporting decision-making processes.

## Task Overview

### Environment Setup
- Configure an Airflow and Postgres environment using GCP (Cloud Composer and SQL instance) and dbt within a private GitHub repository.

### Data Collection
- Download datasets for Airbnb (Sydney listings), Census (G01 and G02 tables), and an intermediary mapping dataset for linking the two.

### Building Data Pipelines
- **Part 1:** Load raw data into Postgres using Airflow.
- **Part 2:** Design a data warehouse using dbt, including layers like raw, staging, warehouse, datamart, and associated views.
- **Part 3:** Conduct ad-hoc analysis by addressing specific questions using SQL queries executed on Postgres.

## Note
For detailed instructions and specifics regarding each task, please refer to the provided [Assignment 3 Brief.docx] file.

## Acknowledgments
This project and its documentation are for educational purposes.
(The project structure is from the **Master of Data Science and Innovation** course of the University of Technology Sydney, and it is an asset of TD School)
