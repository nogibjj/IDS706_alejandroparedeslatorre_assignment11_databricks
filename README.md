# IDS706_AlejandroParedesLaTorre_Assignment10_PySpark
[![CI](https://github.com/nogibjj/IDS706_alejandroparedeslatorre_assignment10_spark/actions/workflows/CI.yml/badge.svg)](https://github.com/nogibjj/IDS706_alejandroparedeslatorre_assignment10_spark/actions/workflows/CI.yml)

This repository provides an ETL pipeline using PySpark to extract data from an external API, transform it for analysis, and load it into a SQLite database.

The data is extracted from the API:

http://universities.hipolabs.com/search?country=United+States

This dataset, in JSON format, contains basic information about universities in the United States.

## Project Overview

The main components of this project include:

- **ETL Pipeline**: Extracts university data, transforms it using PySpark to create a structured format, and loads it into a SQLite database.

## Files and Directories

- `Makefile`: Defines commands to streamline setup and execution processes.
- `Dockerfile`: Contains instructions for creating a Docker image to ensure consistent runtime environments.
- `requirements.txt`: Specifies necessary Python packages, including PySpark.
- `.github/workflows/`: Contains GitHub Actions workflows to automate testing and deployment.
- `.devcontainer/`: Configuration for GitHub Codespaces to set up a reproducible development environment.

## Purpose

The primary objective of this project is to demonstrate data handling using an ETL process with PySpark, while loading the processed data into a SQLite database. This serves as a foundational example for handling, transforming, and managing data in a PySpark-SQLite environment.

## Architecture

The ETL architecture is presented below:

1. **Extract**: Fetch data from the API.
2. **Transform**: Clean and structure data with PySpark, exploding and concatenating fields to create unique records.
3. **Load**: Insert transformed data into SQLite.

## Getting Started

1. **Open GitHub Codespaces**:
   - Load the repository into GitHub Codespaces for a pre-configured environment.
2. **Install Requirements**:
   - Wait for automatic installation of dependencies listed in `requirements.txt`.

## Running the Project

Once set up in Codespaces or locally with Docker, you can initiate the ETL pipeline. Each step of the pipeline is logged to ensure traceability and transparency of operations.

This project demonstrates the capabilities of PySpark for handling large-scale data processing in a structured ETL workflow, combined with a lightweight SQLite database for data storage.