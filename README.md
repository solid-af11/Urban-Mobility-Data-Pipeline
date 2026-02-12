# 🚕 Urban Mobility Intelligence Pipeline

### _End-to-End Analytics Engineering Project_

## 📖 Overview

This project implements a modern **ELT (Extract, Load, Transform)** pipeline to analyze NYC Taxi & Limousine Commission (TLC) data. The goal is to provide actionable insights into urban mobility trends, peak demand periods, and revenue generation.

## 📂 Project Structure

urban-mobility-pipeline/  
├── dags/  
│ └── taxi\_ingest\_dag.py # Airflow orchestration logic (Python)  
├── dbt\_project/ # Data transformation layer (SQL)  
│ ├── models/  
│ │ ├── staging/ # Bronze layer: Cleaning & Type Casting  
│ │ └── marts/ # Gold layer: Business-ready aggregations  
│ ├── schema.yml # Data quality tests and documentation  
│ └── dbt\_project.yml # dbt configuration  
├── docker-compose.yml # Infrastructure as Code (Airflow, Postgres)  
├── .gitignore  
└── README.md # Project documentation

## 🏗️ Technical Architecture

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) to ensure data quality and lineage.

*   **Orchestration:** **Apache Airflow** schedules and manages the workflow.
*   **Infrastructure:** **Docker & Docker Compose** for containerization and environment parity.
*   **Storage (Data Warehouse):** **PostgreSQL** serves as the central OLAP warehouse.
*   **Transformation:** **dbt (data build tool)** handles the SQL modeling and data validation.
*   **Visualization:** **Metabase** (connected to the Gold layer) for executive reporting.

## 🛠️ Key Engineering Features

*   **Memory-Efficient Ingestion:** Python/Pandas streaming to handle large Parquet files without crashing local environments.
*   **Resilient ELT Strategy:** "Schema-on-Read" approach, loading raw data as TEXT to prevent ingestion failures, followed by strict typing in dbt.
*   **Automated Testing & Logging:** Integrated Python logging for real-time monitoring and dbt schema tests for data validation.

## 🚀 Step-by-Step Execution Guide

### 1\. Prerequisites

*   **Docker Desktop** installed.
*   **Python 3.9+** (to run dbt locally).

### 2\. Infrastructure Setup

### docker-compose up -d

### 3\. Data Ingestion

1.  Go to http://localhost:8080.
2.  Login with admin / admin.
3.  Unpause and trigger the nyc\_taxi\_pipeline DAG.

### 4\. Transformation Setup

Create ~/.dbt/profiles.yml:

### 5\. Execute dbt

*   cd dbt\_project
*   dbt deps
*   dbt build

## 3\. Project Impact & Technical Decisions

1.  **ELT vs ETL:** Choose ELT to leverage the processing power of the PostgreSQL warehouse.
2.  **Schema-on-Read:** Loaded raw data as TEXT to ensure ingestion resilience.
3.  **Observability:** Integrated Python logging to track ingestion status.
