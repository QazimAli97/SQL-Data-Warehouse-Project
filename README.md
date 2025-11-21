📦 Data Warehouse & Analytics Project

Welcome to the Data Warehouse and Analytics Project! 🚀
This repository showcases a complete end-to-end data engineering and analytics solution, including data ingestion, ETL pipelines, data modeling, and analytical reporting.
It follows modern industry standards and is designed as a perfect portfolio project for Data Engineering, BI, and SQL roles.

🏗️ Data Architecture (Medallion Architecture)

This project is built using the Medallion Architecture consisting of Bronze, Silver, and Gold layers: 

<img width="6235" height="3216" alt="data_architecture" src="https://github.com/user-attachments/assets/0ea7f4d2-a942-4b83-a5c0-a5da4f0a2f57" />

🟫 Bronze Layer — Raw Data

Stores raw data as-is from ERP and CRM systems.
Data is ingested from CSV files into SQL Server.
No cleaning or transformations are applied here.

🟪 Silver Layer — Cleaned & Standardized

Performs data cleaning, validation, and standardization.
Handles duplicates, missing values, formatting issues, and joins.
Data is transformed into an analysis-ready format.

🟨 Gold Layer — Star Schema

Contains business-ready fact and dimension tables.
Optimized for BI dashboards, SQL queries, and analytics.
Supports key business areas such as sales, customers, and products.

📘 Project Overview

This project includes:

✔ Data Architecture

Modern Data Warehouse design using Medallion Architecture.

✔ ETL (Extract → Transform → Load) Pipelines

Developed in SQL Server to load, cleanse, and model data.

✔ Data Modeling

Fact/Dimension schema for analytical workloads.

✔ Analytics & Reporting

SQL-driven insights on:

Customer behavior

Sales performance

Product trends

🎯 Skills Demonstrated

This repository highlights expertise in:

SQL Development
ETL Pipeline Development
Data Modeling (Star Schema)
Data Warehousing Concepts
BI & Reporting
Problem-Solving and Data Analysis
Perfect for Data Engineer, Data Analyst, and BI Engineer portfolios.

📁 Repository Structure
data-warehouse-project/
│
├── datasets/                         # Raw datasets (ERP + CRM CSV files)
│
├── scripts/                          # Main ETL SQL scripts
│   ├── bronze/                       # Extract & load raw data
│   ├── silver/                       # Data cleaning, validation, transformations
│   ├── gold/                         # Fact & Dimension table creation
│
├── docs/                             # Technical documentation
│   ├── etl.png                       # ETL architecture diagram
│   ├── data_architecture.drawio      # Overall project architecture (Draw.io)
│   ├── data_catalog.md               # Dataset dictionary & metadata
│   ├── data_flow.drawio              # Data flow diagram
│   ├── data_models.drawio            # Star schema (Fact/Dimension)
│   ├── naming-conventions.md         # Naming standards for tables/columns
│
├── tests/                            # Data quality checks & validation scripts
│
├── README.md                         # Project overview
├── LICENSE                           # License details
├── .gitignore                        # Ignored files
└── requirements.txt                  # Dependencies

🚀 Project Requirements
🛠 1. Data Engineering (Build the Data Warehouse)
Objective:

Create a SQL Server–based data warehouse to consolidate ERP + CRM sales data.

Specifications:

Data Sources:
Import raw CSV files from ERP and CRM systems.

Data Quality:
Address duplicate records, missing fields, datatype mismatches, and formatting issues.

Integration:
Combine both source systems into a unified data model.

Scope:
Focus on latest data only (historical tracking not required).

Documentation:
Provide clear business definitions and technical model documentation.

📊 2. BI & Analytics (SQL Reporting)
Goal:

Provide business insights through SQL queries and dashboards.
Key Insights Delivered:
Customer purchasing patterns
Top-performing products
Monthly/Quarterly sales trends
Profitability and revenue drivers
These insights enable better decision-making for stakeholders.
