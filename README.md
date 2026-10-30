# 🚕 Uber Real-Time Data Engineering Platform

An end-to-end **batch + real-time Azure data engineering project** that simulates Uber ride events and integrates historical ride data, static reference data, and live booking events through a modern **Lakehouse / Medallion Architecture** using Azure Databricks, transforming data across **Bronze → Silver → Gold** layers into an analytics-ready dimensional model.

The solution brings together real-time streaming, batch ingestion, lakehouse architecture, **Spark Declarative Pipelines, dimensional modeling, and CDC** within a single Azure-based data platform.

---

## 🏗️ Architecture

```text
                         ┌─────────────────────┐
                         │ Uber Ride Simulator │
                         │      FastAPI        │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Azure Event Hubs    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Azure Databricks    │
                         │ Bronze rides_raw    │
                         └──────────┬──────────┘
                                    │
                                    ▼
┌──────────────────┐      ┌─────────────────────┐
│ Historical Data  │      │ Silver Layer        │
│ Reference Data   │─────▶│ stg_rides           │
└────────┬─────────┘      │ Silver OBT          │
         │                └──────────┬──────────┘
         ▼                           │
┌──────────────────┐                 ▼
│ Azure Data       │      ┌─────────────────────┐
│ Factory          │      │ Gold Layer          │
└────────┬─────────┘      │ Dimensions + Fact   │
         │                │ Auto CDC / SCD      │
         ▼                └─────────────────────┘
┌──────────────────┐
│ ADLS Gen2        │
└──────────────────┘
```

---

## 🎯 Key Features

- Built an end-to-end **batch + streaming pipeline** on Azure.
- Ingested live Uber ride events using **Azure Event Hubs**.
- Ingested historical and reference datasets using **Azure Data Factory + ADLS Gen2**.
- Processed streaming data using **Spark Structured Streaming**.
- Implemented **Spark Declarative Pipelines** for Bronze, Silver, and Gold processing.
- Combined historical and real-time ride data into a unified Silver staging layer.
- Applied explicit JSON schema parsing and watermarking for streaming data.
- Enriched ride events using multiple reference datasets.
- Built a metadata-driven transformation framework using **Jinja2**.
- Created an enriched **Silver One Big Table (OBT)**.
- Designed a **Star Schema** with fact and dimension tables.
- Implemented **Databricks Auto CDC** with **SCD Type 1 and Type 2** patterns.
- Organized datasets using **Unity Catalog**.

---

## 🔄 Data Flow

### Real-Time Path

```text
FastAPI Ride Simulator
        ↓
Azure Event Hubs
        ↓
Spark Structured Streaming
        ↓
Bronze rides_raw
        ↓
Silver stg_rides
```

### Batch Path

```text
Historical / Reference Data
        ↓
Azure Data Factory
        ↓
ADLS Gen2
        ↓
Bronze Delta Tables
        ↓
Silver stg_rides / enrichment
```

Both batch and streaming data are eventually processed through the same Silver transformation layer.

---

## 🥉 Bronze Layer

The Bronze layer stores raw and minimally processed datasets.

Main tables include:

```text
rides_raw
bulk_rides
map_cities
map_vehicle_makes
map_vehicle_types
map_payment_methods
map_ride_statuses
map_cancellation_reasons
```

`rides_raw` receives live Event Hub events, while `bulk_rides` provides the historical initial load.

---

## 🥈 Silver Layer

The Silver layer standardizes and enriches the ride data.

### `stg_rides`

Historical and streaming records are combined using Spark Declarative Pipeline append flows.

```text
bulk_rides ───────┐
                  ├──▶ stg_rides
rides_raw ────────┘
```

### Silver One Big Table

`stg_rides` is enriched using reference tables for:

- city
- vehicle make
- vehicle type
- payment method
- ride status
- cancellation reason

A **Jinja2-based metadata-driven framework** is used to dynamically generate SELECT, JOIN, and filter logic instead of hardcoding repetitive SQL.

---

## 🥇 Gold Layer

The Gold layer converts the enriched Silver dataset into an analytics-ready dimensional model.

Main entities include:

```text
dim_passenger
dim_driver
dim_vehicle
dim_payment
dim_booking
dim_location
fact_rides
```

The model follows a **Star Schema** pattern, with `fact_rides` as the central fact table.

---

## 🔁 Auto CDC & Slowly Changing Dimensions

Databricks **Auto CDC** is used to maintain dimension tables.

The project demonstrates:

- **SCD Type 1** for dimensions where only the latest value is required.
- **SCD Type 2** where historical changes need to be preserved.

The Location dimension demonstrates SCD Type 2 using city update timestamps to track historical versions.

---

## 🧩 Metadata-Driven Transformations

Jinja2 is used to generate reusable SQL based on metadata configuration.

Conceptually:

```text
Configuration
     ↓
Jinja2 Template
     ↓
Generated SQL
     ↓
Silver OBT
```

This reduces repetitive SQL and makes it easier to extend the enrichment logic.

---

## 📂 Project Structure

```text
Uber_Data_Engineer_Project/
│
├── Code_Files/
│   ├── bronze_adls.ipynb
│   ├── silver_obt.ipynb
│   ├── ingest.py
│   ├── silver.py
│   ├── silver_obt.sql
│   └── model.py
│
├── Data/
├── templates/
│
├── api.py
├── connection.py
├── data.py
├── files_array.json
├── requirements.txt
└── README.md
```

### Important Files

- `api.py` – FastAPI-based Uber ride simulator.
- `ingest.py` – real-time Event Hub → Bronze ingestion.
- `bronze_adls.ipynb` – historical/reference data ingestion into Bronze.
- `silver.py` – combines historical and streaming rides.
- `silver_obt.sql` – Silver enrichment and Jinja2 SQL logic.
- `model.py` – Gold dimensional model, Auto CDC, and SCD processing.

---

## 🛠️ Tech Stack

**Azure**
- Azure Databricks
- Azure Event Hubs
- Azure Data Factory
- Azure Data Lake Storage Gen2

**Data Engineering**
- Apache Spark
- PySpark
- Spark Structured Streaming
- Spark Declarative Pipelines
- Delta Lake
- Unity Catalog
- Auto CDC
- SCD Type 1 / Type 2
- Medallion Architecture
- Star Schema

**Development**
- Python
- SQL
- Jinja2
- FastAPI
- Git / GitHub

---

## 📈 Final Pipeline

```text
Source Systems
      ↓
Event Hubs / ADF
      ↓
Bronze
      ↓
Silver Staging
      ↓
Silver OBT
      ↓
Gold Dimensions + Fact
```

The project demonstrates how Azure and Databricks can be used to build a modern lakehouse platform supporting both historical batch ingestion and continuously arriving real-time data.

---

## 🚀 Future Enhancements

Planned improvements after completing the base tutorial:

- End-to-end orchestration
- Data quality expectations
- Monitoring and alerting
- CI/CD
- Automated testing
- Failure recovery and backfill strategy

---
