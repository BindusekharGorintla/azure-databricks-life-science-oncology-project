# Azure Databricks Life Science Oncology Project

Leverage **Autoloader**, **Apache Spark** on **Azure Databricks** to run life science oncology data engineering workloads in the cloud. This project demonstrates how to build a **Data Lakehouse** for healthcare analytics, focusing on cancer patient datasets.

## Description
This repository provides a hands-on guide to building a **Data Lakehouse** platform with **Azure Databricks** for oncology research. You’ll learn how to use **Autoloader** to ingest files from volumes into the **Bronze layer**, and how to apply **Change Data Capture (CDC)** to transform and load data into **Silver** and **Gold** layers. The pipeline ensures schema enforcement, data quality, and reliable analytics for life science use cases.

---

## 🛠 Steps to Build the Oncology Lakehouse

### 1. Set up Azure Databricks
- Create an **Azure Databricks workspace** in your Azure portal.
- Configure **clusters** for Spark jobs.
- Ensure access to **Azure Data Lake Storage Gen2 (ADLS)** or **Volumes** for storing oncology datasets.

### 2. Ingest Data with Autoloader
- Use **Autoloader** to continuously extract files from **volumes** or **ADLS volumes**.
- Automatically detect new files and load them into the **Bronze layer**.
- Store raw oncology data (e.g., patient records, observations, TNM staging).

### 3. Change Data Capture (CDC)

CDC pipelines track incremental changes in source data using **timestamps** or other change indicators, rather than full refreshes.

- Ingest raw data into the **Bronze layer** with append-only writes.
- Use **event timestamps** (e.g., `last_updated`, `ingestion_time`) to identify new or modified records since the last run.
- Apply **schema validation, deduplication, and data quality rules** during transformation.
- Organize data following the **Medallion Architecture**:
  - **Bronze**: Raw ingested data (e.g., patient records with ingestion timestamp).
  - **Silver**: Cleaned and structured datasets (e.g., patient profiles, disease status, clinical observations), filtered by latest timestamp.
  - **Gold**: Curated datasets optimized for advanced analytics, reporting, and machine learning.


### 4. Enable Delta Lake
- Store data in **Delta Lake format** for reliability.
- Benefits:
  - ACID transactions
  - Versioning & time travel
  - Scalable queries

### 5. Query and Analyze
- Use **Databricks SQL** or **Spark SQL** to query Silver/Gold tables.
- Connect BI tools (Power BI, Tableau) for oncology dashboards.
- Build reports on cancer progression, treatment outcomes, and patient cohorts.

### 6. Secure and Monitor
- Apply **RBAC** in Azure for data security.
- Monitor pipelines with **DLT dashboards** and **Databricks jobs**.
- Set alerts for ingestion failures or schema mismatches.

### 7. Scale and Optimize
- Use **auto-scaling clusters** for large oncology datasets.
- Optimize queries with **Z-ordering** and **caching**.
- Leverage **DLT continuous mode** for near real-time updates.

## 👨‍💻 Contributing
Fork the repository, submit pull requests, or raise issues for improvements.

---

## 📧 Contact
For questions or support, reach out to **Bindusekhar Gorintla** at *gorintla.bindusekhar@gmail.com*.
