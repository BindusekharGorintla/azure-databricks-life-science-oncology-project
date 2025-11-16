# Bronze → Silver → Gold Pattern

The foundational pattern for **Delta Lake lakehouse** architectures.

## Overview
The **medallion architecture** organizes data into three layers of increasing quality and structure:

- **Bronze (Raw)**: Immutable landing zone for raw data exactly as received from source systems.
- **Silver (Cleansed)**: Validated, deduplicated, and conformed data ready for consumption.
- **Gold (Curated)**: Business-level aggregates and star schemas optimized for analytics.

---

## When to Use This Pattern

✅ Building a lakehouse on **Azure Databricks** and **Delta Lake**

✅ Need clear separation of **raw vs. trusted data**

✅ Want **incremental processing** with a full **audit trail**

✅ Preparing data for **BI tools** (Power BI, Tableau)

✅ Regulatory environments requiring **data lineage**

---

## Architecture

