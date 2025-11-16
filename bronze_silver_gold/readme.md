### Bronze → Silver → Gold Pattern
The foundational pattern for Delta Lake lakehouse architectures.

## Overview
The medallion architecture organizes data into three layers of increasing quality and structure:

Bronze (Raw): Immutable landing zone for raw data exactly as received from source systems

Silver (Cleansed): Validated, deduplicated, and conformed data ready for consumption

Gold (Curated): Business-level aggregates and star schemas optimized for analytics

### When to Use This Pattern

✅ Building a lakehouse on Azure Databricks and Delta Lake

✅ Need clear separation of raw vs. trusted data

✅ Want incremental processing with full audit trail

✅ Preparing data for BI tools (Power BI, Tableau)

✅ Regulatory environments requiring data lineage

### Architecture

Source Systems → Bronze (Delta) → Silver (Delta) → Gold (Delta) → BI/Analytics
                    ↓                  ↓                ↓
                Raw files       Validated data    Star schemas
                Schema-on-read  Quality checks    Aggregations
                Append-only     Deduplication     SCD tracking

### Example: Healthcare Claims Processing

This example demonstrates processing EDI 837 institutional claims through the medallion layers.

## Bronze Layer

Purpose: Land raw claim files exactly as received

Schema: Flexible, schema-on-read with _metadata columns

Operations: Append-only, no transformations

Columns Added: ingestion_timestamp, source_file, source_system

## Silver Layer

Purpose: Validated, deduplicated claims ready for analysis

Schema: Enforced schema with data quality checks

Operations: MERGE to handle updates, deduplication by claim_id

Quality Checks:

Non-null required fields (claim_id, member_id, provider_id)

Valid date ranges (service_date <= received_date)

Valid procedure codes (CPT/HCPCS)

Duplicate claim detection

## Gold Layer

Purpose: Star schema for claims analytics

Schema: Dimension and fact tables optimized for queries

Operations: Daily refresh with incremental MERGE

Tables:
rxclaims: One row per member line
rxMember - SCD Type 2 for member demographics
rxProvider - Current rx provider master
rxDate - Standard date dimension

## Key Techniques

# 1. Schema Evolution

Bronze layer uses mergeSchema=true to handle evolving source schemas without pipeline breaks.

# 2. Idempotent Processing

MERGE operations use business keys (claim_id) to ensure re-runs don't create duplicates.

# 3. Incremental Loading

Watermark pattern processes only new/changed records based on received_date or last_modified_timestamp.

# 4. Data Quality Gates

Silver layer blocks bad data from reaching Gold, logging quality failures to a separate DQ table.

# 5. Performance Optimization

Bronze: Partitioned by ingestion_date for efficient time-based queries

Silver: Partitioned by service_year_month for analytical workloads

Gold: Z-ORDER by frequently filtered columns (member_id, provider_id, service_date)

## Best Practices

Keep Bronze immutable - Never update Bronze, always append

Enforce schema in Silver - Use .option("mergeSchema", "false") after initial stabilization

Partition strategically - Bronze by ingestion date, Silver/Gold by business date

Add audit columns - created_timestamp, updated_timestamp, source_system, pipeline_run_id

Optimize before Gold - Run OPTIMIZE and Z-ORDER on Silver before promoting to Gold

Test data quality - Automated tests for nulls, duplicates, referential integrity

Document assumptions - Comment on business rules, data quirks, transformation logic

## Performance Considerations
# For 100M+ records:

Enable Photon runtime for 2-3x faster processing

Use OPTIMIZE with file size target (128MB-256MB per file)

Apply Z-ORDER on high-cardinality filter columns

Consider Liquid Clustering (Delta 3.0+) for multi-column sorting

Broadcast small dimension tables in joins

# Cost optimization:

Use spot instances for Bronze/Silver jobs (can tolerate interruption)

Schedule Gold refresh during off-peak hours

Implement incremental refresh in Power BI to reduce query load

Set table retention policies to auto-delete old files

## Files in This Pattern

bronze_rx_claims_load.py - Raw landing to Delta Bronze

silver_rx_claims_load.py - Quality checks and deduplication

gold_rx_claims_load.py - Star schema modeling

Optimize performance with advanced techniques (partitioning strategies, caching)
