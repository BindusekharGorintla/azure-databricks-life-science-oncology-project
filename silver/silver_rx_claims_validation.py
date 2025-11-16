"""
Silver Layer - rx_claims Validation and Cleansing
==============================================
Transforms Bronze raw rx_claims into validated, deduplicated Silver layer.

Key Principles:
- Enforce schema and data quality rules
- Deduplicate by business key (claim_id)
- Handle late-arriving data with MERGE
- Log quality failures without blocking pipeline

Author: Harsha Morram
Pattern: Medallion Architecture - Silver Layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, current_timestamp, lit, coalesce, 
    to_date, datediff, regexp_replace, upper, trim,
    row_number, max as spark_max
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("Silver rx_claims Validation").getOrCreate()

# Configuration
BRONZE_PATH = "/mnt/bronze/healthcare/rx_claims"
SILVER_PATH = "/mnt/silver/healthcare/rx_claims"
DQ_FAILURES_PATH = "/mnt/silver/healthcare/rx_claims_dq_failures"
WATERMARK_TABLE = "control.watermarks"

# Valid procedure code patterns (simplified for demo)
VALID_CPT_PATTERN = "^[0-9]{5}$"  # CPT codes are 5 digits
VALID_HCPCS_PATTERN = "^[A-Z][0-9]{4}$"  # HCPCS Level II


def get_last_processed_timestamp():
    """
    Retrieve high-water mark for incremental processing.
    Returns None if first run.
    """
    try:
        watermark_df = spark.table(WATERMARK_TABLE)
        last_ts = watermark_df \
            .filter(col("table_name") == "silver.rx_claims") \
            .select(spark_max("last_processed_timestamp")) \
            .first()[0]
        return last_ts
    except:
        print("Watermark table not found or first run - processing all Bronze data")
        return None


def update_watermark(max_timestamp):
    """
    Update high-water mark after successful Silver load.
    """
    watermark_data = [(
        "silver.rx_claims",
        max_timestamp,
        current_timestamp()
    )]
    
    watermark_df = spark.createDataFrame(
        watermark_data,
        ["table_name", "last_processed_timestamp", "updated_at"]
    )
    
    watermark_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(WATERMARK_TABLE)


def apply_data_quality_rules(df):
    """
    Apply validation rules and tag records as PASS/FAIL.
    Failed records are logged but not blocked (unless critical).
    
    Quality Dimensions:
    - Completeness: Required fields not null
    - Validity: Dates, codes, amounts in valid ranges
    - Consistency: Business rule checks (service_date <= received_date)
    - Uniqueness: No duplicate claim_ids (handled in dedup step)
    """
    
    validated_df = df.withColumn("dq_status", lit("PASS")) \
        .withColumn("dq_failure_reasons", lit(None).cast("array<string>"))
    
    # Rule 1: Required fields not null
    required_fields = ["claim_id", "member_id", "provider_id", "service_date", "received_date"]
    for field in required_fields:
        validated_df = validated_df.withColumn(
            "dq_failure_reasons",
            when(
                col(field).isNull(),
                coalesce(col("dq_failure_reasons"), lit([])).cast("array<string>") + [f"Missing required field: {field}"]
            ).otherwise(col("dq_failure_reasons"))
        )
    
    # Rule 2: Service date not in future
    validated_df = validated_df.withColumn(
        "dq_failure_reasons",
        when(
            col("service_date") > current_timestamp(),
            coalesce(col("dq_failure_reasons"), lit([])) + ["Service date in future"]
        ).otherwise(col("dq_failure_reasons"))
    )
    
    # Rule 3: Service date before or equal to received date
    validated_df = validated_df.withColumn(
        "dq_failure_reasons",
        when(
            col("service_date") > col("received_date"),
            coalesce(col("dq_failure_reasons"), lit([])) + ["Service date after received date"]
        ).otherwise(col("dq_failure_reasons"))
    )
    
    # Rule 4: Valid procedure codes (CPT or HCPCS)
    validated_df = validated_df.withColumn(
        "dq_failure_reasons",
        when(
            col("procedure_code").isNotNull() & 
            ~(col("procedure_code").rlike(VALID_CPT_PATTERN) | 
              col("procedure_code").rlike(VALID_HCPCS_PATTERN)),
            coalesce(col("dq_failure_reasons"), lit([])) + ["Invalid procedure code format"]
        ).otherwise(col("dq_failure_reasons"))
    )
    
    # Rule 5: Billed amount > 0 (or null for capitated rx_claims)
    validated_df = validated_df.withColumn(
        "dq_failure_reasons",
        when(
            col("billed_amount").isNotNull() & (col("billed_amount") <= 0),
            coalesce(col("dq_failure_reasons"), lit([])) + ["Billed amount must be positive"]
        ).otherwise(col("dq_failure_reasons"))
    )
    
    # Update status if any failures
    validated_df = validated_df.withColumn(
        "dq_status",
        when(col("dq_failure_reasons").isNotNull(), "FAIL").otherwise("PASS")
    )
    
    return validated_df


def cleanse_and_standardize(df):
    """
    Apply cleansing and standardization rules.
    - Trim whitespace from string fields
    - Uppercase codes for consistency
    - Convert dates to proper date types
    - Handle nulls with defaults where appropriate
    """
    
    cleansed_df = df \
        .withColumn("claim_id", upper(trim(col("claim_id")))) \
        .withColumn("member_id", upper(trim(col("member_id")))) \
        .withColumn("provider_id", upper(trim(col("provider_id")))) \
        .withColumn("procedure_code", upper(trim(col("procedure_code")))) \
        .withColumn("service_date", to_date(col("service_date"))) \
        .withColumn("received_date", to_date(col("received_date"))) \
        .withColumn("billed_amount", col("billed_amount").cast("decimal(18,2)")) \
        .withColumn("allowed_amount", col("allowed_amount").cast("decimal(18,2)")) \
        .withColumn("paid_amount", col("paid_amount").cast("decimal(18,2)"))
    
    return cleansed_df


def deduplicate_rx_claims(df):
    """
    Handle duplicate rx_claims by keeping most recent version.
    
    Deduplication Strategy:
    - Group by claim_id (business key)
    - Keep record with latest received_date
    - If tie, keep latest ingestion_timestamp (from Bronze)
    """
    
    window_spec = Window.partitionBy("claim_id").orderBy(
        col("received_date").desc(),
        col("ingestion_timestamp").desc()
    )
    
    deduped_df = df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    return deduped_df


def process_bronze_to_silver():
    """
    Main ETL logic: Bronze → Silver with validation and deduplication.
    """
    
    print("Starting Silver rx_claims processing...")
    
    # Step 1: Read incremental data from Bronze
    last_processed = get_last_processed_timestamp()
    
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    
    if last_processed:
        print(f"Incremental load: processing records after {last_processed}")
        bronze_df = bronze_df.filter(col("ingestion_timestamp") > last_processed)
    else:
        print("Full load: processing all Bronze records")
    
    record_count = bronze_df.count()
    if record_count == 0:
        print("No new records to process")
        return
    
    print(f"Processing {record_count} records from Bronze")
    
    # Step 2: Cleanse and standardize
    cleansed_df = cleanse_and_standardize(bronze_df)
    
    # Step 3: Apply data quality rules
    validated_df = apply_data_quality_rules(cleansed_df)
    
    # Step 4: Split PASS and FAIL records
    pass_df = validated_df.filter(col("dq_status") == "PASS")
    fail_df = validated_df.filter(col("dq_status") == "FAIL")
    
    fail_count = fail_df.count()
    if fail_count > 0:
        print(f"WARNING: {fail_count} records failed data quality checks")
        
        # Log failures to DQ table for review
        fail_df.select(
            "claim_id", "member_id", "provider_id", 
            "dq_failure_reasons", "ingestion_timestamp"
        ).write \
            .format("delta") \
            .mode("append") \
            .save(DQ_FAILURES_PATH)
    
    # Step 5: Deduplicate by claim_id
    deduped_df = deduplicate_rx_claims(pass_df)
    
    # Step 6: Add Silver metadata columns
    silver_df = deduped_df \
        .withColumn("silver_updated_timestamp", current_timestamp()) \
        .withColumn("silver_load_id", lit(spark.sparkContext.getConf().get("spark.databricks.job.id", "manual"))) \
        .drop("dq_status", "dq_failure_reasons")  # Drop DQ columns, already logged
    
    print(f"Silver records ready to load: {silver_df.count()}")
    
    # Step 7: MERGE into Silver Delta table (upsert by claim_id)
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        print("Merging into existing Silver table...")
        
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        
        silver_table.alias("target").merge(
            silver_df.alias("source"),
            "target.claim_id = source.claim_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
    else:
        print("Creating new Silver table...")
        
        # Partition by service_year_month for analytical queries
        silver_df = silver_df.withColumn(
            "service_year_month",
            col("service_date").substr(1, 7)  # YYYY-MM
        )
        
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("service_year_month") \
            .save(SILVER_PATH)
    
    # Register as table
    spark.sql(f"CREATE TABLE IF NOT EXISTS silver.rx_claims USING DELTA LOCATION '{SILVER_PATH}'")
    
    # Step 8: Update watermark for next run
    max_ingestion_ts = bronze_df.agg(spark_max("ingestion_timestamp")).first()[0]
    update_watermark(max_ingestion_ts)
    
    print("Silver processing completed successfully")
    
    return {
        "records_processed": record_count,
        "records_passed": pass_df.count(),
        "records_failed": fail_count,
        "records_loaded": silver_df.count()
    }


if __name__ == "__main__":
    """
    Execute Silver layer processing.
    """
    
    try:
        stats = process_bronze_to_silver()
        
        print(f"\n✓ Silver load successful:")
        print(f"  - Processed: {stats['records_processed']}")
        print(f"  - Passed DQ: {stats['records_passed']}")
        print(f"  - Failed DQ: {stats['records_failed']}")
        print(f"  - Loaded to Silver: {stats['records_loaded']}")
        
    except Exception as e:
        print(f"\n✗ Silver load failed: {str(e)}")
        raise
    
    finally:
        spark.stop(
